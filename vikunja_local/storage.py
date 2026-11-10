"""SQLite-backed durable storage and inter-process OS lock for the EA-010 journal.

C1 §5 / §9: synchronous=FULL, transactions, UNIQUE keys, fail-closed on
corrupt/unavailable/full storage. OS-lock on the canonical journal path is
required for dispatcher serialization; SQLite locking alone is not enough.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import sqlite3
import stat
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

# Internal SQLite user_version for this journal layout. Incompatible change
# requires an explicit migration decision; never wipe receipts/history.
JOURNAL_SQLITE_USER_VERSION = 1

JOURNAL_LOCK_SUFFIX = ".lock"
JOURNAL_INSTALL_SUFFIX = ".install"
# Outside the DB+.install restorable backup pair (C1 §5/§8): cannot be rolled
# back by restoring only those two files, so an old prepared create cannot re-arm.
JOURNAL_AUTHORITY_SUFFIX = ".authority"
SAFE_DIR_MODE = 0o700
SAFE_FILE_MODE = 0o600


class StorageError(RuntimeError):
    """Fail-closed storage / lock failure (no silent wipe or retry-as-success)."""

    def __init__(self, kind: str, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.kind = kind
        self.details = dict(details or {})


def _is_full_error(exc: BaseException) -> bool:
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in (
        errno.ENOSPC,
        errno.EDQUOT,
    ):
        return True
    if isinstance(exc, sqlite3.OperationalError):
        text = str(exc).lower()
        return "database or disk is full" in text or "disk i/o error" in text
    return False


def _classify_open_error(exc: BaseException) -> StorageError:
    if _is_full_error(exc):
        return StorageError("full", "journal storage is full", details={"errno": getattr(exc, "errno", None)})
    if isinstance(exc, sqlite3.DatabaseError):
        return StorageError("corrupt", f"journal database unusable: {exc}")
    if isinstance(exc, OSError):
        return StorageError(
            "unavailable",
            f"journal storage unavailable: {exc}",
            details={"errno": getattr(exc, "errno", None)},
        )
    return StorageError("unavailable", f"journal storage unavailable: {exc}")


def lock_path_for(journal_path: Path) -> Path:
    """Sidecar OS-lock path for the canonical absolute journal file."""
    return Path(str(journal_path) + JOURNAL_LOCK_SUFFIX)


def install_path_for(journal_path: Path) -> Path:
    """Durable installation-identity + freshness sidecar for the canonical journal."""
    return Path(str(journal_path) + JOURNAL_INSTALL_SUFFIX)


def authority_path_for(journal_path: Path) -> Path:
    """Outside-backup authority fence for the canonical journal path.

    Not part of the DB+.install restorable set. Restoring that pair while this
    fence stays advanced (or is absent) fails closed until explicit admission.
    """
    return Path(str(journal_path) + JOURNAL_AUTHORITY_SUFFIX)


@dataclass(frozen=True)
class InstallSidecar:
    """Outside-SQLite install fence: identity plus monotonic journal epoch.

    The epoch cannot be rolled back by restoring only a SQLite snapshot while
    retaining the live ``.install`` sidecar (C1 §5/§8 stale-copy).
    """

    installation_id: str
    journal_epoch: int


@dataclass(frozen=True)
class AuthorityFence:
    """Outside-backup generation fence (not in the DB+.install restore pair)."""

    installation_id: str
    generation: int


def _lexists(path: Path) -> bool:
    try:
        os.lstat(str(path))
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise _classify_open_error(exc) from exc


def canonical_journal_path(path: Path) -> Path:
    """Bind DB identity and OS-lock to one verified canonical absolute path.

    Final-component symlinks are refused (path substitution). Intermediate
    directory symlinks are resolved so lock and sqlite open the same file.
    """
    if not isinstance(path, Path):
        path = Path(path)
    if not path.is_absolute():
        raise StorageError("unavailable", "journal_path must be absolute")
    if path.is_symlink():
        raise StorageError(
            "unavailable",
            "journal_path must not be a symlink; refuse path substitution",
            details={"path": str(path)},
        )
    try:
        parent_real = Path(os.path.realpath(str(path.parent)))
    except OSError as exc:
        raise _classify_open_error(exc) from exc
    canonical = parent_real / path.name
    if canonical.is_symlink():
        raise StorageError(
            "unavailable",
            "journal_path must not be a symlink; refuse path substitution",
            details={"path": str(path), "canonical": str(canonical)},
        )
    return canonical


def _ensure_regular_nofollow(path: Path, *, create: bool) -> tuple[int, int]:
    """Open path with O_NOFOLLOW and return (st_dev, st_ino) of the fd.

    create=True is allowed only for an explicit first install. Missing storage on
    an established installation must fail closed (C1 §3/§5).
    """
    try:
        path.parent.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
        os.chmod(path.parent, SAFE_DIR_MODE)
    except OSError as exc:
        raise _classify_open_error(exc) from exc
    if path.is_symlink():
        raise StorageError(
            "unavailable",
            "journal_path must not be a symlink; refuse path substitution",
            details={"path": str(path)},
        )
    flags = os.O_RDWR | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT
    try:
        fd = os.open(str(path), flags, SAFE_FILE_MODE)
    except FileNotFoundError as exc:
        raise StorageError(
            "unavailable",
            "journal storage missing; refusing automatic recreate",
            details={"errno": getattr(exc, "errno", None), "path": str(path)},
        ) from exc
    except OSError as exc:
        # macOS returns ELOOP (62) when O_NOFOLLOW hits a final-component symlink.
        raise StorageError(
            "unavailable",
            f"cannot open journal file without following symlinks: {exc}",
            details={"errno": getattr(exc, "errno", None), "path": str(path)},
        ) from exc
    try:
        st = os.fstat(fd)
        if stat.S_ISLNK(st.st_mode):
            raise StorageError("unavailable", "journal_path must not be a symlink")
        if not stat.S_ISREG(st.st_mode):
            raise StorageError("unavailable", "journal_path must be a regular file")
        # Hard-linked aliases share an inode but get distinct sidecar lock paths;
        # reject multiply-linked DB files so two processes cannot bypass OS-lock
        # serialization (C1 §5).
        if int(st.st_nlink) != 1:
            raise StorageError(
                "unavailable",
                "journal_path must not be a multiply-linked file; refuse hard-link alias",
                details={"path": str(path), "nlink": int(st.st_nlink)},
            )
        try:
            os.chmod(path, SAFE_FILE_MODE)
        except OSError:
            pass
        return int(st.st_dev), int(st.st_ino)
    finally:
        os.close(fd)


def verify_bound_path(
    path: Path,
    *,
    expected_dev: int | None,
    expected_ino: int | None,
    create: bool,
) -> tuple[int, int]:
    """Re-validate canonical non-symlink identity; optionally bind to prior inode."""
    bound = canonical_journal_path(path)
    if bound != path:
        raise StorageError(
            "unavailable",
            "journal path identity drifted; refuse path substitution",
            details={"path": str(path), "canonical": str(bound)},
        )
    if path.is_symlink():
        raise StorageError(
            "unavailable",
            "journal_path must not be a symlink; refuse path substitution",
            details={"path": str(path)},
        )
    dev, ino = _ensure_regular_nofollow(bound, create=create)
    if expected_dev is not None and expected_ino is not None:
        if (dev, ino) != (expected_dev, expected_ino):
            raise StorageError(
                "unavailable",
                "journal file identity changed after bind; refuse path substitution",
                details={
                    "path": str(bound),
                    "expected": [expected_dev, expected_ino],
                    "found": [dev, ino],
                },
            )
    return dev, ino


@dataclass
class JournalLock:
    """Inter-process exclusive lock for all integration mutations on one journal.

    Local threads are serialized: only the owning thread may reenter. A second
    thread blocks until the owner's critical section fully ends; it cannot
    release another thread's fd. Ownership is (pid, tid); after fork the child
    must not treat the inherited held state as reentrant ownership (C1 §5.2).
    """

    path: Path
    _fd: int | None = None
    _held: bool = False
    _owner_tid: int | None = None
    _owner_pid: int | None = None
    _depth: int = 0
    _gate: threading.Lock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # Per-instance gate (dataclass default shared mutable would be wrong).
        object.__setattr__(self, "_gate", threading.Lock())
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._detach_inherited_after_fork)

    def _owns_current(self, *, tid: int | None = None, pid: int | None = None) -> bool:
        tid = threading.get_ident() if tid is None else tid
        pid = os.getpid() if pid is None else pid
        return (
            self._held
            and self._owner_tid == tid
            and self._owner_pid == pid
        )

    def _detach_inherited_after_fork(self) -> None:
        """Fail-closed child reset: drop inherited lock state without LOCK_UN.

        Parent keeps its flock via its own fd on the shared open-file description.
        Child must not unlock or reuse that description (C1 §5.2).
        """
        fd = self._fd
        self._fd = None
        self._held = False
        self._owner_tid = None
        self._owner_pid = None
        self._depth = 0
        # Inherited threading.Lock may appear held; replace with a fresh gate.
        object.__setattr__(self, "_gate", threading.Lock())
        if fd is not None:
            try:
                # Close only the child's fd reference; do not flock(LOCK_UN).
                os.close(fd)
            except OSError:
                pass

    def acquire(self) -> None:
        tid = threading.get_ident()
        pid = os.getpid()
        if self._held and self._owner_pid is not None and self._owner_pid != pid:
            # Forked child without (or before) at-fork handler: never reenter.
            self._detach_inherited_after_fork()
        if self._owns_current(tid=tid, pid=pid):
            self._depth += 1
            return
        # Non-owner threads block here until the owner fully releases.
        self._gate.acquire()
        acquired_gate = True
        try:
            if self._held and self._owner_pid is not None and self._owner_pid != os.getpid():
                self._detach_inherited_after_fork()
            if self._owns_current(tid=tid, pid=pid):
                # Lost the race only if same thread — should not happen with gate.
                self._depth += 1
                return
            try:
                self.path.parent.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
                os.chmod(self.path.parent, SAFE_DIR_MODE)
            except OSError as exc:
                raise _classify_open_error(exc) from exc
            if self.path.is_symlink():
                raise StorageError(
                    "unavailable",
                    "journal lock path must not be a symlink",
                    details={"path": str(self.path)},
                )
            try:
                fd = os.open(
                    self.path,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                    SAFE_FILE_MODE,
                )
            except OSError as exc:
                raise StorageError(
                    "unavailable",
                    f"cannot open journal lock: {exc}",
                    details={"errno": getattr(exc, "errno", None), "path": str(self.path)},
                ) from exc
            try:
                st = os.fstat(fd)
                if stat.S_ISLNK(st.st_mode):
                    raise StorageError("unavailable", "journal lock path must not be a symlink")
                if not stat.S_ISREG(st.st_mode):
                    raise StorageError("unavailable", "journal lock path must be a regular file")
                os.chmod(self.path, SAFE_FILE_MODE)
                fcntl.flock(fd, fcntl.LOCK_EX)
            except StorageError:
                os.close(fd)
                raise
            except OSError as exc:
                os.close(fd)
                raise StorageError(
                    "unavailable",
                    f"cannot acquire journal lock: {exc}",
                    details={"errno": getattr(exc, "errno", None)},
                ) from exc
            self._fd = fd
            self._held = True
            self._owner_tid = tid
            self._owner_pid = pid
            self._depth = 1
            acquired_gate = False  # ownership retains the gate until final release
        finally:
            if acquired_gate:
                self._gate.release()

    def release(self) -> None:
        if not self._held:
            return
        tid = threading.get_ident()
        pid = os.getpid()
        if self._owner_tid != tid or self._owner_pid != pid:
            raise StorageError(
                "unavailable",
                "journal lock can only be released by the owning thread",
                details={
                    "owner_tid": self._owner_tid,
                    "owner_pid": self._owner_pid,
                    "caller_tid": tid,
                    "caller_pid": pid,
                },
            )
        if self._depth > 1:
            self._depth -= 1
            return
        fd = self._fd
        self._fd = None
        self._held = False
        self._owner_tid = None
        self._owner_pid = None
        self._depth = 0
        try:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
        finally:
            self._gate.release()

    def wait_until_released(self) -> None:
        """Block until the current owner fully releases (close must not steal)."""
        if self._owns_current():
            raise StorageError(
                "unavailable",
                "cannot wait for journal lock release while holding it",
                details={"owner_tid": self._owner_tid, "owner_pid": self._owner_pid},
            )
        # Gate remains held for the entire ownership interval; cycling it waits.
        self._gate.acquire()
        self._gate.release()

    def force_release(self) -> None:
        """Drop the OS-lock completely, ignoring reentrant depth (explicit recovery).

        Must not be used to interrupt a live non-owner critical section; close()
        waits for release instead of stealing the flock (C1 §5.2).
        """
        if not self._held:
            self._depth = 0
            self._owner_tid = None
            self._owner_pid = None
            return
        if not self._owns_current():
            raise StorageError(
                "unavailable",
                "journal lock force_release refused for non-owner critical section",
                details={
                    "owner_tid": self._owner_tid,
                    "owner_pid": self._owner_pid,
                    "caller_tid": threading.get_ident(),
                    "caller_pid": os.getpid(),
                },
            )
        fd = self._fd
        self._fd = None
        self._held = False
        self._owner_tid = None
        self._owner_pid = None
        self._depth = 0
        try:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
        finally:
            try:
                self._gate.release()
            except RuntimeError:
                pass

    def __enter__(self) -> "JournalLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.release()

    @property
    def held(self) -> bool:
        return self._held

    @property
    def owner_tid(self) -> int | None:
        return self._owner_tid

    @property
    def owner_pid(self) -> int | None:
        return self._owner_pid


class SqliteJournalStore:
    """Durable SQLite connection with schema bootstrap and integrity checks."""

    def __init__(self, path: Path):
        canonical = canonical_journal_path(path)
        self.path = canonical
        self.lock = JournalLock(lock_path_for(canonical))
        self._conn: sqlite3.Connection | None = None
        self._bound_dev: int | None = None
        self._bound_ino: int | None = None
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._after_fork_in_child)

    def _after_fork_in_child(self) -> None:
        """Fail-closed: drop inherited SQLite handle without closing the parent's."""
        self._conn = None
        self._bound_dev = None
        self._bound_ino = None
        self.lock._detach_inherited_after_fork()

    def _established_install_evidence(self) -> dict[str, bool]:
        """Sidecar evidence that this path already hosted an installation."""
        return {
            "lock_present": _lexists(lock_path_for(self.path)),
            "install_present": _lexists(install_path_for(self.path)),
            "authority_present": _lexists(authority_path_for(self.path)),
        }

    def _allow_create_missing_journal(self) -> bool:
        """True only for explicit first install: no DB and no sidecar evidence."""
        if _lexists(self.path):
            return False
        evidence = self._established_install_evidence()
        if (
            evidence["lock_present"]
            or evidence["install_present"]
            or evidence["authority_present"]
        ):
            raise StorageError(
                "unavailable",
                "journal storage missing for established installation; recovery required before remint",
                details={"path": str(self.path), **evidence},
            )
        return True

    def _rebind_identity(self, *, create: bool | None = None) -> None:
        """Refuse path substitution after construction and before/while open."""
        rebound = canonical_journal_path(self.path)
        if rebound != self.path:
            raise StorageError(
                "unavailable",
                "journal path identity drifted; refuse path substitution",
                details={"path": str(self.path), "canonical": str(rebound)},
            )
        if self.path.is_symlink() or rebound.is_symlink():
            raise StorageError(
                "unavailable",
                "journal_path must not be a symlink; refuse path substitution",
                details={"path": str(self.path)},
            )
        expected_lock = lock_path_for(self.path)
        if self.lock.path != expected_lock:
            raise StorageError(
                "unavailable",
                "journal lock path is not bound to the canonical journal identity",
                details={"lock": str(self.lock.path), "expected": str(expected_lock)},
            )
        if self.lock.path.is_symlink():
            raise StorageError(
                "unavailable",
                "journal lock path must not be a symlink",
                details={"path": str(self.lock.path)},
            )
        if create is None:
            # After the file is bound, never recreate a missing leaf.
            if self._bound_dev is not None and self._bound_ino is not None:
                create = False
            else:
                create = self._allow_create_missing_journal()
        elif create:
            # Callers may request create only when first-install policy allows it.
            create = self._allow_create_missing_journal()
        else:
            if not _lexists(self.path):
                evidence = self._established_install_evidence()
                raise StorageError(
                    "unavailable",
                    "journal storage missing; refusing automatic recreate",
                    details={"path": str(self.path), **evidence},
                )
        dev, ino = verify_bound_path(
            self.path,
            expected_dev=self._bound_dev,
            expected_ino=self._bound_ino,
            create=create,
        )
        self._bound_dev = dev
        self._bound_ino = ino

    def open(self, *, admit_restored: bool = False) -> None:
        # Always re-validate identity: construction may precede a symlink swap.
        allow_create = self._allow_create_missing_journal() if not _lexists(self.path) else False
        self._rebind_identity(create=allow_create)
        if self._conn is not None:
            # Live install/relational fencing runs under the OS-lock via
            # revalidate_live_connection() in IntegrationJournal._mutation_guard.
            # Do not compare meta↔.install here: open() is called before the
            # mutator lock, and a shared connection may still be inside another
            # thread's uncommitted epoch bump.
            if admit_restored:
                raise StorageError(
                    "unavailable",
                    "admit_restored requires a closed journal; close before restore admission",
                    details={"path": str(self.path)},
                )
            return
        try:
            self.path.parent.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
            os.chmod(self.path.parent, SAFE_DIR_MODE)
        except OSError as exc:
            raise _classify_open_error(exc) from exc
        # Snapshot sidecars before acquire: JournalLock.acquire creates .lock, which
        # must not be mistaken for prior established-install evidence on first boot.
        pre_lock_evidence = self._established_install_evidence()
        # Serialize bootstrap / install-sidecar updates across processes. Open
        # itself runs before _mutation_guard acquires the OS-lock, so concurrent
        # open must not share a fixed .install.tmp or race schema init (C1 §5).
        self.lock.acquire()
        conn: sqlite3.Connection | None = None
        try:
            # Re-bind after lock: substitution while waiting must fail closed.
            allow_create_locked = (
                self._allow_create_missing_journal() if not _lexists(self.path) else False
            )
            self._rebind_identity(create=allow_create_locked)
            if self._conn is not None:
                return
            # O_NOFOLLOW bind above ensures the leaf is a regular file. Re-check
            # immediately before connect and again after so a swapped symlink fails closed.
            if self.path.is_symlink():
                raise StorageError(
                    "unavailable",
                    "journal_path must not be a symlink; refuse path substitution",
                    details={"path": str(self.path)},
                )
            conn = sqlite3.connect(
                str(self.path),
                isolation_level=None,  # manual BEGIN/COMMIT
                timeout=30.0,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            if self.path.is_symlink():
                raise StorageError(
                    "unavailable",
                    "journal_path must not be a symlink; refuse path substitution",
                    details={"path": str(self.path)},
                )
            st = os.lstat(self.path)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                raise StorageError(
                    "unavailable",
                    "journal_path must be a regular non-symlink file",
                    details={"path": str(self.path)},
                )
            if int(st.st_nlink) != 1:
                raise StorageError(
                    "unavailable",
                    "journal_path must not be a multiply-linked file; refuse hard-link alias",
                    details={"path": str(self.path), "nlink": int(st.st_nlink)},
                )
            if self._bound_dev is not None and self._bound_ino is not None:
                if (int(st.st_dev), int(st.st_ino)) != (self._bound_dev, self._bound_ino):
                    raise StorageError(
                        "unavailable",
                        "journal file identity changed during open; refuse path substitution",
                        details={"path": str(self.path)},
                    )
            self._bound_dev = int(st.st_dev)
            self._bound_ino = int(st.st_ino)
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA temp_store=MEMORY")
            # Refuse silently opening a corrupted page when possible.
            integrity = conn.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or str(integrity[0]).lower() != "ok":
                raise StorageError(
                    "corrupt",
                    "journal integrity_check failed",
                    details={"result": None if integrity is None else str(integrity[0])},
                )
            # Re-read version under the OS-lock so a peer bootstrap is visible.
            user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if user_version == 0:
                if admit_restored:
                    raise StorageError(
                        "unavailable",
                        "admit_restored refuses empty/unversioned journal; restore a complete DB+.install set",
                        details={"path": str(self.path), "user_version": user_version},
                    )
                if self._application_tables_present(conn):
                    raise StorageError(
                        "incompatible_schema",
                        "journal user_version is 0 with existing application tables; "
                        "refusing silent bootstrap — recovery/migration requires an explicit decision",
                        details={
                            "found": user_version,
                            "expected": JOURNAL_SQLITE_USER_VERSION,
                            "application_tables_present": True,
                        },
                    )
                # C1 §5: empty/truncated/empty-SQLite under established-install
                # evidence must stay writes-off — never remint issuer/journal.
                # Ignore .lock created by this acquire; honor pre-lock sidecars and
                # any .install/.authority a peer wrote while we waited for the OS-lock.
                post_lock_install = _lexists(install_path_for(self.path))
                post_lock_authority = _lexists(authority_path_for(self.path))
                if (
                    pre_lock_evidence["lock_present"]
                    or pre_lock_evidence["install_present"]
                    or pre_lock_evidence["authority_present"]
                    or post_lock_install
                    or post_lock_authority
                ):
                    raise StorageError(
                        "unavailable",
                        "journal storage empty or truncated for established installation; "
                        "recovery required before remint",
                        details={
                            "path": str(self.path),
                            "lock_present": pre_lock_evidence["lock_present"],
                            "install_present": bool(
                                pre_lock_evidence["install_present"] or post_lock_install
                            ),
                            "authority_present": bool(
                                pre_lock_evidence["authority_present"] or post_lock_authority
                            ),
                        },
                    )
                self._bootstrap(conn)
            elif user_version != JOURNAL_SQLITE_USER_VERSION:
                raise StorageError(
                    "incompatible_schema",
                    "journal schema version is incompatible; migration requires an explicit decision",
                    details={
                        "found": user_version,
                        "expected": JOURNAL_SQLITE_USER_VERSION,
                    },
                )
            else:
                self._assert_required_tables(conn)
                self._assert_required_safety_meta(conn)
                self._ensure_install_identity(conn, require_authority=not admit_restored)
                if admit_restored:
                    self._admit_restored_unlocked(conn)
                self._enforce_relational_consistency(conn)
            self._conn = conn
            conn = None
            try:
                os.chmod(self.path, SAFE_FILE_MODE)
            except OSError:
                # File may be on a volume that ignores mode; durability still required.
                pass
        except StorageError:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            raise
        except Exception as exc:  # noqa: BLE001
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            raise _classify_open_error(exc) from exc
        finally:
            self.lock.release()

    def close(self) -> None:
        # C1 §5.2: never steal a live non-owner critical section's OS-lock.
        # Wait for the owner to exit, or reject close from the owning thread.
        if self.lock._owns_current():
            raise StorageError(
                "unavailable",
                "cannot close journal store while this thread holds the OS-lock",
                details={
                    "owner_tid": self.lock.owner_tid,
                    "owner_pid": self.lock.owner_pid,
                },
            )
        if self.lock.held:
            self.lock.wait_until_released()
        conn = self._conn
        self._conn = None
        self._bound_dev = None
        self._bound_ino = None
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass

    def __enter__(self) -> "SqliteJournalStore":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StorageError("unavailable", "journal store is not open")
        return self._conn

    def begin_immediate(self) -> None:
        try:
            self.conn.execute("BEGIN IMMEDIATE")
        except Exception as exc:  # noqa: BLE001
            raise _classify_open_error(exc) from exc

    def commit(self) -> None:
        try:
            self.conn.execute("COMMIT")
        except Exception as exc:  # noqa: BLE001
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise _classify_open_error(exc) from exc

    def rollback(self) -> None:
        try:
            self.conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> sqlite3.Cursor:
        try:
            return self.conn.execute(sql, params)
        except Exception as exc:  # noqa: BLE001
            raise _classify_open_error(exc) from exc

    def executemany(self, sql: str, seq: list[tuple[Any, ...]]) -> sqlite3.Cursor:
        try:
            return self.conn.executemany(sql, seq)
        except Exception as exc:  # noqa: BLE001
            raise _classify_open_error(exc) from exc

    @contextmanager
    def transaction(self, *, advance_freshness: bool = True) -> Iterator[None]:
        """Commit a durable mutation and advance the outside-SQLite freshness fence.

        ``advance_freshness`` is the default so a restored older SQLite snapshot
        cannot keep the live ``.install`` epoch while replaying creates/dispatches.
        """
        self.begin_immediate()
        try:
            yield
            if advance_freshness:
                self._bump_journal_epoch_unlocked()
            self.commit()
            if advance_freshness:
                self._sync_install_sidecar_from_meta()
        except Exception:
            self.rollback()
            raise

    def _application_tables_present(self, conn: sqlite3.Connection) -> bool:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return any(str(row[0]) for row in rows)

    def _persist_install_sidecar(self, installation_id: str, journal_epoch: int) -> None:
        """Write durable installation identity + freshness next to the journal file.

        Uses a private temporary file in the same directory so concurrent open
        paths cannot chmod/unlink a shared ``.install.tmp`` after a peer rename.
        Caller must already hold the journal OS-lock (open / bootstrap / commit).
        """
        if not isinstance(journal_epoch, int) or isinstance(journal_epoch, bool) or journal_epoch < 0:
            raise StorageError(
                "corrupt",
                "journal_epoch must be a non-negative int for install sidecar",
                details={"journal_epoch": journal_epoch},
            )
        install_path = install_path_for(self.path)
        if install_path.is_symlink():
            raise StorageError(
                "unavailable",
                "journal install path must not be a symlink",
                details={"path": str(install_path)},
            )
        payload = f"{installation_id}\n{journal_epoch}\n".encode("utf-8")
        tmp_path: Path | None = None
        try:
            install_path.parent.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
            os.chmod(install_path.parent, SAFE_DIR_MODE)
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{install_path.name}.",
                suffix=".tmp",
                dir=str(install_path.parent),
            )
            tmp_path = Path(tmp_name)
            try:
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.chmod(tmp_path, SAFE_FILE_MODE)
            os.replace(str(tmp_path), str(install_path))
            tmp_path = None
            os.chmod(install_path, SAFE_FILE_MODE)
        except OSError as exc:
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise _classify_open_error(exc) from exc

    def _read_install_sidecar(self) -> InstallSidecar | None:
        """Return install fence from sidecar, or None if absent.

        Symlink / unreadable / malformed sidecar fails closed. Present-but-blank
        identity is returned as empty installation_id so callers mismatch.
        """
        install_path = install_path_for(self.path)
        if not _lexists(install_path):
            return None
        if install_path.is_symlink():
            raise StorageError(
                "unavailable",
                "journal install path must not be a symlink",
                details={"path": str(install_path)},
            )
        try:
            raw = install_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise _classify_open_error(exc) from exc
        lines = raw.splitlines()
        if len(lines) < 2:
            raise StorageError(
                "unavailable",
                "journal install sidecar missing freshness epoch; "
                "refuse open until reconcile",
                details={"path": str(install_path), "line_count": len(lines)},
            )
        installation_id = lines[0].strip()
        epoch_text = lines[1].strip()
        try:
            journal_epoch = int(epoch_text)
        except ValueError as exc:
            raise StorageError(
                "corrupt",
                "journal install sidecar freshness epoch is not an integer",
                details={"path": str(install_path), "journal_epoch": epoch_text},
            ) from exc
        if journal_epoch < 0:
            raise StorageError(
                "corrupt",
                "journal install sidecar freshness epoch is negative",
                details={"path": str(install_path), "journal_epoch": journal_epoch},
            )
        return InstallSidecar(installation_id=installation_id, journal_epoch=journal_epoch)

    def _meta_value(self, conn: sqlite3.Connection, key: str) -> str | None:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else str(row[0])

    def _parse_journal_epoch(self, raw: str | None, *, source: str) -> int:
        if raw is None:
            raise StorageError(
                "corrupt",
                "journal_epoch meta missing; refusing open until reconcile",
                details={"source": source},
            )
        try:
            epoch = int(str(raw).strip())
        except ValueError as exc:
            raise StorageError(
                "corrupt",
                "journal_epoch meta is not an integer",
                details={"source": source, "value": raw},
            ) from exc
        if epoch < 0:
            raise StorageError(
                "corrupt",
                "journal_epoch meta is negative",
                details={"source": source, "value": epoch},
            )
        return epoch

    def _bump_journal_epoch_unlocked(self) -> int:
        """Increment durable journal_epoch inside the current write transaction."""
        conn = self.conn
        current = self._parse_journal_epoch(
            self._meta_value(conn, "journal_epoch"),
            source="meta",
        )
        nxt = current + 1
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("journal_epoch", str(nxt)),
        )
        return nxt

    def _sync_install_sidecar_from_meta(self) -> None:
        """Rewrite ``.install`` and ``.authority`` from committed meta.

        ``.install`` fences SQLite-only rollback. ``.authority`` fences the full
        DB+.install restorable pair (C1 §5/§8).
        """
        conn = self.conn
        installation_id = self._meta_value(conn, "installation_id")
        if installation_id is None or not str(installation_id).strip():
            raise StorageError(
                "corrupt",
                "installation_id meta missing while syncing install sidecar",
                details={"path": str(self.path)},
            )
        epoch = self._parse_journal_epoch(
            self._meta_value(conn, "journal_epoch"),
            source="meta",
        )
        iid = str(installation_id).strip()
        self._persist_install_sidecar(iid, epoch)
        self._persist_authority_fence(iid, epoch)

    def _persist_authority_fence(self, installation_id: str, generation: int) -> None:
        """Write the outside-backup authority fence next to the journal file.

        Caller must already hold the journal OS-lock. This file is intentionally
        excluded from the DB+.install restorable backup pair.
        """
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
            raise StorageError(
                "corrupt",
                "authority generation must be a non-negative int",
                details={"generation": generation},
            )
        auth_path = authority_path_for(self.path)
        if auth_path.is_symlink():
            raise StorageError(
                "unavailable",
                "journal authority path must not be a symlink",
                details={"path": str(auth_path)},
            )
        payload = f"{installation_id}\n{generation}\n".encode("utf-8")
        tmp_path: Path | None = None
        try:
            auth_path.parent.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
            os.chmod(auth_path.parent, SAFE_DIR_MODE)
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{auth_path.name}.",
                suffix=".tmp",
                dir=str(auth_path.parent),
            )
            tmp_path = Path(tmp_name)
            try:
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.chmod(tmp_path, SAFE_FILE_MODE)
            os.replace(str(tmp_path), str(auth_path))
            tmp_path = None
            os.chmod(auth_path, SAFE_FILE_MODE)
        except OSError as exc:
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise _classify_open_error(exc) from exc

    def _read_authority_fence(self) -> AuthorityFence | None:
        """Return authority fence, or None if absent. Malformed/symlink fails closed."""
        auth_path = authority_path_for(self.path)
        if not _lexists(auth_path):
            return None
        if auth_path.is_symlink():
            raise StorageError(
                "unavailable",
                "journal authority path must not be a symlink",
                details={"path": str(auth_path)},
            )
        try:
            raw = auth_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise _classify_open_error(exc) from exc
        lines = raw.splitlines()
        if len(lines) < 2:
            raise StorageError(
                "unavailable",
                "journal authority fence missing generation; refuse open until reconcile",
                details={"path": str(auth_path), "line_count": len(lines)},
            )
        installation_id = lines[0].strip()
        gen_text = lines[1].strip()
        try:
            generation = int(gen_text)
        except ValueError as exc:
            raise StorageError(
                "corrupt",
                "journal authority generation is not an integer",
                details={"path": str(auth_path), "generation": gen_text},
            ) from exc
        if generation < 0:
            raise StorageError(
                "corrupt",
                "journal authority generation is negative",
                details={"path": str(auth_path), "generation": generation},
            )
        return AuthorityFence(installation_id=installation_id, generation=generation)

    def _admit_restored_unlocked(self, conn: sqlite3.Connection) -> None:
        """Force writes-off and rotate authority after an explicit restored-set admission.

        Requires a consistent DB+.install pair. Never rewinds the outside-backup
        ``.authority`` generation to the restored meta epoch: admission mints a
        new restore incarnation strictly greater than both the live fence and the
        restored journal_epoch, then durably binds DB + ``.install`` to that fence
        under writes-off. A formerly used generation therefore cannot re-admit an
        old prepared snapshot for ordinary open/dispatch (C1 §5/§8).
        """
        installation_id = self._meta_value(conn, "installation_id")
        if installation_id is None or not str(installation_id).strip():
            raise StorageError(
                "corrupt",
                "installation_id meta missing during restore admission",
                details={"path": str(self.path)},
            )
        restored_epoch = self._parse_journal_epoch(
            self._meta_value(conn, "journal_epoch"),
            source="meta",
        )
        iid = str(installation_id).strip()
        live_authority = self._read_authority_fence()
        live_generation = 0
        if live_authority is not None:
            if live_authority.installation_id != iid:
                raise StorageError(
                    "unavailable",
                    "journal authority identity does not match restored meta during "
                    "admit_restored; refuse path substitution until reconcile",
                    details={
                        "path": str(self.path),
                        "meta_installation_id": iid,
                        "authority_installation_id": live_authority.installation_id,
                        "authority_mismatch": True,
                    },
                )
            live_generation = live_authority.generation
        # Rotate a dedicated restore incarnation: never reuse or rewind a
        # previously observed generation (equality-only open accept at fence).
        new_generation = max(live_generation, restored_epoch) + 1
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        conn.execute("BEGIN IMMEDIATE")
        try:
            for key, value in (
                ("restore_drift", "1"),
                ("writes_enabled", "0"),
                ("journal_epoch", str(new_generation)),
            ):
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )
            detail = json.dumps(
                {
                    "reason": "restored_backup_admitted",
                    "installation_id": iid,
                    "restored_journal_epoch": restored_epoch,
                    "prior_authority_generation": live_generation,
                    "journal_epoch": new_generation,
                    "authority_generation": new_generation,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            conn.execute(
                "INSERT INTO history(issuer_id, operation_id, event_type, detail_json, created_at) "
                "VALUES(?, ?, ?, ?, ?)",
                ("", "", "restored_backup_admitted", detail, now),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        # Bind the restored pair to the new fence only after meta commit.
        self._persist_install_sidecar(iid, new_generation)
        self._persist_authority_fence(iid, new_generation)

    def _ensure_install_identity(
        self,
        conn: sqlite3.Connection,
        *,
        require_authority: bool = True,
    ) -> None:
        """Validate meta identity + freshness against durable fences.

        C1 §5: restore / partial-copy / path-substitution stays writes-off.
        An existing sidecar is never overwritten with a foreign DB identity;
        mismatch or missing identity/freshness on an established journal refuses
        open. First-install minting remains only in ``_bootstrap``.

        ``require_authority=False`` is only for explicit ``admit_restored`` open,
        which rotates a monotonic outside-backup authority incarnation under writes-off.
        """
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            ("installation_id",),
        ).fetchone()
        sidecar = self._read_install_sidecar()
        if row is None:
            raise StorageError(
                "unavailable",
                "journal installation identity missing from meta; "
                "recovery required before remint",
                details={
                    "path": str(self.path),
                    "sidecar_present": sidecar is not None,
                    "sidecar_installation_id": None if sidecar is None else sidecar.installation_id,
                },
            )
        installation_id = str(row[0]).strip()
        if not installation_id:
            raise StorageError(
                "corrupt",
                "journal installation_id meta is empty; refusing open until reconcile",
                details={
                    "path": str(self.path),
                    "sidecar_installation_id": None if sidecar is None else sidecar.installation_id,
                },
            )
        meta_epoch = self._parse_journal_epoch(
            self._meta_value(conn, "journal_epoch"),
            source="meta",
        )
        if sidecar is None:
            raise StorageError(
                "unavailable",
                "journal install sidecar missing for established installation; "
                "recovery required before remint",
                details={
                    "path": str(self.path),
                    "meta_installation_id": installation_id,
                    "meta_journal_epoch": meta_epoch,
                },
            )
        if sidecar.installation_id != installation_id:
            # Preserve the on-disk sidecar evidence; do not rewrite it.
            raise StorageError(
                "unavailable",
                "journal installation identity does not match install sidecar; "
                "refuse path substitution until reconcile",
                details={
                    "path": str(self.path),
                    "meta_installation_id": installation_id,
                    "sidecar_installation_id": sidecar.installation_id,
                    "installation_mismatch": True,
                },
            )
        if sidecar.journal_epoch != meta_epoch:
            # Same installation_id with an older SQLite snapshot retained the live
            # sidecar epoch — refuse writes/open until reconcile (C1 §5/§8).
            raise StorageError(
                "unavailable",
                "journal freshness does not match install sidecar; "
                "refuse stale or partial restore until reconcile",
                details={
                    "path": str(self.path),
                    "meta_installation_id": installation_id,
                    "meta_journal_epoch": meta_epoch,
                    "sidecar_journal_epoch": sidecar.journal_epoch,
                    "freshness_mismatch": True,
                    "stale_copy": meta_epoch < sidecar.journal_epoch,
                },
            )
        if require_authority:
            authority = self._read_authority_fence()
            if authority is None:
                raise StorageError(
                    "unavailable",
                    "journal authority fence missing; refuse restore/open until "
                    "explicit admit_restored or reconcile",
                    details={
                        "path": str(self.path),
                        "meta_installation_id": installation_id,
                        "meta_journal_epoch": meta_epoch,
                        "authority_missing": True,
                        "full_backup_rollback": True,
                    },
                )
            if authority.installation_id != installation_id:
                raise StorageError(
                    "unavailable",
                    "journal authority identity does not match meta; "
                    "refuse path substitution until reconcile",
                    details={
                        "path": str(self.path),
                        "meta_installation_id": installation_id,
                        "authority_installation_id": authority.installation_id,
                        "authority_mismatch": True,
                    },
                )
            if authority.generation != meta_epoch:
                # DB+.install restored together roll both back; live authority stays
                # advanced (or diverges) — fail closed (C1 §5/§8, system §Data).
                raise StorageError(
                    "unavailable",
                    "journal authority does not match restored meta; "
                    "refuse full DB+.install rollback until explicit admit_restored",
                    details={
                        "path": str(self.path),
                        "meta_installation_id": installation_id,
                        "meta_journal_epoch": meta_epoch,
                        "authority_generation": authority.generation,
                        "authority_mismatch": True,
                        "full_backup_rollback": meta_epoch < authority.generation,
                    },
                )

    def revalidate_live_connection(self) -> None:
        """Re-check install/authority fences + relational residue under the OS-lock.

        A kept-open connection can observe an in-place SQLite restore into the
        same inode while the live ``.install`` / ``.authority`` fences remain
        advanced, or lose a fence entirely. C1 §5/§8 requires writes-off until
        reconcile — inode/flags alone are not enough.
        """
        conn = self.conn
        self._ensure_install_identity(conn)
        self._enforce_relational_consistency(conn)

    def _history_operation_orphans(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        """History rows with a nonempty operation_id and no parent operations row.

        ``history`` has no FK (receipts/source_mappings do). A partial copy that
        drops operations/source_mappings while retaining intent/dispatch/unknown
        history must still quarantine (C1 §5/§8) so a new key cannot be admitted
        for the same opaque source.
        """
        try:
            rows = conn.execute(
                """
                SELECT h.issuer_id, h.operation_id, h.event_type, COUNT(*) AS n
                FROM history AS h
                WHERE h.operation_id != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM operations AS o
                      WHERE o.issuer_id = h.issuer_id
                        AND o.operation_id = h.operation_id
                  )
                GROUP BY h.issuer_id, h.operation_id, h.event_type
                ORDER BY h.issuer_id, h.operation_id, h.event_type
                """
            ).fetchall()
        except sqlite3.Error as exc:
            raise StorageError(
                "corrupt",
                f"journal history orphan scan failed: {exc}",
            ) from exc
        return [
            {
                "issuer_id": str(row[0]),
                "operation_id": str(row[1]),
                "event_type": str(row[2]),
                "count": int(row[3]),
            }
            for row in rows
        ]

    def _enforce_relational_consistency(self, conn: sqlite3.Connection) -> None:
        """Fail closed on FK orphans and history-only residue; quarantine in place.

        ``PRAGMA foreign_keys=ON`` only blocks new violations. Partial copies that
        drop parent operations while retaining receipts/history must go writes-off
        (C1 §5) without wiping surviving evidence. History has no FK, so scan it
        explicitly.
        """
        try:
            rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        except sqlite3.Error as exc:
            raise StorageError(
                "corrupt",
                f"journal foreign_key_check failed: {exc}",
            ) from exc
        violations = [
            {
                "table": str(row[0]),
                "rowid": row[1],
                "parent": str(row[2]),
                "fkid": row[3],
            }
            for row in rows
        ]
        history_orphans = self._history_operation_orphans(conn)
        if not violations and not history_orphans:
            return
        # Already writes-off: preserve residue without appending duplicate quarantine rows
        # on every subsequent mutation_guard revalidation.
        if self._meta_value(conn, "quarantine") == "1":
            return
        # Prefer foreign_key_check when FK residue exists (declared parents).
        # History-only residue (no FK) uses history_without_operation.
        reason = "foreign_key_check" if violations else "history_without_operation"
        detail_obj: dict[str, Any] = {
            "reason": reason,
            "partial_copy": True,
        }
        if violations:
            detail_obj["foreign_key_violations"] = violations
        if history_orphans:
            detail_obj["history_operation_orphans"] = history_orphans
        # Persist quarantine under the open OS-lock so later mutation guards see it.
        # Do not advance the outside-SQLite epoch here: detection must remain stable
        # if the same partial copy is reopened before reconcile.
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("quarantine", "1"),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("writes_enabled", "0"),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("restore_drift", "1"),
            )
            detail = json.dumps(
                detail_obj,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            created_at = (
                datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
            conn.execute(
                """
                INSERT INTO history(issuer_id, operation_id, event_type, detail_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("", "", "quarantine", detail, created_at),
            )
            conn.execute("COMMIT")
        except Exception as exc:  # noqa: BLE001
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise StorageError(
                "corrupt",
                "journal relational residue present; quarantine persist failed",
                details=detail_obj,
            ) from exc

    def _bootstrap(self, conn: sqlite3.Connection) -> None:
        try:
            # Use an exclusive transaction with discrete statements.
            # sqlite3 executescript() auto-commits and cannot sit inside BEGIN/COMMIT.
            conn.execute("BEGIN IMMEDIATE")
            current = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if current == JOURNAL_SQLITE_USER_VERSION:
                conn.execute("COMMIT")
                self._assert_required_tables(conn)
                self._assert_required_safety_meta(conn)
                self._ensure_install_identity(conn)
                return
            if current not in (0, JOURNAL_SQLITE_USER_VERSION):
                conn.execute("ROLLBACK")
                raise StorageError(
                    "incompatible_schema",
                    "journal schema version is incompatible; migration requires an explicit decision",
                    details={"found": current, "expected": JOURNAL_SQLITE_USER_VERSION},
                )
            if self._application_tables_present(conn):
                conn.execute("ROLLBACK")
                raise StorageError(
                    "incompatible_schema",
                    "journal user_version is 0 with existing application tables; "
                    "refusing silent bootstrap — recovery/migration requires an explicit decision",
                    details={
                        "found": current,
                        "expected": JOURNAL_SQLITE_USER_VERSION,
                        "application_tables_present": True,
                    },
                )
            # Defense in depth: .install/.authority are written only after a completed
            # bootstrap. Do not treat the OS-lock created by open() as established
            # evidence here.
            if _lexists(install_path_for(self.path)) or _lexists(authority_path_for(self.path)):
                conn.execute("ROLLBACK")
                raise StorageError(
                    "unavailable",
                    "journal storage empty or truncated for established installation; "
                    "recovery required before remint",
                    details={
                        "path": str(self.path),
                        "lock_present": _lexists(lock_path_for(self.path)),
                        "install_present": _lexists(install_path_for(self.path)),
                        "authority_present": _lexists(authority_path_for(self.path)),
                    },
                )
            installation_id = str(uuid.uuid4())
            statements = [
                """
                CREATE TABLE meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE operations (
                    issuer_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    op_type TEXT NOT NULL,
                    project_id INTEGER NOT NULL,
                    target_id INTEGER,
                    marker TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    delivery_state TEXT NOT NULL,
                    frozen_due_json TEXT,
                    source_kind TEXT,
                    source_key TEXT,
                    confirmed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (issuer_id, operation_id)
                )
                """,
                """
                CREATE TABLE receipts (
                    issuer_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    vikunja_id INTEGER NOT NULL,
                    project_id INTEGER NOT NULL,
                    marker TEXT NOT NULL,
                    effect_json TEXT NOT NULL,
                    witnessed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (issuer_id, operation_id),
                    FOREIGN KEY (issuer_id, operation_id)
                        REFERENCES operations(issuer_id, operation_id)
                )
                """,
                """
                CREATE TABLE source_mappings (
                    source_kind TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    issuer_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (source_kind, source_key),
                    UNIQUE (issuer_id, operation_id),
                    FOREIGN KEY (issuer_id, operation_id)
                        REFERENCES operations(issuer_id, operation_id)
                )
                """,
                """
                CREATE TABLE history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issuer_id TEXT NOT NULL,
                    operation_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """,
            ]
            for statement in statements:
                conn.execute(statement)
            conn.execute(f"PRAGMA user_version={JOURNAL_SQLITE_USER_VERSION}")
            # First install only: exact inserts (not OR IGNORE) so a raced/partial
            # nonempty DB cannot silently restore permissive safety defaults.
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("schema_version", str(JOURNAL_SQLITE_USER_VERSION)),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("writes_enabled", "1"),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("restore_drift", "0"),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("quarantine", "0"),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("installation_id", installation_id),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("journal_epoch", "0"),
            )
            conn.execute("COMMIT")
            self._persist_install_sidecar(installation_id, 0)
            self._persist_authority_fence(installation_id, 0)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise _classify_open_error(exc) from exc

    def _assert_required_tables(self, conn: sqlite3.Connection) -> None:
        required = {"meta", "operations", "receipts", "source_mappings", "history"}
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        present = {str(row[0]) for row in rows}
        missing = sorted(required - present)
        if missing:
            raise StorageError(
                "corrupt",
                "journal is missing required tables; refusing wipe/recreate",
                details={"missing": missing},
            )

    def _assert_required_safety_meta(self, conn: sqlite3.Connection) -> None:
        """Existing journals must carry exact safety flags; omissions fail closed.

        Bootstrap inserts writes_enabled/restore_drift/quarantine. A partial copy
        that drops any of them must not reopen as writable (C1 §5).
        """
        required = {
            "writes_enabled": frozenset({"0", "1"}),
            "restore_drift": frozenset({"0", "1"}),
            "quarantine": frozenset({"0", "1"}),
        }
        missing: list[str] = []
        invalid: dict[str, str | None] = {}
        for key, allowed in required.items():
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                missing.append(key)
                continue
            value = str(row[0])
            if value not in allowed:
                invalid[key] = value
        epoch_row = conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            ("journal_epoch",),
        ).fetchone()
        if epoch_row is None:
            missing.append("journal_epoch")
        else:
            try:
                epoch = int(str(epoch_row[0]).strip())
                if epoch < 0:
                    invalid["journal_epoch"] = str(epoch_row[0])
            except ValueError:
                invalid["journal_epoch"] = str(epoch_row[0])
        if missing or invalid:
            raise StorageError(
                "corrupt",
                "journal safety metadata missing or invalid; refusing open until reconcile",
                details={"missing": missing, "invalid": invalid},
            )
