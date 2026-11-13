"""Manual consistent copy and isolated restore for EA-010 (s09 / I10).

Composition (C6 / FR-24 / NFR-15): Vikunja SQLite tasks DB, attachments tree,
safe config, secrets in a local 0600 tree (never Git/logs), journal DB+.install
restorable pair, and exported links/mappings. The journal ``.authority`` fence
is outside the restorable pair.

This is a one-shot manual rehearsal helper — not a scheduler, retention manager,
RPO/RTO product, or working-Mini restore tool. A copy that lives only on the
same Mini does not protect against physical loss of that device.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import sqlite3
import stat as stat_mod
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .api import MARKER_RE, extract_marker
from .journal import (
    IntegrationJournal,
    JournalError,
    OperationRecord,
    OperationState,
    RestoreRecoverySection,
    new_operation_id,
)
from .storage import JournalLock, install_path_for, lock_path_for

# Test-only crash injection points (process-death regressions). Production stays None.
_test_crash_hook: Callable[[str], None] | None = None
# Test-only: observe destination mode immediately before final chmod (pre-chmod boundary).
_test_pre_chmod_hook: Callable[[Path, int], None] | None = None
# Test-only: runs after O_NOFOLLOW bind, immediately before sqlite3.connect (URI open race).
_test_pre_sqlite_connect_hook: Callable[[Path, str], None] | None = None


def _maybe_crash(point: str) -> None:
    hook = _test_crash_hook
    if hook is not None:
        hook(point)


def _maybe_pre_chmod(path: Path, mode: int) -> None:
    hook = _test_pre_chmod_hook
    if hook is not None:
        hook(path, mode)


def _maybe_pre_sqlite_connect(path: Path, purpose: str) -> None:
    hook = _test_pre_sqlite_connect_hook
    if hook is not None:
        hook(path, purpose)

COPY_SCHEMA = "ea010-consistent-copy/v1"
LINKS_SCHEMA = "ea010-consistent-copy-links/v1"
# Durable effect.source for receipts committed under restore recovery.
# Lets resume/repeat with the original copy links treat the recovery delta
# as attributable, not as backup drift.
RESTORE_RECEIPT_SOURCE = "restore_exact_reconcile"
SAFE_DIR_MODE = 0o700
SAFE_FILE_MODE = 0o600

# Restorable components. ``.authority`` is intentionally absent.
COMPONENT_VIKUNJA_DB = "vikunja_db"
COMPONENT_ATTACHMENTS = "attachments"
COMPONENT_SAFE_CONFIG = "safe_config"
COMPONENT_SECRETS = "secrets"
COMPONENT_JOURNAL_DB = "journal_db"
COMPONENT_JOURNAL_INSTALL = "journal_install"
COMPONENT_LINKS = "links"

REQUIRED_COMPONENTS = (
    COMPONENT_VIKUNJA_DB,
    COMPONENT_ATTACHMENTS,
    COMPONENT_SAFE_CONFIG,
    COMPONENT_SECRETS,
    COMPONENT_JOURNAL_DB,
    COMPONENT_JOURNAL_INSTALL,
    COMPONENT_LINKS,
)


class RestoreError(RuntimeError):
    """Fail-closed copy/restore error."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_nofollow(path: Path) -> str:
    """Hash a regular file without following a substituted destination symlink."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise RestoreError(
            "symlink refused in consistent copy",
            code="symlink_refused",
            details={"path": str(path)},
        ) from exc
    try:
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(fd)


def _require_matching_digest(path: Path, expected_sha256: str, *, component: str) -> str:
    """Fail closed when a verified-manifest component no longer matches on disk."""
    observed = _sha256_nofollow(path)
    if observed != expected_sha256:
        raise RestoreError(
            f"component digest mismatch: {component}",
            code="copy_drift",
            details={
                "component": component,
                "path": str(path),
                "expected": expected_sha256,
                "observed": observed,
            },
        )
    return observed


def _read_verified_bytes(path: Path, expected_sha256: str, *, component: str) -> bytes:
    """Read a component once via O_NOFOLLOW and bind the digest to those bytes.

    Callers must deserialize/materialize the returned payload rather than
    reopening the path; a post-digest rewrite must not change what restore
    admits or publishes (manifest generation binding).
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        if path.is_symlink() or exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise _symlink_refused(path, cause=exc) from exc
        raise RestoreError(
            f"component unreadable: {component}",
            code="source_missing",
            details={"path": str(path), "component": component},
        ) from exc
    try:
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            chunks.append(chunk)
        observed = digest.hexdigest()
        if observed != expected_sha256:
            raise RestoreError(
                f"component digest mismatch: {component}",
                code="copy_drift",
                details={
                    "component": component,
                    "path": str(path),
                    "expected": expected_sha256,
                    "observed": observed,
                },
            )
        return b"".join(chunks)
    finally:
        os.close(fd)


def _read_verified_utf8(path: Path, expected_sha256: str, *, component: str) -> str:
    """Decode exactly the bytes validated by ``_read_verified_bytes``."""
    payload = _read_verified_bytes(path, expected_sha256, component=component)
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RestoreError(
            f"component is not utf-8: {component}",
            code="component_encoding",
            details={"path": str(path), "component": component},
        ) from exc


def _require_abs(path: Path | str, *, label: str) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        raise RestoreError(f"{label} must be absolute", code="path_not_absolute", details={"path": str(path)})
    return resolved


def _canonical_root(path: Path | str, *, label: str) -> Path:
    """Absolute, symlink-leaf-refused, resolve()-bound root for overlap checks."""
    absolute = _require_abs(path, label=label)
    if absolute.is_symlink():
        raise RestoreError(
            f"{label} must not be a symlink",
            code="symlink_refused",
            details={"path": str(absolute)},
        )
    return absolute.resolve()


def _path_is_within(inner: Path, outer: Path) -> bool:
    try:
        inner.relative_to(outer)
    except ValueError:
        return False
    return True


def _assert_disjoint_roots(*named: tuple[str, Path]) -> None:
    """Reject equal or nested roots (including resolve() aliases) before mutation."""
    for index, (left_label, left) in enumerate(named):
        for right_label, right in named[index + 1 :]:
            if left == right or _path_is_within(left, right) or _path_is_within(right, left):
                raise RestoreError(
                    f"{left_label} and {right_label} paths overlap",
                    code="path_overlap",
                    details={left_label: str(left), right_label: str(right)},
                )


def _symlink_refused(path: Path, *, cause: BaseException | None = None) -> RestoreError:
    err = RestoreError(
        "symlink refused in consistent copy",
        code="symlink_refused",
        details={"path": str(path)},
    )
    if cause is not None:
        err.__cause__ = cause
    return err


def _open_dir_nofollow(path: Path | str, *, dir_fd: int | None = None) -> int:
    """Open a directory descriptor without following a leaf symlink."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        if dir_fd is None:
            return os.open(path, flags)
        return os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        probe = Path(path) if dir_fd is None else None
        if exc.errno in {errno.ELOOP, errno.ENOTDIR} or (probe is not None and probe.is_symlink()):
            raise _symlink_refused(Path(path), cause=exc) from exc
        raise


def _ensure_dir(path: Path, mode: int = SAFE_DIR_MODE) -> int:
    """Create ``path`` with no-follow at every component; return its dir fd.

    Newly created directories are fchmod'd to ``mode``. Pre-existing ancestors are
    opened but never chmod'd, so a substituted foreign directory cannot have its
    mode tightened through this helper.
    """
    absolute = _require_abs(path, label="directory")
    if absolute.is_symlink():
        raise _symlink_refused(absolute)

    fd = _open_dir_nofollow(absolute.anchor)
    try:
        for part in absolute.parts[1:]:
            created = False
            try:
                next_fd = _open_dir_nofollow(part, dir_fd=fd)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode, dir_fd=fd)
                    created = True
                except FileExistsError:
                    created = False
                except OSError as exc:
                    if exc.errno in {errno.EEXIST, errno.ELOOP}:
                        # Concurrent create or symlink substitution at this name.
                        pass
                    else:
                        raise
                try:
                    next_fd = _open_dir_nofollow(part, dir_fd=fd)
                except FileNotFoundError as exc:
                    raise RestoreError(
                        "directory disappeared during create",
                        code="dest_missing",
                        details={"path": str(absolute)},
                    ) from exc
            if created:
                os.fchmod(next_fd, mode)
            os.close(fd)
            fd = next_fd
        owned = fd
        fd = -1
        return owned
    finally:
        if fd >= 0:
            os.close(fd)


def _tighten_empty_root(path: Path, mode: int = SAFE_DIR_MODE) -> None:
    """Require an empty absolute root and force mode 0700 before component writes."""
    if path.is_symlink():
        raise _symlink_refused(path)
    if path.exists():
        if not path.is_dir():
            raise RestoreError(
                "destination exists and is not a directory",
                code="dest_not_empty",
                details={"path": str(path)},
            )
        if any(path.iterdir()):
            raise RestoreError(
                "destination must be empty",
                code="dest_not_empty",
                details={"path": str(path)},
            )
        # Owned empty restore/copy root: force 0700 before component writes.
        root_fd = _open_dir_nofollow(path)
        try:
            os.fchmod(root_fd, mode)
        finally:
            os.close(root_fd)
    else:
        root_fd = _ensure_dir(path, mode)
        os.close(root_fd)


def _destination_present(path: Path) -> bool:
    """True when the path name already exists, including a dangling symlink."""
    return path.exists() or path.is_symlink()


def _prepare_secure_file(dest: Path, mode: int = SAFE_FILE_MODE) -> int:
    """Exclusively create a regular 0600 destination and return its open fd.

    Parent traversal uses owned directory descriptors with O_NOFOLLOW at every
    component. The leaf is created via openat(O_EXCL|O_NOFOLLOW) so neither a
    parent symlink nor a leaf symlink can redirect the write. Callers must keep
    writing through the returned descriptor instead of reopening the path.
    """
    if dest.is_symlink():
        raise _symlink_refused(dest)
    if _destination_present(dest):
        raise RestoreError(
            "destination already exists",
            code="dest_exists",
            details={"path": str(dest)},
        )
    parent_fd = _ensure_dir(dest.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        try:
            fd = os.open(dest.name, flags, mode, dir_fd=parent_fd)
        except FileExistsError as exc:
            raise RestoreError(
                "destination already exists",
                code="dest_exists",
                details={"path": str(dest)},
            ) from exc
        except OSError as exc:
            # O_NOFOLLOW against an existing symlink surfaces as ELOOP/EEXIST.
            if dest.is_symlink() or exc.errno == errno.ELOOP:
                raise _symlink_refused(dest, cause=exc) from exc
            raise
        try:
            st = os.fstat(fd)
            if not stat_mod.S_ISREG(st.st_mode):
                os.close(fd)
                raise RestoreError(
                    "destination must be a regular file",
                    code="dest_not_file",
                    details={"path": str(dest)},
                )
            os.fchmod(fd, mode)
        except Exception:
            os.close(fd)
            raise
        return fd
    finally:
        os.close(parent_fd)


def _finalize_secure_fd(fd: int, path: Path, mode: int = SAFE_FILE_MODE) -> None:
    _maybe_pre_chmod(path, os.fstat(fd).st_mode & 0o777)
    os.fchmod(fd, mode)


def _write_fd_stream(fd: int, source_handle: Any) -> None:
    while True:
        chunk = source_handle.read(1024 * 1024)
        if not chunk:
            break
        os.write(fd, chunk)
    os.fsync(fd)


def _write_secure(path: Path, data: bytes | str, mode: int = SAFE_FILE_MODE) -> None:
    payload = data if isinstance(data, bytes) else data.encode("utf-8")
    fd = _prepare_secure_file(path, mode)
    try:
        os.write(fd, payload)
        os.fsync(fd)
        _finalize_secure_fd(fd, path, mode)
    finally:
        os.close(fd)


def _copy_file_secure(
    src: Path,
    dest: Path,
    mode: int = SAFE_FILE_MODE,
    *,
    expected_sha256: str | None = None,
) -> str:
    """Copy a regular file through an exclusive no-follow destination fd.

    When ``expected_sha256`` is set (verified restore materialization), the bytes
    read from the source handle must match that digest or the copy fails closed
    as generation/manifest drift. Destination bytes are re-hashed via O_NOFOLLOW
    before success.
    """
    if src.is_symlink() or not src.is_file():
        raise RestoreError(
            f"source file missing: {src}",
            code="source_missing",
            details={"path": str(src)},
        )
    if dest.is_symlink():
        raise _symlink_refused(dest)
    digest = hashlib.sha256()
    fd = _prepare_secure_file(dest, mode)
    try:
        src_fd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            while True:
                chunk = os.read(src_fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                os.write(fd, chunk)
            os.fsync(fd)
        finally:
            os.close(src_fd)
        got = digest.hexdigest()
        if expected_sha256 is not None and got != expected_sha256:
            raise RestoreError(
                "source generation drifted from verified manifest",
                code="copy_drift",
                details={
                    "path": str(src),
                    "expected": expected_sha256,
                    "observed": got,
                },
            )
        _finalize_secure_fd(fd, dest, mode)
    finally:
        os.close(fd)
    materialized = _sha256_nofollow(dest)
    want = expected_sha256 if expected_sha256 is not None else got
    if materialized != want:
        raise RestoreError(
            "copied file digest mismatch",
            code="copy_digest_mismatch",
            details={"path": str(dest), "expected": want, "observed": materialized},
        )
    return got


def _read_fd_to_owned_temp_file(
    src_fd: int, dest: Path, mode: int = SAFE_FILE_MODE
) -> str:
    """Write bytes from a bound descriptor into a file inside a tool-owned temp dir.

    The destination parent must already be a private mkdtemp directory we created;
    do not route through ``_prepare_secure_file`` (system temp may sit under a
    platform symlink such as macOS ``/var`` → ``/private/var``).
    """
    os.lseek(src_fd, 0, os.SEEK_SET)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        out_fd = os.open(dest, flags, mode)
    except OSError as exc:
        if dest.is_symlink() or exc.errno in {errno.ELOOP, errno.EEXIST}:
            raise _symlink_refused(dest, cause=exc) from exc
        raise RestoreError(
            "cannot create owned-temp sqlite copy",
            code="sqlite_copy_failed",
            details={"path": str(dest), "errno": getattr(exc, "errno", None)},
        ) from exc
    try:
        digest = hashlib.sha256()
        while True:
            chunk = os.read(src_fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            os.write(out_fd, chunk)
        os.fsync(out_fd)
        os.fchmod(out_fd, mode)
    finally:
        os.close(out_fd)
    got = digest.hexdigest()
    if _sha256_nofollow(dest) != got:
        raise RestoreError(
            "copied file digest mismatch",
            code="copy_digest_mismatch",
            details={"path": str(dest)},
        )
    return got


def _open_companion_nofollow(path: Path, *, label: str) -> int | None:
    """Open a WAL/SHM companion with O_NOFOLLOW; None when absent."""
    if path.is_symlink():
        raise _symlink_refused(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if path.is_symlink() or exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise _symlink_refused(path, cause=exc) from exc
        raise RestoreError(
            f"{label} companion unreadable",
            code="sqlite_unreadable",
            details={"path": str(path), "errno": getattr(exc, "errno", None)},
        ) from exc
    try:
        st = os.fstat(fd)
        if not stat_mod.S_ISREG(st.st_mode):
            raise RestoreError(
                f"{label} companion must be a regular file",
                code="sqlite_unreadable",
                details={"path": str(path)},
            )
        return fd
    except Exception:
        os.close(fd)
        raise


def _copy_sqlite_consistent(src: Path, dest: Path, mode: int = SAFE_FILE_MODE) -> str:
    """Copy a SQLite DB without writable opens on the supplied source.

    Reads the source DB (and any WAL/SHM companions) only through O_NOFOLLOW
    descriptors into a tool-owned temporary directory. Any SQLite
    writes/checkpoint run exclusively against that private copy, so a transient
    source-path substitution cannot redirect truncation into a foreign DB
    (NFR-13/15/18). This is not a live concurrent backup protocol.
    """
    bound_fd, _bound_dev, _bound_ino = _open_bound_source_regular_file(
        src, label="sqlite_source", writable=False
    )
    wal_path, shm_path = _sqlite_companion_paths(src)
    wal_fd = _open_companion_nofollow(wal_path, label="sqlite_wal")
    shm_fd = _open_companion_nofollow(shm_path, label="sqlite_shm")
    tmp_dir = Path(tempfile.mkdtemp(prefix="ea010-sqlite-owned-"))
    try:
        os.chmod(tmp_dir, SAFE_DIR_MODE)
        tmp_db = tmp_dir / "copy.db"
        _maybe_pre_sqlite_connect(src, "owned_temp_copy")
        _read_fd_to_owned_temp_file(bound_fd, tmp_db, mode)
        if wal_fd is not None:
            _read_fd_to_owned_temp_file(wal_fd, Path(str(tmp_db) + "-wal"), mode)
        if shm_fd is not None:
            _read_fd_to_owned_temp_file(shm_fd, Path(str(tmp_db) + "-shm"), mode)

        # Writable SQLite work stays on the tool-owned temp copy only.
        try:
            owned_conn = sqlite3.connect(
                f"file:{tmp_db}?mode=rw", uri=True, timeout=0.2
            )
        except sqlite3.Error as exc:
            raise RestoreError(
                f"sqlite owned-temp copy unreadable: {exc}",
                code="sqlite_copy_failed",
                details={"path": str(tmp_db)},
            ) from exc
        try:
            owned_conn.execute("BEGIN IMMEDIATE")
            owned_conn.execute("COMMIT")
            owned_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as exc:
            try:
                owned_conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise RestoreError(
                f"sqlite owned-temp checkpoint failed: {exc}",
                code="sqlite_copy_failed",
                details={"path": str(tmp_db)},
            ) from exc
        finally:
            owned_conn.close()

        digest = sha256_file(tmp_db)
        out_fd = _prepare_secure_file(dest, mode)
        try:
            with tmp_db.open("rb") as source_handle:
                _write_fd_stream(out_fd, source_handle)
            _finalize_secure_fd(out_fd, dest, mode)
        finally:
            os.close(out_fd)
        if _sha256_nofollow(dest) != digest:
            raise RestoreError(
                "copied file digest mismatch",
                code="copy_digest_mismatch",
                details={"path": str(dest)},
            )
        return digest
    finally:
        os.close(bound_fd)
        if wal_fd is not None:
            os.close(wal_fd)
        if shm_fd is not None:
            os.close(shm_fd)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _sqlite_companion_paths(db_path: Path) -> tuple[Path, Path]:
    return Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")


def _nonempty_sqlite_companions(db_path: Path) -> list[str]:
    wal, shm = _sqlite_companion_paths(db_path)
    return [
        str(path)
        for path in (wal, shm)
        if path.exists() and path.stat().st_size > 0
    ]


def _refuse_symlink_ancestors(path: Path, *, label: str) -> None:
    """Refuse any symlink in the path or its ancestors before follow-prone opens."""
    absolute = _require_abs(path, label=label)
    probe = absolute
    while True:
        if probe.is_symlink():
            raise _symlink_refused(probe)
        parent = probe.parent
        if parent == probe:
            break
        probe = parent


def _open_bound_source_regular_file(
    path: Path,
    *,
    label: str,
    writable: bool = False,
) -> tuple[int, int, int]:
    """Open a source regular file with O_NOFOLLOW and keep the descriptor.

    Refuses leaf or ancestor symlink substitution so a foreign target cannot be
    opened for WAL checkpoint / mode=rw through a path alias (NFR-13/15).
    Returns (fd, st_dev, st_ino); caller must close fd.
    """
    _refuse_symlink_ancestors(path, label=label)
    flags = os.O_NOFOLLOW | (os.O_RDWR if writable else os.O_RDONLY)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise RestoreError(
            f"{label} source missing",
            code="source_missing",
            details={"path": str(path)},
        ) from exc
    except OSError as exc:
        if path.is_symlink() or exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise _symlink_refused(path, cause=exc) from exc
        raise RestoreError(
            f"{label} source unreadable",
            code="source_missing",
            details={"path": str(path), "errno": getattr(exc, "errno", None)},
        ) from exc
    try:
        st = os.fstat(fd)
        if stat_mod.S_ISLNK(st.st_mode) or not stat_mod.S_ISREG(st.st_mode):
            raise RestoreError(
                f"{label} source must be a regular file",
                code="source_missing",
                details={"path": str(path)},
            )
        return fd, int(st.st_dev), int(st.st_ino)
    except Exception:
        os.close(fd)
        raise


def _bind_source_regular_file(
    path: Path,
    *,
    label: str,
    writable: bool = False,
) -> tuple[int, int]:
    """Bind a source regular file with O_NOFOLLOW; return durable (st_dev, st_ino)."""
    fd, dev, ino = _open_bound_source_regular_file(
        path, label=label, writable=writable
    )
    os.close(fd)
    return dev, ino


def _regular_fd_inodes(*, exclude: set[int]) -> set[tuple[int, int]]:
    """Snapshot (st_dev, st_ino) for regular files currently open in this process."""
    found: set[tuple[int, int]] = set()
    for fdnum in range(0, 1024):
        if fdnum in exclude:
            continue
        try:
            st = os.fstat(fdnum)
        except OSError:
            continue
        if stat_mod.S_ISREG(st.st_mode):
            found.add((int(st.st_dev), int(st.st_ino)))
    return found


def _require_sqlite_owns_bound_inode(
    expected_dev: int,
    expected_ino: int,
    *,
    exclude_fds: set[int],
    before_inodes: set[tuple[int, int]],
    path: Path,
    label: str,
) -> None:
    """Refuse when sqlite3.connect did not open the O_NOFOLLOW-bound inode.

    Pathname rechecks alone cannot prove ownership: a transient substitution
    during connect can restore the original name while the handle stays on a
    foreign DB. Require a newly opened descriptor on the bound inode before any
    writable SQL (NFR-13/15/18).
    """
    after_inodes = _regular_fd_inodes(exclude=exclude_fds)
    opened = after_inodes - before_inodes
    expected = (int(expected_dev), int(expected_ino))
    if expected not in opened:
        raise RestoreError(
            f"{label} sqlite handle is not bound to the approved source inode",
            code="symlink_refused",
            details={
                "path": str(path),
                "expected_dev": expected_dev,
                "expected_ino": expected_ino,
                "opened_inodes": sorted(
                    [{"st_dev": d, "st_ino": i} for d, i in opened],
                    key=lambda item: (item["st_dev"], item["st_ino"]),
                ),
            },
        )


def _revalidate_bound_regular_file(
    path: Path,
    *,
    label: str,
    expected_dev: int,
    expected_ino: int,
) -> None:
    """Re-bind the same path and refuse when durable identity drifted."""
    _refuse_symlink_ancestors(path, label=label)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        if path.is_symlink() or exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise _symlink_refused(path, cause=exc) from exc
        raise RestoreError(
            f"{label} source identity lost after bind",
            code="source_missing",
            details={"path": str(path)},
        ) from exc
    try:
        st = os.fstat(fd)
        if not stat_mod.S_ISREG(st.st_mode):
            raise RestoreError(
                f"{label} source must remain a regular file",
                code="source_missing",
                details={"path": str(path)},
            )
        if (int(st.st_dev), int(st.st_ino)) != (int(expected_dev), int(expected_ino)):
            raise RestoreError(
                f"{label} source identity changed across sqlite boundary",
                code="symlink_refused",
                details={
                    "path": str(path),
                    "expected_dev": expected_dev,
                    "expected_ino": expected_ino,
                    "observed_dev": int(st.st_dev),
                    "observed_ino": int(st.st_ino),
                },
            )
    finally:
        os.close(fd)


def _assert_sqlite_source_quiescent(db_path: Path, *, label: str) -> dict[str, Any]:
    """Quiet-source probe for the one-user synthetic rehearsal (no source writes).

    Binds the source DB and any WAL/SHM companions with O_NOFOLLOW only. Never
    opens the supplied source writable and never checkpoints it in place: the
    isolated suite must stop the owned stand first, and capture copies DB +
    companions into a tool-owned temp directory where any needed checkpoint
    runs. An active writer that still holds a POSIX lock fails closed
    (source_not_quiescent). This is not a concurrent live backup protocol
    (NFR-13/15/18).
    """
    bound_fd, bound_dev, bound_ino = _open_bound_source_regular_file(
        db_path, label=label, writable=False
    )
    wal_path, shm_path = _sqlite_companion_paths(db_path)
    wal_fd: int | None = None
    shm_fd: int | None = None
    try:
        wal_fd = _open_companion_nofollow(wal_path, label=f"{label}_wal")
        shm_fd = _open_companion_nofollow(shm_path, label=f"{label}_shm")
        # Do not flock/O_EXLOCK the live source: on macOS that can block against
        # SQLite's own locks. The isolated suite stops the owned stand first;
        # create_manual_copy refuses generation drift across capture instead of
        # growing a live concurrent backup protocol.
        _revalidate_bound_regular_file(
            db_path,
            label=label,
            expected_dev=bound_dev,
            expected_ino=bound_ino,
        )
        return {
            "ok": True,
            "method": "stopped_stand_nofollow_bind_owned_temp_checkpoint",
            "path": str(db_path),
            "st_dev": bound_dev,
            "st_ino": bound_ino,
            "companions_bound": {
                "wal": wal_fd is not None,
                "shm": shm_fd is not None,
            },
        }
    finally:
        os.close(bound_fd)
        if wal_fd is not None:
            os.close(wal_fd)
        if shm_fd is not None:
            os.close(shm_fd)


def _read_install_sidecar_epoch(install_path: Path) -> tuple[str, int]:
    if not install_path.is_file():
        raise RestoreError(
            "journal install sidecar missing",
            code="journal_pair_incomplete",
            details={"path": str(install_path)},
        )
    try:
        lines = install_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RestoreError(
            "journal install sidecar unreadable",
            code="journal_pair_incomplete",
            details={"path": str(install_path), "error": str(exc)},
        ) from exc
    if len(lines) < 2:
        raise RestoreError(
            "journal install sidecar malformed",
            code="journal_pair_inconsistent",
            details={"path": str(install_path), "line_count": len(lines)},
        )
    installation_id = lines[0].strip()
    try:
        epoch = int(lines[1].strip())
    except ValueError as exc:
        raise RestoreError(
            "journal install sidecar epoch malformed",
            code="journal_pair_inconsistent",
            details={"path": str(install_path)},
        ) from exc
    if not installation_id or epoch < 0:
        raise RestoreError(
            "journal install sidecar identity/epoch invalid",
            code="journal_pair_inconsistent",
            details={"path": str(install_path)},
        )
    return installation_id, epoch


def _verify_journal_db_install_pair(journal_db: Path, install_path: Path) -> dict[str, Any]:
    """Require copied journal meta freshness to match the copied .install sidecar."""
    if not journal_db.is_file():
        raise RestoreError(
            "journal db missing for pair verification",
            code="journal_pair_incomplete",
            details={"path": str(journal_db)},
        )
    installation_id, sidecar_epoch = _read_install_sidecar_epoch(install_path)
    try:
        conn = sqlite3.connect(f"file:{journal_db}?mode=ro", uri=True)
        try:
            row_id = conn.execute(
                "SELECT value FROM meta WHERE key='installation_id'"
            ).fetchone()
            row_epoch = conn.execute(
                "SELECT value FROM meta WHERE key='journal_epoch'"
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        raise RestoreError(
            f"journal pair meta unreadable: {exc}",
            code="journal_pair_incomplete",
            details={"path": str(journal_db)},
        ) from exc
    if row_id is None or row_epoch is None:
        raise RestoreError(
            "journal pair meta incomplete",
            code="journal_pair_incomplete",
            details={"path": str(journal_db)},
        )
    meta_id = str(row_id[0]).strip()
    try:
        meta_epoch = int(str(row_epoch[0]).strip())
    except ValueError as exc:
        raise RestoreError(
            "journal pair meta epoch malformed",
            code="journal_pair_inconsistent",
            details={"path": str(journal_db)},
        ) from exc
    if meta_id != installation_id or meta_epoch != sidecar_epoch:
        raise RestoreError(
            "journal DB freshness differs from install sidecar",
            code="journal_pair_inconsistent",
            details={
                "meta_installation_id": meta_id,
                "sidecar_installation_id": installation_id,
                "meta_journal_epoch": meta_epoch,
                "sidecar_journal_epoch": sidecar_epoch,
            },
        )
    return {
        "installation_id": meta_id,
        "journal_epoch": meta_epoch,
        "ok": True,
    }


def _positive_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _observed_task_identity_ok(task_id: int, task: Any) -> bool:
    """Require Mapping with positive non-bool task.id equal to inventory key."""
    if not isinstance(task, Mapping):
        return False
    observed_id = _positive_int(task.get("id"))
    return observed_id is not None and observed_id == task_id


def _projects_match_strict(expected: Any, observed: Any) -> bool:
    """Strict durable/observed project equality; missing/null/bool never match."""
    left = _positive_int(expected)
    right = _positive_int(observed)
    return left is not None and right is not None and left == right


def _tree_file_hashes(root: Path) -> dict[str, str]:
    if not root.is_dir():
        raise RestoreError(
            f"attachments root missing: {root}",
            code="attachments_missing",
            details={"path": str(root)},
        )
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RestoreError("symlink refused under attachments", code="symlink_refused", details={"path": str(path)})
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        out[rel] = sha256_file(path)
    return out


def _copy_tree_secure(
    src: Path,
    dest: Path,
    *,
    expected_hashes: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy an attachments tree into an owned destination directory.

    When ``expected_hashes`` is provided it is the verified-manifest authority:
    a concurrent source generation change or dest ancestor substitution fails
    closed before success. Directory creation and file opens use no-follow
    descriptors at every component.
    """
    if dest.is_symlink():
        raise _symlink_refused(dest)
    if _destination_present(dest):
        raise RestoreError(
            "attachments destination must be empty",
            code="dest_not_empty",
            details={"path": str(dest)},
        )
    dest_fd = _ensure_dir(dest)
    os.close(dest_fd)
    if expected_hashes is not None:
        hashes = {str(key): str(value) for key, value in expected_hashes.items()}
        observed = _tree_file_hashes(src)
        if observed != hashes:
            raise RestoreError(
                "attachments digest drift from verified manifest",
                code="copy_drift",
                details={"expected": hashes, "observed": observed},
            )
    else:
        hashes = _tree_file_hashes(src)
    copied: dict[str, str] = {}
    for rel, digest in hashes.items():
        source = src / rel
        target = dest / rel
        got = _copy_file_secure(source, target, expected_sha256=digest)
        if got != digest:
            raise RestoreError(
                "attachment digest drift during copy",
                code="copy_digest_mismatch",
                details={"path": rel, "expected": digest, "observed": got},
            )
        copied[rel] = got
    if dest.is_symlink():
        raise _symlink_refused(dest)
    materialized = _tree_file_hashes(dest)
    if materialized != hashes:
        raise RestoreError(
            "materialized attachments digest mismatch",
            code="copy_digest_mismatch",
            details={"expected": hashes, "observed": materialized},
        )
    return copied


@dataclass(frozen=True)
class ContourPaths:
    """Paths of a synthetic Vikunja + journal contour to copy or restore into."""

    root: Path
    vikunja_db: Path
    attachments: Path
    config: Path
    journal: Path

    @classmethod
    def from_stand_root(cls, root: Path | str, *, journal_name: str = "integration.sqlite3") -> "ContourPaths":
        base = _canonical_root(root, label="contour_root")
        return cls(
            root=base,
            vikunja_db=base / "stand.db",
            attachments=base / "files",
            config=base / "config.json",
            journal=base / "journal" / journal_name,
        )


def export_links(journal_path: Path) -> dict[str, Any]:
    """Export receipt/mapping summary without task bodies or secrets."""
    path = _require_abs(journal_path, label="journal")
    if not path.is_file():
        raise RestoreError("journal missing for links export", code="journal_missing", details={"path": str(path)})
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        mappings = [
            {
                "source_kind": row["source_kind"],
                "source_key": row["source_key"],
                "issuer_id": row["issuer_id"],
                "operation_id": row["operation_id"],
            }
            for row in conn.execute(
                "SELECT source_kind, source_key, issuer_id, operation_id "
                "FROM source_mappings ORDER BY source_kind, source_key"
            )
        ]
        receipts = [
            {
                "issuer_id": row["issuer_id"],
                "operation_id": row["operation_id"],
                "vikunja_id": row["vikunja_id"],
                "project_id": row["project_id"],
                "marker": row["marker"],
            }
            for row in conn.execute(
                "SELECT issuer_id, operation_id, vikunja_id, project_id, marker "
                "FROM receipts ORDER BY issuer_id, operation_id"
            )
        ]
    finally:
        conn.close()
    return {
        "schema": LINKS_SCHEMA,
        "exported_at": _utc_now(),
        "mappings": mappings,
        "receipts": receipts,
    }


def _strip_secrets_from_config(raw: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    config = json.loads(json.dumps(raw))
    secrets: dict[str, Any] = {}
    service = config.get("service")
    if isinstance(service, dict) and "secret" in service:
        secrets["service.secret"] = service.pop("secret")
        service["secret"] = {"ref": "secrets/service.secret", "present": False}
    return config, secrets


def create_manual_copy(
    source: ContourPaths,
    dest: Path | str,
    *,
    secrets_extra: Mapping[str, Any] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Create one manual consistent copy. Never schedules or retains prior copies."""
    source_root = _canonical_root(source.root, label="source_root")
    dest_root = _canonical_root(dest, label="copy_dest")
    # Refuse equal/nested/alias overlap before mkdir/chmod/materialization so a
    # nested dest cannot inject DB/config into the live attachments tree (NFR-13/15).
    _assert_disjoint_roots(("source", source_root), ("copy", dest_root))
    _tighten_empty_root(dest_root)

    if not source.attachments.is_dir() or source.attachments.is_symlink():
        if source.attachments.is_symlink():
            raise _symlink_refused(source.attachments)
        raise RestoreError(
            "attachments root missing",
            code="attachments_missing",
            details={"path": str(source.attachments)},
        )
    _refuse_symlink_ancestors(source.attachments, label="attachments")

    components: dict[str, Any] = {}

    # Bind every follow-prone source identity before any capture so a symlink
    # cannot redirect reads into a foreign target. Writable SQLite work never
    # targets the supplied source — only a tool-owned temp copy.
    _bind_source_regular_file(source.vikunja_db, label="vikunja_db")
    _bind_source_regular_file(source.journal, label="journal_db")
    _bind_source_regular_file(source.config, label="config")

    # Narrow rehearsal guard: prove the stopped synthetic source is bound and
    # quiet (no source writes), capture DB+attachments via owned-temp copy, then
    # refuse if generation drifted. Stand lifecycle stop remains the suite boundary.
    vikunja_quiescence = _assert_sqlite_source_quiescent(source.vikunja_db, label="vikunja_db")
    pre_db_sha = sha256_file(source.vikunja_db)
    pre_attachment_hashes = _tree_file_hashes(source.attachments)
    vikunja_dest = dest_root / "vikunja" / "stand.db"
    components[COMPONENT_VIKUNJA_DB] = {
        "path": "vikunja/stand.db",
        "sha256": _copy_sqlite_consistent(source.vikunja_db, vikunja_dest),
        "method": "owned_temp_wal_checkpoint",
    }
    attachments_dest = dest_root / "attachments"
    attachment_hashes = _copy_tree_secure(source.attachments, attachments_dest)
    post_attachment_hashes = _tree_file_hashes(source.attachments)
    post_db_sha = sha256_file(source.vikunja_db)
    post_quiescence = _assert_sqlite_source_quiescent(source.vikunja_db, label="vikunja_db")
    if (
        attachment_hashes != pre_attachment_hashes
        or post_attachment_hashes != pre_attachment_hashes
        or post_db_sha != pre_db_sha
    ):
        raise RestoreError(
            "vikunja source not quiescent; generation changed during capture",
            code="source_not_quiescent",
            details={
                "path": str(source.vikunja_db),
                "attachments": str(source.attachments),
                "pre_db_sha256": pre_db_sha,
                "post_db_sha256": post_db_sha,
                "pre_attachments": pre_attachment_hashes,
                "copied_attachments": attachment_hashes,
                "post_attachments": post_attachment_hashes,
            },
        )
    components[COMPONENT_ATTACHMENTS] = {
        "path": "attachments",
        "files": attachment_hashes,
        "tree_sha256": sha256_bytes(
            json.dumps(attachment_hashes, sort_keys=True, separators=(",", ":")).encode()
        ),
    }
    vikunja_quiescence = {
        **vikunja_quiescence,
        "post_capture": post_quiescence,
        "generation_stable": True,
        "db_sha256": pre_db_sha,
    }

    raw_config = json.loads(source.config.read_text(encoding="utf-8"))
    safe_config, extracted = _strip_secrets_from_config(raw_config)
    safe_bytes = (json.dumps(safe_config, indent=2, sort_keys=True) + "\n").encode()
    safe_path = dest_root / "safe_config" / "config.json"
    _write_secure(safe_path, safe_bytes)
    components[COMPONENT_SAFE_CONFIG] = {
        "path": "safe_config/config.json",
        "sha256": sha256_bytes(safe_bytes),
    }

    secrets_dir = dest_root / "secrets"
    secrets_fd = _ensure_dir(secrets_dir)
    os.close(secrets_fd)
    secrets_payload = {
        "extracted": extracted,
        "extra": dict(secrets_extra or {}),
        "note": "Local protected copy only; never commit or log secret values.",
    }
    if "service.secret" in extracted:
        _write_secure(secrets_dir / "service.secret", str(extracted["service.secret"]))
    # Store only non-secret metadata markers alongside secret files; values in
    # extracted are written to dedicated 0600 files above, not re-echoed in logs.
    meta_only = {
        "extracted_keys": sorted(extracted),
        "extra_keys": sorted((secrets_extra or {}).keys()),
        "note": secrets_payload["note"],
    }
    _write_secure(secrets_dir / "secrets-meta.json", json.dumps(meta_only, indent=2, sort_keys=True) + "\n")
    if secrets_extra:
        # Synthetic restore credentials / handles stay in the protected tree.
        _write_secure(
            secrets_dir / "extra.json",
            json.dumps(dict(secrets_extra), indent=2, sort_keys=True) + "\n",
        )
    components[COMPONENT_SECRETS] = {
        "path": "secrets",
        "mode": oct(SAFE_DIR_MODE),
        "files": {
            rel: sha256_file(secrets_dir / rel)
            for rel in sorted(p.name for p in secrets_dir.iterdir() if p.is_file())
        },
    }

    # Hold the canonical journal OS-lock across DB backup / .install / links so
    # a concurrent prepare_intent cannot advance epoch between unlocked reads.
    journal_dest = dest_root / "journal" / "integration.sqlite3"
    install_src = install_path_for(source.journal)
    install_dest = install_path_for(journal_dest)
    journal_lock = JournalLock(lock_path_for(source.journal))
    with journal_lock:
        _maybe_crash("manual_copy_journal_locked")
        components[COMPONENT_JOURNAL_DB] = {
            "path": "journal/integration.sqlite3",
            "sha256": _copy_sqlite_consistent(source.journal, journal_dest),
            "method": "sqlite_backup_api",
        }
        components[COMPONENT_JOURNAL_INSTALL] = {
            "path": "journal/integration.sqlite3.install",
            "sha256": _copy_file_secure(install_src, install_dest),
        }
        # Explicitly do not copy .authority — outside the restorable pair.
        pair = _verify_journal_db_install_pair(journal_dest, install_dest)
        # Derive links from the copied generation, not a later live read.
        links = export_links(journal_dest)
    links_bytes = (json.dumps(links, indent=2, sort_keys=True) + "\n").encode()
    links_path = dest_root / "links" / "mappings.json"
    _write_secure(links_path, links_bytes)
    components[COMPONENT_LINKS] = {
        "path": "links/mappings.json",
        "sha256": sha256_bytes(links_bytes),
        "mapping_count": len(links["mappings"]),
        "receipt_count": len(links["receipts"]),
        "journal_epoch": pair["journal_epoch"],
        "installation_id": pair["installation_id"],
        "derived_from": "copied_journal",
    }

    manifest = {
        "schema": COPY_SCHEMA,
        "created_at": _utc_now(),
        "copy_id": uuid.uuid4().hex,
        "manual": True,
        "scheduler": False,
        "rpo": None,
        "rto": None,
        "retention": None,
        "authority_included": False,
        "mini_local_copy_not_device_loss_protection": True,
        "working_contour_restore": False,
        "source_quiescence": {
            "vikunja_db": vikunja_quiescence,
            "attachments": {
                "ok": True,
                "method": "captured_after_stock_sqlite_quiescence_with_stable_generation",
                "path": str(source.attachments),
            },
            "journal": {
                "ok": True,
                "method": "canonical_os_lock_held_for_db_install_links",
                "path": str(source.journal),
            },
        },
        "note": note
        or (
            "Manual consistent copy for isolated synthetic restore. "
            "Same-Mini retention is not protection against physical device loss."
        ),
        "components": components,
        "source": {
            "root": str(source.root),
            "vikunja_db": str(source.vikunja_db),
            "attachments": str(source.attachments),
            "config": str(source.config),
            "journal": str(source.journal),
        },
    }
    _write_secure(
        dest_root / "manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )
    return manifest


def load_copy_manifest(copy_dir: Path | str) -> dict[str, Any]:
    root = _require_abs(copy_dir, label="copy_dir")
    path = root / "manifest.json"
    if not path.is_file():
        raise RestoreError("copy manifest missing", code="manifest_missing", details={"path": str(path)})
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RestoreError("copy manifest unreadable", code="manifest_corrupt", details={"error": str(exc)}) from exc
    if data.get("schema") != COPY_SCHEMA:
        raise RestoreError("unexpected copy schema", code="manifest_schema", details={"schema": data.get("schema")})
    return data


def verify_consistent_copy(copy_dir: Path | str) -> dict[str, Any]:
    """Verify every required component exists and matches the manifest hashes."""
    root = _require_abs(copy_dir, label="copy_dir")
    manifest = load_copy_manifest(root)
    components = manifest.get("components") or {}
    missing = [name for name in REQUIRED_COMPONENTS if name not in components]
    if missing:
        raise RestoreError(
            "consistent copy incomplete",
            code="partial_copy",
            details={"missing_components": missing},
        )
    checked: dict[str, Any] = {}
    for name in REQUIRED_COMPONENTS:
        meta = components[name]
        if name == COMPONENT_ATTACHMENTS:
            files_meta = meta.get("files") or {}
            attachments_root = root / str(meta["path"])
            if not attachments_root.is_dir():
                raise RestoreError(
                    "attachments tree missing",
                    code="partial_copy",
                    details={"component": name},
                )
            observed = _tree_file_hashes(attachments_root)
            if observed != files_meta:
                raise RestoreError(
                    "attachments digest drift",
                    code="copy_drift",
                    details={"component": name, "expected": files_meta, "observed": observed},
                )
            checked[name] = {"files": len(observed), "ok": True}
            continue
        if name == COMPONENT_SECRETS:
            secrets_root = root / str(meta["path"])
            if not secrets_root.is_dir():
                raise RestoreError("secrets tree missing", code="partial_copy", details={"component": name})
            mode = secrets_root.stat().st_mode & 0o777
            if mode != SAFE_DIR_MODE:
                raise RestoreError(
                    "secrets directory mode unsafe",
                    code="secrets_mode",
                    details={"mode": oct(mode)},
                )
            files_meta = meta.get("files") or {}
            observed = {
                rel: sha256_file(secrets_root / rel)
                for rel in sorted(p.name for p in secrets_root.iterdir() if p.is_file())
            }
            if observed != files_meta:
                raise RestoreError(
                    "secrets digest drift",
                    code="copy_drift",
                    details={"component": name},
                )
            for rel in observed:
                fmode = (secrets_root / rel).stat().st_mode & 0o777
                if fmode != SAFE_FILE_MODE:
                    raise RestoreError(
                        "secret file mode unsafe",
                        code="secrets_mode",
                        details={"file": rel, "mode": oct(fmode)},
                    )
            checked[name] = {"files": len(observed), "ok": True}
            continue
        rel = str(meta["path"])
        path = root / rel
        if not path.is_file():
            raise RestoreError(
                f"component missing: {name}",
                code="partial_copy",
                details={"component": name, "path": rel},
            )
        digest = sha256_file(path)
        if digest != meta.get("sha256"):
            raise RestoreError(
                f"component digest mismatch: {name}",
                code="copy_drift",
                details={"component": name, "expected": meta.get("sha256"), "observed": digest},
            )
        checked[name] = {"sha256": digest, "ok": True}
    journal_meta = components.get(COMPONENT_JOURNAL_DB) or {}
    install_meta = components.get(COMPONENT_JOURNAL_INSTALL) or {}
    pair = _verify_journal_db_install_pair(
        root / str(journal_meta["path"]),
        root / str(install_meta["path"]),
    )
    checked["journal_db_install_pair"] = pair
    if manifest.get("scheduler") is not False:
        raise RestoreError("copy must declare scheduler=false", code="scheduler_forbidden")
    if manifest.get("authority_included") is not False:
        raise RestoreError("copy must not include authority fence", code="authority_forbidden")
    return {
        "ok": True,
        "copy_id": manifest.get("copy_id"),
        "manifest": manifest,
        "checked": checked,
        "mini_local_copy_not_device_loss_protection": True,
        "rpo": None,
        "rto": None,
        "retention": None,
    }


def _assert_empty_target(target: Path) -> None:
    try:
        _tighten_empty_root(target)
    except RestoreError as exc:
        if exc.code == "dest_not_empty":
            raise RestoreError(
                "restore target must be an empty directory",
                code="target_not_empty",
                details=exc.details,
            ) from exc
        raise


def _rewrite_config_for_target(
    safe_config: dict[str, Any],
    *,
    target: Path,
    service_secret: str | None,
    listen_host: str | None = None,
    listen_port: int | None = None,
) -> dict[str, Any]:
    config = json.loads(json.dumps(safe_config))
    service = config.setdefault("service", {})
    if service_secret is not None:
        service["secret"] = service_secret
    elif isinstance(service.get("secret"), dict):
        raise RestoreError(
            "service secret missing from protected secrets tree",
            code="secret_missing",
        )
    service["rootpath"] = str(target)
    if listen_host is not None and listen_port is not None:
        service["interface"] = f"{listen_host}:{listen_port}"
        service["publicurl"] = f"http://{listen_host}:{listen_port}/"
    database = config.setdefault("database", {})
    database["type"] = "sqlite"
    database["path"] = str(target / "stand.db")
    files = config.setdefault("files", {})
    files["basepath"] = str(target / "files")
    return config


def restore_into_empty_target(
    copy_dir: Path | str,
    target: Path | str,
    *,
    listen_host: str | None = None,
    listen_port: int | None = None,
    admit_journal: bool = True,
) -> dict[str, Any]:
    """Restore a verified consistent copy into a separate empty synthetic target.

    Writes stay off until ``reconcile_restored_contour`` confirms Vikunja IDs,
    attachment checksums, and journal mappings. Never restores over a working
    contour and never invents creates for unknown/old operations.
    """
    copy_root = _canonical_root(copy_dir, label="copy_dir")
    target_root = _canonical_root(target, label="restore_target")
    # Separate-empty-target boundary: never materialize into the backup tree.
    _assert_disjoint_roots(("copy", copy_root), ("target", target_root))
    verification = verify_consistent_copy(copy_root)
    _assert_empty_target(target_root)

    # Keep the single verified generation; do not reload manifest after verify.
    manifest = verification["manifest"]
    components = manifest["components"]

    # Materialize Vikunja DB + attachments against verified manifest digests.
    _copy_file_secure(
        copy_root / components[COMPONENT_VIKUNJA_DB]["path"],
        target_root / "stand.db",
        expected_sha256=str(components[COMPONENT_VIKUNJA_DB]["sha256"]),
    )
    _copy_tree_secure(
        copy_root / str(components[COMPONENT_ATTACHMENTS]["path"]),
        target_root / "files",
        expected_hashes=components[COMPONENT_ATTACHMENTS]["files"],
    )

    safe_config_path = copy_root / str(components[COMPONENT_SAFE_CONFIG]["path"])
    # Single no-follow read: digest and deserialize the same bytes (no reopen).
    safe_config = json.loads(
        _read_verified_utf8(
            safe_config_path,
            str(components[COMPONENT_SAFE_CONFIG]["sha256"]),
            component=COMPONENT_SAFE_CONFIG,
        )
    )
    secrets_meta = components[COMPONENT_SECRETS]
    secrets_root = copy_root / str(secrets_meta["path"])
    secret_path = secrets_root / "service.secret"
    service_secret = None
    if secret_path.is_file() and not secret_path.is_symlink():
        secret_digest = (secrets_meta.get("files") or {}).get("service.secret")
        if secret_digest is not None:
            service_secret = _read_verified_utf8(
                secret_path,
                str(secret_digest),
                component=COMPONENT_SECRETS,
            )
        else:
            # No per-file digest in the manifest: still one no-follow read, never reopen.
            try:
                secret_fd = os.open(secret_path, os.O_RDONLY | os.O_NOFOLLOW)
            except OSError as exc:
                if secret_path.is_symlink() or exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise _symlink_refused(secret_path, cause=exc) from exc
                raise RestoreError(
                    "component unreadable: secrets",
                    code="source_missing",
                    details={"path": str(secret_path), "component": COMPONENT_SECRETS},
                ) from exc
            try:
                chunks: list[bytes] = []
                while True:
                    chunk = os.read(secret_fd, 1024 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
                service_secret = b"".join(chunks).decode("utf-8")
            finally:
                os.close(secret_fd)
    rewritten = _rewrite_config_for_target(
        safe_config,
        target=target_root,
        service_secret=service_secret,
        listen_host=listen_host,
        listen_port=listen_port,
    )
    _write_secure(target_root / "config.json", json.dumps(rewritten, indent=2, sort_keys=True) + "\n")

    journal_dir = target_root / "journal"
    journal_fd = _ensure_dir(journal_dir)
    os.close(journal_fd)
    journal_path = journal_dir / "integration.sqlite3"
    _copy_file_secure(
        copy_root / components[COMPONENT_JOURNAL_DB]["path"],
        journal_path,
        expected_sha256=str(components[COMPONENT_JOURNAL_DB]["sha256"]),
    )
    _copy_file_secure(
        copy_root / components[COMPONENT_JOURNAL_INSTALL]["path"],
        install_path_for(journal_path),
        expected_sha256=str(components[COMPONENT_JOURNAL_INSTALL]["sha256"]),
    )
    links_path = copy_root / str(components[COMPONENT_LINKS]["path"])
    links = json.loads(
        _read_verified_utf8(
            links_path,
            str(components[COMPONENT_LINKS]["sha256"]),
            component=COMPONENT_LINKS,
        )
    )
    _write_secure(
        target_root / "links" / "mappings.json",
        json.dumps(links, indent=2, sort_keys=True) + "\n",
    )

    writes_enabled = False
    restore_drift = True
    admitted = False
    if admit_journal:
        journal = IntegrationJournal(journal_path)
        journal.admit_restored_backup()
        try:
            # Probe: unknown/old ops must not gain create permission.
            probe_op = new_operation_id()
            issuer = None
            try:
                # Readable open after admit still has writes off.
                rows = journal.history()
                for row in rows:
                    if row.get("event_type") == "restored_backup_admitted":
                        admitted = True
                        break
                # can_dispatch on any known op must remain false under drift.
                for mapping in links.get("mappings") or []:
                    if not journal.can_dispatch(mapping["issuer_id"], mapping["operation_id"]):
                        continue
                    raise RestoreError(
                        "restored journal allowed dispatch before reconcile",
                        code="writes_not_closed",
                    )
                # Ensure a fresh unknown id also cannot dispatch.
                if issuer is None and links.get("mappings"):
                    issuer = links["mappings"][0]["issuer_id"]
                if issuer is not None and journal.can_dispatch(issuer, probe_op):
                    raise RestoreError(
                        "unknown operation gained create permission after restore",
                        code="unknown_create_allowed",
                    )
            finally:
                meta_writes = None
                meta_drift = None
                try:
                    conn = sqlite3.connect(str(journal_path))
                    meta_writes = conn.execute(
                        "SELECT value FROM meta WHERE key='writes_enabled'"
                    ).fetchone()
                    meta_drift = conn.execute(
                        "SELECT value FROM meta WHERE key='restore_drift'"
                    ).fetchone()
                    conn.close()
                except sqlite3.Error:
                    pass
                writes_enabled = bool(meta_writes and meta_writes[0] == "1")
                restore_drift = bool(meta_drift and meta_drift[0] == "1")
                journal.close()
            if writes_enabled or not restore_drift:
                raise RestoreError(
                    "restore admission did not force writes-off",
                    code="writes_not_closed",
                    details={"writes_enabled": writes_enabled, "restore_drift": restore_drift},
                )
            if not admitted:
                raise RestoreError("restored_backup_admitted history missing", code="admit_missing")
        except JournalError as exc:
            raise RestoreError(
                f"journal admit/restore failed: {exc}",
                code="journal_admit_failed",
                details=dict(getattr(exc, "details", {}) or {}),
            ) from exc

    return {
        "ok": True,
        "target": str(target_root),
        "copy_id": manifest.get("copy_id"),
        "verification": verification,
        "writes_enabled": writes_enabled,
        "restore_drift": restore_drift,
        "admitted": admitted,
        "links": links,
        "journal_path": str(journal_path),
        "vikunja_db": str(target_root / "stand.db"),
        "attachments_root": str(target_root / "files"),
        "working_contour_restore": False,
        "mini_local_copy_not_device_loss_protection": True,
        "scheduler": False,
        "rpo": None,
        "rto": None,
        "retention": None,
    }


def attachment_checksums(attachments_root: Path | str) -> dict[str, str]:
    return _tree_file_hashes(_require_abs(attachments_root, label="attachments_root"))


def _durable_write_flags(journal_path: Path) -> dict[str, bool]:
    conn = sqlite3.connect(str(journal_path))
    try:
        writes = conn.execute("SELECT value FROM meta WHERE key='writes_enabled'").fetchone()
        drift = conn.execute("SELECT value FROM meta WHERE key='restore_drift'").fetchone()
        return {
            "writes_enabled": bool(writes and writes[0] == "1"),
            "restore_drift": bool(drift and drift[0] == "1"),
        }
    finally:
        conn.close()


def _receipt_fingerprint(receipt: Mapping[str, Any]) -> tuple[Any, ...]:
    project_id = receipt.get("project_id")
    return (
        str(receipt.get("issuer_id") or ""),
        str(receipt.get("operation_id") or ""),
        int(receipt["vikunja_id"]) if isinstance(receipt.get("vikunja_id"), int) else receipt.get("vikunja_id"),
        int(project_id) if isinstance(project_id, int) and not isinstance(project_id, bool) else project_id,
        receipt.get("marker"),
    )


def _mapping_fingerprint(mapping: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(mapping.get("source_kind") or ""),
        str(mapping.get("source_key") or ""),
        str(mapping.get("issuer_id") or ""),
        str(mapping.get("operation_id") or ""),
    )


def _receipt_is_restore_recovered(receipt: Mapping[str, Any]) -> bool:
    """True when the durable receipt was committed by restore exact-match recovery."""
    if receipt.get("restore_recovered") is True:
        return True
    effect = receipt.get("effect")
    if isinstance(effect, Mapping) and effect.get("source") == RESTORE_RECEIPT_SOURCE:
        return True
    return False


def _load_durable_inventory(journal: IntegrationJournal) -> dict[str, list[dict[str, Any]]]:
    """Complete receipt/mapping inventory from the durable journal (authoritative)."""
    receipts: list[dict[str, Any]] = []
    mappings: list[dict[str, Any]] = []
    for op in journal.list_operations():
        if op.source_kind is not None and op.source_key is not None:
            mappings.append(
                {
                    "source_kind": op.source_kind,
                    "source_key": op.source_key,
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                }
            )
        if op.receipt is None:
            continue
        effect = op.receipt.get("effect")
        effect_map = effect if isinstance(effect, Mapping) else {}
        receipts.append(
            {
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "op_type": op.op_type,
                "target_id": op.target_id,
                "vikunja_id": int(op.receipt["vikunja_id"]),
                "project_id": int(op.receipt["project_id"]),
                "marker": op.receipt.get("marker"),
                "effect": dict(effect_map),
                "restore_recovered": effect_map.get("source") == RESTORE_RECEIPT_SOURCE,
            }
        )
    receipts.sort(key=_receipt_fingerprint)
    mappings.sort(key=_mapping_fingerprint)
    return {"receipts": receipts, "mappings": mappings}


def _receipt_inventory_drift(
    *,
    supplied_receipts: list[Any],
    durable_receipts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Compare copy links to durable receipts, allowing restore recovery deltas.

    Every supplied fingerprint must still exist unchanged. Extra durable
    receipts are allowed only when durably marked as restore recoveries so a
    mid-recovery restart or post-success repeat with the original links is not
    treated as backup drift.
    """
    supplied_set = {_receipt_fingerprint(r) for r in supplied_receipts}
    durable_set = {_receipt_fingerprint(r) for r in durable_receipts}
    missing = supplied_set - durable_set
    extra = durable_set - supplied_set
    if not missing and not extra:
        return None
    durable_by_fp = {_receipt_fingerprint(r): r for r in durable_receipts}
    unexplained = sorted(
        fp for fp in extra if not _receipt_is_restore_recovered(durable_by_fp[fp])
    )
    if not missing and not unexplained:
        return None
    return {
        "reason": "receipt_inventory_mismatch",
        "supplied": sorted(supplied_set),
        "durable": sorted(durable_set),
        "missing_from_durable": sorted(missing),
        "unexplained_durable_extra": unexplained,
    }


def _observed_inventory_identity_mismatches(
    observed_tasks: Mapping[Any, Any],
) -> list[dict[str, Any]]:
    """Fence every supplied observed identity before marker correlation.

    Invalid/missing/nonpositive/bool inventory keys, non-mapping values, and
    key/id conflicts must fail closed. Silently skipping malformed extras would
    let a valid confirmed receipt reopen writes while ambiguous evidence remains.
    """
    mismatches: list[dict[str, Any]] = []
    for inventory_key, task in observed_tasks.items():
        key_id = _positive_int(inventory_key)
        if key_id is None:
            mismatches.append(
                {
                    "reason": "observed_identity_malformed",
                    "inventory_key": inventory_key,
                    "inventory_key_type": type(inventory_key).__name__,
                    "observed_id": task.get("id") if isinstance(task, Mapping) else None,
                }
            )
            continue
        if not isinstance(task, Mapping):
            mismatches.append(
                {
                    "reason": "observed_identity_malformed",
                    "inventory_key": key_id,
                    "observed_id": None,
                    "value_type": type(task).__name__,
                }
            )
            continue
        observed_id = task.get("id")
        if not _observed_task_identity_ok(key_id, task):
            mismatches.append(
                {
                    "reason": "observed_identity_malformed",
                    "inventory_key": key_id,
                    "observed_id": observed_id,
                }
            )
    mismatches.sort(
        key=lambda item: (
            str(item.get("inventory_key")),
            str(item.get("observed_id")),
        )
    )
    return mismatches


def _exact_marker_task_ids(
    observed_tasks: Mapping[int, Mapping[str, Any]],
    marker: str,
) -> list[int]:
    """Exact-grammar marker correlation across a pre-fenced observed inventory.

    Callers must run ``_observed_inventory_identity_mismatches`` first. Only
    objects with a positive non-bool ``task.id`` equal to the inventory key
    count as proof; malformed rows are never treated as absent evidence.
    """
    matches: list[int] = []
    for task_id, task in observed_tasks.items():
        if _positive_int(task_id) is None:
            continue
        if not _observed_task_identity_ok(task_id, task):
            continue
        extracted = extract_marker(task.get("description") if isinstance(task, Mapping) else None)
        if extracted is not None and extracted["marker"] == marker:
            matches.append(task_id)
    return sorted(matches)


def _unaccounted_observed_markers(
    journal: IntegrationJournal,
    observed_tasks: Mapping[int, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Reject exact EA010 markers in observed Vikunja absent from the journal.

    Receipt/pending validation only covers identities already present in the
    restored journal. An older journal against a newer Vikunja can otherwise
    publish writes and then replay the lost identity as a fresh create.
    Inventory identity fencing must already have rejected malformed extras.
    """
    known_ops = {(op.issuer_id, op.operation_id) for op in journal.list_operations()}
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for task_id, task in observed_tasks.items():
        if _positive_int(task_id) is None:
            continue
        if not _observed_task_identity_ok(task_id, task):
            continue
        extracted = extract_marker(task.get("description") if isinstance(task, Mapping) else None)
        if extracted is None:
            continue
        issuer_id = extracted["issuer_id"]
        operation_id = extracted["operation_id"]
        key = (issuer_id, operation_id)
        if key in known_ops:
            continue
        entry = by_identity.setdefault(
            key,
            {
                "reason": "observed_marker_unaccounted",
                "issuer_id": issuer_id,
                "operation_id": operation_id,
                "marker": extracted["marker"],
                "matches": [],
            },
        )
        entry["matches"].append(task_id)
    mismatches: list[dict[str, Any]] = []
    for entry in by_identity.values():
        entry["matches"] = sorted(entry["matches"])
        mismatches.append(entry)
    mismatches.sort(
        key=lambda item: (str(item.get("issuer_id") or ""), str(item.get("operation_id") or ""))
    )
    return mismatches


def _fail_reconcile(
    journal: IntegrationJournal,
    path: Path,
    *,
    code: str,
    message: str,
    mismatches: list[dict[str, Any]] | None = None,
) -> None:
    """Keep the writes fence closed and surface durable flags with the failure."""
    try:
        journal.set_restore_drift(True)
    except JournalError:
        pass
    flags = _durable_write_flags(path)
    details: dict[str, Any] = {
        "creates_issued": False,
        "writes_enabled": flags["writes_enabled"],
        "restore_drift": flags["restore_drift"],
    }
    if mismatches is not None:
        details["mismatches"] = mismatches
    raise RestoreError(message, code=code, details=details)


def _decode_supplied_link_lists(
    links: Mapping[str, Any],
) -> tuple[list[Any], list[Any]]:
    """Decode copy receipts/mappings; reject non-list shapes with RestoreError.

    Must run only after set_restore_drift(True) so callers can fence first and
    surface durable writes_enabled/restore_drift via _fail_reconcile.
    """
    decoded: dict[str, list[Any]] = {}
    for key in ("receipts", "mappings"):
        raw = links.get(key)
        if raw is None:
            decoded[key] = []
            continue
        if not isinstance(raw, list):
            raise RestoreError(
                f"malformed links.{key}; expected list",
                code="reconcile_malformed_links",
                details={
                    "field": key,
                    "value_type": type(raw).__name__,
                },
            )
        for index, item in enumerate(raw):
            if not isinstance(item, Mapping):
                raise RestoreError(
                    f"malformed links.{key}[{index}]; expected mapping",
                    code="reconcile_malformed_links",
                    details={
                        "field": key,
                        "index": index,
                        "value_type": type(item).__name__,
                    },
                )
        decoded[key] = list(raw)
    return decoded["receipts"], decoded["mappings"]


def _mark_conflict_mismatch(
    journal: IntegrationJournal | RestoreRecoverySection,
    op: OperationRecord,
    *,
    reason: str,
    mismatches: list[dict[str, Any]],
    details: Mapping[str, Any],
) -> None:
    try:
        journal.mark_conflict(
            op.issuer_id,
            op.operation_id,
            reason=reason,
            details=dict(details),
        )
    except JournalError as exc:
        mismatches.append(
            {
                "reason": "mark_conflict_failed",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "error": str(exc),
            }
        )
    payload = {
        "reason": details.get("mismatch_reason", reason),
        "issuer_id": op.issuer_id,
        "operation_id": op.operation_id,
    }
    payload.update({k: v for k, v in details.items() if k != "mismatch_reason"})
    mismatches.append(payload)


def _retain_unknown_mutation(
    journal: IntegrationJournal | RestoreRecoverySection,
    op: OperationRecord,
    *,
    observed_tasks: Mapping[int, Mapping[str, Any]],
    mismatches: list[dict[str, Any]],
) -> None:
    """Preserve update/complete uncertainty; never invent a create-style receipt.

    Exact task markers identify the object but cannot prove a mutation effect.
    Matches dispatcher recover path: retain durable unknown without causal proof.
    """
    marker = op.marker if isinstance(op.marker, str) else None
    if marker is None:
        mismatches.append(
            {
                "reason": "pending_marker_missing",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "state": op.state.value,
                "op_type": op.op_type,
            }
        )
        return
    if marker != "":
        if MARKER_RE.fullmatch(marker) is None:
            mismatches.append(
                {
                    "reason": "pending_marker_invalid",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "marker": marker,
                    "op_type": op.op_type,
                }
            )
            return
        matches = _exact_marker_task_ids(observed_tasks, marker)
        if len(matches) > 1:
            _mark_conflict_mismatch(
                journal,
                op,
                reason="restore_multiple_exact_marker_matches",
                mismatches=mismatches,
                details={
                    "mismatch_reason": "multiple_exact_matches",
                    "marker": marker,
                    "matches": matches,
                    "op_type": op.op_type,
                },
            )
            return
        if op.target_id is not None and matches and matches[0] != op.target_id:
            mismatches.append(
                {
                    "reason": "pending_target_mismatch",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "expected": op.target_id,
                    "observed": matches[0],
                    "op_type": op.op_type,
                }
            )
            return
    else:
        # Unmarked manual mutation: immutable target ID/project is the proof surface.
        if op.target_id is None:
            mismatches.append(
                {
                    "reason": "pending_target_missing",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "op_type": op.op_type,
                }
            )
            return
        observed = observed_tasks.get(op.target_id)
        if observed is None:
            # Absent target → retain unknown; never create.
            if op.state is not OperationState.UNKNOWN:
                try:
                    journal.mark_unknown(
                        op.issuer_id,
                        op.operation_id,
                        reason="restore_reconcile_mutation_absent_target",
                        details={
                            "target_id": op.target_id,
                            "forbid_create": True,
                            "op_type": op.op_type,
                        },
                    )
                except JournalError as exc:
                    mismatches.append(
                        {
                            "reason": "mark_unknown_failed",
                            "issuer_id": op.issuer_id,
                            "operation_id": op.operation_id,
                            "error": str(exc),
                        }
                    )
            return
        if not _observed_task_identity_ok(op.target_id, observed):
            mismatches.append(
                {
                    "reason": "observed_task_id_mismatch",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "expected": op.target_id,
                    "observed": observed.get("id") if isinstance(observed, Mapping) else None,
                    "op_type": op.op_type,
                }
            )
            return
        observed_project = observed.get("project_id") if isinstance(observed, Mapping) else None
        if not _projects_match_strict(op.project_id, observed_project):
            mismatches.append(
                {
                    "reason": "pending_project_mismatch",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "expected": op.project_id,
                    "observed": observed_project,
                    "op_type": op.op_type,
                }
            )
            return

    if op.state is not OperationState.UNKNOWN:
        try:
            journal.mark_unknown(
                op.issuer_id,
                op.operation_id,
                reason="restore_reconcile_mutation_without_causal_proof",
                details={
                    "marker": marker,
                    "target_id": op.target_id,
                    "op_type": op.op_type,
                    "causal_proof": False,
                    "resend_forbidden": True,
                },
            )
        except JournalError as exc:
            mismatches.append(
                {
                    "reason": "mark_unknown_failed",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "error": str(exc),
                }
            )


def _recover_create_pending(
    journal: IntegrationJournal | RestoreRecoverySection,
    op: OperationRecord,
    *,
    observed_tasks: Mapping[int, Mapping[str, Any]],
    mismatches: list[dict[str, Any]],
) -> None:
    """Recover create intents only via exact-one marker correlation (never a new create)."""
    marker = op.marker
    if not isinstance(marker, str) or not marker:
        mismatches.append(
            {
                "reason": "pending_marker_missing",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "state": op.state.value,
                "op_type": op.op_type,
            }
        )
        return
    if MARKER_RE.fullmatch(marker) is None:
        mismatches.append(
            {
                "reason": "pending_marker_invalid",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "marker": marker,
                "op_type": op.op_type,
            }
        )
        return
    matches = _exact_marker_task_ids(observed_tasks, marker)
    if len(matches) > 1:
        _mark_conflict_mismatch(
            journal,
            op,
            reason="restore_multiple_exact_marker_matches",
            mismatches=mismatches,
            details={
                "mismatch_reason": "multiple_exact_matches",
                "marker": marker,
                "matches": matches,
                "op_type": op.op_type,
            },
        )
        return
    if len(matches) == 0:
        if op.state is not OperationState.UNKNOWN:
            try:
                journal.mark_unknown(
                    op.issuer_id,
                    op.operation_id,
                    reason="restore_reconcile_absent_task",
                    details={"marker": marker, "forbid_create": True},
                )
            except JournalError as exc:
                mismatches.append(
                    {
                        "reason": "mark_unknown_failed",
                        "issuer_id": op.issuer_id,
                        "operation_id": op.operation_id,
                        "error": str(exc),
                    }
                )
        return

    task_id = matches[0]
    observed = observed_tasks[task_id]
    if not _observed_task_identity_ok(task_id, observed):
        mismatches.append(
            {
                "reason": "observed_task_id_mismatch",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "expected": task_id,
                "observed": observed.get("id") if isinstance(observed, Mapping) else None,
                "op_type": op.op_type,
            }
        )
        return
    observed_project = observed.get("project_id") if isinstance(observed, Mapping) else None
    if not _projects_match_strict(op.project_id, observed_project):
        mismatches.append(
            {
                "reason": "pending_project_mismatch",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "expected": op.project_id,
                "observed": observed_project,
            }
        )
        return
    # record_receipt accepts only dispatch_started/unknown — never prepared.
    if op.state is OperationState.PREPARED:
        try:
            journal.mark_unknown(
                op.issuer_id,
                op.operation_id,
                reason="restore_reconcile_recover_existing",
                details={"marker": marker, "vikunja_id": task_id},
            )
        except JournalError as exc:
            mismatches.append(
                {
                    "reason": "pending_recover_mark_unknown_failed",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "error": str(exc),
                }
            )
            return
    try:
        journal.record_receipt(
            op.issuer_id,
            op.operation_id,
            vikunja_id=task_id,
            project_id=op.project_id,
            marker=marker,
            effect={
                "id": task_id,
                "title": observed.get("title") if isinstance(observed, Mapping) else None,
                "project_id": observed_project if isinstance(observed_project, int) else op.project_id,
                "source": RESTORE_RECEIPT_SOURCE,
            },
        )
    except JournalError as exc:
        mismatches.append(
            {
                "reason": "pending_receipt_recover_failed",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "vikunja_id": task_id,
                "error": str(exc),
                "outcome": getattr(exc, "outcome", None).value
                if getattr(exc, "outcome", None) is not None
                else None,
            }
        )


def _recover_or_retain_pending(
    journal: IntegrationJournal | RestoreRecoverySection,
    op: OperationRecord,
    *,
    observed_tasks: Mapping[int, Mapping[str, Any]],
    mismatches: list[dict[str, Any]],
) -> None:
    """Quarantine/reconcile pending intents under the restore recovery fence.

    Creates: exact-one marker → recover receipt (never a new create); zero →
    retain unknown; multiple → conflict. Update/complete: preserve operation-
    specific uncertainty without causal proof (immutable preimage/target).
    """
    if op.state is OperationState.CONFLICT:
        mismatches.append(
            {
                "reason": "durable_conflict",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
            }
        )
        return
    if op.state is OperationState.CONFIRMED and op.receipt is not None:
        return
    if op.op_type in {"update", "complete"}:
        _retain_unknown_mutation(
            journal,
            op,
            observed_tasks=observed_tasks,
            mismatches=mismatches,
        )
        return
    if op.op_type == "create":
        _recover_create_pending(
            journal,
            op,
            observed_tasks=observed_tasks,
            mismatches=mismatches,
        )
        return
    mismatches.append(
        {
            "reason": "pending_unsupported_op_type",
            "issuer_id": op.issuer_id,
            "operation_id": op.operation_id,
            "op_type": op.op_type,
            "state": op.state.value,
        }
    )


def _validate_durable_receipt(
    receipt: Mapping[str, Any],
    *,
    observed_tasks: Mapping[int, Mapping[str, Any]],
    mismatches: list[dict[str, Any]],
) -> None:
    """Validate one durable receipt against observed Vikunja facts.

    Create receipts require a nonempty exact EA010 marker. Update/complete may
    carry an empty marker for unmarked manual tasks and are proven by immutable
    target ID/project without inventing a create marker.
    """
    vikunja_id = receipt.get("vikunja_id")
    marker = receipt.get("marker")
    op_type = receipt.get("op_type")
    if not isinstance(vikunja_id, int) or isinstance(vikunja_id, bool):
        mismatches.append({"reason": "receipt_missing_vikunja_id", "receipt": dict(receipt)})
        return
    if not isinstance(marker, str):
        mismatches.append({"reason": "receipt_marker_missing", "receipt": dict(receipt)})
        return

    if marker == "":
        if op_type not in {"update", "complete"}:
            mismatches.append(
                {
                    "reason": "receipt_marker_missing",
                    "receipt": dict(receipt),
                    "op_type": op_type,
                }
            )
            return
        observed = observed_tasks.get(vikunja_id)
        if observed is None:
            mismatches.append(
                {
                    "reason": "vikunja_id_missing",
                    "vikunja_id": vikunja_id,
                    "marker": "",
                    "op_type": op_type,
                }
            )
            return
        if not _observed_task_identity_ok(vikunja_id, observed):
            mismatches.append(
                {
                    "reason": "observed_task_id_mismatch",
                    "vikunja_id": vikunja_id,
                    "observed": observed.get("id") if isinstance(observed, Mapping) else None,
                    "op_type": op_type,
                }
            )
            return
        observed_project = observed.get("project_id") if isinstance(observed, Mapping) else None
        if not _projects_match_strict(receipt.get("project_id"), observed_project):
            mismatches.append(
                {
                    "reason": "project_mismatch",
                    "vikunja_id": vikunja_id,
                    "expected": receipt.get("project_id"),
                    "observed": observed_project,
                    "op_type": op_type,
                }
            )
            return
        target_id = receipt.get("target_id")
        if (
            isinstance(target_id, int)
            and not isinstance(target_id, bool)
            and target_id != vikunja_id
        ):
            mismatches.append(
                {
                    "reason": "receipt_target_mismatch",
                    "expected": target_id,
                    "observed": vikunja_id,
                    "op_type": op_type,
                }
            )
        return

    if not marker or MARKER_RE.fullmatch(marker) is None:
        mismatches.append({"reason": "receipt_marker_missing", "receipt": dict(receipt)})
        return
    matches = _exact_marker_task_ids(observed_tasks, marker)
    if len(matches) == 0:
        mismatches.append(
            {
                "reason": "vikunja_id_missing",
                "vikunja_id": vikunja_id,
                "marker": marker,
            }
        )
        return
    if len(matches) > 1:
        mismatches.append(
            {
                "reason": "multiple_exact_matches",
                "vikunja_id": vikunja_id,
                "marker": marker,
                "matches": matches,
            }
        )
        return
    if matches[0] != vikunja_id:
        mismatches.append(
            {
                "reason": "receipt_target_mismatch",
                "expected": vikunja_id,
                "observed": matches[0],
                "marker": marker,
            }
        )
        return
    observed = observed_tasks.get(vikunja_id)
    if not _observed_task_identity_ok(vikunja_id, observed):
        mismatches.append(
            {
                "reason": "observed_task_id_mismatch",
                "vikunja_id": vikunja_id,
                "observed": observed.get("id") if isinstance(observed, Mapping) else None,
                "marker": marker,
            }
        )
        return
    observed_project = observed.get("project_id") if isinstance(observed, Mapping) else None
    if not _projects_match_strict(receipt.get("project_id"), observed_project):
        mismatches.append(
            {
                "reason": "project_mismatch",
                "vikunja_id": vikunja_id,
                "expected": receipt.get("project_id"),
                "observed": observed_project,
            }
        )


def _classify_pending_readonly(
    op: OperationRecord,
    *,
    observed_tasks: Mapping[int, Mapping[str, Any]],
    mismatches: list[dict[str, Any]],
) -> None:
    """Read-only preflight for pending intents before recovery mutations."""
    if op.state is OperationState.CONFLICT:
        mismatches.append(
            {
                "reason": "durable_conflict",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
            }
        )
        return
    marker = op.marker
    if not isinstance(marker, str):
        mismatches.append(
            {
                "reason": "pending_marker_missing",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "state": op.state.value,
                "op_type": op.op_type,
            }
        )
        return
    if marker == "":
        if op.op_type not in {"update", "complete"}:
            mismatches.append(
                {
                    "reason": "pending_marker_missing",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "state": op.state.value,
                    "op_type": op.op_type,
                }
            )
            return
        if op.target_id is None:
            mismatches.append(
                {
                    "reason": "pending_target_missing",
                    "issuer_id": op.issuer_id,
                    "operation_id": op.operation_id,
                    "op_type": op.op_type,
                }
            )
        return
    if MARKER_RE.fullmatch(marker) is None:
        mismatches.append(
            {
                "reason": "pending_marker_invalid",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "marker": marker,
                "op_type": op.op_type,
            }
        )
        return
    matches = _exact_marker_task_ids(observed_tasks, marker)
    if len(matches) > 1:
        mismatches.append(
            {
                "reason": "multiple_exact_matches",
                "issuer_id": op.issuer_id,
                "operation_id": op.operation_id,
                "marker": marker,
                "matches": matches,
                "op_type": op.op_type,
            }
        )


def reconcile_restored_contour(
    *,
    journal_path: Path | str,
    links: Mapping[str, Any],
    observed_tasks: Mapping[int, Mapping[str, Any]],
    observed_attachment_checksums: Mapping[str, str],
    expected_attachment_checksums: Mapping[str, str],
    allow_writes_on_success: bool = True,
) -> dict[str, Any]:
    """Clear restore drift only after Vikunja/receipt/mapping/attachment facts match.

    One canonical OS-lock covers the entire reconcile generation: fence, durable
    inventory load, validation, pending recovery, and publish_writes. Publication
    is bound to the generation claimed at section entry so a stale success cannot
    erase a newer failure (FR-EA010-24 / AC-EA010-14 / NFR-15). Every supplied
    receipt/mapping must still match, while durable receipts committed by this
    restore's exact-match recovery may extend the copy set so restart/repeat
    with the original links is not treated as backup drift. A copy with zero
    receipts is allowed when the durable inventory is empty/pending-only (or
    already holds only restore-attributed receipts); omitting receipts from a
    nonempty pre-restore confirmed inventory still fails closed. Create receipts
    use exact-marker grammar; unmarked update/complete use immutable target
    ID/project. The complete observed inventory is identity-fenced first:
    invalid/missing/nonpositive/bool IDs and key/id conflicts keep writes closed
    even when a valid confirmed task is also present. Exact EA010 markers
    observed in Vikunja whose issuer/operation pair is absent from the restored
    journal keep writes fenced (old/inconsistent copy). Old prepared intents are
    never rearmed solely because another receipt matched.
    """
    path = _require_abs(journal_path, label="journal_path")
    mismatches: list[dict[str, Any]] = []
    durable_receipts: list[dict[str, Any]] = []
    durable_mappings: list[dict[str, Any]] = []

    journal = IntegrationJournal(path)
    journal.open()
    try:
        # Entire generation stays under one OS-lock: fence+claim through publish.
        with journal.restore_recovery_section() as section:
            _maybe_crash("reconcile_section_enter")

            # Decode/shape-check links only after the fence: list(non-iterable) must
            # not escape before restore_drift closes writes (FR-EA010-24 / NFR-15).
            try:
                supplied_receipts, supplied_mappings = _decode_supplied_link_lists(links)
            except RestoreError as exc:
                if exc.code != "reconcile_malformed_links":
                    raise
                _fail_reconcile(
                    journal,
                    path,
                    code=exc.code,
                    message=(
                        "malformed restore links; writes remain closed until reconcile"
                    ),
                    mismatches=[{"reason": "malformed_links", **exc.details}],
                )

            durable = _load_durable_inventory(journal)
            durable_receipts = durable["receipts"]
            durable_mappings = durable["mappings"]
            _maybe_crash("after_load_durable_inventory")

            # Validate the complete observed inventory after the writes fence and
            # before exact-marker correlation or unaccounted-marker exclusion.
            mismatches.extend(_observed_inventory_identity_mismatches(observed_tasks))

            if not supplied_receipts:
                # Distinguish omitted/tampered links (non-recovery confirmed inventory
                # present) from a legitimately receipt-empty / pending-only copy.
                pre_restore_confirmed = [
                    r for r in durable_receipts if not _receipt_is_restore_recovered(r)
                ]
                if pre_restore_confirmed:
                    _fail_reconcile(
                        journal,
                        path,
                        code="reconcile_empty",
                        message="no receipts to reconcile",
                        mismatches=[
                            {
                                "reason": "receipts_omitted_from_nonempty_inventory",
                                "durable_non_recovery_count": len(pre_restore_confirmed),
                            }
                        ],
                    )

            receipt_drift = _receipt_inventory_drift(
                supplied_receipts=supplied_receipts,
                durable_receipts=durable_receipts,
            )
            if receipt_drift is not None:
                mismatches.append(receipt_drift)

            supplied_mapping_set = {_mapping_fingerprint(m) for m in supplied_mappings}
            durable_mapping_set = {_mapping_fingerprint(m) for m in durable_mappings}
            if supplied_mapping_set != durable_mapping_set:
                mismatches.append(
                    {
                        "reason": "mapping_inventory_mismatch",
                        "supplied": sorted(supplied_mapping_set),
                        "durable": sorted(durable_mapping_set),
                    }
                )

            # Authoritative receipt validation: create via exact marker; unmarked
            # update/complete via immutable target ID/project (empty marker allowed).
            for receipt in durable_receipts:
                _validate_durable_receipt(
                    receipt,
                    observed_tasks=observed_tasks,
                    mismatches=mismatches,
                )

            for mapping in durable_mappings:
                found = journal.lookup_source(
                    mapping["source_kind"], mapping["source_key"]
                )
                if found is None:
                    mismatches.append({"reason": "mapping_missing", "mapping": mapping})
                    continue
                if (
                    found.operation_id != mapping["operation_id"]
                    or found.issuer_id != mapping["issuer_id"]
                ):
                    mismatches.append({"reason": "mapping_mismatch", "mapping": mapping})

            if dict(observed_attachment_checksums) != dict(expected_attachment_checksums):
                mismatches.append(
                    {
                        "reason": "attachment_checksum_drift",
                        "expected": dict(expected_attachment_checksums),
                        "observed": dict(observed_attachment_checksums),
                    }
                )

            # Observed exact markers whose operation is entirely absent from the
            # restored journal (old backup vs newer Vikunja) must keep writes off.
            mismatches.extend(_unaccounted_observed_markers(journal, observed_tasks))

            # Read-only classification of pending ops while the fence is still held.
            pending_ops = [
                op
                for op in section.list_operations()
                if not (op.state is OperationState.CONFIRMED and op.receipt is not None)
            ]
            for op in pending_ops:
                _classify_pending_readonly(
                    op,
                    observed_tasks=observed_tasks,
                    mismatches=mismatches,
                )

            if mismatches:
                _fail_reconcile(
                    journal,
                    path,
                    code="reconcile_failed",
                    message="restore reconcile failed; writes remain closed",
                    mismatches=mismatches,
                )

            # Keep dispatch closed throughout recovery; publish writes only after
            # every pending op is retained/recovered under the same OS-locked generation.
            try:
                _maybe_crash("recovery_section_enter")
                for op in section.list_operations():
                    _recover_or_retain_pending(
                        section,
                        op,
                        observed_tasks=observed_tasks,
                        mismatches=mismatches,
                    )
                    _maybe_crash("after_pending_recovery_item")
                for op in section.list_operations():
                    if op.allows_first_http_dispatch():
                        mismatches.append(
                            {
                                "reason": "prepared_intent_still_armed",
                                "issuer_id": op.issuer_id,
                                "operation_id": op.operation_id,
                                "marker": op.marker,
                            }
                        )
                if mismatches:
                    _fail_reconcile(
                        journal,
                        path,
                        code="reconcile_failed",
                        message="restore reconcile failed; writes remain closed",
                        mismatches=mismatches,
                    )
                _maybe_crash("before_publish_writes")
                if allow_writes_on_success:
                    section.publish_writes()
                _maybe_crash("after_publish_writes")
            except RestoreError:
                raise
            except JournalError as exc:
                try:
                    journal.set_restore_drift(True)
                except Exception:
                    pass
                flags = _durable_write_flags(path)
                raise RestoreError(
                    f"restore reconcile aborted; writes remain closed: {exc}",
                    code="reconcile_failed",
                    details={
                        "creates_issued": False,
                        "writes_enabled": flags["writes_enabled"],
                        "restore_drift": flags["restore_drift"],
                        "error": str(exc),
                        **(exc.details or {}),
                    },
                ) from exc
            except Exception:
                try:
                    journal.set_restore_drift(True)
                except Exception:
                    pass
                raise

            flags = _durable_write_flags(path)
            writes_enabled = flags["writes_enabled"] if allow_writes_on_success else False
            restore_drift = flags["restore_drift"] if allow_writes_on_success else True
    except RestoreError:
        raise
    except Exception as exc:
        try:
            journal.set_restore_drift(True)
        except Exception:
            pass
        flags = _durable_write_flags(path)
        raise RestoreError(
            f"restore reconcile aborted; writes remain closed: {exc}",
            code="reconcile_failed",
            details={
                "creates_issued": False,
                "writes_enabled": flags["writes_enabled"],
                "restore_drift": flags["restore_drift"],
                "error": str(exc),
            },
        ) from exc
    finally:
        journal.close()

    return {
        "ok": True,
        "writes_enabled": writes_enabled,
        "restore_drift": restore_drift,
        "matched_receipts": len(durable_receipts),
        "matched_mappings": len(durable_mappings),
        "attachment_files": len(expected_attachment_checksums),
        "creates_issued": False,
    }


def refuse_partial_or_corrupt_restore(copy_dir: Path | str, target: Path | str) -> dict[str, Any]:
    """Helper used by tests/CLI: verify failure modes without mutating a target."""
    try:
        verify_consistent_copy(copy_dir)
    except RestoreError as exc:
        return {
            "ok": False,
            "refused": True,
            "code": exc.code,
            "details": exc.details,
            "writes_enabled": False,
            "target_untouched": not Path(target).exists() or not any(Path(target).iterdir())
            if Path(target).exists()
            else True,
        }
    raise RestoreError("expected partial/corrupt copy to fail verification", code="expected_failure")
