"""Real isolated Vikunja stand: profile, manifest, lifecycle, cleanup."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import stat as stat_mod
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

DEFAULT_PROFILE_PATH = Path("/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json")
PROFILE_ENV = "EA010_ISOLATED_STAND_PROFILE"
SCHEMA = "ea010-isolated-stand-profile/v1"
MANIFEST_SCHEMA = "ea010-stand-resource-manifest/v1"
JOURNAL_SCHEMA = "ea010-stand-run-journal/v1"
OWNERSHIP_SCHEMA = "ea010-stand-ownership-claim/v1"
LIVE_LOCK_NAME = ".ea010-live.lock"
OWNERSHIP_DIR_NAME = ".ea010-ownership"
LAUNCH_RECEIPT_NAME = "launch-receipt.json"
RUN_ID_RE = re.compile(r"^[0-9a-f]{16,64}$")
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})
SAFE_DIR_MODE = 0o700
SAFE_FILE_MODE = 0o600


def ownership_dir(profile: "StandProfile") -> Path:
    return profile.run_root / OWNERSHIP_DIR_NAME


def ownership_claim_path(profile: "StandProfile", run_id: str) -> Path:
    return ownership_dir(profile) / f"{validate_run_id(run_id)}.json"


def _ensure_ownership_dir(profile: "StandProfile") -> Path:
    path = ownership_dir(profile)
    path.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
    os.chmod(path, SAFE_DIR_MODE)
    return path


def write_ownership_claim(profile: "StandProfile", claim: dict[str, Any]) -> Path:
    """Persist a durable ownership/cleanup claim outside the run tree being deleted."""
    run_id = validate_run_id(str(claim.get("run_id") or ""))
    bound = assert_immediate_run_child(profile.run_root, run_id, Path(claim["run_dir"]))
    payload = {
        "schema": OWNERSHIP_SCHEMA,
        **claim,
        "run_id": run_id,
        "run_dir": str(bound),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _ensure_ownership_dir(profile)
    path = ownership_claim_path(profile, run_id)
    atomic_write_secure(
        path,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        profile.sensitive_file_mode,
    )
    return path


def load_ownership_claim(profile: "StandProfile", run_id: str) -> dict[str, Any] | None:
    path = ownership_claim_path(profile, run_id)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StandError(f"Ownership claim unreadable: {path}: {exc}") from exc
    if data.get("schema") != OWNERSHIP_SCHEMA:
        raise StandError(f"Unexpected ownership claim schema at {path}")
    return data


def clear_ownership_claim(profile: "StandProfile", run_id: str) -> None:
    path = ownership_claim_path(profile, run_id)
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise StandError(f"Failed to clear ownership claim {path}: {exc}") from exc


def ownership_claim_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": OWNERSHIP_SCHEMA,
        "run_id": manifest.get("run_id"),
        "run_dir": manifest.get("run_dir"),
        "status": manifest.get("status"),
        "process": manifest.get("process"),
        "launch_receipt": manifest.get("launch_receipt"),
        "owned_roots": list(manifest.get("owned_roots") or []),
        "owned_children": list(manifest.get("owned_children") or []),
        "ports": manifest.get("ports"),
        "listen_host": manifest.get("listen_host"),
        "cleanup_failure": manifest.get("cleanup_failure"),
        "source": manifest.get("source"),
        "binary": manifest.get("binary"),
        "created_at": manifest.get("created_at"),
    }


def assert_owned_root_under_run_root(run_root: Path, owned: Path) -> Path:
    """Require an owned restore/copy root to stay inside the approved run_root.

    Cleanup must bind to the registered directory identity (NFR-18). Leaf or
    ancestor symlink substitution is refused even when ``Path.resolve()`` would
    still land on a sibling under ``run_root`` — otherwise recursive delete can
    erase a foreign tree while leaving the renamed original owned directory.
    """
    root = run_root.resolve()
    claimed = Path(owned)
    if not claimed.is_absolute():
        raise StandError(f"owned root must be absolute, got {owned}")
    # abspath collapses '.'/'..' without following symlinks (unlike resolve()).
    claimed_abs = Path(os.path.abspath(str(claimed)))
    if claimed_abs == root:
        raise StandError("owned root must not be the approved run_root itself")
    try:
        claimed_abs.relative_to(root)
    except ValueError as exc:
        raise StandError(
            f"owned root escaped approved run_root: {claimed_abs} not under {root}"
        ) from exc
    # Refuse symlink substitution of the registered name or any ancestor under
    # run_root before trusting resolve() for normalization.
    probe = claimed_abs
    while True:
        if probe.is_symlink():
            raise StandError(
                f"owned root identity changed (symlink substitution): {claimed_abs}"
            )
        if probe == root:
            break
        parent = probe.parent
        if parent == probe:
            break
        probe = parent
    resolved = claimed_abs.resolve()
    if resolved == root:
        raise StandError("owned root must not be the approved run_root itself")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise StandError(
            f"owned root escaped approved run_root: {resolved} not under {root}"
        ) from exc
    # After refusing symlinks, resolve() only normalizes; identity stays claimed.
    if resolved != claimed_abs:
        raise StandError(
            f"owned root identity changed (resolved away from claim): "
            f"{claimed_abs} -> {resolved}"
        )
    return claimed_abs


def owned_root_path(raw: Any) -> Path:
    """Extract the registered path from a durable owned-root record or legacy string."""
    if isinstance(raw, Mapping):
        path_value = raw.get("path")
        if not path_value:
            raise StandError("owned root record missing path")
        return Path(str(path_value))
    return Path(raw)


def capture_owned_root_identity(run_root: Path, owned: Path) -> dict[str, Any]:
    """Persist path + durable directory/parent identity for later delete binding."""
    bound = assert_owned_root_under_run_root(run_root, owned)
    parent = bound.parent
    try:
        parent_fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise StandError(
            f"owned root parent identity unavailable: {parent}: {exc}"
        ) from exc
    try:
        parent_st = os.fstat(parent_fd)
        try:
            dir_fd = os.open(
                bound.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise StandError(
                f"owned root identity unavailable: {bound}: {exc}"
            ) from exc
        try:
            st = os.fstat(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        os.close(parent_fd)
    return {
        "path": str(bound),
        "st_dev": int(st.st_dev),
        "st_ino": int(st.st_ino),
        "parent_st_dev": int(parent_st.st_dev),
        "parent_st_ino": int(parent_st.st_ino),
    }


def assert_owned_root_record_identity(run_root: Path, raw: Any) -> Path:
    """Require a durable owned-root record still names the claimed directory inode.

    Path-string membership alone is insufficient: after claim -> rename aside ->
    mkdir replacement at the same path, writes/launch must refuse the foreign
    directory (NFR-18). Validates parent + directory st_dev/st_ino via O_NOFOLLOW.
    """
    if not isinstance(raw, Mapping):
        raise StandError(
            "owned root missing durable identity; refuse path-only launch"
        )
    try:
        expected_dev = int(raw["st_dev"])
        expected_ino = int(raw["st_ino"])
        expected_parent_dev = int(raw["parent_st_dev"])
        expected_parent_ino = int(raw["parent_st_ino"])
    except (KeyError, TypeError, ValueError) as exc:
        raise StandError(
            "owned root durable identity incomplete; refuse launch"
        ) from exc
    bound = assert_owned_root_under_run_root(run_root, owned_root_path(raw))
    parent = bound.parent
    try:
        parent_fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise StandError(
            f"owned root parent identity changed: {parent}: {exc}"
        ) from exc
    try:
        parent_st = os.fstat(parent_fd)
        if (int(parent_st.st_dev), int(parent_st.st_ino)) != (
            expected_parent_dev,
            expected_parent_ino,
        ):
            raise StandError(
                f"owned root parent identity mismatch: {parent}"
            )
        try:
            dir_fd = os.open(
                bound.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise StandError(
                f"owned root identity changed: {bound}: {exc}"
            ) from exc
        try:
            st = os.fstat(dir_fd)
            if (int(st.st_dev), int(st.st_ino)) != (expected_dev, expected_ino):
                raise StandError(
                    f"owned root identity mismatch (directory substitution): {bound}"
                )
        finally:
            os.close(dir_fd)
    finally:
        os.close(parent_fd)
    return bound


def _rmtree_via_dir_fd(dir_fd: int) -> None:
    """Delete directory contents through a bound descriptor (no path re-walk)."""
    for name in os.listdir(dir_fd):
        try:
            st = os.lstat(name, dir_fd=dir_fd)
        except OSError as exc:
            raise StandError(f"owned root entry unreadable during delete: {name}: {exc}") from exc
        if stat_mod.S_ISLNK(st.st_mode) or not stat_mod.S_ISDIR(st.st_mode):
            try:
                os.unlink(name, dir_fd=dir_fd)
            except OSError as exc:
                raise StandError(
                    f"owned root unlink failed during delete: {name}: {exc}"
                ) from exc
            continue
        child_fd = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=dir_fd,
        )
        try:
            _rmtree_via_dir_fd(child_fd)
        finally:
            os.close(child_fd)
        try:
            os.rmdir(name, dir_fd=dir_fd)
        except OSError as exc:
            raise StandError(
                f"owned root rmdir failed during delete: {name}: {exc}"
            ) from exc


# Test-only: runs after identity bind, before descriptor delete (delete-time race).
_test_owned_root_pre_delete_hook: Callable[[Path], None] | None = None


def delete_owned_root_record(run_root: Path, raw: Any) -> Path:
    """Delete one claimed owned root only when durable identity still matches.

    Opens parent + directory with O_NOFOLLOW, verifies st_dev/st_ino (and parent
    identity), then removes through the bound descriptors. Path-only regular
    directory substitution and delete-time races therefore refuse foreign trees
    (NFR-18).
    """
    if not isinstance(raw, Mapping):
        raise StandError(
            "owned root missing durable identity; refuse path-only deletion"
        )
    try:
        expected_dev = int(raw["st_dev"])
        expected_ino = int(raw["st_ino"])
        expected_parent_dev = int(raw["parent_st_dev"])
        expected_parent_ino = int(raw["parent_st_ino"])
    except (KeyError, TypeError, ValueError) as exc:
        raise StandError(
            "owned root durable identity incomplete; refuse deletion"
        ) from exc
    bound = assert_owned_root_under_run_root(run_root, owned_root_path(raw))
    parent = bound.parent
    try:
        parent_fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        raise StandError(
            f"owned root parent identity changed: {parent}: {exc}"
        ) from exc
    try:
        parent_st = os.fstat(parent_fd)
        if (int(parent_st.st_dev), int(parent_st.st_ino)) != (
            expected_parent_dev,
            expected_parent_ino,
        ):
            raise StandError(
                f"owned root parent identity mismatch: {parent}"
            )
        try:
            dir_fd = os.open(
                bound.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return bound
        except OSError as exc:
            raise StandError(
                f"owned root identity changed: {bound}: {exc}"
            ) from exc
        try:
            st = os.fstat(dir_fd)
            if (int(st.st_dev), int(st.st_ino)) != (expected_dev, expected_ino):
                raise StandError(
                    f"owned root identity mismatch (directory substitution): {bound}"
                )
            hook = _test_owned_root_pre_delete_hook
            if hook is not None:
                hook(bound)
            # Re-check after the test/race window before mutating.
            st_after = os.fstat(dir_fd)
            if (int(st_after.st_dev), int(st_after.st_ino)) != (
                expected_dev,
                expected_ino,
            ):
                raise StandError(
                    f"owned root identity mismatch after bind: {bound}"
                )
            _rmtree_via_dir_fd(dir_fd)
        finally:
            os.close(dir_fd)
        # Remove the directory name via the bound parent descriptor.
        try:
            # Confirm the name still refers to the claimed inode before rmdir.
            verify_fd = os.open(
                bound.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return bound
        except OSError as exc:
            raise StandError(
                f"owned root identity changed before rmdir: {bound}: {exc}"
            ) from exc
        try:
            verify_st = os.fstat(verify_fd)
            if (int(verify_st.st_dev), int(verify_st.st_ino)) != (
                expected_dev,
                expected_ino,
            ):
                raise StandError(
                    f"owned root identity mismatch before rmdir: {bound}"
                )
        finally:
            os.close(verify_fd)
        try:
            os.rmdir(bound.name, dir_fd=parent_fd)
        except OSError as exc:
            raise StandError(f"owned root rmdir failed ({bound}): {exc}") from exc
    finally:
        os.close(parent_fd)
    return bound


class StandError(RuntimeError):
    """Fail-closed stand or cleanup error.

    Optional ``child_rc`` / ``child_evidence`` / ``signal_receipt_path`` carry
    structured reexec-child snapshots so aggregate ordinary failures can journal
    run/IDs/cleanup without relying on process-local memory alone.
    """

    def __init__(
        self,
        message: str,
        *,
        child_rc: int | None = None,
        child_evidence: dict[str, Any] | None = None,
        signal_receipt_path: str | Path | None = None,
    ) -> None:
        super().__init__(message)
        self.child_rc = child_rc
        self.child_evidence = dict(child_evidence) if isinstance(child_evidence, dict) else None
        self.signal_receipt_path = (
            str(signal_receipt_path) if signal_receipt_path is not None else None
        )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None


_OPENER = build_opener(ProxyHandler({}), _NoRedirect())


def _mode_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 8)


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


def write_secure(path: Path, data: bytes | str, mode: int = 0o600) -> None:
    payload = data if isinstance(data, bytes) else data.encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_EXCL
    fd = os.open(path, flags, mode)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    os.chmod(path, mode)


def atomic_write_secure(path: Path, data: bytes | str, mode: int = 0o600) -> None:
    payload = data if isinstance(data, bytes) else data.encode()
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}-{secrets.token_hex(4)}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        # Re-open to fsync the directory entry durability best-effort.
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        os.chmod(path, mode)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise StandError(f"Unsafe or absolute run_id rejected: {run_id!r}")
    if run_id in {".", ".."} or "/" in run_id or "\\" in run_id:
        raise StandError(f"Unsafe run_id rejected: {run_id!r}")
    return run_id


def is_path_inside(parent: Path, child: Path) -> bool:
    """True only when child is strictly inside parent (not equal)."""
    parent_r = parent.resolve()
    child_r = child.resolve()
    try:
        child_r.relative_to(parent_r)
    except ValueError:
        return False
    return child_r != parent_r


def assert_immediate_run_child(run_root: Path, run_id: str, run_dir: Path) -> Path:
    """Require run_dir == resolved(run_root / run_id); reject root/siblings/escapes."""
    validate_run_id(run_id)
    root = run_root.resolve()
    expected = (root / run_id).resolve()
    if expected.parent != root:
        raise StandError("run_dir escaped approved run_root parentage")
    claimed = Path(run_dir)
    if not claimed.is_absolute():
        raise StandError(f"run_dir must be absolute, got {run_dir}")
    claimed_resolved = claimed.resolve()
    if claimed_resolved == root:
        raise StandError("run_dir must not be the approved run_root itself")
    # Reject startswith false friends (approved vs approved-sibling) even before
    # comparing resolved paths, and reject any non-immediate child.
    try:
        relative = claimed_resolved.relative_to(root)
    except ValueError as exc:
        raise StandError(
            f"run_dir must be the immediate run_id child of approved root; "
            f"expected {expected}, got {claimed_resolved}"
        ) from exc
    if len(relative.parts) != 1 or relative.parts[0] != run_id:
        raise StandError(
            f"run_dir must be the immediate run_id child of approved root; "
            f"expected {expected}, got {claimed_resolved}"
        )
    if claimed_resolved != expected:
        raise StandError(
            f"run_dir must be the immediate run_id child of approved root; "
            f"expected {expected}, got {claimed_resolved}"
        )
    if claimed.name != run_id:
        raise StandError(f"run_dir basename must equal run_id {run_id}, got {claimed.name}")
    return expected


def bind_manifest_paths(
    profile: StandProfile,
    manifest: dict[str, Any],
    *,
    manifest_path: Path | None = None,
) -> Path:
    run_id = validate_run_id(str(manifest.get("run_id") or ""))
    if "run_dir" not in manifest:
        raise StandError("Manifest missing run_dir")
    run_dir = assert_immediate_run_child(profile.run_root, run_id, Path(manifest["run_dir"]))
    if manifest_path is not None:
        expected_manifest = run_dir / "resource-manifest.json"
        if manifest_path.resolve() != expected_manifest.resolve():
            raise StandError(
                f"Manifest path is not bound to run_id child: {manifest_path} != {expected_manifest}"
            )
    return run_dir


def _assert_not_git_worktree(path: Path) -> None:
    """Reject run roots inside any Git worktree. No mutations."""
    probe_paths = []
    current = path
    # Include path and existing parents so a not-yet-created leaf is still checked.
    for _ in range(64):
        probe_paths.append(current)
        if current.parent == current:
            break
        current = current.parent
    for probe in probe_paths:
        if not probe.exists():
            continue
        result = subprocess.run(
            ["git", "-C", str(probe), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip() == "true":
            raise StandError(f"run_root must not be inside a Git worktree: {path}")


def _validate_ports(ports: dict[str, Any]) -> dict[str, int]:
    required = ("vikunja", "fault_proxy", "synthetic_hermes")
    if any(name not in ports for name in required):
        raise StandError("Stand profile ports incomplete")
    parsed: dict[str, int] = {}
    for name in required:
        try:
            value = int(ports[name])
        except (TypeError, ValueError) as exc:
            raise StandError(f"Stand profile port {name} is not an integer") from exc
        if value <= 0 or value > 65535:
            raise StandError(f"Stand profile port {name} out of range: {value}")
        parsed[name] = value
    if len(set(parsed.values())) != len(parsed):
        raise StandError("Stand profile ports must be distinct")
    return parsed


def _validate_limits(limits: dict[str, Any]) -> dict[str, int]:
    required = ("wall_seconds", "disk_bytes", "owned_process_tree_rss_bytes")
    parsed: dict[str, int] = {}
    for key in required:
        if key not in limits:
            raise StandError(f"Stand profile limit missing: {key}")
        try:
            value = int(limits[key])
        except (TypeError, ValueError) as exc:
            raise StandError(f"Stand profile limit {key} is not an integer") from exc
        if value <= 0:
            raise StandError(f"Stand profile limit {key} must be positive: {value}")
        parsed[key] = value
    return parsed


def _validate_mode(value: Any, *, expected: int, label: str) -> int:
    mode = _mode_int(value)
    if mode != expected:
        raise StandError(f"{label} must be {oct(expected)}, got {oct(mode)}")
    if mode & 0o077:
        raise StandError(f"{label} must not grant group/other bits: {oct(mode)}")
    return mode


@dataclass
class StandProfile:
    raw: dict[str, Any]
    path: Path
    binary_path: Path
    binary_version: str
    binary_sha256: str
    run_root: Path
    listen_host: str
    ports: dict[str, int]
    limits: dict[str, int]
    fixture_prefix: str
    run_directory_mode: int
    sensitive_file_mode: int

    @property
    def vikunja_port(self) -> int:
        return int(self.ports["vikunja"])

    @property
    def all_ports(self) -> tuple[int, ...]:
        return tuple(int(self.ports[name]) for name in ("vikunja", "fault_proxy", "synthetic_hermes"))


def resolve_profile_path(explicit: str | Path | None = None) -> Path:
    if explicit is not None:
        return Path(explicit).expanduser()
    env = os.environ.get(PROFILE_ENV)
    if env:
        return Path(env).expanduser()
    return DEFAULT_PROFILE_PATH


def load_profile(explicit: str | Path | None = None) -> StandProfile:
    """Load and validate profile. Performs no cleanup/chmod/create side effects."""
    path = resolve_profile_path(explicit)
    if not path.is_file():
        raise StandError(f"Stand profile missing or unreadable: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StandError(f"Stand profile unreadable: {path}: {exc}") from exc
    if raw.get("schema") != SCHEMA:
        raise StandError(f"Unexpected stand profile schema: {raw.get('schema')!r}")
    if raw.get("mode") != "isolated":
        raise StandError(f"Stand profile mode must be isolated, got {raw.get('mode')!r}")
    binary = raw.get("binary") or {}
    for key in ("path", "version", "sha256"):
        if key not in binary:
            raise StandError(f"Stand profile binary.{key} missing")
    binary_version = str(binary["version"])
    if not binary_version.startswith("v") or "2.8.0" not in binary_version:
        raise StandError(f"Stand profile binary.version not admitted: {binary_version!r}")
    binary_sha256 = str(binary["sha256"]).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", binary_sha256):
        raise StandError("Stand profile binary.sha256 must be 64 lowercase hex chars")
    if "run_root" not in raw:
        raise StandError("Stand profile run_root missing")
    run_root = Path(raw["run_root"]).expanduser()
    binary_path = Path(binary["path"]).expanduser()
    if not run_root.is_absolute() or run_root.resolve() != run_root:
        raise StandError("run_root must be a resolved absolute path without symlinks")
    if not binary_path.is_absolute() or binary_path.resolve() != binary_path:
        raise StandError("binary.path must be a resolved absolute path without symlinks")
    _assert_not_git_worktree(run_root)
    listen_host = str(raw.get("listen_host") or "")
    if listen_host not in LOOPBACK_HOSTS:
        raise StandError(f"listen_host must be loopback, got {listen_host!r}")
    ports = _validate_ports(raw.get("ports") or {})
    limits = _validate_limits(raw.get("limits") or {})
    run_directory_mode = _validate_mode(
        raw.get("run_directory_mode", "0700"),
        expected=SAFE_DIR_MODE,
        label="run_directory_mode",
    )
    sensitive_file_mode = _validate_mode(
        raw.get("sensitive_file_mode", "0600"),
        expected=SAFE_FILE_MODE,
        label="sensitive_file_mode",
    )
    fixture_prefix = str(raw.get("fixture_prefix") or "EA010-SYNTHETIC-")
    if not fixture_prefix.startswith("EA010-SYNTHETIC-"):
        raise StandError(f"fixture_prefix must start with EA010-SYNTHETIC-, got {fixture_prefix!r}")
    return StandProfile(
        raw=raw,
        path=path.resolve(),
        binary_path=binary_path,
        binary_version=binary_version,
        binary_sha256=binary_sha256,
        run_root=run_root,
        listen_host=listen_host,
        ports=ports,
        limits=limits,
        fixture_prefix=fixture_prefix,
        run_directory_mode=run_directory_mode,
        sensitive_file_mode=sensitive_file_mode,
    )


def verify_binary(profile: StandProfile) -> dict[str, str]:
    path = profile.binary_path
    if not path.is_file() or not os.access(path, os.X_OK):
        raise StandError(f"Pinned Vikunja binary missing or not executable: {path}")
    digest = sha256_file(path)
    if digest != profile.binary_sha256:
        raise StandError(
            f"Pinned binary SHA256 mismatch: expected {profile.binary_sha256}, got {digest}"
        )
    result = subprocess.run(
        [str(path), "version"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise StandError(f"Unable to read binary version: rc={result.returncode}")
    version_line = (result.stdout or result.stderr or "").strip().splitlines()[0]
    if profile.binary_version not in version_line:
        raise StandError(
            f"Binary version mismatch: expected {profile.binary_version!r}, got {version_line!r}"
        )
    return {"path": str(path), "sha256": digest, "version_line": version_line}


def listeners(port: int) -> set[int]:
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fp"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode not in (0, 1) or result.stderr.strip():
        raise StandError(f"Cannot verify listeners on port {port}")
    return {int(line[1:]) for line in result.stdout.splitlines() if line.startswith("p")}


def assert_ports_free(ports: tuple[int, ...], host: str = "127.0.0.1") -> None:
    for port in ports:
        owned = listeners(port)
        if owned:
            raise StandError(f"Port {port} already has listener PIDs {sorted(owned)}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
            except OSError as exc:
                raise StandError(f"Port {port} is busy and cannot be bound: {exc}") from exc


def process_start_identity(pid: int) -> dict[str, str]:
    result = subprocess.run(
        ["/bin/ps", "-p", str(pid), "-o", "pid=,lstart=,args="],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise StandError(f"Cannot read process identity for PID {pid}")
    line = result.stdout.strip()
    parts = line.split(None, 6)
    if len(parts) < 7:
        raise StandError(f"Unexpected ps identity format for PID {pid}: {line!r}")
    lstart = " ".join(parts[1:6])
    args = parts[6]
    return {"pid": str(pid), "lstart": lstart, "args": args}


def process_matches_identity(identity: dict[str, str]) -> bool:
    """Match owned process by PID + start time.

    Args are recorded for evidence but are not required for the match: the
    stand launches through a short Python exec wrapper, so argv changes once
    while the PID/start identity stays stable.
    """
    try:
        current = process_start_identity(int(identity["pid"]))
    except StandError:
        return False
    return current["pid"] == str(identity["pid"]) and current["lstart"] == identity.get("lstart")


def process_rss_bytes(pid: int) -> int:
    result = subprocess.run(
        ["/bin/ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise StandError(f"Cannot read RSS for PID {pid}")
    return int(result.stdout.strip()) * 1024


def process_tree_pids(root_pid: int) -> list[int]:
    seen: set[int] = set()
    ordered: list[int] = []
    queue = [root_pid]
    while queue:
        pid = queue.pop(0)
        if pid in seen:
            continue
        seen.add(pid)
        ordered.append(pid)
        result = subprocess.run(
            ["/usr/bin/pgrep", "-P", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode not in (0, 1):
            continue
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                queue.append(int(line))
    return ordered


def process_tree_rss_bytes(root_pid: int) -> int:
    total = 0
    for pid in process_tree_pids(root_pid):
        try:
            total += process_rss_bytes(pid)
        except StandError:
            continue
    return total


def directory_disk_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for item in path.rglob("*"):
        try:
            if item.is_file() and not item.is_symlink():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def http_json(
    host: str,
    port: int,
    path: str,
    method: str = "GET",
    body: Any | None = None,
    token: str | None = None,
    timeout: float = 5.0,
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    payload = None
    if body is not None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
    request = Request(
        f"http://{host}:{port}/api/v1/{path.lstrip('/')}",
        data=payload,
        headers=headers,
        method=method,
    )
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            raw = response.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise StandError("Unexpectedly large API response")
            if not raw:
                return response.status, None
            return response.status, json.loads(raw.decode())
    except HTTPError as exc:
        try:
            raw = exc.read(1024 * 1024) if hasattr(exc, "read") else b""
        finally:
            try:
                exc.close()
            except Exception:
                pass
        data = None
        if raw:
            try:
                data = json.loads(raw.decode())
            except json.JSONDecodeError:
                data = raw.decode(errors="replace")
        return exc.code, data


class LiveRunLock:
    """Exclusive flock for one live stand under an approved run_root."""

    def __init__(self, run_root: Path):
        self.run_root = run_root
        self.path = run_root / LIVE_LOCK_NAME
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        self.run_root.mkdir(mode=SAFE_DIR_MODE, exist_ok=True)
        os.chmod(self.run_root, SAFE_DIR_MODE)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, SAFE_FILE_MODE)
        try:
            os.chmod(self.path, SAFE_FILE_MODE)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise StandError(
                f"Refusing concurrent live stand under {self.run_root}: lock busy"
            ) from exc
        except Exception:
            os.close(fd)
            raise
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        self._fd = fd

    def release(self) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> "LiveRunLock":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


@dataclass
class TokenHandle:
    handle: str
    token_id: int | None = None
    kind: str = "api_token"


class LimitWatchdog:
    """Periodic ownership-limit enforcement across idle/callback time."""

    def __init__(self, stand: "IsolatedStand", interval: float = 0.25):
        self.stand = stand
        self.interval = interval
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="ea010-limit-watchdog", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise StandError(f"Resource limit breached: {self._error}") from self._error

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.stand._enforce_limits()
            except Exception as exc:
                # StandError (budget breach) and unexpected monitor faults both
                # must durably fail the run — a dead watchdog is not fail-closed.
                self._error = exc
                try:
                    self.stand.stop_process(sig=signal.SIGKILL, wait_timeout=3.0)
                except Exception:
                    pass
                return


# Aggregate/suite-all signal coordination: reexec component children and live
# stand evidence must be reachable from the process-wide SIGTERM/SIGHUP path.
# Reexec inners publish to process-local memory *and* a durable receipt path
# outside the disposable stand so the parent can ingest evidence before it
# redelivers the signal to itself.
STAND_SIGNAL_RECEIPT_ENV = "EA010_STAND_SIGNAL_RECEIPT"
STAND_SESSION_READY_ENV = "EA010_STAND_SESSION_READY"
# Test/harness hook: after fixtures are written (and optional ready marker),
# raise so reexec parents can exercise ordinary-failure evidence ingest.
SYNTHETIC_FAIL_AFTER_SESSION_ENV = "EA010_SYNTHETIC_FAIL_AFTER_SESSION"
_ACTIVE_COMPONENT_PROCS_LOCK = threading.RLock()
_ACTIVE_COMPONENT_PROCS: list[subprocess.Popen[Any]] = []
_ACTIVE_COMPONENT_RECEIPT_PATHS: dict[int, Path] = {}
_ACTIVE_STANDS_LOCK = threading.RLock()
_ACTIVE_STANDS: list["IsolatedStand"] = []
_STAND_SIGNAL_EVIDENCE: dict[str, Any] | None = None


def allocate_component_signal_receipt(profile: "StandProfile", *, suite: str) -> Path:
    """Durable path under run_root (outside disposable stand dirs) for child evidence."""
    safe_suite = re.sub(r"[^A-Za-z0-9._-]+", "_", suite) or "component"
    root = profile.run_root / "journals" / "component-signal"
    root.mkdir(mode=SAFE_DIR_MODE, parents=True, exist_ok=True)
    os.chmod(root, SAFE_DIR_MODE)
    return root / f"{safe_suite}-{os.getpid()}-{uuid.uuid4().hex[:16]}.json"


def register_active_component_proc(
    proc: subprocess.Popen[Any], *, signal_receipt_path: Path | None = None
) -> None:
    """Register a suite reexec/child process owned by the current aggregate."""
    with _ACTIVE_COMPONENT_PROCS_LOCK:
        if proc not in _ACTIVE_COMPONENT_PROCS:
            _ACTIVE_COMPONENT_PROCS.append(proc)
        if signal_receipt_path is not None:
            _ACTIVE_COMPONENT_RECEIPT_PATHS[id(proc)] = Path(signal_receipt_path)


def unregister_active_component_proc(proc: subprocess.Popen[Any]) -> None:
    with _ACTIVE_COMPONENT_PROCS_LOCK:
        try:
            _ACTIVE_COMPONENT_PROCS.remove(proc)
        except ValueError:
            pass
        _ACTIVE_COMPONENT_RECEIPT_PATHS.pop(id(proc), None)


# Signals the aggregate cancels owned component children for. Blocked around
# Popen+register so a handler cannot run in the acquisition window and miss
# the newly spawned child.
_COMPONENT_CANCEL_SIGNALS = (signal.SIGTERM, signal.SIGHUP)


def _unblock_component_cancel_signals_in_child() -> None:
    """Reset cancel-signal mask in the forked child before exec.

    The parent blocks SIGTERM/SIGHUP around Popen+register; without this
    reset the child would inherit the blocked mask and ignore later cancel.
    """
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, set(_COMPONENT_CANCEL_SIGNALS))


def spawn_registered_component_proc(
    popen_args: list[str],
    *,
    signal_receipt_path: Path | None = None,
    **popen_kwargs: Any,
) -> subprocess.Popen[Any]:
    """Popen a component child and register it atomically vs cancel signals.

    Blocks SIGTERM/SIGHUP for this thread across process creation and
    registration so an aggregate handler cannot observe an empty registry,
    redeliver, and orphan the newly spawned child. Pending cancel signals are
    delivered only after registration completes. The child unblocks those
    signals in ``preexec_fn`` so inherited mask cannot make it uncancellable.
    """
    blocked = set(_COMPONENT_CANCEL_SIGNALS)
    mask_supported = hasattr(signal, "pthread_sigmask")
    previous: set[int] | None = None
    if mask_supported:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
    proc: subprocess.Popen[Any] | None = None
    try:
        # Preserve a caller preexec_fn (if any) and always unblock cancel signals
        # in the child so it does not inherit the parent's temporary block mask.
        caller_preexec = popen_kwargs.pop("preexec_fn", None)

        def _preexec() -> None:
            _unblock_component_cancel_signals_in_child()
            if caller_preexec is not None:
                caller_preexec()

        proc = subprocess.Popen(popen_args, preexec_fn=_preexec, **popen_kwargs)
        register_active_component_proc(proc, signal_receipt_path=signal_receipt_path)
        return proc
    except Exception:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except OSError:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
            unregister_active_component_proc(proc)
        raise
    finally:
        if mask_supported and previous is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _read_component_signal_receipt(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        if not path.is_file():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 — signal path must not raise
        return {"error": f"{type(exc).__name__}: {exc}", "path": str(path)}
    if isinstance(payload, dict):
        return payload
    return {"error": "signal receipt is not an object", "path": str(path)}


def await_component_signal_receipt(
    path: Path | None, *, timeout: float = 2.0
) -> dict[str, Any] | None:
    """Poll a durable reexec child receipt until usable evidence appears."""
    if path is None:
        return None
    evidence: dict[str, Any] | None = None
    deadline = time.monotonic() + max(0.0, float(timeout))
    while True:
        evidence = _read_component_signal_receipt(path)
        if evidence is not None and "error" not in evidence:
            return evidence
        if evidence is not None and evidence.get("run_id"):
            return evidence
        if time.monotonic() >= deadline:
            break
        time.sleep(0.05)
    return evidence


def cancel_active_component_procs(
    signum: int, *, wait_timeout: float = 90.0
) -> dict[str, Any]:
    """Forward *signum* to registered component children and wait for exit.

    Uses poll (not wait) so a concurrent ``communicate()`` in the parent suite
    runner cannot deadlock the signal handler. Escalates to SIGKILL after timeout.
    After the child exits, ingest any durable terminal snapshot written outside
    the disposable stand so the aggregate can journal run/IDs/cleanup before
    redelivering the signal to itself.
    """
    with _ACTIVE_COMPONENT_PROCS_LOCK:
        procs = list(_ACTIVE_COMPONENT_PROCS)
        receipt_paths = {id(proc): _ACTIVE_COMPONENT_RECEIPT_PATHS.get(id(proc)) for proc in procs}
    details: list[dict[str, Any]] = []
    for proc in procs:
        receipt_path = receipt_paths.get(id(proc))
        entry: dict[str, Any] = {
            "pid": proc.pid,
            "returncode_before": proc.poll(),
            "signal_receipt_path": str(receipt_path) if receipt_path is not None else None,
        }
        if proc.poll() is None:
            try:
                os.kill(proc.pid, signum)
                entry["signaled"] = int(signum)
            except ProcessLookupError:
                entry["signaled"] = False
            deadline = time.monotonic() + wait_timeout
            while proc.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            if proc.poll() is None:
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                    entry["forced_kill"] = True
                except ProcessLookupError:
                    entry["forced_kill"] = False
                kill_deadline = time.monotonic() + 5.0
                while proc.poll() is None and time.monotonic() < kill_deadline:
                    time.sleep(0.05)
        entry["returncode"] = proc.poll()
        entry["still_alive"] = proc.poll() is None
        # Child may finish writing the receipt slightly after exit status appears.
        evidence = await_component_signal_receipt(receipt_path, timeout=2.0)
        if evidence is not None:
            entry["signal_evidence"] = evidence
        details.append(entry)
        unregister_active_component_proc(proc)
    return {"children": details, "signum": int(signum)}


def register_active_stand(stand: "IsolatedStand") -> None:
    with _ACTIVE_STANDS_LOCK:
        if stand not in _ACTIVE_STANDS:
            _ACTIVE_STANDS.append(stand)


def unregister_active_stand(stand: "IsolatedStand") -> None:
    with _ACTIVE_STANDS_LOCK:
        try:
            _ACTIVE_STANDS.remove(stand)
        except ValueError:
            pass


def cleanup_active_stands_best_effort() -> list[dict[str, Any]]:
    """Stop any in-process stands still registered with the current process."""
    with _ACTIVE_STANDS_LOCK:
        stands = list(_ACTIVE_STANDS)
    results: list[dict[str, Any]] = []
    for stand in stands:
        if stand.cleaned:
            results.append({"run_id": stand.run_id, "already_cleaned": True})
            continue
        try:
            results.append(stand.cleanup())
        except Exception as exc:  # noqa: BLE001 — best-effort signal path
            results.append(
                {
                    "run_id": getattr(stand, "run_id", None),
                    "error": f"{type(exc).__name__}: {exc}",
                    "cleaned": bool(getattr(stand, "cleaned", False)),
                }
            )
    return results


def publish_stand_signal_evidence(payload: dict[str, Any]) -> None:
    """Retain live run/ID/cleanup evidence across SystemExit(128+signum).

    Always keeps a process-local copy for in-process aggregate stands. When
    ``EA010_STAND_SIGNAL_RECEIPT`` is set (reexec children), also persist the
    snapshot outside the disposable stand so the parent can ingest it after
    the child exits and before aggregate signal redelivery.
    """
    global _STAND_SIGNAL_EVIDENCE
    snapshot = dict(payload)
    _STAND_SIGNAL_EVIDENCE = snapshot
    raw = (os.environ.get(STAND_SIGNAL_RECEIPT_ENV) or "").strip()
    if not raw:
        return
    path = Path(raw).expanduser()
    try:
        if not path.is_absolute():
            return
        path.parent.mkdir(mode=SAFE_DIR_MODE, parents=True, exist_ok=True)
        atomic_write_secure(
            path,
            json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
            SAFE_FILE_MODE,
        )
    except Exception:
        # Signal path must still exit with the original status.
        pass


def consume_stand_signal_evidence() -> dict[str, Any] | None:
    global _STAND_SIGNAL_EVIDENCE
    payload = _STAND_SIGNAL_EVIDENCE
    _STAND_SIGNAL_EVIDENCE = None
    return payload


def peek_stand_signal_evidence() -> dict[str, Any] | None:
    return dict(_STAND_SIGNAL_EVIDENCE) if _STAND_SIGNAL_EVIDENCE is not None else None


def implementation_source_digest(worktree: Path) -> dict[str, Any]:
    """Full candidate implementation revision/diff digest.

    Includes every tracked modification and untracked implementation file under
    the worktree. Accepted process/document inputs under ``docs/`` are excluded
    by explicit policy (BR/ADR/NFR/UX/SR/plan/process markers are read-only
    inputs, not implementation evidence).
    """
    root = Path(worktree)
    head_proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    head = head_proc.stdout.strip() if head_proc.returncode == 0 else "unknown"
    # Exclude docs/ from the tracked diff so accepted inputs do not bind digests.
    diff = subprocess.run(
        ["git", "diff", "HEAD", "--", ".", ":!docs/"],
        cwd=root,
        capture_output=True,
        check=False,
    )
    untracked = subprocess.run(
        ["git", "ls-files", "-o", "--exclude-standard"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    material = bytearray(diff.stdout or b"")
    paths: list[str] = []
    for rel in (untracked.stdout or "").splitlines():
        if rel.startswith("docs/"):
            continue
        path = root / rel
        if not path.is_file():
            continue
        paths.append(rel)
        material.extend(rel.encode())
        material.extend(b"\0")
        material.extend(path.read_bytes())
        material.extend(b"\0")
    digest = sha256_bytes(bytes(material))
    return {
        "worktree": str(root),
        "head": head,
        "sha256": digest,
        "diff_sha256": digest,
        "untracked_implementation_files": paths,
        "exclude_policy": "docs/** accepted process/document inputs",
    }


def cleanup_owned_manifests_under_run_root(
    profile: "StandProfile", *, extra_manifest_paths: list[Path] | None = None
) -> list[dict[str, Any]]:
    """Best-effort identity-checked cleanup of leftover owned stand manifests."""
    results: list[dict[str, Any]] = []
    candidates: list[Path] = list(extra_manifest_paths or [])
    try:
        for child in sorted(profile.run_root.iterdir()):
            if not child.is_dir() or child.name.startswith("."):
                continue
            if not RUN_ID_RE.match(child.name):
                continue
            manifest_path = child / "resource-manifest.json"
            if manifest_path.is_file():
                candidates.append(manifest_path)
    except OSError:
        pass
    seen: set[str] = set()
    for manifest_path in candidates:
        key = str(manifest_path)
        if key in seen:
            continue
        seen.add(key)
        try:
            manifest = load_manifest(manifest_path)
            results.append(
                {
                    "manifest_path": key,
                    "cleanup": cleanup_manifest(
                        profile, manifest, manifest_path=manifest_path
                    ),
                }
            )
        except Exception as exc:  # noqa: BLE001 — signal-path best effort
            results.append({"manifest_path": key, "error": f"{type(exc).__name__}: {exc}"})
    return results


@dataclass
class IsolatedStand:
    profile: StandProfile
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    worktree: Path | None = None
    journal_path: Path | None = None
    # Test-only hooks (never set in production runners).
    _test_crash_after_popen: Callable[["IsolatedStand", subprocess.Popen[Any]], None] | None = field(
        default=None, repr=False
    )
    _test_popen: Callable[..., subprocess.Popen[Any]] | None = field(default=None, repr=False)
    _test_time_anchor: float | None = field(default=None, repr=False)
    _child: subprocess.Popen[Any] | None = field(default=None, init=False, repr=False)
    _started_at: float | None = field(default=None, init=False, repr=False)
    _session_token: str | None = field(default=None, init=False, repr=False)
    _password: str | None = field(default=None, init=False, repr=False)
    _username: str = field(default="ea010_stand", init=False, repr=False)
    _token_secrets: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _signal_handlers: dict[int, Any] = field(default_factory=dict, init=False, repr=False)
    _live_lock: LiveRunLock | None = field(default=None, init=False, repr=False)
    _watchdog: LimitWatchdog | None = field(default=None, init=False, repr=False)
    _handshake_read_fd: int | None = field(default=None, init=False, repr=False)
    _handshake_write_fd: int | None = field(default=None, init=False, repr=False)
    _created_run_dir: bool = field(default=False, init=False, repr=False)
    _owned_children: dict[str, subprocess.Popen[Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    _owned_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    manifest: dict[str, Any] = field(default_factory=dict, init=False)
    cleaned: bool = field(default=False, init=False)
    readiness: dict[str, Any] | None = field(default=None, init=False)
    synthetic_ids: dict[str, Any] = field(default_factory=dict, init=False)
    file_modes: dict[str, str] = field(default_factory=dict, init=False)
    secondary_cleanup_error: BaseException | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)

    @property
    def run_dir(self) -> Path:
        return assert_immediate_run_child(self.profile.run_root, self.run_id, self.profile.run_root / self.run_id)

    @property
    def manifest_path(self) -> Path:
        return self.run_dir / "resource-manifest.json"

    @property
    def secrets_path(self) -> Path:
        return self.run_dir / "secrets.json"

    @property
    def launch_receipt_path(self) -> Path:
        return self.run_dir / LAUNCH_RECEIPT_NAME

    @property
    def durable_claim_path(self) -> Path:
        return ownership_claim_path(self.profile, self.run_id)

    def fixture_name(self, suffix: str) -> str:
        return f"{self.profile.fixture_prefix}{self.run_id}-{suffix}"

    def _now_monotonic(self) -> float:
        if self._test_time_anchor is not None:
            return self._test_time_anchor
        return time.monotonic()

    def _enforce_limits(self) -> None:
        if self._started_at is not None:
            elapsed = self._now_monotonic() - self._started_at
            if elapsed > self.profile.limits["wall_seconds"]:
                raise StandError(f"Wall-clock limit exceeded: {elapsed:.1f}s")
        disk = 0
        if self.run_dir.exists():
            disk += directory_disk_bytes(self.run_dir)
        for root in self.manifest.get("owned_roots") or []:
            path = owned_root_path(root)
            if path.exists():
                disk += directory_disk_bytes(path)
        if disk > self.profile.limits["disk_bytes"]:
            raise StandError(f"Disk limit exceeded: {disk} bytes")
        rss = 0
        if self._child is not None and self._child.poll() is None:
            rss += process_tree_rss_bytes(self._child.pid)
        # Snapshot under the owned-children lock so concurrent stop/start cannot
        # raise "dictionary changed size during iteration" and silently kill the
        # watchdog (NFR-18 resource-budget monitor must stay fail-closed).
        with self._owned_lock:
            owned_snapshot = list(self._owned_children.values())
        for child in owned_snapshot:
            if child.poll() is None:
                rss += process_tree_rss_bytes(child.pid)
        if rss > self.profile.limits["owned_process_tree_rss_bytes"]:
            raise StandError(f"RSS limit exceeded: {rss} bytes")

    def _save_ownership_claim(self, extra: dict[str, Any] | None = None) -> None:
        """Mirror recoverable ownership outside the run tree (survives partial rmtree)."""
        if not self.manifest:
            claim = {
                "schema": OWNERSHIP_SCHEMA,
                "run_id": self.run_id,
                "run_dir": str(self.run_dir),
                "status": "prepared",
                "process": None,
            }
        else:
            claim = ownership_claim_from_manifest(self.manifest)
            claim["run_id"] = self.run_id
            claim["run_dir"] = str(self.run_dir)
        if extra:
            claim.update(extra)
        write_ownership_claim(self.profile, claim)

    def _clear_ownership_claim(self) -> None:
        clear_ownership_claim(self.profile, self.run_id)

    def _save_manifest(self) -> None:
        self.manifest["schema"] = MANIFEST_SCHEMA
        self.manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
        # Durable outside-tree claim first so partial recursive delete cannot erase
        # the only recovery record.
        self._save_ownership_claim()
        if self.run_dir.exists():
            atomic_write_secure(
                self.manifest_path,
                json.dumps(self.manifest, indent=2, sort_keys=True) + "\n",
                self.profile.sensitive_file_mode,
            )

    def _save_secrets(self) -> None:
        payload = {
            "session_token_handle": "session",
            "tokens": {
                handle: {"secret_present": True}
                for handle in self._token_secrets
            },
            # Secrets stay outside Git; file mode 0600 under approved run-root.
            "values": dict(self._token_secrets),
            "password": self._password,
            "username": self._username,
        }
        atomic_write_secure(
            self.secrets_path,
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            self.profile.sensitive_file_mode,
        )

    def _save_launch_receipt(self, payload: dict[str, Any]) -> None:
        atomic_write_secure(
            self.launch_receipt_path,
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            self.profile.sensitive_file_mode,
        )

    def _release_live_lock(self) -> None:
        lock = self._live_lock
        self._live_lock = None
        if lock is not None:
            lock.release()

    def _stop_watchdog(self) -> None:
        watchdog = self._watchdog
        self._watchdog = None
        if watchdog is not None:
            watchdog.stop()

    def _close_handshake_fd(self, attr: str) -> None:
        fd = getattr(self, attr)
        setattr(self, attr, None)
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def _close_handshake(self) -> None:
        # Close any still-owned pipe ends; safe if already transferred/cleared.
        self._close_handshake_fd("_handshake_read_fd")
        self._close_handshake_fd("_handshake_write_fd")

    def prepare(self) -> dict[str, Any]:
        """Validate inputs, take exclusive live lock, then recover orphans and stage files."""
        validate_run_id(self.run_id)
        # Exclusive live-run lock before any destructive recovery.
        if self._live_lock is None:
            lock = LiveRunLock(self.profile.run_root)
            lock.acquire()
            self._live_lock = lock
        try:
            # Binary pin first; owned-orphan recovery under the lock must run before
            # rejecting remaining foreign listeners so SIGKILL orphans can be reclaimed.
            verify_binary(self.profile)
            cleanup_orphaned_runs(
                self.profile,
                require_exclusive_owner=True,
                owned_live_lock=self._live_lock,
            )
            # Foreign listeners that remain after owned recovery still refuse admission.
            assert_ports_free(self.profile.all_ports, self.profile.listen_host)
            os.chmod(self.profile.run_root, self.profile.run_directory_mode)
            if self.run_dir.exists():
                raise StandError(f"Run directory already exists: {self.run_dir}")
            self.run_dir.mkdir(mode=self.profile.run_directory_mode)
            # Only a directory created by this invocation may be rolled back on failure.
            self._created_run_dir = True
            os.chmod(self.run_dir, self.profile.run_directory_mode)
            (self.run_dir / "files").mkdir(mode=self.profile.run_directory_mode)
            config = {
                "service": {
                    "interface": f"{self.profile.listen_host}:{self.profile.vikunja_port}",
                    "publicurl": f"http://{self.profile.listen_host}:{self.profile.vikunja_port}/",
                    "rootpath": str(self.run_dir),
                    "secret": secrets.token_hex(32),
                    "enableregistration": True,
                },
                "database": {"type": "sqlite", "path": str(self.run_dir / "stand.db")},
                "files": {"basepath": str(self.run_dir / "files"), "maxsize": "1MB"},
                "mailer": {"enabled": False},
                "log": {"enabled": False},
                "cors": {"origins": [f"http://{self.profile.listen_host}:{self.profile.vikunja_port}"]},
            }
            config_path = self.run_dir / "config.json"
            write_secure(
                config_path,
                json.dumps(config, indent=2, sort_keys=True) + "\n",
                self.profile.sensitive_file_mode,
            )
            source = self._source_digest()
            self.manifest = {
                "schema": MANIFEST_SCHEMA,
                "run_id": self.run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "profile_path": str(self.profile.path),
                "profile_sha256": sha256_file(self.profile.path),
                "binary": verify_binary(self.profile),
                "run_dir": str(self.run_dir),
                "ports": dict(self.profile.ports),
                "listen_host": self.profile.listen_host,
                "fixture_prefix": self.profile.fixture_prefix,
                "process": None,
                "owned_roots": [],
                "owned_children": [],
                "token_handles": [],
                "synthetic_ids": {},
                "source": source,
                "status": "prepared",
            }
            bind_manifest_paths(self.profile, self.manifest, manifest_path=self.manifest_path)
            self._save_manifest()
            return self.manifest
        except BaseException:
            # Roll back only a directory this invocation proved it created.
            rollback_ok = True
            if (
                self._created_run_dir
                and self.run_dir.exists()
                and not (self.manifest.get("process") or {}).get("pid")
            ):
                # Durable claim before delete so a partial rmtree remains reclaimable.
                try:
                    self._save_ownership_claim({"status": self.manifest.get("status") or "prepared"})
                except Exception:
                    pass
                try:
                    shutil.rmtree(self.run_dir)
                except OSError as exc:
                    rollback_ok = False
                    self._record_cleanup_failure(
                        [f"prepare rollback rmtree failed: {exc}"],
                        {"directory_retained": self.run_dir.exists()},
                    )
                if self.run_dir.exists():
                    rollback_ok = False
                    if self.manifest.get("status") != "cleanup_failed":
                        self._record_cleanup_failure(
                            ["prepare rollback left owned directory"],
                            {"directory_retained": True},
                        )
                else:
                    self._created_run_dir = False
                    try:
                        self._clear_ownership_claim()
                    except StandError:
                        rollback_ok = False
            if rollback_ok:
                self._release_live_lock()
            raise

    def _source_digest(self) -> dict[str, Any]:
        # Shared full-candidate digest (docs/ excluded by explicit policy).
        return implementation_source_digest(self.worktree or Path.cwd())

    def start(self) -> None:
        if not self.manifest:
            self.prepare()
        if self._live_lock is None:
            raise StandError("Cannot start without exclusive live-run lock")
        config_path = self.run_dir / "config.json"
        read_fd, write_fd = os.pipe()
        # Own both ends before spawn can fail so unwind/close never leaks an end.
        self._handshake_read_fd = read_fd
        self._handshake_write_fd = write_fd
        # Crash-safe handshake: child blocks before exec until identity is durable.
        wrapper = (
            "import os,resource,select,sys\n"
            "os.umask(0o077)\n"
            f"limit={int(self.profile.limits['disk_bytes'])}\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE,(limit,limit))\n"
            "ready,_,_ = select.select([sys.stdin],[],[],15)\n"
            "if not ready:\n"
            "    sys.exit(75)\n"
            "if os.read(0,1) != b'G':\n"
            "    sys.exit(76)\n"
            "os.execv(sys.argv[1], sys.argv[1:])\n"
        )
        popen = self._test_popen or subprocess.Popen
        try:
            child = popen(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    wrapper,
                    str(self.profile.binary_path),
                    "--config",
                    str(config_path),
                    "web",
                ],
                cwd=str(self.run_dir),
                env={
                    "HOME": str(self.run_dir),
                    "PATH": "/usr/bin:/bin",
                    "GOMEMLIMIT": "1GiB",
                },
                stdin=read_fd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        except BaseException:
            # Spawn failed: nothing was transferred to a child; close both ends.
            self._close_handshake()
            raise
        # Successful spawn: close only the parent's copy of the read end.
        # Do not close write_fd here — it remains owned until handshake release.
        self._close_handshake_fd("_handshake_read_fd")
        self._child = child
        self._started_at = self._now_monotonic()
        try:
            # Durable pre-identity receipt before any crash-window probe / release.
            receipt = {
                "schema": "ea010-stand-launch-receipt/v1",
                "run_id": self.run_id,
                "pid": child.pid,
                "phase": "pre-identity",
                "at": datetime.now(timezone.utc).isoformat(),
            }
            self._save_launch_receipt(receipt)
            self.manifest["launch_receipt"] = receipt
            self.manifest["process"] = {"pid": child.pid, "identity": None}
            self.manifest["status"] = "launching"
            self._save_manifest()
            if self._test_crash_after_popen is not None:
                # Injected crash must happen before identity persistence + release.
                self._test_crash_after_popen(self, child)
            identity = process_start_identity(child.pid)
            self.manifest["process"] = {
                "pid": child.pid,
                "identity": identity,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            self.manifest["status"] = "starting"
            self._save_manifest()
            receipt = {
                "schema": "ea010-stand-launch-receipt/v1",
                "run_id": self.run_id,
                "pid": child.pid,
                "phase": "identity-persisted",
                "identity": identity,
                "at": datetime.now(timezone.utc).isoformat(),
            }
            self._save_launch_receipt(receipt)
            self.manifest["launch_receipt"] = receipt
            self._save_manifest()
            # Release the child only after recoverable identity is on disk.
            os.write(write_fd, b"G")
            self._close_handshake()
        except BaseException:
            self._close_handshake()
            try:
                if child.poll() is None:
                    os.kill(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
            except Exception:
                pass
            raise
        self._watchdog = LimitWatchdog(self)
        self._watchdog.start()

    def claim_owned_root(self, *, prefix: str) -> Path:
        """Create and durably register a copy/restore root before use (NFR-18)."""
        if self._live_lock is None or not self._live_lock.held:
            raise StandError("Cannot claim owned root without exclusive live-run lock")
        if not self.manifest:
            raise StandError("Cannot claim owned root before prepare()")
        path = Path(
            tempfile.mkdtemp(prefix=prefix, dir=str(self.profile.run_root))
        )
        os.chmod(path, self.profile.run_directory_mode)
        record = capture_owned_root_identity(self.profile.run_root, path)
        roots = list(self.manifest.get("owned_roots") or [])
        roots.append(record)
        self.manifest["owned_roots"] = roots
        self._save_manifest()
        return Path(record["path"])

    def start_owned_vikunja(
        self,
        *,
        target: Path,
        role: str,
    ) -> subprocess.Popen[Any]:
        """Crash-safe launch of a restored Vikunja child with durable identity.

        Mirrors IsolatedStand.start handshake/receipt protocol so SIGKILL of the
        supervisor can reclaim the child from the ownership claim without private
        in-memory knowledge (implementation-plan §6 / NFR-18).
        """
        if self._live_lock is None or not self._live_lock.held:
            raise StandError("Cannot start owned Vikunja without exclusive live-run lock")
        if not role or not str(role).strip():
            raise StandError("owned Vikunja role must be non-empty")
        role_key = str(role).strip()
        with self._owned_lock:
            if role_key in self._owned_children:
                raise StandError(f"owned Vikunja role already running: {role_key}")
        bound_target = assert_owned_root_under_run_root(self.profile.run_root, target)
        matched_record: Mapping[str, Any] | None = None
        for item in self.manifest.get("owned_roots") or []:
            item_path = assert_owned_root_under_run_root(
                self.profile.run_root, owned_root_path(item)
            )
            if str(item_path) == str(bound_target):
                if not isinstance(item, Mapping):
                    raise StandError(
                        "owned Vikunja target lacks durable identity record; "
                        f"refuse launch: {bound_target}"
                    )
                matched_record = item
                break
        if matched_record is None:
            raise StandError(
                f"owned Vikunja target must be claim_owned_root-registered first: {bound_target}"
            )
        # NFR-18: path membership is not enough — require claimed directory inode
        # before reading config, writing receipts, or releasing the child.
        bound_target = assert_owned_root_record_identity(
            self.profile.run_root, matched_record
        )
        config_path = bound_target / "config.json"
        if not config_path.is_file():
            raise StandError(f"restored config missing at {config_path}")
        # Source contour must not still own the profile port.
        if self._child is not None and self._child.poll() is None:
            raise StandError("stop source Vikunja before starting an owned restore child")
        busy = listeners(self.profile.vikunja_port)
        if busy:
            raise StandError(
                f"Cannot start owned Vikunja; port {self.profile.vikunja_port} busy: {sorted(busy)}"
            )

        # Re-bind after config/port checks in case of rename+mkdir substitution.
        bound_target = assert_owned_root_record_identity(
            self.profile.run_root, matched_record
        )
        config_path = bound_target / "config.json"

        read_fd, write_fd = os.pipe()
        self._handshake_read_fd = read_fd
        self._handshake_write_fd = write_fd
        wrapper = (
            "import os,resource,select,sys\n"
            "os.umask(0o077)\n"
            f"limit={int(self.profile.limits['disk_bytes'])}\n"
            "resource.setrlimit(resource.RLIMIT_FSIZE,(limit,limit))\n"
            "ready,_,_ = select.select([sys.stdin],[],[],15)\n"
            "if not ready:\n"
            "    sys.exit(75)\n"
            "if os.read(0,1) != b'G':\n"
            "    sys.exit(76)\n"
            "os.execv(sys.argv[1], sys.argv[1:])\n"
        )
        popen = self._test_popen or subprocess.Popen
        try:
            child = popen(
                [
                    sys.executable,
                    "-I",
                    "-c",
                    wrapper,
                    str(self.profile.binary_path),
                    "--config",
                    str(config_path),
                    "web",
                ],
                cwd=str(bound_target),
                env={
                    "HOME": str(bound_target),
                    "PATH": "/usr/bin:/bin",
                    "GOMEMLIMIT": "1GiB",
                },
                stdin=read_fd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        except BaseException:
            self._close_handshake()
            raise
        self._close_handshake_fd("_handshake_read_fd")
        with self._owned_lock:
            self._owned_children[role_key] = child
        # Refuse path writes into a substituted directory before any receipt.
        bound_target = assert_owned_root_record_identity(
            self.profile.run_root, matched_record
        )
        receipt_path = bound_target / LAUNCH_RECEIPT_NAME
        try:
            receipt = {
                "schema": "ea010-stand-launch-receipt/v1",
                "run_id": self.run_id,
                "role": role_key,
                "root": str(bound_target),
                "pid": child.pid,
                "phase": "pre-identity",
                "at": datetime.now(timezone.utc).isoformat(),
            }
            atomic_write_secure(
                receipt_path,
                json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                self.profile.sensitive_file_mode,
            )
            # Keep run_dir launch-receipt in lockstep so orphan cleanup cannot
            # reload a stale source receipt over the active owned child.
            self._save_launch_receipt(receipt)
            child_record = {
                "role": role_key,
                "root": str(bound_target),
                "pid": child.pid,
                "identity": None,
                "launch_receipt": receipt,
                "status": "launching",
            }
            children = [
                item
                for item in (self.manifest.get("owned_children") or [])
                if item.get("role") != role_key
            ]
            children.append(child_record)
            self.manifest["owned_children"] = children
            # Also point primary process fields at the active owned child so
            # cleanup_manifest / ownership-claim recovery can signal it when the
            # source contour is already stopped.
            self.manifest["process"] = {"pid": child.pid, "identity": None}
            self.manifest["launch_receipt"] = receipt
            self.manifest["status"] = f"owned_launching:{role_key}"
            self._save_manifest()
            if self._test_crash_after_popen is not None:
                self._test_crash_after_popen(self, child)
            identity = process_start_identity(child.pid)
            identity_receipt = {
                "schema": "ea010-stand-launch-receipt/v1",
                "run_id": self.run_id,
                "role": role_key,
                "root": str(bound_target),
                "pid": child.pid,
                "phase": "identity-persisted",
                "identity": identity,
                "at": datetime.now(timezone.utc).isoformat(),
            }
            child_record = {
                "role": role_key,
                "root": str(bound_target),
                "pid": child.pid,
                "identity": identity,
                "launch_receipt": identity_receipt,
                "status": "starting",
            }
            atomic_write_secure(
                receipt_path,
                json.dumps(identity_receipt, indent=2, sort_keys=True) + "\n",
                self.profile.sensitive_file_mode,
            )
            self._save_launch_receipt(identity_receipt)
            children = [
                item
                for item in (self.manifest.get("owned_children") or [])
                if item.get("role") != role_key
            ]
            children.append(child_record)
            self.manifest["owned_children"] = children
            self.manifest["process"] = {
                "pid": child.pid,
                "identity": identity,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "role": role_key,
                "root": str(bound_target),
            }
            self.manifest["launch_receipt"] = identity_receipt
            self.manifest["status"] = f"owned_starting:{role_key}"
            self._save_manifest()
            # Final inode bind before releasing the child handshake.
            bound_target = assert_owned_root_record_identity(
                self.profile.run_root, matched_record
            )
            os.write(write_fd, b"G")
            self._close_handshake()
        except BaseException:
            self._close_handshake()
            try:
                if child.poll() is None:
                    os.kill(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
            except Exception:
                pass
            with self._owned_lock:
                self._owned_children.pop(role_key, None)
            raise

        # Wait until this owned child alone owns the profile port and answers /info.
        deadline = time.monotonic() + 25.0
        last = "not ready"
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise StandError(f"owned Vikunja exited rc={child.returncode}")
            owned = listeners(self.profile.vikunja_port)
            if owned and child.pid not in owned:
                time.sleep(0.1)
                continue
            if child.pid not in owned:
                time.sleep(0.1)
                continue
            try:
                status, info = http_json(
                    self.profile.listen_host, self.profile.vikunja_port, "info"
                )
                if status == 200 and isinstance(info, dict):
                    identity = process_start_identity(child.pid)
                    ready_receipt = {
                        "schema": "ea010-stand-launch-receipt/v1",
                        "run_id": self.run_id,
                        "role": role_key,
                        "root": str(bound_target),
                        "pid": child.pid,
                        "phase": "ready",
                        "identity": identity,
                        "at": datetime.now(timezone.utc).isoformat(),
                    }
                    atomic_write_secure(
                        receipt_path,
                        json.dumps(ready_receipt, indent=2, sort_keys=True) + "\n",
                        self.profile.sensitive_file_mode,
                    )
                    self._save_launch_receipt(ready_receipt)
                    child_record = {
                        "role": role_key,
                        "root": str(bound_target),
                        "pid": child.pid,
                        "identity": identity,
                        "launch_receipt": ready_receipt,
                        "status": "ready",
                    }
                    children = [
                        item
                        for item in (self.manifest.get("owned_children") or [])
                        if item.get("role") != role_key
                    ]
                    children.append(child_record)
                    self.manifest["owned_children"] = children
                    self.manifest["process"] = {
                        "pid": child.pid,
                        "identity": identity,
                        "started_at": datetime.now(timezone.utc).isoformat(),
                        "role": role_key,
                        "root": str(bound_target),
                    }
                    self.manifest["launch_receipt"] = ready_receipt
                    self.manifest["status"] = f"owned_ready:{role_key}"
                    self._save_manifest()
                    if self._watchdog is None:
                        self._watchdog = LimitWatchdog(self)
                        self._watchdog.start()
                    return child
                last = f"info HTTP {status}"
            except Exception as exc:  # noqa: BLE001
                last = str(exc)
            time.sleep(0.1)
        try:
            os.kill(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
        except Exception:
            pass
        with self._owned_lock:
            self._owned_children.pop(role_key, None)
        raise StandError(f"owned Vikunja not ready: {last}")

    def stop_owned_vikunja(self, role: str | None = None) -> None:
        """Stop one or all owned restore children and clear primary process binding."""
        with self._owned_lock:
            roles = [role] if role is not None else list(self._owned_children)
        for role_key in roles:
            with self._owned_lock:
                child = self._owned_children.pop(role_key, None)
            if child is None:
                continue
            records = [
                item
                for item in (self.manifest.get("owned_children") or [])
                if item.get("role") == role_key
            ]
            identity = (records[0].get("identity") if records else None) or (
                (self.manifest.get("process") or {}).get("identity")
            )
            if child.poll() is None:
                if identity and not process_matches_identity(identity):
                    raise StandError(
                        f"Refusing to signal owned child {role_key}: PID identity mismatch"
                    )
                try:
                    os.kill(child.pid, signal.SIGTERM)
                    child.wait(timeout=5)
                except Exception:
                    try:
                        if identity is None or process_matches_identity(identity):
                            os.kill(child.pid, signal.SIGKILL)
                            child.wait(timeout=5)
                    except Exception:
                        pass
            remaining = [
                item
                for item in (self.manifest.get("owned_children") or [])
                if item.get("role") != role_key
            ]
            self.manifest["owned_children"] = remaining
            process = self.manifest.get("process") or {}
            if process.get("role") == role_key or process.get("pid") == child.pid:
                self.manifest["process"] = None
                self.manifest["launch_receipt"] = None
            if self.manifest_path.exists():
                self._save_manifest()

    def _refresh_process_identity(self) -> None:
        if self._child is None or self._child.poll() is not None:
            return
        identity = process_start_identity(self._child.pid)
        process = dict(self.manifest.get("process") or {})
        process.update({"pid": self._child.pid, "identity": identity})
        self.manifest["process"] = process
        self._save_manifest()

    def _assert_sensitive_file_modes(self) -> dict[str, str]:
        expected = self.profile.sensitive_file_mode
        observed: dict[str, str] = {}
        candidates = [
            self.run_dir / "config.json",
            self.manifest_path,
            self.run_dir / "stand.db",
            self.run_dir / "stand.db-wal",
            self.run_dir / "stand.db-shm",
        ]
        for path in candidates:
            if not path.exists():
                continue
            mode = path.stat().st_mode & 0o777
            observed[path.name] = oct(mode)
            if mode != expected:
                raise StandError(
                    f"Sensitive file mode for {path.name} is {oct(mode)}, expected {oct(expected)}"
                )
        self.file_modes = observed
        self.manifest["file_modes"] = observed
        self._save_manifest()
        return observed

    def wait_ready(self, timeout: float = 20.0) -> dict[str, Any]:
        if self._child is None:
            raise StandError("Stand process has not been started")
        deadline = time.monotonic() + timeout
        last_error = "not ready"
        while time.monotonic() < deadline:
            if self._watchdog is not None:
                self._watchdog.raise_if_failed()
            self._enforce_limits()
            if self._child.poll() is not None:
                raise StandError(f"Isolated Vikunja exited during startup rc={self._child.returncode}")
            try:
                owned = listeners(self.profile.vikunja_port)
                if owned != {self._child.pid}:
                    time.sleep(0.1)
                    continue
                status, info = http_json(
                    self.profile.listen_host,
                    self.profile.vikunja_port,
                    "info",
                )
                if status == 200 and isinstance(info, dict):
                    # Capture post-exec identity once the binary owns the port.
                    self._refresh_process_identity()
                    self._assert_sensitive_file_modes()
                    self.readiness = {"http": status, "info": info, "file_modes": dict(self.file_modes)}
                    self.manifest["readiness"] = {
                        "http": status,
                        "version": info.get("version"),
                    }
                    self.manifest["status"] = "ready"
                    self._save_manifest()
                    return self.readiness
                last_error = f"info HTTP {status}"
            except (URLError, TimeoutError, StandError, json.JSONDecodeError) as exc:
                last_error = str(exc)
            time.sleep(0.1)
        raise StandError(f"Isolated Vikunja did not become ready: {last_error}")

    def api(
        self,
        path: str,
        method: str = "GET",
        body: Any | None = None,
        token: str | None = None,
    ) -> tuple[int, Any]:
        if self._watchdog is not None:
            self._watchdog.raise_if_failed()
        if self._child is None or self._child.poll() is not None:
            raise StandError("Stand process is not running")
        owned = listeners(self.profile.vikunja_port)
        if owned != {self._child.pid}:
            raise StandError(f"Listener ownership changed on {self.profile.vikunja_port}: {owned}")
        self._enforce_limits()
        return http_json(
            self.profile.listen_host,
            self.profile.vikunja_port,
            path,
            method=method,
            body=body,
            token=token,
        )

    def create_synthetic_session(self) -> dict[str, Any]:
        self._password = secrets.token_urlsafe(24)
        self._username = f"ea010_{self.run_id[:8]}"
        status, _ = self.api(
            "register",
            "POST",
            {
                "username": self._username,
                "email": f"{self._username}@example.invalid",
                "password": self._password,
            },
        )
        if status != 200:
            raise StandError(f"Synthetic registration failed: HTTP {status}")
        status, login = self.api(
            "login",
            "POST",
            {"username": self._username, "password": self._password},
        )
        if status != 200 or not isinstance(login, dict) or "token" not in login:
            raise StandError(f"Synthetic login failed: HTTP {status}")
        self._session_token = login["token"]
        self._token_secrets["session"] = self._session_token
        status, project = self.api(
            "projects",
            "PUT",
            {"title": self.fixture_name("project")},
            token=self._session_token,
        )
        if status != 201 or not isinstance(project, dict):
            raise StandError(f"Synthetic project creation failed: HTTP {status}")
        marker = self.fixture_name("task")
        status, task = self.api(
            f"projects/{project['id']}/tasks",
            "PUT",
            {"title": self.fixture_name("title"), "description": marker},
            token=self._session_token,
        )
        if status != 201 or not isinstance(task, dict):
            raise StandError(f"Synthetic task creation failed: HTTP {status}")
        expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        status, api_token = self.api(
            "tokens",
            "PUT",
            {
                "title": self.fixture_name("token"),
                "expires_at": expires,
                "permissions": {"tasks": ["read_all"]},
            },
            token=self._session_token,
        )
        if status != 201 or not isinstance(api_token, dict):
            raise StandError(f"Synthetic API token creation failed: HTTP {status}")
        handle = "api_token_primary"
        self._token_secrets[handle] = api_token["token"]
        self.synthetic_ids = {
            "project_id": project["id"],
            "task_id": task["id"],
            "marker": marker,
            "api_token_id": api_token["id"],
            "api_token_handle": handle,
            "username": self._username,
        }
        self.manifest["token_handles"] = ["session", handle]
        self.manifest["synthetic_ids"] = {
            "project_id": project["id"],
            "task_id": task["id"],
            "marker": marker,
            "api_token_id": api_token["id"],
            "api_token_handle": handle,
        }
        self.manifest["status"] = "fixtures_ready"
        self._save_manifest()
        self._save_secrets()
        ready_raw = (os.environ.get(STAND_SESSION_READY_ENV) or "").strip()
        if ready_raw:
            try:
                ready_path = Path(ready_raw).expanduser()
                if ready_path.is_absolute():
                    ready_path.parent.mkdir(mode=SAFE_DIR_MODE, parents=True, exist_ok=True)
                    atomic_write_secure(
                        ready_path,
                        json.dumps(
                            {
                                "run_id": self.run_id,
                                "synthetic_ids": dict(self.synthetic_ids),
                            },
                            indent=2,
                            sort_keys=True,
                        )
                        + "\n",
                        SAFE_FILE_MODE,
                    )
            except Exception:
                pass
        if (os.environ.get(SYNTHETIC_FAIL_AFTER_SESSION_ENV) or "").strip() == "1":
            raise RuntimeError("controlled failure after synthetic session")
        return dict(self.synthetic_ids)

    def revoke_token(self, handle: str = "api_token_primary") -> dict[str, Any]:
        if not self._session_token:
            raise StandError("Cannot revoke token without an active session")
        token_id = self.synthetic_ids.get("api_token_id")
        if handle != self.synthetic_ids.get("api_token_handle") or token_id is None:
            raise StandError(f"Unknown token handle for revoke: {handle}")
        # Prove session still works before revoke.
        status, _ = self.api("tokens", token=self._session_token)
        if status != 200:
            raise StandError(f"Active session required for revoke; tokens list HTTP {status}")
        status, _ = self.api(f"tokens/{token_id}", "DELETE", token=self._session_token)
        if status != 200:
            raise StandError(f"Token revoke failed: HTTP {status}")
        # Read-back: revoked token must not authorize; session remains valid.
        revoked_secret = self._token_secrets.get(handle)
        if not revoked_secret:
            raise StandError("Missing local token secret for revoke read-back")
        revoked_status, _ = self.api("tasks", token=revoked_secret)
        session_status, tokens = self.api("tokens", token=self._session_token)
        if session_status != 200:
            raise StandError(f"Session read-back after revoke failed: HTTP {session_status}")
        remaining_ids = []
        if isinstance(tokens, list):
            remaining_ids = [row.get("id") for row in tokens]
        elif isinstance(tokens, dict):
            remaining_ids = [row.get("id") for row in tokens.get("tokens", tokens.get("data", []))]
        evidence = {
            "handle": handle,
            "token_id": token_id,
            "delete_http": status,
            "revoked_token_http": revoked_status,
            "session_list_http": session_status,
            "token_absent_after_revoke": token_id not in remaining_ids,
        }
        if revoked_status not in (401, 403):
            raise StandError(f"Revoked token still authorized: HTTP {revoked_status}")
        if token_id in remaining_ids:
            raise StandError("Revoked token id still present in session token list")
        self.manifest.setdefault("revoke_readback", []).append(evidence)
        self.manifest["token_handles"] = [
            item for item in self.manifest.get("token_handles", []) if item != handle
        ]
        self._token_secrets.pop(handle, None)
        self._save_manifest()
        self._save_secrets()
        return evidence

    def stop_process(self, sig: int = signal.SIGTERM, wait_timeout: float = 8.0) -> None:
        # Always stop owned restore children first so profile ports/roots clear.
        try:
            self.stop_owned_vikunja()
        except StandError:
            # Fall through to primary stop + cleanup failure accounting.
            pass
        child = self._child
        if child is None:
            return
        if child.poll() is None:
            identity = (self.manifest.get("process") or {}).get("identity")
            # When process is bound to an owned restore child, stop_owned_vikunja
            # already handled it; avoid double-signalling a cleared handle.
            if (self.manifest.get("process") or {}).get("role"):
                return
            if identity and not process_matches_identity(identity):
                raise StandError("Refusing to signal process: PID identity mismatch")
            os.kill(child.pid, sig)
            try:
                child.wait(timeout=wait_timeout)
            except subprocess.TimeoutExpired:
                current = identity or process_start_identity(child.pid)
                if process_matches_identity(current):
                    os.kill(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
        self.manifest["process_exit"] = {
            "rc": child.returncode,
            "stopped_at": datetime.now(timezone.utc).isoformat(),
        }
        if self.manifest_path.exists():
            self._save_manifest()

    def _record_cleanup_failure(self, errors: list[str], extra: dict[str, Any] | None = None) -> None:
        payload = {
            "at": datetime.now(timezone.utc).isoformat(),
            "errors": list(errors),
        }
        if extra:
            payload.update(extra)
        if not self.manifest:
            self.manifest = {
                "schema": MANIFEST_SCHEMA,
                "run_id": self.run_id,
                "run_dir": str(self.run_dir),
                "process": None,
            }
        self.manifest["cleanup_failure"] = payload
        self.manifest["status"] = "cleanup_failed"
        # Always persist outside the tree first; in-tree may already be gone.
        try:
            self._save_ownership_claim({"cleanup_failure": payload, "status": "cleanup_failed"})
        except Exception as exc:  # noqa: BLE001
            errors.append(f"failed to persist durable ownership claim: {exc}")
        if self.run_dir.exists() and self.manifest_path.exists():
            try:
                atomic_write_secure(
                    self.manifest_path,
                    json.dumps(self.manifest, indent=2, sort_keys=True) + "\n",
                    self.profile.sensitive_file_mode,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"failed to persist in-tree cleanup_failure: {exc}")
        elif self.run_dir.exists():
            # Manifest already removed by partial rmtree; recreate from durable claim.
            try:
                atomic_write_secure(
                    self.manifest_path,
                    json.dumps(self.manifest, indent=2, sort_keys=True) + "\n",
                    self.profile.sensitive_file_mode,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"failed to recreate in-tree cleanup_failure: {exc}")

    def cleanup(self, *, revoke: bool = True) -> dict[str, Any]:
        errors: list[str] = []
        revoke_evidence = None
        stop_error = None
        listeners_after: dict[str, list[int]] = {}
        still_matches = False
        self._stop_watchdog()
        self._close_handshake()
        # Revoke may raise transport/IO faults from http_json (URLError/TimeoutError/
        # JSONDecodeError/OSError). Accumulate them, and always reach owned-process
        # stop + port read-back in finally so a revoke fault cannot orphan the child
        # after watchdog stop (implementation-plan §6 / NFR-18).
        try:
            if revoke and self._session_token and self.synthetic_ids.get("api_token_id") is not None:
                if self.synthetic_ids.get("api_token_handle") in self._token_secrets:
                    try:
                        revoke_evidence = self.revoke_token(
                            self.synthetic_ids["api_token_handle"]
                        )
                    except (StandError, URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
                        errors.append(f"token revoke failed: {exc}")
        finally:
            try:
                self.stop_process()
            except StandError as exc:
                stop_error = str(exc)
                errors.append(f"stop failed: {exc}")
            process = self.manifest.get("process") or {}
            identity = process.get("identity")
            still_matches = bool(identity and process_matches_identity(identity))
            try:
                for port in self.profile.all_ports:
                    listeners_after[str(port)] = sorted(listeners(port))
            except StandError as exc:
                errors.append(str(exc))
        busy = {port: pids for port, pids in listeners_after.items() if pids}

        # Retain manifest/recovery state until process ownership is cleared.
        if errors or still_matches or busy:
            self._record_cleanup_failure(
                errors,
                {
                    "stop_error": stop_error,
                    "still_matches_identity": still_matches,
                    "listeners_after": listeners_after,
                    "directory_retained": self.run_dir.exists(),
                },
            )
            self.cleaned = False
            # Keep live lock until a later successful cleanup; release only on success.
            raise StandError(
                f"Cleanup failed; recovery state retained: "
                f"{({'run_id': self.run_id, 'errors': errors, 'listeners_after': listeners_after, 'still_matches': still_matches})}"
            )

        # Bound deletion to validated owned roots + the immediate run_id child.
        try:
            bound = bind_manifest_paths(self.profile, self.manifest, manifest_path=self.manifest_path)
        except StandError as exc:
            errors.append(str(exc))
            self._record_cleanup_failure(errors)
            self.cleaned = False
            raise StandError(f"Cleanup failed; recovery state retained: {errors}") from exc
        owned_records = list(self.manifest.get("owned_roots") or [])
        owned_paths: list[Path] = []
        for raw in owned_records:
            try:
                owned_paths.append(
                    assert_owned_root_under_run_root(
                        self.profile.run_root, owned_root_path(raw)
                    )
                )
            except StandError as exc:
                errors.append(str(exc))
        if errors:
            self._record_cleanup_failure(
                errors,
                {
                    "stop_error": stop_error,
                    "still_matches_identity": still_matches,
                    "listeners_after": listeners_after,
                    "directory_retained": self.run_dir.exists(),
                },
            )
            self.cleaned = False
            raise StandError(
                f"Cleanup failed; recovery state retained: "
                f"{({'run_id': self.run_id, 'errors': errors, 'listeners_after': listeners_after})}"
            )
        # Checkpoint ownership outside the tree before recursive delete.
        try:
            self._save_ownership_claim()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"failed to checkpoint ownership before rmtree: {exc}")
            self._record_cleanup_failure(
                errors,
                {
                    "stop_error": stop_error,
                    "still_matches_identity": still_matches,
                    "listeners_after": listeners_after,
                    "directory_retained": True,
                },
            )
            self.cleaned = False
            raise StandError(
                f"Cleanup failed; recovery state retained: "
                f"{({'run_id': self.run_id, 'errors': errors, 'listeners_after': listeners_after})}"
            ) from exc
        for raw, owned in zip(owned_records, owned_paths):
            try:
                delete_owned_root_record(self.profile.run_root, raw)
            except StandError as exc:
                errors.append(str(exc))
        leftovers = [
            str(path)
            for path in owned_paths
            if path.exists() or path.is_symlink()
        ]
        # Only destroy the run_dir after every owned root identity-bound delete
        # succeeded; otherwise retain the in-tree recovery record (NFR-18).
        if not errors and not leftovers and bound.exists():
            try:
                shutil.rmtree(bound)
            except OSError as exc:
                errors.append(f"rmtree failed: {exc}")
        still_there = self.run_dir.exists()
        if errors or leftovers or still_there:
            self._record_cleanup_failure(
                errors + ([f"owned roots retained: {leftovers}"] if leftovers else []),
                {
                    "stop_error": stop_error,
                    "still_matches_identity": still_matches,
                    "listeners_after": listeners_after,
                    "directory_retained": still_there,
                    "owned_roots_retained": leftovers,
                },
            )
            self.cleaned = False
            raise StandError(
                f"Cleanup failed; recovery state retained: "
                f"{({'run_id': self.run_id, 'errors': errors, 'listeners_after': listeners_after, 'owned_roots_retained': leftovers})}"
            )
        result = {
            "run_id": self.run_id,
            "directory_removed": not still_there,
            "owned_roots_removed": True,
            "listeners_after": listeners_after,
            "revoke_readback": revoke_evidence,
            "errors": errors,
        }
        try:
            self._clear_ownership_claim()
        except StandError as exc:
            self.cleaned = False
            raise StandError(
                f"Cleanup removed run dir but failed to clear ownership claim: {exc}"
            ) from exc
        self.cleaned = True
        self._child = None
        with self._owned_lock:
            self._owned_children.clear()
        self._release_live_lock()
        return result

    def live_evidence_snapshot(self) -> dict[str, Any]:
        """Capture run/resource/ID state before destructive signal teardown."""
        return {
            "run_id": self.run_id,
            "synthetic_ids": dict(self.synthetic_ids),
            "source": dict(self.manifest.get("source") or {})
            if isinstance(self.manifest.get("source"), dict)
            else self.manifest.get("source"),
            "binary": dict(self.manifest.get("binary") or {})
            if isinstance(self.manifest.get("binary"), dict)
            else self.manifest.get("binary"),
            "readiness": self.readiness,
            "cleaned": self.cleaned,
            "manifest_path": str(self.manifest_path) if self._created_run_dir else None,
        }

    def install_signal_handlers(self) -> None:
        def _handler(signum: int, _frame: Any) -> None:
            evidence = self.live_evidence_snapshot()
            try:
                cleanup_result = self.cleanup()
                evidence["cleanup"] = {
                    "confirmed": bool(self.cleaned),
                    **(cleanup_result if isinstance(cleanup_result, dict) else {}),
                }
            except Exception as exc:  # noqa: BLE001 — still exit with signal status
                evidence["cleanup"] = {
                    "confirmed": bool(self.cleaned),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            finally:
                publish_stand_signal_evidence(evidence)
                raise SystemExit(128 + signum)

        for signum in (signal.SIGTERM, signal.SIGHUP):
            self._signal_handlers[signum] = signal.signal(signum, _handler)

    def restore_signal_handlers(self) -> None:
        for signum, previous in self._signal_handlers.items():
            signal.signal(signum, previous)
        self._signal_handlers.clear()

    def _surface_secondary_cleanup_error(self, primary: BaseException) -> None:
        secondary = self.secondary_cleanup_error
        if secondary is None:
            return
        # Keep the original acquire/readiness error primary; attach secondary as note.
        try:
            primary.add_note(f"secondary cleanup failure: {secondary!r}")
        except Exception:
            pass

    def _unwind_after_failed_acquire(self, *, revoke: bool = False) -> None:
        """Best-effort owned-resource unwind when context acquisition fails.

        Python does not call ``__exit__`` when ``__enter__`` raises, so this path
        must restore process-wide signal handlers and stop only resources this
        stand actually acquired. When cleanup cannot be verified, the recovery
        manifest/live lock are retained. Cleanup failures must not replace the
        original acquire failure: they are recorded on ``secondary_cleanup_error``
        and this helper never raises.
        """
        def _record_secondary(exc: BaseException) -> None:
            # Preserve the first secondary; reinforce/unwind must not clear it.
            if self.secondary_cleanup_error is None:
                self.secondary_cleanup_error = exc

        try:
            has_owned_process = bool(
                self._child is not None or (self.manifest.get("process") or {}).get("pid")
            )
            if has_owned_process:
                if not self.cleaned:
                    try:
                        self.cleanup(revoke=revoke)
                    except BaseException as exc:
                        # Retain recovery manifest when cleanup cannot be verified.
                        _record_secondary(exc)
            else:
                # Admission / pre-start failure: never signal foreign listeners.
                try:
                    self._stop_watchdog()
                    self._close_handshake()
                    if (
                        self._created_run_dir
                        and self.run_dir.exists()
                        and not (self.manifest.get("process") or {}).get("pid")
                    ):
                        try:
                            self._save_ownership_claim(
                                {"status": (self.manifest or {}).get("status") or "prepared"}
                            )
                        except Exception as exc:  # noqa: BLE001
                            _record_secondary(exc)
                        try:
                            shutil.rmtree(self.run_dir)
                        except OSError as exc:
                            failure = StandError(f"pre-spawn rollback rmtree failed: {exc}")
                            _record_secondary(failure)
                            self._record_cleanup_failure(
                                [str(failure)],
                                {"directory_retained": self.run_dir.exists()},
                            )
                        if self.run_dir.exists():
                            if self.secondary_cleanup_error is None:
                                failure = StandError(
                                    "pre-spawn rollback left owned directory"
                                )
                                _record_secondary(failure)
                            if self.manifest.get("status") != "cleanup_failed":
                                self._record_cleanup_failure(
                                    [str(self.secondary_cleanup_error or "directory retained")],
                                    {"directory_retained": True},
                                )
                        else:
                            self._created_run_dir = False
                            self.cleaned = True
                            try:
                                self._clear_ownership_claim()
                            except StandError as exc:
                                self.cleaned = False
                                _record_secondary(exc)
                    # Keep the flock only while an owned created directory remains.
                    # Preexisting collisions / never-created dirs must release.
                    retain_lock = bool(
                        self._created_run_dir and self.run_dir.exists() and not self.cleaned
                    )
                    if not retain_lock:
                        self._release_live_lock()
                except BaseException as exc:
                    _record_secondary(exc)
        except BaseException as exc:
            # Last-resort: unwind itself must stay non-throwing.
            _record_secondary(exc)
        finally:
            try:
                unregister_active_stand(self)
            except BaseException as exc:
                _record_secondary(exc)
            try:
                self.restore_signal_handlers()
            except BaseException as exc:
                _record_secondary(exc)
            try:
                self._stop_watchdog()
            except BaseException as exc:
                _record_secondary(exc)
            try:
                self._close_handshake()
            except BaseException as exc:
                _record_secondary(exc)
            if self.cleaned:
                try:
                    self._release_live_lock()
                except BaseException as exc:
                    _record_secondary(exc)

    def __enter__(self) -> "IsolatedStand":
        self.secondary_cleanup_error = None
        register_active_stand(self)
        self.install_signal_handlers()
        try:
            self.prepare()
            self.start()
            self.wait_ready()
            return self
        except BaseException as exc:
            self._unwind_after_failed_acquire(revoke=False)
            self._surface_secondary_cleanup_error(exc)
            raise

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if not self.cleaned:
                if exc_type is not None:
                    # Ordinary exceptional exits must publish the same structured
                    # run/IDs/cleanup snapshot that signal handlers retain; otherwise
                    # aggregate receipts lose live evidence after teardown deletes
                    # the owned manifest/claim.
                    evidence = self.live_evidence_snapshot()
                    try:
                        cleanup_result = self.cleanup()
                        evidence["cleanup"] = {
                            "confirmed": bool(self.cleaned),
                            **(cleanup_result if isinstance(cleanup_result, dict) else {}),
                        }
                    except Exception as cleanup_exc:  # noqa: BLE001 — still surface primary
                        evidence["cleanup"] = {
                            "confirmed": bool(self.cleaned),
                            "error": f"{type(cleanup_exc).__name__}: {cleanup_exc}",
                        }
                    finally:
                        publish_stand_signal_evidence(evidence)
                else:
                    self.cleanup()
        finally:
            unregister_active_stand(self)
            self.restore_signal_handlers()
            self._stop_watchdog()
            self._close_handshake()
            if self.cleaned:
                self._release_live_lock()


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StandError(f"Manifest unreadable: {path}: {exc}") from exc
    if data.get("schema") != MANIFEST_SCHEMA:
        raise StandError(f"Unexpected manifest schema at {path}")
    return data


def _pids_agree(*values: Any) -> bool:
    present = [value for value in values if value is not None and value != ""]
    if not present:
        return True
    normalized = {str(int(value)) for value in present}
    return len(normalized) == 1


def _durable_process_identity(manifest: dict[str, Any]) -> dict[str, str] | None:
    """Return a strong start identity only when PID bindings are consistent."""
    process = manifest.get("process") or {}
    receipt = manifest.get("launch_receipt") or {}
    identity = process.get("identity")
    process_pid = process.get("pid")
    receipt_pid = receipt.get("pid")
    manifest_run_id = manifest.get("run_id")
    receipt_run_id = receipt.get("run_id")

    if receipt and receipt_run_id is not None and manifest_run_id is not None:
        if str(receipt_run_id) != str(manifest_run_id):
            raise StandError(
                "Manifest/receipt run_id binding mismatch; refusing process signalling"
            )

    if isinstance(identity, dict) and identity.get("pid") and identity.get("lstart"):
        identity_pid = identity.get("pid")
        if not _pids_agree(identity_pid, process_pid, receipt_pid):
            raise StandError(
                "Manifest process PID/identity binding mismatch; "
                "refusing process signalling"
            )
        return {
            "pid": str(int(identity_pid)),
            "lstart": str(identity.get("lstart")),
            "args": str(identity.get("args") or ""),
        }

    receipt_identity = receipt.get("identity")
    if (
        isinstance(receipt_identity, dict)
        and receipt_identity.get("pid")
        and receipt_identity.get("lstart")
    ):
        identity_pid = receipt_identity.get("pid")
        if not _pids_agree(identity_pid, process_pid, receipt_pid):
            raise StandError(
                "Manifest/receipt PID/identity binding mismatch; "
                "refusing process signalling"
            )
        return {
            "pid": str(int(identity_pid)),
            "lstart": str(receipt_identity.get("lstart")),
            "args": str(receipt_identity.get("args") or ""),
        }
    if process_pid is not None and receipt_pid is not None and not _pids_agree(process_pid, receipt_pid):
        raise StandError(
            "Manifest/receipt PID binding mismatch; refusing process signalling"
        )
    return None


def _pid_still_alive(pid: int) -> bool:
    try:
        process_start_identity(pid)
        return True
    except StandError:
        return False


def _stop_manifest_process(manifest: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    process = manifest.get("process") or {}
    pid = process.get("pid")
    try:
        identity = _durable_process_identity(manifest)
    except StandError as exc:
        return False, [str(exc)]
    stopped = False
    if identity:
        # Signal only the PID that was validated together with start identity.
        signal_pid = int(identity["pid"])
        if process_matches_identity(identity):
            try:
                os.kill(signal_pid, signal.SIGTERM)
            except OSError as exc:
                errors.append(f"SIGTERM failed: {exc}")
            for _ in range(50):
                if not process_matches_identity(identity):
                    stopped = True
                    break
                time.sleep(0.1)
            if process_matches_identity(identity):
                try:
                    os.kill(signal_pid, signal.SIGKILL)
                except OSError as exc:
                    errors.append(f"SIGKILL failed: {exc}")
                time.sleep(0.2)
                stopped = not process_matches_identity(identity)
        else:
            stopped = True  # PID gone or reused by unrelated process; do not kill.
    elif pid:
        # Never signal an identity-less PID from a receipt number alone. An
        # unreleased handshake child exits on EOF when its manager dies; wait
        # briefly for that self-exit, then refuse and retain unknown state.
        for _ in range(30):
            if not _pid_still_alive(int(pid)):
                stopped = True
                break
            time.sleep(0.1)
        if not stopped:
            errors.append(
                "Manifest process lacks durable start identity; "
                "refusing blind kill of unreclaimed PID"
            )
            stopped = False
    else:
        stopped = True
    return stopped, errors


def _stop_owned_children_from_manifest(manifest: dict[str, Any]) -> tuple[bool, list[str]]:
    """Stop every durable owned restore child recorded on the manifest/claim."""
    errors: list[str] = []
    all_stopped = True
    for record in list(manifest.get("owned_children") or []):
        if not isinstance(record, dict):
            errors.append("owned_children entry is not an object")
            all_stopped = False
            continue
        synthetic = {
            "run_id": manifest.get("run_id"),
            "process": {
                "pid": record.get("pid"),
                "identity": record.get("identity"),
            },
            "launch_receipt": record.get("launch_receipt") or {
                "run_id": manifest.get("run_id"),
                "pid": record.get("pid"),
                "identity": record.get("identity"),
            },
        }
        # Prefer target-local launch receipt when present.
        root = record.get("root")
        if root:
            receipt_path = Path(root) / LAUNCH_RECEIPT_NAME
            if receipt_path.is_file():
                try:
                    synthetic["launch_receipt"] = json.loads(
                        receipt_path.read_text(encoding="utf-8")
                    )
                except (OSError, json.JSONDecodeError):
                    pass
        stopped, child_errors = _stop_manifest_process(synthetic)
        errors.extend(child_errors)
        if not stopped:
            all_stopped = False
    return all_stopped, errors


def cleanup_manifest(profile: StandProfile, manifest: dict[str, Any], *, manifest_path: Path | None = None) -> dict[str, Any]:
    bound_dir = bind_manifest_paths(profile, manifest, manifest_path=manifest_path)
    run_id = validate_run_id(str(manifest.get("run_id") or ""))
    # Attach launch receipt into manifest view when present for pre-identity recovery.
    receipt_path = bound_dir / LAUNCH_RECEIPT_NAME
    if receipt_path.is_file():
        try:
            manifest = dict(manifest)
            manifest["launch_receipt"] = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    # Durable outside-tree claim before any recursive delete.
    try:
        write_ownership_claim(profile, ownership_claim_from_manifest(manifest))
    except StandError:
        # Still attempt stop/delete; failure persistence below will re-raise claim errors.
        pass
    owned_stopped, owned_errors = _stop_owned_children_from_manifest(manifest)
    errors: list[str] = list(owned_errors)
    process = manifest.get("process") or {}
    process_role = process.get("role")
    owned_roles = {
        item.get("role")
        for item in (manifest.get("owned_children") or [])
        if isinstance(item, dict)
    }
    if process_role and process_role in owned_roles:
        # Primary process binding already covered by owned_children stop.
        stopped = owned_stopped
    else:
        primary_stopped, primary_errors = _stop_manifest_process(manifest)
        errors.extend(primary_errors)
        stopped = bool(primary_stopped and owned_stopped)
    listeners_after = {str(port): sorted(listeners(port)) for port in profile.all_ports}
    busy = {port: pids for port, pids in listeners_after.items() if pids}
    still_matches = False
    if not (process_role and process_role in owned_roles):
        try:
            identity = _durable_process_identity(manifest)
        except StandError as exc:
            identity = None
            if str(exc) not in errors:
                errors.append(str(exc))
        still_matches = bool(identity and process_matches_identity(identity))
    for record in manifest.get("owned_children") or []:
        if not isinstance(record, dict):
            continue
        child_identity = record.get("identity")
        if isinstance(child_identity, dict) and process_matches_identity(child_identity):
            still_matches = True
            break
    owned_records = list(manifest.get("owned_roots") or [])
    owned_roots: list[Path] = []
    for raw in owned_records:
        try:
            owned_roots.append(
                assert_owned_root_under_run_root(profile.run_root, owned_root_path(raw))
            )
        except StandError as exc:
            errors.append(str(exc))
    if errors or not stopped or busy or still_matches:
        # Retain recovery state; do not destroy the only retry information.
        failure_payload = {
            "at": datetime.now(timezone.utc).isoformat(),
            "errors": errors,
            "process_stopped": stopped,
            "still_matches_identity": still_matches,
            "listeners_after": listeners_after,
            "directory_retained": True,
            "owned_roots": list(owned_records),
        }
        failure = {
            "schema": MANIFEST_SCHEMA,
            **manifest,
            "status": "cleanup_failed",
            "cleanup_failure": failure_payload,
        }
        try:
            write_ownership_claim(profile, ownership_claim_from_manifest(failure))
        except StandError as exc:
            errors.append(f"failed to persist durable ownership claim: {exc}")
        if manifest_path is not None and bound_dir.exists():
            try:
                atomic_write_secure(
                    manifest_path if manifest_path.parent == bound_dir else bound_dir / "resource-manifest.json",
                    json.dumps(failure, indent=2, sort_keys=True) + "\n",
                    profile.sensitive_file_mode,
                )
            except OSError as exc:
                errors.append(f"failed to persist in-tree cleanup_failure: {exc}")
        raise StandError(
            f"Orphan cleanup failed; recovery state retained: "
            f"{({'run_id': manifest.get('run_id'), 'errors': errors, 'listeners_after': listeners_after, 'stopped': stopped})}"
        )
    for raw in owned_records:
        try:
            delete_owned_root_record(profile.run_root, raw)
        except StandError as exc:
            errors.append(str(exc))
    leftovers = [
        str(path) for path in owned_roots if path.exists() or path.is_symlink()
    ]
    if not errors and not leftovers and bound_dir.exists():
        try:
            shutil.rmtree(bound_dir)
        except OSError as exc:
            errors.append(f"rmtree failed: {exc}")
    if errors or leftovers or bound_dir.exists():
        failure_payload = {
            "at": datetime.now(timezone.utc).isoformat(),
            "errors": errors + ([f"owned roots retained: {leftovers}"] if leftovers else []),
            "process_stopped": stopped,
            "listeners_after": listeners_after,
            "directory_retained": bound_dir.exists(),
            "owned_roots_retained": leftovers,
        }
        failure = {
            "schema": MANIFEST_SCHEMA,
            **manifest,
            "status": "cleanup_failed",
            "cleanup_failure": failure_payload,
        }
        write_ownership_claim(profile, ownership_claim_from_manifest(failure))
        if bound_dir.exists():
            try:
                atomic_write_secure(
                    bound_dir / "resource-manifest.json",
                    json.dumps(failure, indent=2, sort_keys=True) + "\n",
                    profile.sensitive_file_mode,
                )
            except OSError:
                pass
        raise StandError(
            f"Orphan cleanup failed; recovery state retained: "
            f"{({'run_id': run_id, 'errors': failure_payload['errors'], 'listeners_after': listeners_after, 'owned_roots_retained': leftovers})}"
        )
    result = {
        "run_id": manifest.get("run_id"),
        "process_stopped": stopped,
        "directory_removed": not bound_dir.exists(),
        "owned_roots_removed": True,
        "listeners_after": listeners_after,
    }
    clear_ownership_claim(profile, run_id)
    return result


def cleanup_orphaned_runs(
    profile: StandProfile,
    *,
    require_exclusive_owner: bool = True,
    owned_live_lock: LiveRunLock | None = None,
) -> list[dict[str, Any]]:
    """Clean orphaned run dirs using PID/start identity under the live lock.

    Every entry point must either pass an already-held LiveRunLock capability or
    acquire the lock nonblocking. Another live owner is refused so active run
    directories and services cannot be deleted/signalled by a concurrent recovery.
    """
    if not require_exclusive_owner and owned_live_lock is None:
        raise StandError(
            "Orphan cleanup refuses to run without exclusive live-lock ownership"
        )
    acquired: LiveRunLock | None = None
    try:
        if owned_live_lock is not None:
            if not owned_live_lock.held:
                raise StandError("owned live lock is not held")
            if owned_live_lock.run_root.resolve() != profile.run_root.resolve():
                raise StandError("owned live lock belongs to a different run_root")
        else:
            acquired = LiveRunLock(profile.run_root)
            acquired.acquire()
        results: list[dict[str, Any]] = []
        if not profile.run_root.exists():
            return results
        seen: set[str] = set()
        for child in sorted(profile.run_root.iterdir()):
            if not child.is_dir():
                continue
            # Skip non run-id children (journals, lock siblings, ownership dir).
            if child.name in {OWNERSHIP_DIR_NAME, LIVE_LOCK_NAME}:
                continue
            try:
                validate_run_id(child.name)
            except StandError:
                continue
            manifest_path = child / "resource-manifest.json"
            if not manifest_path.is_file():
                continue
            manifest = load_manifest(manifest_path)
            seen.add(child.name)
            results.append(cleanup_manifest(profile, manifest, manifest_path=manifest_path))
        # Reclaim bound directories whose in-tree manifest was erased mid-rmtree
        # but still have a durable ownership claim outside the tree.
        claims_root = ownership_dir(profile)
        if claims_root.is_dir():
            for claim_path in sorted(claims_root.glob("*.json")):
                try:
                    claim = json.loads(claim_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise StandError(f"Ownership claim unreadable: {claim_path}: {exc}") from exc
                if claim.get("schema") != OWNERSHIP_SCHEMA:
                    raise StandError(f"Unexpected ownership claim schema at {claim_path}")
                run_id = validate_run_id(str(claim.get("run_id") or claim_path.stem))
                if run_id in seen:
                    continue
                bound = assert_immediate_run_child(
                    profile.run_root, run_id, Path(claim.get("run_dir") or (profile.run_root / run_id))
                )
                if not bound.exists():
                    clear_ownership_claim(profile, run_id)
                    continue
                # Prefer a recreated/remaining in-tree manifest when present.
                manifest_path = bound / "resource-manifest.json"
                if manifest_path.is_file():
                    manifest = load_manifest(manifest_path)
                else:
                    manifest = {
                        "schema": MANIFEST_SCHEMA,
                        "run_id": run_id,
                        "run_dir": str(bound),
                        "status": claim.get("status") or "cleanup_failed",
                        "process": claim.get("process"),
                        "launch_receipt": claim.get("launch_receipt"),
                        "owned_roots": list(claim.get("owned_roots") or []),
                        "owned_children": list(claim.get("owned_children") or []),
                        "ports": claim.get("ports") or dict(profile.ports),
                        "listen_host": claim.get("listen_host") or profile.listen_host,
                        "cleanup_failure": claim.get("cleanup_failure"),
                        "source": claim.get("source"),
                        "binary": claim.get("binary"),
                        "created_at": claim.get("created_at"),
                    }
                seen.add(run_id)
                results.append(
                    cleanup_manifest(profile, manifest, manifest_path=manifest_path)
                )
        return results
    finally:
        if acquired is not None:
            acquired.release()


def resolve_existing_path_prefix(path: Path) -> Path:
    """Resolve path with symlink-safe existing prefix; keep missing leaf names."""
    path = path.expanduser()
    if not path.is_absolute():
        raise StandError(f"Path must be absolute before containment checks: {path}")
    current = path
    missing: list[str] = []
    while True:
        if current.exists() or current.is_symlink():
            resolved = current.resolve()
            for part in reversed(missing):
                resolved = resolved / part
            return resolved
        if current.parent == current:
            raise StandError(f"Cannot resolve path containment for {path}")
        missing.append(current.name)
        current = current.parent


def validate_journal_path(profile: StandProfile, journal: str | Path) -> Path:
    """Require journal path under approved run_root, outside Git, no symlink escape."""
    raw = Path(journal).expanduser()
    root = profile.run_root.resolve()
    if not raw.is_absolute():
        candidate = root / raw
    else:
        candidate = raw
    resolved = resolve_existing_path_prefix(candidate)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise StandError(
            f"Journal path must stay under approved run_root {root}; got {resolved}"
        ) from exc
    if resolved == root or len(relative.parts) == 0:
        raise StandError("Journal path must be a file under approved run_root, not the root itself")
    if any(part in {".", ".."} for part in relative.parts):
        raise StandError(f"Journal path rejected unsafe relative parts: {resolved}")
    # Defense in depth: refuse Git worktrees even if a future profile drifts.
    _assert_not_git_worktree(resolved.parent if resolved.parent.exists() else root)
    return resolved


def assert_suite_result_contract(result: Any) -> dict[str, Any]:
    """Fail closed unless a suite reports a nonempty executed-scenario contract."""
    if not isinstance(result, dict):
        raise StandError(
            f"Suite result must be a dict with executed scenarios; got {type(result).__name__}"
        )
    if not result:
        raise StandError("Suite produced empty result; no scenarios executed")
    count = result.get("executed_count")
    if not isinstance(count, int) or count <= 0:
        raise StandError(
            f"Suite executed_count must be a positive int; got {count!r}"
        )
    return result


def append_journal(
    path: Path,
    *,
    argv: list[str],
    rc: int,
    suite: str,
    mode: str,
    extra: dict[str, Any] | None = None,
    mode_bits: int = 0o600,
    profile: StandProfile | None = None,
) -> None:
    if profile is not None:
        path = validate_journal_path(profile, path)
    else:
        # Callers without a profile still must not invent parents via relative CWD.
        if not path.is_absolute():
            raise StandError("Journal path must be absolute when profile is not provided")
        path = resolve_existing_path_prefix(path)
        _assert_not_git_worktree(path.parent if path.parent.exists() else path)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    if profile is not None:
        # Re-check after mkdir in case of races/symlink swaps on parent.
        path = validate_journal_path(profile, path)
    existing: dict[str, Any]
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
    else:
        existing = {"schema": JOURNAL_SCHEMA, "entries": []}
    entry = {
        "at": datetime.now(timezone.utc).isoformat(),
        "argv": list(argv),
        "rc": rc,
        "suite": suite,
        "mode": mode,
    }
    if extra:
        entry["extra"] = extra
    existing.setdefault("entries", []).append(entry)
    atomic_write_secure(path, json.dumps(existing, indent=2, sort_keys=True) + "\n", mode_bits)


def run_stand_cycle(
    profile: StandProfile | None = None,
    *,
    worktree: Path | None = None,
    revoke_token: bool = True,
    on_ready: Callable[[IsolatedStand], None] | None = None,
    stand: IsolatedStand | None = None,
) -> dict[str, Any]:
    if stand is None:
        profile = profile or load_profile()
        stand = IsolatedStand(profile=profile, worktree=worktree)
    stand.secondary_cleanup_error = None
    try:
        with stand:
            fixtures = stand.create_synthetic_session()
            if on_ready is not None:
                # Watchdog continues during callback/idle time.
                if stand._watchdog is not None:
                    stand._watchdog.raise_if_failed()
                on_ready(stand)
                if stand._watchdog is not None:
                    stand._watchdog.raise_if_failed()
                stand._enforce_limits()
            revoke_evidence = None
            if revoke_token:
                revoke_evidence = stand.revoke_token()
            result = {
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "synthetic_ids": fixtures,
                "revoke_readback": revoke_evidence,
                "source": stand.manifest.get("source"),
                "binary": stand.manifest.get("binary"),
                "file_modes": dict(stand.file_modes),
            }
        result["cleanup"] = {
            "confirmed": stand.cleaned,
        }
        return result
    except BaseException as exc:
        # Covers __enter__ failures (no __exit__) and reinforces handler restore
        # if a with-body / cleanup path left process-wide handlers installed.
        # Unwind is non-throwing so the original acquire/body error stays primary.
        stand._unwind_after_failed_acquire(revoke=False)
        stand._surface_secondary_cleanup_error(exc)
        raise
