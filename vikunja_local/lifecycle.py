"""EA-010 s08: read-only existing-service preflight + Hermes three-patch rehearsal.

Owner clarification SHA256 f50fa84a380c9538b2dfcc3ab4363ab07cd9fa4775acaa569687ee75317f10d4
excludes a universal filesystem apply/rollback engine. This module keeps:

- a small read-only check of pinned C4/C5 evidence and service policy;
- one-shot exact three-patch rehearsal on a newly created temporary tree
  under the approved stand run-root: ordered apply → hash → reverse → hash,
  with deletion on every outcome. No cross-invocation state, resumable
  deployment, or per-file restore machinery.

Coordinator rehearsal procedure SHA256
6286618c4fe26a197db1526b1fe31ffd24257c06f62c149409db6d090144ba34.
"""

from __future__ import annotations

import json
import os
import plistlib
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

SERVICE_SCHEMA = "ea010-service-config/v1"
REHEARSAL_SCHEMA = "ea010-hermes-patch-rehearsal/v1"
STAND_PROFILE_SCHEMA = "ea010-isolated-stand-profile/v1"
SUPERVISOR_LABEL = "local.cto.vikunja"
C4_C5_SCOPE_SHA256 = "772a44d23db6a113a7a57dd02a40abdf51e114917e11b516cebd826e47565def"
OBSERVED_BINDINGS_SHA256 = "97fb13487a3437a93a4fb5bfa0460156aa7822c89e7f49c16723b4dd3d3e78b6"
OWNER_SCOPE_CLARIFICATION_SHA256 = (
    "f50fa84a380c9538b2dfcc3ab4363ab07cd9fa4775acaa569687ee75317f10d4"
)
PATCH_REHEARSAL_PROCEDURE_SHA256 = (
    "6286618c4fe26a197db1526b1fe31ffd24257c06f62c149409db6d090144ba34"
)
DEFAULT_STAND_PROFILE = Path("/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json")
STAND_PROFILE_ENV = "EA010_ISOLATED_STAND_PROFILE"
UI_MANIFEST_REL = Path("integration/hermes/ui-manifest.json")
BACKEND_MANIFEST_REL = Path("integration/hermes/backend-manifest.json")
MODES = ("dry-run", "rehearse")
FILE_MODE = 0o600
DIR_MODE = 0o700
SESSION_ENABLED = b"1\n"
SESSION_DISABLED = b"0\n"
SESSION_FLAG_NAME = "test-session-send.enabled"

SOURCE_RELS = (
    "plugins/personal-tasks/dashboard/plugin_api.py",
    "scripts/ea_core.py",
    "plugins/personal-tasks/dashboard/dist/index.js",
)
PATCH_ORDER = (
    "integration/hermes/backend.patch",
    "integration/hermes/ui-readiness.patch",
    "integration/hermes/ui.patch",
)


class LifecycleError(RuntimeError):
    def __init__(self, message: str, *, code: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _require_abs(path: Path | str, *, label: str) -> Path:
    value = Path(path)
    if not value.is_absolute():
        raise LifecycleError(f"{label} must be absolute", code="path_not_absolute", details={"path": str(value)})
    return value


def _canonical(path: Path) -> Path:
    if path.exists() and path.is_symlink():
        raise LifecycleError(f"symlink rejected: {path}", code="symlink_rejected", details={"path": str(path)})
    return path.resolve()


def _is_strict_descendant(root: Path, child: Path) -> bool:
    try:
        child.relative_to(root)
    except ValueError:
        return False
    return child != root


def _assert_not_git_worktree(path: Path) -> None:
    cur = path
    for _ in range(64):
        if (cur / ".git").exists():
            raise LifecycleError(
                f"path must not be inside a Git worktree: {path}",
                code="git_worktree_forbidden",
                details={"path": str(path), "git": str(cur / ".git")},
            )
        if cur.parent == cur:
            return
        cur = cur.parent


def load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise LifecycleError("JSON root must be an object", code="json_root", details={"path": str(path)})
    return raw


def load_approved_run_root(*, profile_path: Path | str | None = None) -> Path:
    if profile_path is None:
        env = os.environ.get(STAND_PROFILE_ENV)
        path = Path(env).expanduser() if env else DEFAULT_STAND_PROFILE
    else:
        path = Path(profile_path).expanduser()
    path = _require_abs(path, label="stand_profile")
    if not path.is_file():
        raise LifecycleError(f"stand profile missing: {path}", code="stand_profile_missing")
    raw = load_json(path)
    if raw.get("schema") != STAND_PROFILE_SCHEMA:
        raise LifecycleError("unexpected stand profile schema", code="stand_profile_schema")
    if raw.get("mode") != "isolated":
        raise LifecycleError("stand profile mode must be isolated", code="stand_profile_mode")
    run_root = _require_abs(raw.get("run_root", ""), label="stand_profile.run_root")
    resolved = run_root.resolve()
    if str(resolved) != str(run_root):
        raise LifecycleError(
            "stand profile run_root must be a resolved absolute path without symlinks",
            code="stand_profile_run_root",
            details={"configured": str(run_root), "resolved": str(resolved)},
        )
    _assert_not_git_worktree(resolved)
    return resolved


def _bind_synthetic_contour(contour_root: Path, *, run_root: Path | None = None) -> Path:
    approved = run_root or load_approved_run_root()
    root = _canonical(_require_abs(contour_root, label="contour_root"))
    if root.is_symlink():
        raise LifecycleError("contour_root must not be a symlink", code="symlink_rejected")
    if not _is_strict_descendant(approved, root):
        raise LifecycleError(
            "synthetic contour_root must be a disposable child of the approved stand run-root",
            code="contour_unbound",
            details={"contour_root": str(root), "approved_run_root": str(approved)},
        )
    if not root.is_dir():
        raise LifecycleError("contour_root must be a directory", code="contour_missing")
    return root


def _atomic_write(path: Path, data: bytes, *, mode: int = FILE_MODE) -> None:
    path = _require_abs(path, label="atomic_write.path")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        try:
            os.write(fd, data)
            os.fsync(fd)
            os.fchmod(fd, mode)
        finally:
            os.close(fd)
        os.replace(tmp, path)
        tmp = Path()
        os.chmod(path, mode)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _parse_hex(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise LifecycleError(f"{label} must be a sha256 hex digest", code="hash_format", details={"label": label})
    try:
        int(value, 16)
    except ValueError as exc:
        raise LifecycleError(f"{label} must be a sha256 hex digest", code="hash_format") from exc
    return value.lower()


def read_only_bindings_preflight(bindings_doc: Path | str) -> dict[str, Any]:
    """Read-only check that the owner evidence document still matches the pinned digest."""
    path = _require_abs(bindings_doc, label="bindings_doc")
    if not path.is_file():
        raise LifecycleError("observed bindings document missing", code="bindings_missing", details={"path": str(path)})
    digest = sha256_file(path)
    ok = digest == OBSERVED_BINDINGS_SHA256
    result = {
        "ok": ok,
        "path": str(path),
        "sha256": digest,
        "expected_sha256": OBSERVED_BINDINGS_SHA256,
        "role": "read_only_preflight_evidence_not_write_whitelist",
        "writes_performed": 0,
    }
    if not ok:
        raise LifecycleError("observed bindings digest mismatch", code="bindings_drift", details=result)
    return result


def read_only_scope_preflight(scope_doc: Path | str) -> dict[str, Any]:
    path = _require_abs(scope_doc, label="scope_doc")
    if not path.is_file():
        raise LifecycleError("C4/C5 scope document missing", code="scope_missing", details={"path": str(path)})
    digest = sha256_file(path)
    ok = digest == C4_C5_SCOPE_SHA256
    result = {
        "ok": ok,
        "path": str(path),
        "sha256": digest,
        "expected_sha256": C4_C5_SCOPE_SHA256,
        "writes_performed": 0,
    }
    if not ok:
        raise LifecycleError("C4/C5 scope digest mismatch", code="scope_drift", details=result)
    return result


def _require_admitted_program_arguments(raw: Mapping[str, Any]) -> list[str]:
    admitted = raw.get("admitted_program_arguments")
    if not isinstance(admitted, list) or not admitted:
        raise LifecycleError(
            "admitted_program_arguments must be an explicit nonempty list",
            code="admitted_argv_missing",
        )
    normalized: list[str] = []
    for index, item in enumerate(admitted):
        if not isinstance(item, str) or not item.strip():
            raise LifecycleError(
                "admitted_program_arguments entries must be nonempty strings",
                code="admitted_argv_missing",
                details={"index": index},
            )
        normalized.append(item)
    _require_abs(normalized[0], label="admitted_program_arguments[0]")
    return normalized


def load_service_config(path: Path | str) -> dict[str, Any]:
    config_path = _require_abs(path, label="service_config")
    raw = load_json(config_path)
    if raw.get("schema") != SERVICE_SCHEMA:
        raise LifecycleError("unexpected service config schema", code="service_schema")
    if raw.get("supervisor_label") != SUPERVISOR_LABEL:
        raise LifecycleError("supervisor_label must be local.cto.vikunja", code="supervisor_label")
    if raw.get("contour") != "isolated_synthetic":
        raise LifecycleError("service config admits only isolated_synthetic", code="contour_forbidden")
    if raw.get("forbid_second_supervisor") is not True:
        raise LifecycleError("forbid_second_supervisor must be true", code="supervisor_policy")
    if raw.get("reuse_existing_supervisor") is not True:
        raise LifecycleError("reuse_existing_supervisor must be true", code="supervisor_reuse")
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise LifecycleError("paths object required", code="service_paths")
    for key in (
        "contour_root",
        "plist",
        "config",
        "installation_manifest",
        "database",
        "files",
        "binary",
        "rehearsal_manifest",
    ):
        value = paths.get(key)
        if not isinstance(value, str) or not value:
            raise LifecycleError(f"paths.{key} required", code="service_path_missing", details={"key": key})
        _require_abs(value, label=f"paths.{key}")
    raw["admitted_program_arguments"] = _require_admitted_program_arguments(raw)
    return raw


def describe_post_login(config: Mapping[str, Any]) -> dict[str, Any]:
    text = str(config.get("post_login_pilot") or "")
    if not text.strip():
        raise LifecycleError("post_login_pilot text required", code="post_login_missing")
    return {
        "supervisor_label": SUPERVISOR_LABEL,
        "post_login_pilot": text,
        "production_touch": False,
        "working_service_mutations": False,
        "filevault_sleep_ssh_power_unchanged": True,
        "note": "Describe-only; does not start/stop launchd or touch the working Mini service.",
    }


def existing_service_preflight(config: Mapping[str, Any]) -> dict[str, Any]:
    """Small read-only check of pinned evidence + synthetic supervisor fixture policy."""
    observed = config.get("observed_read_only_bindings_doc") or {}
    bindings_path = observed.get("path")
    if not bindings_path:
        raise LifecycleError("observed_read_only_bindings_doc.path required", code="bindings_path")
    bindings = read_only_bindings_preflight(bindings_path)
    scope = config.get("c4_c5_scope") or {}
    scope_path = scope.get("path")
    scope_result = read_only_scope_preflight(scope_path) if scope_path else None

    paths = config["paths"]
    contour = _bind_synthetic_contour(Path(str(paths["contour_root"])))
    plist_path = _canonical(_require_abs(paths["plist"], label="paths.plist"))
    if not _is_strict_descendant(contour, plist_path) and plist_path.parent != contour:
        # Allow plist directly under contour or deeper.
        if not str(plist_path).startswith(str(contour) + os.sep):
            raise LifecycleError("plist must stay inside synthetic contour", code="path_escape")
    if not plist_path.is_file():
        raise LifecycleError("synthetic supervisor plist missing", code="supervisor_missing", details={"path": str(plist_path)})
    data = plistlib.loads(plist_path.read_bytes())
    if data.get("Label") != SUPERVISOR_LABEL:
        raise LifecycleError("plist Label mismatch", code="supervisor_label")
    if data.get("RunAtLoad") is not True:
        raise LifecycleError("post-login pilot requires RunAtLoad=true", code="supervisor_run_at_load")
    argv = list(data.get("ProgramArguments") or [])
    admitted = _require_admitted_program_arguments(config)
    binary = _canonical(_require_abs(paths["binary"], label="paths.binary"))
    config_path = _canonical(_require_abs(paths["config"], label="paths.config"))
    admitted_binary = _canonical(_require_abs(admitted[0], label="admitted_program_arguments[0]"))
    if admitted_binary != binary:
        raise LifecycleError(
            "admitted argv[0] must bind to paths.binary",
            code="admitted_argv_binding",
            details={"admitted": admitted[0], "binary": str(binary)},
        )
    try:
        config_index = admitted.index("--config")
    except ValueError as exc:
        raise LifecycleError(
            "admitted argv must include --config bound to paths.config",
            code="admitted_argv_binding",
            details={"admitted": admitted},
        ) from exc
    if config_index + 1 >= len(admitted):
        raise LifecycleError(
            "admitted argv --config is missing its path argument",
            code="admitted_argv_binding",
            details={"admitted": admitted},
        )
    admitted_config = _canonical(
        _require_abs(admitted[config_index + 1], label="admitted_program_arguments[--config]")
    )
    if admitted_config != config_path:
        raise LifecycleError(
            "admitted --config must bind to paths.config",
            code="admitted_argv_binding",
            details={"admitted_config": str(admitted_config), "config": str(config_path)},
        )
    if not binary.is_file():
        raise LifecycleError(
            "configured binary missing",
            code="binary_missing",
            details={"path": str(binary)},
        )
    if not config_path.is_file():
        raise LifecycleError(
            "configured config missing",
            code="config_missing",
            details={"path": str(config_path)},
        )
    if argv != admitted:
        raise LifecycleError(
            "plist ProgramArguments drift from admitted binding",
            code="supervisor_argv",
            details={"expected": admitted, "actual": argv},
        )
    agents_dir = plist_path.parent
    competing = []
    for sibling in sorted(agents_dir.glob("*.plist")):
        try:
            other = plistlib.loads(sibling.read_bytes())
        except Exception as exc:
            raise LifecycleError(
                "unreadable competing LaunchAgent",
                code="second_supervisor",
                details={"path": str(sibling)},
            ) from exc
        if sibling.resolve() == plist_path.resolve():
            continue
        if other.get("Label") == SUPERVISOR_LABEL:
            competing.append(str(sibling))
            continue
        other_args = list(other.get("ProgramArguments") or [])
        if other_args and argv and other_args[0] == argv[0]:
            # Same binary head on another agent is a second contour for this fixture.
            competing.append(str(sibling))
    if competing:
        raise LifecycleError(
            "second supervisor / competing contour present",
            code="second_supervisor",
            details={"competing": competing},
        )
    return {
        "ok": True,
        "mode": "preflight",
        "supervisor_label": SUPERVISOR_LABEL,
        "bindings": bindings,
        "scope": scope_result,
        "plist": str(plist_path),
        "supervisor_plist_count": 1,
        "writes_performed": 0,
        "working_service_mutations": False,
        "owner_scope_clarification_sha256": OWNER_SCOPE_CLARIFICATION_SHA256,
        "patch_rehearsal_procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
    }


@dataclass(frozen=True)
class PublishedPatch:
    key: str
    relative_path: str
    absolute_path: Path
    sha256: str


@dataclass(frozen=True)
class PublishedSource:
    relative_path: str
    absolute_path: Path
    from_sha256: str
    to_sha256: str


@dataclass(frozen=True)
class PatchSequence:
    worktree: Path
    sources: tuple[PublishedSource, ...]
    patches: tuple[PublishedPatch, ...]
    ui_manifest_sha256: str
    backend_manifest_sha256: str


@dataclass(frozen=True)
class RehearsalContour:
    raw: dict[str, Any]
    contour_root: Path
    session_flag: Path
    preserve: tuple[Path, ...]
    sequence: PatchSequence
    worktree: Path


def _session_disabled_marker(flag_path: Path) -> Path:
    return Path(str(flag_path) + ".disabled")


def _session_enabled(flag_path: Path) -> bool:
    return flag_path.is_file() and flag_path.read_bytes() == SESSION_ENABLED


def _pause_test_session(flag_path: Path) -> None:
    marker = _session_disabled_marker(flag_path)
    if flag_path.exists():
        data = flag_path.read_bytes()
        _atomic_write(marker, SESSION_DISABLED if data == SESSION_ENABLED else data)
        flag_path.unlink()
    else:
        _atomic_write(marker, SESSION_DISABLED)


def _ensure_test_session(flag_path: Path) -> None:
    marker = _session_disabled_marker(flag_path)
    if marker.exists() and not flag_path.exists():
        marker.unlink()
    _atomic_write(flag_path, SESSION_ENABLED)


def _assert_session_marker_paths(
    flag_path: Path,
    *,
    contour_root: Path,
    preserve: Sequence[Path],
    sequence: PatchSequence,
) -> None:
    """Reject session markers that collide with preserve/source files.

    Marker writes happen before patch try/except and during reverse pause; overlapping
    paths would destroy journal/receipt/source bytes. Validation must fail closed
    with no writes.
    """
    disabled = _session_disabled_marker(flag_path)
    dedicated = contour_root / SESSION_FLAG_NAME
    if flag_path != dedicated:
        raise LifecycleError(
            "test_session.enabled_flag_path must be the dedicated contour marker "
            f"{SESSION_FLAG_NAME}",
            code="session_marker_conflict",
            details={
                "enabled_flag_path": str(flag_path),
                "required": str(dedicated),
            },
        )
    reserved: dict[str, str] = {}
    for path in preserve:
        reserved[str(path)] = "preserve"
    for source in sequence.sources:
        reserved[str(contour_root / source.relative_path)] = "source"
    for path, role in (
        (flag_path, "session_flag"),
        (disabled, "session_disabled_marker"),
    ):
        conflict = reserved.get(str(path))
        if conflict is not None:
            raise LifecycleError(
                f"session marker collides with {conflict}: {path}",
                code="session_marker_conflict",
                details={
                    "marker": str(path),
                    "role": role,
                    "collides_with": conflict,
                },
            )


def load_published_patch_sequence(worktree: Path | str) -> PatchSequence:
    root = _canonical(_require_abs(worktree, label="worktree"))
    ui_path = root / UI_MANIFEST_REL
    backend_path = root / BACKEND_MANIFEST_REL
    if not ui_path.is_file():
        raise LifecycleError("ui-manifest missing", code="ui_manifest_missing", details={"path": str(ui_path)})
    if not backend_path.is_file():
        raise LifecycleError("backend-manifest missing", code="backend_manifest_missing")
    ui = load_json(ui_path)
    backend = load_json(backend_path)
    if ui.get("schema") != "ea010-hermes-ui-manifest/v1":
        raise LifecycleError("unexpected ui-manifest schema", code="ui_manifest_schema")
    if backend.get("schema") != "ea010-hermes-backend-manifest/v1":
        raise LifecycleError("unexpected backend-manifest schema", code="backend_manifest_schema")
    if ui.get("production_apply") is True or backend.get("production_apply") is True:
        raise LifecycleError("published manifests forbid production_apply", code="production_forbidden")

    baseline = ui.get("baseline") or {}
    patches_meta = ui.get("patches") or {}
    sources = (
        PublishedSource(
            relative_path="plugins/personal-tasks/dashboard/plugin_api.py",
            absolute_path=_require_abs(baseline["plugin_api.py"]["absolute"], label="plugin_api absolute"),
            from_sha256=_parse_hex(baseline["plugin_api.py"]["sha256"], label="plugin_api from"),
            to_sha256=_parse_hex(patches_meta["ui_readiness"]["to_sha256"], label="plugin_api to"),
        ),
        PublishedSource(
            relative_path="scripts/ea_core.py",
            absolute_path=_require_abs(baseline["ea_core.py"]["absolute"], label="ea_core absolute"),
            from_sha256=_parse_hex(baseline["ea_core.py"]["sha256"], label="ea_core from"),
            to_sha256=_parse_hex(patches_meta["ui_readiness"]["to_core_sha256"], label="ea_core to"),
        ),
        PublishedSource(
            relative_path="plugins/personal-tasks/dashboard/dist/index.js",
            absolute_path=_require_abs(baseline["dist/index.js"]["absolute"], label="index.js absolute"),
            from_sha256=_parse_hex(baseline["dist/index.js"]["sha256"], label="index.js from"),
            to_sha256=_parse_hex(patches_meta["ui"]["to_sha256"], label="index.js to"),
        ),
    )
    patch_specs = (
        ("backend", "integration/hermes/backend.patch", backend["patch"]["sha256"]),
        ("ui_readiness", "integration/hermes/ui-readiness.patch", patches_meta["ui_readiness"]["sha256"]),
        ("ui", "integration/hermes/ui.patch", patches_meta["ui"]["sha256"]),
    )
    patches: list[PublishedPatch] = []
    for key, rel, expected in patch_specs:
        path = root / rel
        if not path.is_file():
            raise LifecycleError(f"published patch missing: {rel}", code="patch_missing", details={"path": str(path)})
        digest = sha256_file(path)
        expected_hex = _parse_hex(expected, label=f"{key}.sha256")
        if digest != expected_hex:
            raise LifecycleError(
                f"published patch hash mismatch: {rel}",
                code="patch_hash",
                details={"expected": expected_hex, "actual": digest, "path": str(path)},
            )
        patches.append(PublishedPatch(key=key, relative_path=rel, absolute_path=path, sha256=digest))
    return PatchSequence(
        worktree=root,
        sources=sources,
        patches=tuple(patches),
        ui_manifest_sha256=sha256_file(ui_path),
        backend_manifest_sha256=sha256_file(backend_path),
    )


def verify_live_baseline_sources(sequence: PatchSequence) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for source in sequence.sources:
        if not source.absolute_path.is_file():
            raise LifecycleError(
                f"baseline source missing: {source.relative_path}",
                code="source_missing",
                details={"path": str(source.absolute_path)},
            )
        digest = sha256_file(source.absolute_path)
        if digest != source.from_sha256:
            raise LifecycleError(
                f"baseline source drift: {source.relative_path}",
                code="drift",
                details={
                    "path": str(source.absolute_path),
                    "expected": source.from_sha256,
                    "actual": digest,
                },
            )
        hashes[source.relative_path] = digest
    return hashes


def _source_hashes_in_contour(contour: Path, sequence: PatchSequence) -> dict[str, str]:
    out: dict[str, str] = {}
    for source in sequence.sources:
        path = contour / source.relative_path
        if not path.is_file():
            raise LifecycleError(
                f"contour source missing: {source.relative_path}",
                code="source_missing",
                details={"path": str(path)},
            )
        out[source.relative_path] = sha256_file(path)
    return out



def _assert_hashes(actual: Mapping[str, str], expected: Mapping[str, str], *, code: str) -> None:
    for key, want in expected.items():
        got = actual.get(key)
        if got != want:
            raise LifecycleError(
                f"hash mismatch for {key}",
                code=code,
                details={"path": key, "expected": want, "actual": got},
            )


def _preserve_snapshot(paths: Sequence[Path]) -> dict[str, str]:
    return {str(path): sha256_file(path) for path in paths if path.is_file()}


def _expected_from(sequence: PatchSequence) -> dict[str, str]:
    return {s.relative_path: s.from_sha256 for s in sequence.sources}


def _expected_to(sequence: PatchSequence) -> dict[str, str]:
    return {s.relative_path: s.to_sha256 for s in sequence.sources}


def _contour_status(contour: RehearsalContour) -> tuple[str, dict[str, str]]:
    current = _source_hashes_in_contour(contour.contour_root, contour.sequence)
    expected_from = _expected_from(contour.sequence)
    expected_to = _expected_to(contour.sequence)
    if current == expected_from:
        return "ready", current
    if current == expected_to:
        return "already_applied", current
    raise LifecycleError(
        "contour sources drifted from admitted baseline/result hashes",
        code="drift",
        details={
            "actual": current,
            "admitted_baseline": expected_from,
            "admitted_result": expected_to,
        },
    )


def _rehearse_patch_on_staging(
    contour: RehearsalContour,
    patch: PublishedPatch,
    *,
    before_hashes: Mapping[str, str],
) -> dict[str, str]:
    """Apply one patch on a writable sibling copy; return hash-verified after state."""
    run_root = load_approved_run_root()
    probe_dir = Path(tempfile.mkdtemp(prefix="EA010-SYNTHETIC-patch-probe-", dir=str(run_root)))
    try:
        for source in contour.sequence.sources:
            src = contour.contour_root / source.relative_path
            dest = probe_dir / source.relative_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        probe_before = _source_hashes_in_contour(probe_dir, contour.sequence)
        _assert_hashes(probe_before, before_hashes, code="staging_before_hash")
        _git_apply(probe_dir, patch.absolute_path, check_only=True)
        _git_apply(probe_dir, patch.absolute_path, check_only=False)
        after = _source_hashes_in_contour(probe_dir, contour.sequence)
        if after == dict(before_hashes):
            raise LifecycleError(
                "staging rehearsal produced no source changes",
                code="partial_failure",
                details={"patch": patch.key},
            )
        return after
    finally:
        shutil.rmtree(probe_dir, ignore_errors=True)


def _git_apply(
    contour: Path,
    patch: Path,
    *,
    reverse: bool = False,
    check_only: bool = False,
) -> None:
    cmd = ["git", "apply"]
    if reverse:
        cmd.append("--reverse")
    if check_only:
        cmd.append("--check")
    cmd.extend(["--unidiff-zero", str(patch)])
    try:
        completed = subprocess.run(
            cmd,
            cwd=str(contour),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise LifecycleError(
            f"git apply failed to start: {exc}",
            code="no_permission",
            details={"patch": str(patch), "errno": getattr(exc, "errno", None)},
        ) from exc
    if completed.returncode != 0:
        raise LifecycleError(
            "git apply failed",
            code="drift"
            if "patch does not apply" in (completed.stderr or "").lower() or completed.returncode == 1
            else "partial_failure",
            details={
                "patch": str(patch),
                "argv": cmd,
                "rc": completed.returncode,
                "stderr": (completed.stderr or "")[-2000:],
                "stdout": (completed.stdout or "")[-1000:],
                "reverse": reverse,
                "check_only": check_only,
            },
        )


def validate_rehearsal_manifest(
    raw: Mapping[str, Any],
    *,
    worktree: Path | str,
    approved_run_root: Path | None = None,
) -> RehearsalContour:
    if raw.get("schema") != REHEARSAL_SCHEMA:
        raise LifecycleError("unexpected rehearsal schema", code="schema", details={"schema": raw.get("schema")})
    if raw.get("contour") != "isolated_synthetic":
        raise LifecycleError(
            "only isolated_synthetic contour is admitted",
            code="contour_forbidden",
            details={"contour": raw.get("contour")},
        )
    if raw.get("production_apply") is True:
        raise LifecycleError("production_apply is forbidden", code="production_forbidden")
    root = _bind_synthetic_contour(Path(str(raw.get("contour_root"))), run_root=approved_run_root)
    wt = _canonical(_require_abs(worktree, label="worktree"))
    sequence = load_published_patch_sequence(wt)
    expected_proc = raw.get("patch_rehearsal_procedure_sha256")
    if expected_proc is not None and _parse_hex(expected_proc, label="procedure") != PATCH_REHEARSAL_PROCEDURE_SHA256:
        raise LifecycleError("rehearsal procedure digest mismatch", code="procedure_digest")
    session = raw.get("test_session") or {}
    if not isinstance(session, dict):
        raise LifecycleError("test_session object required", code="session_missing")
    flag = root / SESSION_FLAG_NAME
    if session.get("enabled_flag_path"):
        flag = _canonical(_require_abs(session["enabled_flag_path"], label="test_session.enabled_flag_path"))
        if not str(flag).startswith(str(root) + os.sep) and flag.parent != root:
            raise LifecycleError("session flag escapes contour", code="path_escape")
    preserve_raw = raw.get("preserve")
    if not isinstance(preserve_raw, list) or not preserve_raw:
        raise LifecycleError("preserve list required", code="preserve_missing")
    preserve: list[Path] = []
    for item in preserve_raw:
        if not isinstance(item, dict):
            raise LifecycleError("preserve entries must be objects", code="preserve_entry")
        path = _canonical(_require_abs(item.get("path"), label="preserve.path"))
        if not str(path).startswith(str(root) + os.sep):
            raise LifecycleError("preserve path escapes contour", code="path_escape")
        preserve.append(path)
    _assert_session_marker_paths(
        flag,
        contour_root=root,
        preserve=preserve,
        sequence=sequence,
    )
    return RehearsalContour(
        raw=dict(raw),
        contour_root=root,
        session_flag=flag,
        preserve=tuple(preserve),
        sequence=sequence,
        worktree=wt,
    )


def assert_fresh_contour_root(contour_root: Path | str) -> Path:
    """Refuse nonempty or previously seeded contours before any chmod/copy/write."""
    root = _require_abs(contour_root, label="contour_root")
    if root.exists() and root.is_symlink():
        raise LifecycleError("contour_root must not be a symlink", code="symlink_rejected", details={"path": str(root)})
    if not root.exists():
        return root
    if not root.is_dir():
        raise LifecycleError(
            "contour_root must be a fresh directory path",
            code="contour_not_fresh",
            details={"contour_root": str(root)},
        )
    entries = sorted(root.iterdir(), key=lambda item: item.name)
    seeded_markers = (
        SESSION_FLAG_NAME,
        "integration-state",
        "plugins",
        "scripts",
    )
    if entries or any((root / marker).exists() for marker in seeded_markers):
        raise LifecycleError(
            "one-shot rehearsal requires a fresh empty disposable directory",
            code="contour_not_fresh",
            details={
                "contour_root": str(root),
                "entries": [item.name for item in entries[:20]],
            },
        )
    return root


def seed_disposable_copy(
    contour_root: Path | str,
    *,
    worktree: Path | str,
    approved_run_root: Path | None = None,
) -> RehearsalContour:
    """Create a fresh disposable copy of the three baseline sources under run-root."""
    run_root = approved_run_root or load_approved_run_root()
    root = _require_abs(contour_root, label="contour_root")
    # Refuse before creating directories outside the approved stand run-root.
    if not _is_strict_descendant(run_root.resolve(), root if root.exists() else root.parent.resolve()):
        if root.parent.resolve() != run_root.resolve() and not _is_strict_descendant(
            run_root.resolve(), root.parent.resolve()
        ):
            raise LifecycleError(
                "synthetic contour_root must be a disposable child of the approved stand run-root",
                code="contour_unbound",
                details={"contour_root": str(root), "approved_run_root": str(run_root.resolve())},
            )
    assert_fresh_contour_root(root)
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, DIR_MODE)
    bound = _bind_synthetic_contour(root, run_root=run_root)
    sequence = load_published_patch_sequence(worktree)
    verify_live_baseline_sources(sequence)
    for source in sequence.sources:
        dest = bound / source.relative_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source.absolute_path, dest)
        os.chmod(dest, FILE_MODE)
        if sha256_file(dest) != source.from_sha256:
            raise LifecycleError(
                f"copied source hash mismatch: {source.relative_path}",
                code="copy_hash",
                details={"expected": source.from_sha256, "actual": sha256_file(dest)},
            )
    state_dir = bound / "integration-state"
    receipts = state_dir / "receipts"
    receipts.mkdir(parents=True, exist_ok=True)
    os.chmod(state_dir, DIR_MODE)
    os.chmod(receipts, DIR_MODE)
    # Fixed admitted preserve targets only — no arbitrary path override hook.
    defaults = {
        str(state_dir / "journal.sqlite3"): b"SYNTH-JOURNAL-v1\n",
        str(state_dir / "pending.json"): b'{"pending":[{"id":"p-synth-1"}],"keep":true}\n',
        str(receipts / "r1.json"): b'{"receipt_id":"r1","task_id":1}\n',
        str(state_dir / "legacy-links.json"): b'{"links":[{"proposal":"prop-1","task_id":1}]}\n',
    }
    preserve_paths: list[Path] = []
    for path_str, data in defaults.items():
        path = Path(path_str)
        if not _is_strict_descendant(bound, path):
            raise LifecycleError(
                "preserve seed path escapes contour",
                code="preserve_path_escape",
                details={"path": str(path), "contour_root": str(bound)},
            )
        if path.exists():
            path.unlink()
        _atomic_write(path, data)
        preserve_paths.append(path)
    flag = bound / SESSION_FLAG_NAME
    disabled = _session_disabled_marker(flag)
    if disabled.exists():
        disabled.unlink()
    _ensure_test_session(flag)
    raw = {
        "schema": REHEARSAL_SCHEMA,
        "contour": "isolated_synthetic",
        "contour_root": str(bound),
        "production_apply": False,
        "default_mode": "dry-run",
        "patch_rehearsal_procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
        "owner_scope_clarification_sha256": OWNER_SCOPE_CLARIFICATION_SHA256,
        "test_session": {"enabled_flag_path": str(flag)},
        "preserve": [
            {"path": str(preserve_paths[0]), "kind": "journal"},
            {"path": str(preserve_paths[1]), "kind": "pending"},
            {"path": str(preserve_paths[2]), "kind": "receipt"},
            {"path": str(preserve_paths[3]), "kind": "backlink"},
        ],
        "notes": [
            "Disposable Hermes three-patch one-shot rehearsal only; working Mini service untouched.",
            "Uses published s07 patches and ui-manifest baseline hashes.",
        ],
    }
    return validate_rehearsal_manifest(raw, worktree=worktree, approved_run_root=run_root)


def preflight(contour: RehearsalContour) -> dict[str, Any]:
    verify_live_baseline_sources(contour.sequence)
    status, current = _contour_status(contour)
    preserve = []
    for path in contour.preserve:
        if not path.is_file():
            raise LifecycleError(
                f"preserve artifact missing: {path}",
                code="preserve_missing",
                details={"path": str(path)},
            )
        preserve.append({"path": str(path), "sha256": sha256_file(path)})
    return {
        "ok": True,
        "mode": "preflight",
        "status": status,
        "contour_root": str(contour.contour_root),
        "source_hashes": current,
        "patch_hashes": {p.relative_path: p.sha256 for p in contour.sequence.patches},
        "preserve": preserve,
        "test_session_send_enabled": _session_enabled(contour.session_flag),
        "writes_performed": 0,
        "working_service_mutations": False,
        "procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
    }


def dry_run(contour: RehearsalContour) -> dict[str, Any]:
    base = preflight(contour)
    planned = []
    probe_result_hashes: dict[str, str] | None = None
    if base["status"] == "already_applied":
        for patch in contour.sequence.patches:
            planned.append({"key": patch.key, "action": "skip_idempotent", "path": patch.relative_path})
    else:
        run_root = load_approved_run_root()
        probe_dir = Path(
            tempfile.mkdtemp(prefix="EA010-SYNTHETIC-dry-run-probe-", dir=str(run_root))
        )
        try:
            for source in contour.sequence.sources:
                src = contour.contour_root / source.relative_path
                dest = probe_dir / source.relative_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
            for patch in contour.sequence.patches:
                _git_apply(probe_dir, patch.absolute_path, check_only=True)
                _git_apply(probe_dir, patch.absolute_path, check_only=False)
                planned.append({"key": patch.key, "action": "apply", "path": patch.relative_path})
            probe_result_hashes = _source_hashes_in_contour(probe_dir, contour.sequence)
            _assert_hashes(probe_result_hashes, _expected_to(contour.sequence), code="result_hash")
        finally:
            shutil.rmtree(probe_dir, ignore_errors=True)
    current = _source_hashes_in_contour(contour.contour_root, contour.sequence)
    expected = (
        _expected_to(contour.sequence)
        if base["status"] == "already_applied"
        else _expected_from(contour.sequence)
    )
    _assert_hashes(current, expected, code="drift")
    return {
        **base,
        "mode": "dry-run",
        "planned": planned,
        "probe_result_hashes": probe_result_hashes,
        "writes_performed": 0,
        "note": "dry-run probes sequential git apply on a throwaway sibling copy; admitted contour unchanged",
    }


def apply_patches(contour: RehearsalContour) -> dict[str, Any]:
    """Apply the published three patches in order on an in-memory contour (no durable state)."""
    base = preflight(contour)
    if base["status"] == "already_applied":
        return {
            "ok": True,
            "mode": "apply",
            "writes_performed": 0,
            "skipped_idempotent": [p.key for p in contour.sequence.patches],
            "applied": [],
            "result_hashes": base["source_hashes"],
            "working_service_mutations": False,
        }
    before_preserve = _preserve_snapshot(contour.preserve)
    if not _session_enabled(contour.session_flag):
        _ensure_test_session(contour.session_flag)
    applied: list[dict[str, Any]] = []
    try:
        for patch in contour.sequence.patches:
            before_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
            expected_after = _rehearse_patch_on_staging(
                contour, patch, before_hashes=before_hashes
            )
            _git_apply(contour.contour_root, patch.absolute_path, check_only=True)
            _git_apply(contour.contour_root, patch.absolute_path, check_only=False)
            after_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
            _assert_hashes(after_hashes, expected_after, code="result_hash")
            applied.append({"key": patch.key, "path": patch.relative_path, "sha256": patch.sha256})
        result_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
        _assert_hashes(result_hashes, _expected_to(contour.sequence), code="result_hash")
        after_preserve = _preserve_snapshot(contour.preserve)
        if before_preserve != after_preserve:
            raise LifecycleError(
                "preserve artifacts mutated during apply",
                code="preserve_mutated",
                details={"before": before_preserve, "after": after_preserve},
            )
    except LifecycleError as exc:
        try:
            _pause_test_session(contour.session_flag)
        except Exception:  # noqa: BLE001
            pass
        raise LifecycleError(
            str(exc),
            code="partial_failure" if applied else exc.code,
            details={
                **exc.details,
                "written_before_failure": applied,
                "session_paused": True,
            },
        ) from exc
    except OSError as exc:
        try:
            _pause_test_session(contour.session_flag)
        except Exception:  # noqa: BLE001
            pass
        raise LifecycleError(
            f"apply failed: {exc}",
            code="partial_failure" if applied else "no_permission",
            details={
                "written_before_failure": applied,
                "session_paused": True,
                "errno": getattr(exc, "errno", None),
            },
        ) from exc
    return {
        "ok": True,
        "mode": "apply",
        "writes_performed": len(applied),
        "applied": applied,
        "skipped_idempotent": [],
        "result_hashes": result_hashes,
        "preserve": after_preserve,
        "test_session_send_enabled": _session_enabled(contour.session_flag),
        "working_service_mutations": False,
    }


def reverse_patches(contour: RehearsalContour) -> dict[str, Any]:
    """Reverse the exact three-patch result hashes back to baseline (no durable state)."""
    before_preserve = _preserve_snapshot(contour.preserve)
    for path in contour.preserve:
        if not path.is_file():
            raise LifecycleError(
                f"preserve artifact missing before reverse: {path}",
                code="preserve_missing",
                details={"path": str(path)},
            )
    _pause_test_session(contour.session_flag)
    paused_preserve = _preserve_snapshot(contour.preserve)
    if before_preserve != paused_preserve:
        raise LifecycleError(
            "preserve artifacts mutated by session pause",
            code="preserve_mutated",
            details={"before": before_preserve, "after": paused_preserve},
        )
    status, current = _contour_status(contour)
    if status == "ready":
        return {
            "ok": True,
            "mode": "reverse",
            "restored": [],
            "writes_performed": 0,
            "dispatch_disabled": True,
            "test_session_send_enabled": False,
            "preserve": before_preserve,
            "working_service_mutations": False,
        }
    restored: list[dict[str, Any]] = []
    try:
        for patch in reversed(contour.sequence.patches):
            _git_apply(contour.contour_root, patch.absolute_path, reverse=True, check_only=True)
            _git_apply(contour.contour_root, patch.absolute_path, reverse=True, check_only=False)
            restored.append({"key": patch.key, "path": patch.relative_path, "sha256": patch.sha256})
        restored_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
        _assert_hashes(restored_hashes, _expected_from(contour.sequence), code="restore_hash")
        after_preserve = _preserve_snapshot(contour.preserve)
        if before_preserve != after_preserve:
            raise LifecycleError(
                "preserve artifacts mutated during reverse",
                code="preserve_mutated",
                details={"before": before_preserve, "after": after_preserve},
            )
    except LifecycleError as exc:
        try:
            _pause_test_session(contour.session_flag)
        except Exception:  # noqa: BLE001
            pass
        raise LifecycleError(
            str(exc),
            code=exc.code,
            details={**exc.details, "session_paused": True, "restored_before_failure": restored},
        ) from exc
    except OSError as exc:
        try:
            _pause_test_session(contour.session_flag)
        except Exception:  # noqa: BLE001
            pass
        raise LifecycleError(
            f"reverse failed: {exc}",
            code="partial_failure",
            details={
                "session_paused": True,
                "errno": getattr(exc, "errno", None),
                "restored_before_failure": restored,
            },
        ) from exc
    return {
        "ok": True,
        "mode": "reverse",
        "restored": restored,
        "writes_performed": len(restored),
        "restored_hashes": restored_hashes,
        "dispatch_disabled": True,
        "test_session_send_enabled": False,
        "preserve": after_preserve,
        "working_service_mutations": False,
    }


# Compatibility alias used by older suite wording ("undo" == reverse of own patches).
undo_patches = reverse_patches


def _make_owned_tree_writable(root: Path) -> list[str]:
    """Best-effort chmod of an owned disposable tree so rmtree can unlink read-only dirs."""
    errors: list[str] = []
    if not root.exists():
        return errors
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        for name in filenames:
            path = Path(dirpath) / name
            try:
                os.chmod(path, FILE_MODE)
            except OSError as exc:
                errors.append(f"chmod file {path}: {exc}")
        for name in dirnames:
            path = Path(dirpath) / name
            try:
                os.chmod(path, DIR_MODE)
            except OSError as exc:
                errors.append(f"chmod dir {path}: {exc}")
        try:
            os.chmod(dirpath, DIR_MODE)
        except OSError as exc:
            errors.append(f"chmod walk {dirpath}: {exc}")
    try:
        os.chmod(root, DIR_MODE)
    except OSError as exc:
        errors.append(f"chmod root {root}: {exc}")
    return errors


def delete_owned_contour(contour_root: Path | str) -> dict[str, Any]:
    """Delete an owned disposable contour and report verified absence (NFR-18)."""
    root = Path(contour_root)
    if not root.exists():
        return {
            "contour_deleted": True,
            "contour_root": str(root),
            "remaining": [],
            "cleanup_errors": [],
        }
    cleanup_errors = _make_owned_tree_writable(root)
    try:
        shutil.rmtree(root)
    except OSError as exc:
        cleanup_errors.append(f"rmtree: {exc}")
        cleanup_errors.extend(_make_owned_tree_writable(root))
        try:
            shutil.rmtree(root)
        except OSError as exc2:
            cleanup_errors.append(f"rmtree_retry: {exc2}")
    remaining: list[str] = []
    if root.exists():
        try:
            remaining = sorted(
                "." if path == root else str(path.relative_to(root))
                for path in [root, *root.rglob("*")]
            )
        except OSError as exc:
            remaining = [f"<list_failed: {exc}>"]
    return {
        "contour_deleted": not root.exists(),
        "contour_root": str(root),
        "remaining": remaining,
        "cleanup_errors": cleanup_errors,
    }


def run_exact_three_patch_rehearsal(
    worktree: Path | str,
    *,
    mode: str = "rehearse",
) -> dict[str, Any]:
    """One-shot temporary-tree rehearsal with finally cleanup on every outcome."""
    if mode not in MODES:
        raise LifecycleError(f"unknown mode {mode}", code="mode")
    wt = _canonical(_require_abs(worktree, label="worktree"))
    run_root = load_approved_run_root()
    run_root.mkdir(parents=True, exist_ok=True)
    contour_root = Path(
        tempfile.mkdtemp(prefix="EA010-SYNTHETIC-hermes-patches-", dir=str(run_root))
    )
    outcome: dict[str, Any] | None = None
    caught: LifecycleError | None = None
    try:
        contour = seed_disposable_copy(contour_root, worktree=wt, approved_run_root=run_root)
        if mode == "dry-run":
            result = dry_run(contour)
            outcome = {
                "ok": True,
                "mode": "dry-run",
                "result": result,
                "contour_root": str(contour_root),
                "procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
                "working_service_mutations": False,
            }
        else:
            phases: dict[str, Any] = {}
            phases["preflight"] = preflight(contour)
            phases["apply"] = apply_patches(contour)
            apply_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
            _assert_hashes(apply_hashes, _expected_to(contour.sequence), code="result_hash")
            phases["apply_hashes"] = apply_hashes
            phases["repeat"] = apply_patches(contour)
            if phases["repeat"]["writes_performed"] != 0:
                raise LifecycleError(
                    "repeat must be idempotent on admitted result hashes",
                    code="repeat_not_idempotent",
                    details={"writes_performed": phases["repeat"]["writes_performed"]},
                )
            before_preserve = _preserve_snapshot(contour.preserve)
            phases["reverse"] = reverse_patches(contour)
            reverse_hashes = _source_hashes_in_contour(contour.contour_root, contour.sequence)
            _assert_hashes(reverse_hashes, _expected_from(contour.sequence), code="restore_hash")
            phases["reverse_hashes"] = reverse_hashes
            after_preserve = _preserve_snapshot(contour.preserve)
            if before_preserve != after_preserve:
                raise LifecycleError(
                    "preserve artifacts mutated across reverse",
                    code="preserve_mutated",
                    details={"before": before_preserve, "after": after_preserve},
                )
            phases["preserve"] = after_preserve
            outcome = {
                "ok": True,
                "mode": "rehearse",
                "phases": phases,
                "contour_root": str(contour_root),
                "procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
                "working_service_mutations": False,
                "note": "exact ordered apply/hash/repeat/reverse/hash on one temporary tree; deleted on return",
            }
    except LifecycleError as exc:
        caught = LifecycleError(
            str(exc),
            code=exc.code,
            details={
                **exc.details,
                "contour_root": str(contour_root),
            },
        )
    finally:
        cleanup = delete_owned_contour(contour_root)

    if caught is not None:
        details = {
            **caught.details,
            "contour_root": str(contour_root),
            "contour_deleted": cleanup["contour_deleted"],
        }
        if cleanup["remaining"]:
            details["cleanup_remaining"] = cleanup["remaining"]
        if cleanup["cleanup_errors"]:
            details["cleanup_errors"] = cleanup["cleanup_errors"]
        if not cleanup["contour_deleted"]:
            raise LifecycleError(
                "owned contour cleanup failed after one-shot rehearsal error",
                code="cleanup_failed",
                details={
                    **details,
                    "prior_code": caught.code,
                    "prior_message": str(caught),
                },
            ) from caught
        raise LifecycleError(str(caught), code=caught.code, details=details) from caught

    assert outcome is not None
    outcome["contour_deleted"] = cleanup["contour_deleted"]
    if cleanup["remaining"]:
        outcome["cleanup_remaining"] = cleanup["remaining"]
    if cleanup["cleanup_errors"]:
        outcome["cleanup_errors"] = cleanup["cleanup_errors"]
    if not cleanup["contour_deleted"]:
        raise LifecycleError(
            "owned contour cleanup failed after one-shot rehearsal",
            code="cleanup_failed",
            details={
                "contour_root": str(contour_root),
                "contour_deleted": False,
                "cleanup_remaining": cleanup["remaining"],
                "cleanup_errors": cleanup["cleanup_errors"],
                "prior_result": {k: v for k, v in outcome.items() if k != "prior_result"},
            },
        )
    return outcome


def run_mode(contour: RehearsalContour, mode: str) -> dict[str, Any]:
    """In-process helper for tests; CLI uses run_exact_three_patch_rehearsal instead."""
    if mode == "dry-run":
        return dry_run(contour)
    if mode == "rehearse":
        raise LifecycleError(
            "rehearse owns its temporary tree; call run_exact_three_patch_rehearsal",
            code="mode",
        )
    raise LifecycleError(f"unknown mode {mode}", code="mode")
