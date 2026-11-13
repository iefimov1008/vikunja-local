"""Approved-pilot mode (s10): fail-closed without an exact local permit.

Coding workers run only isolated suites. A live pilot requires a coordinator-
supplied operational permit and resources manifest; without them this module
refuses before any write to the environment or service. An unexecuted pilot
never yields a success result (nonzero / not-run or authorised-not-executed).

Exact authorization validates typed window currency, authorized_actions versus
the requested suite, typed service_bindings, and effective profile/resource
binding before any runner call. Matching ``EA010_APPROVED_PILOT_EXECUTOR`` alone
never relabels an isolated C7 suite as an operational pilot: receipts stay
separate from actions actually executed/read back on the approved contour.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .stand import StandError, load_profile

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

PERMIT_ENV = "EA010_APPROVED_PILOT_PERMIT"
EXECUTOR_ENV = "EA010_APPROVED_PILOT_EXECUTOR"
PERMIT_SCHEMA = "ea010-approved-pilot-permit/v1"
RESOURCES_SCHEMA = "ea010-approved-pilot-resources/v1"
PROJECT_ID = "vikunja-local-ea010-install-integration"
# Identities that are never treated as the operational pilot executor.
CODING_WORKER_EXECUTOR_ALIASES = frozenset(
    {
        "",
        "coding-worker",
        "cursor-implementation",
        "implementation-worker",
    }
)

# Manual V1/V2 checkpoints that require real owner/device receipts. Without
# receipts they stay not-run — never simulated success.
MANUAL_CHECKPOINTS: tuple[str, ...] = (
    "owner_synthetic_acceptance",
    "macbook_access_and_links",
    "mini_restart_post_login",
)


class PilotRefused(StandError):
    """Raised when approved-pilot cannot start (missing/invalid permit)."""

    def __init__(self, message: str, *, status: str = "not-run", details: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.details = details or {}


def default_checkpoint_status() -> dict[str, dict[str, str]]:
    """Honest not-run markers for owner/device/restart checkpoints."""
    return {
        name: {
            "status": "not-run",
            "reason": "no live owner/device receipt for this dispatch",
        }
        for name in MANUAL_CHECKPOINTS
    }


def permit_path_from_env(env: dict[str, str] | None = None) -> Path | None:
    source = env if env is not None else os.environ
    raw = (source.get(PERMIT_ENV) or "").strip()
    if not raw:
        return None
    return Path(raw)


def runtime_executor_from_env(env: dict[str, str] | None = None) -> str:
    """Claimed pilot executor identity for this process (empty ⇒ coding worker)."""
    source = env if env is not None else os.environ
    return (source.get(EXECUTOR_ENV) or "").strip()


def is_permit_bound_operational_executor(permit: dict[str, Any], env: dict[str, str] | None = None) -> bool:
    """True only when the live process identity matches the permit-bound executor."""
    claimed = runtime_executor_from_env(env)
    permitted = permit.get("executor")
    if not isinstance(permitted, str) or not permitted.strip():
        return False
    permitted = permitted.strip()
    if claimed.lower() in CODING_WORKER_EXECUTOR_ALIASES:
        return False
    if permitted.lower() in CODING_WORKER_EXECUTOR_ALIASES:
        return False
    return claimed == permitted


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require_nonempty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PilotRefused(
            f"approved-pilot {label} must be a non-empty string",
            status="not-run",
        )
    return value.strip()


def _parse_aware_instant(value: str, label: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PilotRefused(
            f"approved-pilot {label} must be a timezone-aware ISO-8601 instant, got {value!r}",
            status="not-run",
        ) from exc
    if parsed.tzinfo is None:
        raise PilotRefused(
            f"approved-pilot {label} must be timezone-aware, got {value!r}",
            status="not-run",
        )
    return parsed.astimezone(timezone.utc)


def _validate_window(window: Any) -> dict[str, str]:
    if not isinstance(window, dict):
        raise PilotRefused(
            "approved-pilot permit window must be an object with starts_at/ends_at",
            status="not-run",
        )
    starts_raw = _require_nonempty_str(window.get("starts_at"), "window.starts_at")
    ends_raw = _require_nonempty_str(window.get("ends_at"), "window.ends_at")
    starts = _parse_aware_instant(starts_raw, "window.starts_at")
    ends = _parse_aware_instant(ends_raw, "window.ends_at")
    if starts >= ends:
        raise PilotRefused(
            "approved-pilot permit window is inverted or empty (starts_at >= ends_at)",
            status="not-run",
            details={"starts_at": starts_raw, "ends_at": ends_raw},
        )
    return {"starts_at": starts_raw, "ends_at": ends_raw}


def assert_window_current(
    window: dict[str, str],
    *,
    now: datetime | None = None,
) -> datetime:
    """Refuse expired or not-yet-open windows using timezone-aware comparison."""
    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    starts = _parse_aware_instant(window["starts_at"], "window.starts_at")
    ends = _parse_aware_instant(window["ends_at"], "window.ends_at")
    if instant < starts:
        raise PilotRefused(
            "approved-pilot permit window has not started yet",
            status="not-run",
            details={
                "now": instant.isoformat(),
                "starts_at": window["starts_at"],
                "ends_at": window["ends_at"],
            },
        )
    if instant > ends:
        raise PilotRefused(
            "approved-pilot permit window has expired",
            status="not-run",
            details={
                "now": instant.isoformat(),
                "starts_at": window["starts_at"],
                "ends_at": window["ends_at"],
            },
        )
    return instant


def _validate_authorized_actions(actions: Any) -> list[str]:
    if not isinstance(actions, list) or not actions:
        raise PilotRefused(
            "approved-pilot authorized_actions must be a non-empty list",
            status="not-run",
        )
    cleaned: list[str] = []
    for idx, item in enumerate(actions):
        if not isinstance(item, str) or not item.strip():
            raise PilotRefused(
                f"approved-pilot authorized_actions[{idx}] must be a non-empty string",
                status="not-run",
            )
        cleaned.append(item.strip())
    return cleaned


def suite_action_authorized(actions: list[str], suite: str) -> bool:
    """True when authorized_actions exactly cover the requested suite effect."""
    name = str(suite).strip()
    if not name:
        return False
    allowed = {a.strip() for a in actions if isinstance(a, str) and a.strip()}
    if name in allowed:
        return True
    if f"suite:{name}" in allowed:
        return True
    if "suite:*" in allowed or "*" in allowed:
        return True
    return False


def assert_suite_action_authorized(actions: list[str], suite: str) -> None:
    if not suite_action_authorized(actions, suite):
        raise PilotRefused(
            "approved-pilot authorized_actions do not cover the requested suite",
            status="not-run",
            details={
                "suite": suite,
                "authorized_actions": list(actions),
                "required": [str(suite).strip(), f"suite:{str(suite).strip()}", "suite:*", "*"],
            },
        )


def _validate_service_binding(item: Any, idx: int) -> dict[str, Any]:
    if item is None or not isinstance(item, dict):
        raise PilotRefused(
            f"approved-pilot service_bindings[{idx}] must be a non-null object",
            status="not-run",
        )
    label = item.get("label")
    if not isinstance(label, str) or not label.strip():
        raise PilotRefused(
            f"approved-pilot service_bindings[{idx}].label must be a non-empty string",
            status="not-run",
        )
    cleaned = {"label": label.strip()}
    for key in ("host", "listen_host", "target", "origin"):
        value = item.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise PilotRefused(
                f"approved-pilot service_bindings[{idx}].{key} must be a non-empty string when set",
                status="not-run",
            )
        cleaned[key] = value.strip()
    port = item.get("port")
    if port is not None:
        if not isinstance(port, int) or isinstance(port, bool) or port <= 0 or port > 65535:
            raise PilotRefused(
                f"approved-pilot service_bindings[{idx}].port must be a positive int port",
                status="not-run",
            )
        cleaned["port"] = port
    return cleaned


def _validate_resources(resources: dict[str, Any]) -> dict[str, Any]:
    bindings = resources.get("service_bindings")
    if not isinstance(bindings, list) or not bindings:
        raise PilotRefused(
            "approved-pilot resources_manifest.service_bindings must be a non-empty list",
            status="not-run",
        )
    cleaned_bindings = [_validate_service_binding(item, idx) for idx, item in enumerate(bindings)]
    owned = resources.get("owned_paths")
    if not isinstance(owned, list) or not owned:
        raise PilotRefused(
            "approved-pilot resources_manifest.owned_paths must be a non-empty list",
            status="not-run",
        )
    cleaned_owned: list[str] = []
    for idx, path in enumerate(owned):
        if not isinstance(path, str) or not path.strip() or not Path(path).is_absolute():
            raise PilotRefused(
                f"approved-pilot owned_paths[{idx}] must be an absolute path string",
                status="not-run",
            )
        cleaned_owned.append(str(Path(path).resolve()))
    run_root = resources.get("run_root")
    if not isinstance(run_root, str) or not run_root.strip() or not Path(run_root).is_absolute():
        raise PilotRefused(
            "approved-pilot resources_manifest.run_root must be a non-empty absolute path",
            status="not-run",
        )
    resolved_root = str(Path(run_root).resolve())
    resources = {
        **resources,
        "service_bindings": cleaned_bindings,
        "owned_paths": cleaned_owned,
        "run_root": resolved_root,
    }
    return resources


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def assert_profile_resource_binding(
    resources: dict[str, Any],
    *,
    profile: Any | None = None,
) -> dict[str, Any]:
    """Refuse when the effective stand profile targets paths/hosts outside the permit."""
    effective = profile if profile is not None else load_profile()
    permit_root = Path(str(resources["run_root"])).resolve()
    owned = [Path(p).resolve() for p in resources.get("owned_paths") or []]
    profile_root = Path(str(effective.run_root)).resolve()
    allowed_roots = [permit_root, *owned]
    if not any(profile_root == root or _path_is_within(profile_root, root) for root in allowed_roots):
        raise PilotRefused(
            "approved-pilot refused: effective profile run_root is outside permit run_root/owned_paths",
            status="not-run",
            details={
                "profile_run_root": str(profile_root),
                "permit_run_root": str(permit_root),
                "owned_paths": [str(p) for p in owned],
            },
        )
    listen_host = str(getattr(effective, "listen_host", "") or "").strip()
    ports = getattr(effective, "ports", None) or {}
    vikunja_port = None
    if isinstance(ports, dict):
        raw_port = ports.get("vikunja")
        if isinstance(raw_port, int):
            vikunja_port = raw_port
    bindings = list(resources.get("service_bindings") or [])
    host_ok = False
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        for key in ("host", "listen_host", "target"):
            value = binding.get(key)
            if isinstance(value, str) and value.strip() and listen_host and value.strip() == listen_host:
                host_ok = True
        origin = binding.get("origin")
        if isinstance(origin, str) and listen_host and listen_host in origin:
            host_ok = True
        if vikunja_port is not None and binding.get("port") == vikunja_port:
            host_ok = True
        # Label-only bindings are accepted only when path binding already matched;
        # they still must be non-null objects (validated earlier).
        if set(binding.keys()) <= {"label"} and profile_root == permit_root:
            host_ok = True
    if listen_host and not host_ok and not any(
        set(b.keys()) <= {"label"} for b in bindings if isinstance(b, dict)
    ):
        raise PilotRefused(
            "approved-pilot refused: service_bindings do not match effective profile listen target",
            status="not-run",
            details={
                "listen_host": listen_host,
                "vikunja_port": vikunja_port,
                "service_bindings": bindings,
            },
        )
    return {
        "profile_run_root": str(profile_root),
        "permit_run_root": str(permit_root),
        "listen_host": listen_host or None,
        "vikunja_port": vikunja_port,
    }


def _receipt_verified(value: Any) -> bool:
    """Accept only explicit verified receipt objects for named checkpoints."""
    if not isinstance(value, dict):
        return False
    if value.get("verified") is not True:
        return False
    receipt_id = value.get("receipt_id")
    attested_at = value.get("attested_at")
    if isinstance(receipt_id, str) and receipt_id.strip():
        return True
    if isinstance(attested_at, str) and attested_at.strip():
        return True
    return False


def validate_checkpoint_receipts(receipts: Any) -> dict[str, dict[str, Any]]:
    """Require a verified receipt for every named manual checkpoint."""
    if not isinstance(receipts, dict):
        raise PilotRefused(
            "approved-pilot owner_action_receipts must be an object keyed by checkpoint",
            status="not-run",
        )
    missing: list[str] = []
    invalid: list[str] = []
    verified: dict[str, dict[str, Any]] = {}
    for name in MANUAL_CHECKPOINTS:
        if name not in receipts:
            missing.append(name)
            continue
        row = receipts[name]
        if not _receipt_verified(row):
            invalid.append(name)
            continue
        verified[name] = row
    if missing or invalid:
        raise PilotRefused(
            "approved-pilot owner_action_receipts missing or unverified for required checkpoints",
            status="not-run",
            details={
                "missing_checkpoints": missing,
                "invalid_checkpoints": invalid,
                "required_checkpoints": list(MANUAL_CHECKPOINTS),
            },
        )
    return verified


def load_permit(path: Path) -> dict[str, Any]:
    """Load and validate an exact local operational permit + resources manifest."""
    if not path.is_absolute():
        raise PilotRefused(
            f"approved-pilot permit path must be absolute, got {path}",
            status="not-run",
            details={"permit_path": str(path)},
        )
    if not path.is_file():
        raise PilotRefused(
            f"approved-pilot permit file missing: {path}",
            status="not-run",
            details={"permit_path": str(path)},
        )
    if path.is_symlink():
        raise PilotRefused(
            f"approved-pilot permit must not be a symlink: {path}",
            status="not-run",
            details={"permit_path": str(path)},
        )
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PilotRefused(
            f"approved-pilot permit unreadable: {path}: {exc}",
            status="not-run",
            details={"permit_path": str(path)},
        ) from exc
    if not isinstance(payload, dict):
        raise PilotRefused("approved-pilot permit must be a JSON object", status="not-run")
    if payload.get("schema") != PERMIT_SCHEMA:
        raise PilotRefused(
            f"approved-pilot permit schema must be {PERMIT_SCHEMA!r}, got {payload.get('schema')!r}",
            status="not-run",
        )
    if payload.get("project_id") != PROJECT_ID:
        raise PilotRefused(
            f"approved-pilot permit project_id mismatch: {payload.get('project_id')!r}",
            status="not-run",
        )
    resources = payload.get("resources_manifest")
    if not isinstance(resources, dict) or resources.get("schema") != RESOURCES_SCHEMA:
        raise PilotRefused(
            "approved-pilot requires exact embedded resources_manifest "
            f"with schema {RESOURCES_SCHEMA!r}",
            status="not-run",
        )
    expected = payload.get("resources_manifest_sha256")
    if not isinstance(expected, str) or len(expected) != 64:
        raise PilotRefused(
            "approved-pilot permit missing resources_manifest_sha256",
            status="not-run",
        )
    material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode("utf-8")
    actual = _sha256_bytes(material)
    if actual != expected:
        raise PilotRefused(
            "approved-pilot resources_manifest_sha256 mismatch",
            status="not-run",
            details={"expected": expected, "actual": actual},
        )
    for key in ("window", "authorized_actions", "executor"):
        if key not in payload:
            raise PilotRefused(
                f"approved-pilot permit missing required field {key!r}",
                status="not-run",
            )
    for key in ("service_bindings", "owned_paths", "run_root"):
        if key not in resources:
            raise PilotRefused(
                f"approved-pilot resources_manifest missing {key!r}",
                status="not-run",
            )
    # Exact typed operational fields (fail closed on placeholders / nulls).
    payload["window"] = _validate_window(payload.get("window"))
    payload["authorized_actions"] = _validate_authorized_actions(payload.get("authorized_actions"))
    payload["executor"] = _require_nonempty_str(payload.get("executor"), "executor")
    payload["resources_manifest"] = _validate_resources(resources)
    return payload


def assert_no_pilot_side_effects(
    *,
    probe_roots: list[Path],
    before: dict[str, set[str]],
) -> None:
    """Ensure fail-closed refusal did not create new files under probe roots."""
    for root in probe_roots:
        key = str(root)
        if not root.exists():
            continue
        after = {str(p) for p in root.rglob("*") if p.is_file()}
        created = sorted(after - before.get(key, set()))
        if created:
            raise StandError(
                f"approved-pilot fail-closed wrote files under {root}: {created[:20]}"
            )


def snapshot_files(roots: list[Path]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for root in roots:
        if root.exists():
            out[str(root)] = {str(p) for p in root.rglob("*") if p.is_file()}
        else:
            out[str(root)] = set()
    return out


def _receipt_present_ledger(receipts: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Receipt presence only — never claims operational execution/read-back."""
    return {
        name: {
            "status": "receipt-present-not-executed",
            "reason": (
                "verified receipt present; operational contour was not executed "
                "or read back on this candidate"
            ),
            "receipt_id": (receipts[name].get("receipt_id") or receipts[name].get("attested_at")),
        }
        for name in MANUAL_CHECKPOINTS
    }


def authorize_exact_operational_permit(
    *,
    permit: dict[str, Any],
    suite: str,
    now: datetime | None = None,
    profile: Any | None = None,
) -> dict[str, Any]:
    """Validate current window, suite action scope, and effective resource binding.

    Does not invoke any suite runner. Callers that lack a real operational contour
    executor must still refuse after this returns.
    """
    window = permit.get("window")
    if not isinstance(window, dict):
        raise PilotRefused("approved-pilot window missing after load", status="not-run")
    observed_now = assert_window_current(window, now=now)
    actions = list(permit.get("authorized_actions") or [])
    assert_suite_action_authorized(actions, suite)
    resources = permit.get("resources_manifest")
    if not isinstance(resources, dict):
        raise PilotRefused("approved-pilot resources_manifest missing after load", status="not-run")
    binding = assert_profile_resource_binding(resources, profile=profile)
    return {
        "now": observed_now.isoformat(),
        "suite": suite,
        "authorized_actions": actions,
        "binding": binding,
    }


def refuse_isolated_relabel_as_operational(
    *,
    permit: dict[str, Any],
    permit_path: Path,
    suite: str,
    argv: list[str],
    receipts: dict[str, dict[str, Any]],
    authorization: dict[str, Any],
    runtime_executor: str,
) -> None:
    """Never execute or relabel an isolated C7 suite as approved-pilot."""
    checkpoint_ledger = _receipt_present_ledger(receipts)
    resources = permit.get("resources_manifest") or {}
    raise PilotRefused(
        "approved-pilot refused: operational contour not executed or verified; "
        "isolated suite runners must not be relabeled as approved-pilot",
        status="authorised-not-executed-operational-contour-unverified",
        details={
            "status": "authorised-not-executed-operational-contour-unverified",
            "reason": (
                "exact permit authorization passed (window/actions/bindings), but this "
                "package has no operational-contour executor that uses and reads back "
                "permit-bound services/paths/device actions; refusing rather than "
                "invoking mode=isolated and rewriting mode=approved-pilot"
            ),
            "permit_path": str(permit_path),
            "checkpoints": checkpoint_ledger,
            "mutations": False,
            "executed_count": 0,
            "runner_calls": 0,
            "argv": list(argv),
            "suite": suite,
            "executor": permit.get("executor"),
            "runtime_executor": runtime_executor or None,
            "window": permit.get("window"),
            "authorized_actions": list(permit.get("authorized_actions") or []),
            "resources_run_root": resources.get("run_root"),
            "service_bindings": list(resources.get("service_bindings") or []),
            "owned_paths": list(resources.get("owned_paths") or []),
            "authorization": authorization,
        },
    )


def run_approved_pilot(
    *,
    mode: str,
    worktree: Path,
    argv: list[str],
    env: dict[str, str] | None = None,
    probe_roots: list[Path] | None = None,
    suite: str | None = None,
    now: datetime | None = None,
    profile: Any | None = None,
) -> dict[str, Any]:
    """Gate for --mode approved-pilot.

    Without an exact local permit/resources manifest this refuses with status
    not-run and must not mutate the environment or service. Coding workers
    (missing/mismatched ``EA010_APPROVED_PILOT_EXECUTOR``) never execute a live
    pilot even with a fully authorised permit. A permit-bound operational
    identity still refuses here until a real operational contour executor uses
    and verifies permit-bound services — never by relabeling isolated runs.
    """
    if mode != "approved-pilot":
        raise StandError(f"pilot_suite requires mode approved-pilot, got {mode!r}")

    checkpoints = default_checkpoint_status()
    permit_path = permit_path_from_env(env)
    roots = list(probe_roots or [])
    before = snapshot_files(roots) if roots else {}
    runtime_executor = runtime_executor_from_env(env)

    def _refuse(message: str, *, status: str, details: dict[str, Any]) -> None:
        if roots:
            assert_no_pilot_side_effects(probe_roots=roots, before=before)
        raise PilotRefused(message, status=status, details=details)

    if suite is not None:
        # Suite validation is the caller's responsibility via get_suite; surface
        # empty names here so approved-pilot cannot bypass unknown-suite checks.
        if not str(suite).strip():
            _refuse(
                "approved-pilot refused: suite must be specified",
                status="not-run",
                details={
                    "status": "not-run",
                    "reason": "empty suite",
                    "checkpoints": checkpoints,
                    "mutations": False,
                    "runner_calls": 0,
                    "argv": list(argv),
                },
            )

    if permit_path is None:
        _refuse(
            "approved-pilot refused: no local exact operational permit "
            f"(set {PERMIT_ENV} to an absolute permit JSON)",
            status="not-run",
            details={
                "status": "not-run",
                "reason": f"missing {PERMIT_ENV}; approved-pilot is fail-closed",
                "checkpoints": checkpoints,
                "mutations": False,
                "runner_calls": 0,
                "argv": list(argv),
                "worktree": str(worktree),
                "suite": suite,
            },
        )

    try:
        permit = load_permit(permit_path)
    except PilotRefused:
        if roots:
            assert_no_pilot_side_effects(probe_roots=roots, before=before)
        raise

    allow_live = permit.get("allow_live_pilot") is True
    if not allow_live:
        _refuse(
            "approved-pilot refused: live pilot not authorised for this coding worker",
            status="not-run",
            details={
                "status": "not-run",
                "reason": "permit present but allow_live_pilot is not true; coding worker does not run live pilot",
                "permit_path": str(permit_path),
                "checkpoints": checkpoints,
                "mutations": False,
                "runner_calls": 0,
                "argv": list(argv),
                "suite": suite,
                "runtime_executor": runtime_executor or None,
            },
        )

    try:
        receipts = validate_checkpoint_receipts(permit.get("owner_action_receipts"))
    except PilotRefused as refused:
        details = {
            "status": "not-run",
            "reason": "permit lacks verified owner_action_receipts for each manual checkpoint",
            "permit_path": str(permit_path),
            "checkpoints": checkpoints,
            "mutations": False,
            "runner_calls": 0,
            "argv": list(argv),
            "suite": suite,
            "runtime_executor": runtime_executor or None,
            **(refused.details or {}),
        }
        _refuse(str(refused), status="not-run", details=details)

    if suite is None or not str(suite).strip():
        _refuse(
            "approved-pilot refused: suite name required before exact authorization",
            status="not-run",
            details={
                "status": "not-run",
                "reason": "empty suite for exact authorization",
                "checkpoints": checkpoints,
                "mutations": False,
                "runner_calls": 0,
                "argv": list(argv),
                "executor": permit.get("executor"),
            },
        )

    # Exact authorization (window / actions / resource binding) before any runner.
    try:
        authorization = authorize_exact_operational_permit(
            permit=permit,
            suite=str(suite),
            now=now,
            profile=profile,
        )
    except PilotRefused as refused:
        details = {
            "status": "not-run",
            "permit_path": str(permit_path),
            "checkpoints": _receipt_present_ledger(receipts),
            "mutations": False,
            "runner_calls": 0,
            "executed_count": 0,
            "argv": list(argv),
            "suite": suite,
            "runtime_executor": runtime_executor or None,
            **(refused.details or {}),
        }
        _refuse(str(refused), status="not-run", details=details)

    # Permit + receipts + exact auth are present. Coding-worker / mismatched
    # identity refuses without mutation (never simulated success).
    if not is_permit_bound_operational_executor(permit, env):
        checkpoint_ledger = _receipt_present_ledger(receipts)
        _refuse(
            "approved-pilot refused: authorised but not executed by coding worker",
            status="authorised-not-executed-by-coding-worker",
            details={
                "status": "authorised-not-executed-by-coding-worker",
                "reason": (
                    "coding-worker execution policy: set "
                    f"{EXECUTOR_ENV} to the permit executor for a separately "
                    "authorised coordinator/operational pilot run"
                ),
                "permit_path": str(permit_path),
                "checkpoints": checkpoint_ledger,
                "mutations": False,
                "executed_count": 0,
                "runner_calls": 0,
                "argv": list(argv),
                "suite": suite,
                "executor": permit.get("executor"),
                "runtime_executor": runtime_executor or None,
                "window": permit.get("window"),
                "authorized_actions": permit.get("authorized_actions"),
                "authorization": authorization,
            },
        )

    # Matching operational identity still must not invoke/relabel isolated suites.
    try:
        refuse_isolated_relabel_as_operational(
            permit=permit,
            permit_path=permit_path,
            suite=str(suite),
            argv=argv,
            receipts=receipts,
            authorization=authorization,
            runtime_executor=runtime_executor,
        )
    except PilotRefused:
        if roots:
            assert_no_pilot_side_effects(probe_roots=roots, before=before)
        raise
    raise StandError("approved-pilot refuse_isolated_relabel_as_operational returned")  # pragma: no cover


def describe_fail_closed_contract() -> dict[str, Any]:
    """Machine-readable contract for docs and unit tests."""
    return {
        "mode": "approved-pilot",
        "permit_env": PERMIT_ENV,
        "executor_env": EXECUTOR_ENV,
        "permit_schema": PERMIT_SCHEMA,
        "resources_schema": RESOURCES_SCHEMA,
        "without_permit": "not-run / non-zero rc / no environment or service writes",
        "unexecuted_pilot": "non-zero rc; never ok=true / executed_count>0",
        "exact_authorization": (
            "timezone-aware current window; authorized_actions must cover suite; "
            "typed non-null service_bindings; effective profile run_root/hosts must "
            "bind to permit run_root/owned_paths before any runner call"
        ),
        "operational_executor": (
            f"{EXECUTOR_ENV} must exactly match permit.executor after "
            "allow_live_pilot=true, verified owner_action_receipts, and exact "
            "authorization; even then isolated C7 runners are never relabeled as "
            "approved-pilot until a real operational contour is executed/read back"
        ),
        "manual_checkpoints": list(MANUAL_CHECKPOINTS),
        "checkpoint_default": "not-run",
        "receipt_requirement": "verified=true plus receipt_id or attested_at for each checkpoint",
        "receipt_vs_execution": (
            "receipt-present-not-executed remains until operational contour "
            "actions are actually executed and read back"
        ),
        "historical_p9_p10": "input only; not current end-to-end acceptance on this SHA",
    }
