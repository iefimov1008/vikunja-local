"""Isolated Hermes bridge suite (s06 / C2): real patched router/Core + stock Vikunja."""

from __future__ import annotations

import base64
import hashlib
import hmac
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from vikunja_local.api import Outcome, TokenRef, VikunjaClient, extract_marker
from vikunja_local.hermes_bridge import (
    SOURCE_KIND,
    BridgeConfig,
    HermesBridge,
    HermesOutboxStore,
    acceptance_payload_digest,
)
from vikunja_local.journal import IntegrationJournal

from .stand import IsolatedStand, StandError, load_profile, sha256_file

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

WRITE_PERMS = {"tasks": ["create", "read_all", "read_one", "update"]}
BASELINE_API = Path(
    "/Users/efimovivan/Projects/cto-agent-environment/plugins/personal-tasks/dashboard/plugin_api.py"
)
BASELINE_CORE = Path("/Users/efimovivan/Projects/cto-agent-environment/scripts/ea_core.py")
FROM_API = "938dc648daee8f5484a76de6bac0ed86f79b28463c89a1d17c6af02e833b0f51"
FROM_CORE = "23deacba5cca06d314c5ec5717bbb3f6dd7f074974670324ab7f64baaecf10ef"
ORIGIN = "http://127.0.0.1:19347"
TITLE = "EA010-SYNTHETIC hermes-bridge accept"
CALENDAR_TITLE = "EA010-SYNTHETIC Calendar title"
CALENDAR_STARTS = "2026-11-05T09:00:00+03:00"
CALENDAR_TZ = "Europe/Moscow"


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "vikunja_local" / "hermes_bridge.py",
        worktree / "integration" / "hermes" / "backend.patch",
        worktree / "integration" / "hermes" / "backend-manifest.json",
        worktree / "tests" / "test_hermes_bridge.py",
        worktree / "tests" / "support" / "hermes_bridge_suite.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(sha256_file(path).encode())
        digest.update(b"\0")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "sha256": digest.hexdigest(),
        "head": head.stdout.strip() if head.returncode == 0 else "unknown",
    }


def _hermes_python() -> str:
    override = os.environ.get("EA010_HERMES_PYTHON")
    if override:
        path = Path(override)
        _assert(path.is_file() and os.access(path, os.X_OK), f"EA010_HERMES_PYTHON unusable: {override}")
        return str(path)
    roots = [
        Path("/Users/efimovivan/.hermes/installs"),
    ]
    candidates: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        candidates.extend(root.glob("**/environments/*/venv/bin/python"))
        candidates.extend(root.glob("**/test-environment/*/venv/bin/python"))
    for cand in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
        probe = subprocess.run(
            [str(cand), "-c", "import fastapi, fastapi.testclient"],
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0:
            return str(cand)
    raise StandError(
        "Real Hermes router requires FastAPI; set EA010_HERMES_PYTHON to a Hermes venv python"
    )


def _token_path(stand: IsolatedStand, name: str) -> Path:
    path = stand.run_dir / "secrets" / name
    path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path.parent, 0o700)
    return path


def _store_token(stand: IsolatedStand, name: str, secret: str) -> Path:
    path = _token_path(stand, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        os.write(fd, secret.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return path


def _create_api_token(stand: IsolatedStand, *, title: str, permissions: dict[str, list[str]]) -> dict[str, Any]:
    if not stand._session_token:
        raise StandError("session required to mint API token")
    expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    status, payload = stand.api(
        "tokens",
        "PUT",
        {"title": title, "expires_at": expires, "permissions": permissions},
        token=stand._session_token,
    )
    if status != 201 or not isinstance(payload, dict) or "token" not in payload:
        raise StandError(f"API token create failed: HTTP {status}")
    return payload


def _prepare_patched_tree(worktree: Path, dest: Path) -> dict[str, Any]:
    """Copy approved baselines into dest, apply backend.patch, verify to-hashes."""
    manifest_path = worktree / "integration" / "hermes" / "backend-manifest.json"
    patch_path = worktree / "integration" / "hermes" / "backend.patch"
    _assert(manifest_path.is_file(), "backend-manifest.json missing")
    _assert(patch_path.is_file(), "backend.patch missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _assert(sha256_file(BASELINE_API) == FROM_API, "plugin_api.py baseline hash drift")
    _assert(sha256_file(BASELINE_CORE) == FROM_CORE, "ea_core.py baseline hash drift")
    _assert(sha256_file(patch_path) == manifest["patch"]["sha256"], "patch sha mismatch vs manifest")

    if dest.exists():
        shutil.rmtree(dest)
    (dest / "plugins/personal-tasks/dashboard").mkdir(parents=True)
    (dest / "scripts").mkdir(parents=True)
    shutil.copy2(BASELINE_API, dest / "plugins/personal-tasks/dashboard/plugin_api.py")
    shutil.copy2(BASELINE_CORE, dest / "scripts/ea_core.py")
    apply = subprocess.run(
        ["patch", "-p1", "-i", str(patch_path)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(apply.returncode == 0, f"patch apply failed: {apply.stdout}\n{apply.stderr}")
    to_api = sha256_file(dest / "plugins/personal-tasks/dashboard/plugin_api.py")
    to_core = sha256_file(dest / "scripts/ea_core.py")
    expected_api = next(s["to_sha256"] for s in manifest["sources"] if s["path"].endswith("plugin_api.py"))
    expected_core = next(s["to_sha256"] for s in manifest["sources"] if s["path"].endswith("ea_core.py"))
    _assert(to_api == expected_api, f"patched plugin_api to-hash {to_api} != {expected_api}")
    _assert(to_core == expected_core, f"patched ea_core to-hash {to_core} != {expected_core}")

    # Reverse check on a separate copy; leave dest patched for the run.
    rev = dest.parent / (dest.name + "-reverse-check")
    if rev.exists():
        shutil.rmtree(rev)
    shutil.copytree(dest, rev)
    reverse = subprocess.run(
        ["patch", "-p1", "-R", "-i", str(patch_path)],
        cwd=rev,
        capture_output=True,
        text=True,
    )
    _assert(reverse.returncode == 0, f"reverse patch failed: {reverse.stdout}\n{reverse.stderr}")
    _assert(sha256_file(rev / "plugins/personal-tasks/dashboard/plugin_api.py") == FROM_API, "reverse api hash")
    _assert(sha256_file(rev / "scripts/ea_core.py") == FROM_CORE, "reverse core hash")
    shutil.rmtree(rev)
    return {
        "from_api": FROM_API,
        "from_core": FROM_CORE,
        "to_api": to_api,
        "to_core": to_core,
        "patch_sha256": manifest["patch"]["sha256"],
        "reverse_ok": True,
        "patched_tree": str(dest),
    }


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    _assert(spec is not None and spec.loader is not None, f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _count_marker(client: VikunjaClient, marker: str) -> int:
    listed = client.list_project_tasks()
    _assert(listed.outcome is Outcome.CONFIRMED, f"list failed: {listed.outcome}")
    count = 0
    for item in listed.details.get("items") or []:
        extracted = extract_marker(item.get("description"))
        if extracted and extracted["marker"] == marker:
            count += 1
    return count


def run_hermes_bridge_scenarios(stand: IsolatedStand, worktree: Path) -> dict[str, Any]:
    import fastapi
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient

    _ = fastapi
    # Core private reads refuse group/other bits (same as P10 spike).
    os.umask(0o077)
    patch_info = _prepare_patched_tree(worktree, stand.run_dir / "hermes-baseline")
    plugin_path = Path(patch_info["patched_tree"]) / "plugins/personal-tasks/dashboard/plugin_api.py"
    core_path = Path(patch_info["patched_tree"]) / "scripts/ea_core.py"

    # Point patched plugin at the patched Core copy (not production).
    plugin = _load_module("ea010_s06_plugin_api", plugin_path)
    plugin.CORE_PATH = core_path
    plugin._module = None

    session = stand.create_synthetic_session()
    project_id = int(session["project_id"])
    write_token = _create_api_token(
        stand, title=stand.fixture_name("hermes-write"), permissions=WRITE_PERMS
    )
    token_path = _store_token(stand, "hermes-write.token", write_token["token"])
    stand._token_secrets["hermes_write"] = write_token["token"]
    stand.manifest.setdefault("token_handles", []).append("hermes_write")
    stand.synthetic_ids["hermes_write_token_id"] = write_token.get("id")
    stand._save_secrets()
    stand._save_manifest()

    runtime = stand.run_dir / "hermes-runtime"
    tasks_dir = stand.run_dir / "hermes-tasks"
    runtime.mkdir(mode=0o700)
    tasks_dir.mkdir(mode=0o700)
    policy = stand.run_dir / "policy.json"
    policy.write_text(
        json.dumps({"text_analysis": {"preferred_agent": "codex", "preferred_model": "fixture"}}),
        encoding="utf-8",
    )
    settings = {
        "owner": "s06-owner",
        "runtime": str(runtime),
        "tasks": str(tasks_dir),
        "policy": str(policy),
    }
    settings_path = stand.run_dir / "plugin-settings.json"
    settings_path.write_text(json.dumps(settings), encoding="utf-8")
    os.chmod(settings_path, 0o600)
    os.chmod(policy, 0o600)
    plugin.SETTINGS_PATH = settings_path

    journal_path = stand.run_dir / "journal" / "integration.sqlite3"
    journal_path.parent.mkdir(mode=0o700, exist_ok=True)
    bridge_cfg_path = stand.run_dir / "bridge-config.json"
    origin_vikunja = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    bridge_cfg = {
        "journal_path": str(journal_path),
        "api_origin": origin_vikunja,
        "project_id": project_id,
        "token_ref": {"kind": "file", "path": str(token_path)},
        "task_url_template": origin_vikunja + "/tasks/{id}",
        "hermes_origin": ORIGIN,
        "page_size": 2,
    }
    bridge_cfg_path.write_text(json.dumps(bridge_cfg, indent=2), encoding="utf-8")
    os.chmod(bridge_cfg_path, 0o600)
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(bridge_cfg_path)

    # Seed Core via real Core class from patched module.
    core = plugin.core(settings)


    def _write_private(path: Path, data: str) -> None:
        path.write_text(data, encoding="utf-8")
        os.chmod(path, 0o600)

    def prepare(mid: str, title: str) -> str:
        text = f"# Synthetic fixture\nOwner: {title}\n"
        name = f"transcript-{mid}.md"
        path = runtime / name
        path.write_text(text, encoding="utf-8")
        os.chmod(path, 0o600)
        version = hashlib.sha256(text.encode()).hexdigest()
        registry["meetings"][mid] = {
            "first_seen": "2026-11-05T12:00:00+03:00",
            "status": "saved",
            "transcript": {"sha256": version, "file": name},
        }
        return version

    def seed(mid: str, title: str, version: str, *, legacy: bool = False) -> dict[str, Any]:
        core.ingest(
            mid,
            version,
            {
                "summary": "Synthetic s06",
                "decisions": [],
                "unresolved": [],
                "actions": [
                    {
                        "text": title,
                        "kind": "self_commitment",
                        "owner": "Owner",
                        "speaker": "Owner",
                        "due_hint": None,
                        "line_start": 2,
                        "line_end": 2,
                        "quote": f"Owner: {title}",
                    }
                ],
            },
        )
        prop = next(x for x in core.snapshot()["proposals"] if x["meeting_id"] == mid)
        if legacy:
            core.decision(prop["id"], prop["revision"], "accept", title=title, due=None)
        return next(x for x in core.snapshot()["proposals"] if x["meeting_id"] == mid)

    registry = {
        "schema": 1,
        "scope": "owned_and_invited",
        "first_day": "2026-10-30",
        "last_complete_day": "2026-11-05",
        "meetings": {},
    }
    legacy_title = stand.fixture_name("legacy-markdown")
    crash_title_seed = stand.fixture_name("crash-before-dispatch")
    pending_title_seed = stand.fixture_name("pending-rollback")
    crash_gap_title_seed = stand.fixture_name("crash-accept-gap")
    crash_receipt_title_seed = stand.fixture_name("crash-before-receipt")
    crash_backlink_title_seed = stand.fixture_name("crash-before-backlink")
    dated_drift_title_seed = stand.fixture_name("dated-frozen-drift")
    legacy_version = prepare("a" * 32, legacy_title)
    proposal_version = prepare("b" * 32, TITLE)
    crash_version_seed = prepare("c" * 32, crash_title_seed)
    pending_version_seed = prepare("d" * 32, pending_title_seed)
    crash_gap_version_seed = prepare("e" * 32, crash_gap_title_seed)
    crash_receipt_version_seed = prepare("f" * 32, crash_receipt_title_seed)
    crash_backlink_version_seed = prepare("0" * 32, crash_backlink_title_seed)
    dated_drift_version_seed = prepare("1" * 32, dated_drift_title_seed)
    _write_private(runtime / ".collector-state.json", json.dumps(registry))
    legacy = seed("a" * 32, legacy_title, legacy_version, legacy=True)
    proposal = seed("b" * 32, TITLE, proposal_version)
    crash_prop_seed = seed("c" * 32, crash_title_seed, crash_version_seed)
    pending_prop_seed = seed("d" * 32, pending_title_seed, pending_version_seed)
    crash_gap_prop_seed = seed("e" * 32, crash_gap_title_seed, crash_gap_version_seed)
    crash_receipt_prop_seed = seed("f" * 32, crash_receipt_title_seed, crash_receipt_version_seed)
    crash_backlink_prop_seed = seed("0" * 32, crash_backlink_title_seed, crash_backlink_version_seed)
    dated_drift_prop_seed = seed("1" * 32, dated_drift_title_seed, dated_drift_version_seed)
    # Verified calendar projection for happy-path meeting (canonical provenance).
    checked_at = "2026-11-05T12:00:00+03:00"
    calendar_meetings = {
        mid: {
            "title": CALENDAR_TITLE if mid == "b" * 32 else f"EA010-SYNTHETIC calendar {mid[:8]}",
            "starts_at": CALENDAR_STARTS,
            "ends_at": "2026-11-05T10:00:00+03:00",
            "timezone": CALENDAR_TZ,
            "verification_status": "verified",
            "checked_at": checked_at,
            "time_basis": "calendar",
            "matched_by": "meeting_and_activity_ids",
        }
        for mid in (
            "a" * 32,
            "b" * 32,
            "c" * 32,
            "d" * 32,
            "e" * 32,
            "f" * 32,
            "0" * 32,
            "1" * 32,
        )
    }
    analysis_home = runtime / ".analysis"
    analysis_home.mkdir(mode=0o700, exist_ok=True)
    _write_private(
        analysis_home / "calendar-metadata.json",
        json.dumps(
            {
                "schema": "ea009-calendar/v1",
                "checked_at": checked_at,
                "meetings": calendar_meetings,
            },
            ensure_ascii=False,
        ),
    )
    legacy_files = list(tasks_dir.glob("*.md"))
    _assert(len(legacy_files) == 1, "legacy Markdown missing")
    legacy_sha_before = sha256_file(legacy_files[0])

    owner_password = uuid.uuid4().hex
    other_password = uuid.uuid4().hex
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    endpoint = (
        f"/api/plugins/personal-tasks/proposals/{proposal['id']}/decision?profile=default"
    )
    payload = {
        "revision": proposal["revision"],
        "action": "accept",
        "title": TITLE,
        "due": None,
        "due_confirmed": True,
        "tracker_target": "vikunja",
        "source_version": proposal["version"],
    }

    @app.middleware("http")
    async def authenticate(request: Request, call_next):  # noqa: ANN001
        # Intentionally do not Host-fence: product Origin guard must reject
        # matching foreign Host+Origin against BridgeConfig.hermes_origin.
        auth = request.headers.get("authorization", "")
        for user, pwd in (("s06-owner", owner_password), ("s06-other", other_password)):
            expected = "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()
            if hmac.compare_digest(auth, expected):
                request.state.session = SimpleNamespace(user_id=user)
        response = await call_next(request)
        return response

    app.include_router(plugin.router, prefix="/api/plugins/personal-tasks")

    client_api = VikunjaClient(
        api_origin=origin_vikunja,
        project_id=project_id,
        token_ref=TokenRef(kind="file", path=token_path),
        page_size=2,
        max_items_per_page=50,
    )

    scenarios: dict[str, Any] = {"patch": patch_info}
    synthetic_ids: dict[str, Any] = {
        "project_id": project_id,
        "proposal_id": proposal["id"],
        "legacy_proposal_id": legacy["id"],
        "hermes_write_token_id": write_token.get("id"),
    }
    executed = 0

    with TestClient(app, base_url=ORIGIN) as client:
        owner = ("s06-owner", owner_password)
        other = ("s06-other", other_password)

        def attempt(auth=None, origin=ORIGIN, data=None, host=None):  # noqa: ANN001
            headers = {"Origin": origin}
            if host is not None:
                headers["Host"] = host
            return client.post(
                endpoint,
                json=data or payload,
                auth=auth,
                headers=headers,
            ).status_code

        omitted_due = {k: v for k, v in payload.items() if k != "due"}
        negatives = {
            "anonymous": attempt(),
            "other": attempt(other),
            "foreign_origin": attempt(owner, "http://other.invalid"),
            # Matching foreign Host+Origin must still hit configured-origin guard.
            "foreign_host_origin": attempt(
                owner,
                "http://foreign.invalid",
                host="foreign.invalid",
            ),
            "stale_revision": attempt(owner, data={**payload, "revision": payload["revision"] + 1}),
            "unconfirmed_due": attempt(owner, data={**payload, "due_confirmed": False}),
            "omitted_due": attempt(owner, data=omitted_due),
            "client_supplied_acceptance": attempt(owner, data={**payload, "accepted": True}),
            "stale_source_version": attempt(
                owner, data={**payload, "source_version": "0" * 64}
            ),
        }
        _assert(negatives["anonymous"] == 401, f"anonymous: {negatives}")
        _assert(negatives["other"] == 403, f"other: {negatives}")
        _assert(negatives["foreign_origin"] == 403, f"origin: {negatives}")
        _assert(negatives["foreign_host_origin"] == 403, f"host+origin: {negatives}")
        _assert(negatives["stale_revision"] == 409, f"stale rev: {negatives}")
        _assert(negatives["unconfirmed_due"] == 422, f"due: {negatives}")
        _assert(negatives["omitted_due"] == 422, f"omitted due: {negatives}")
        _assert(negatives["client_supplied_acceptance"] == 422, f"accepted: {negatives}")
        _assert(negatives["stale_source_version"] == 409, f"stale ver: {negatives}")
        # Negatives must not create Vikunja tasks for this title.
        listed = client_api.list_project_tasks()
        _assert(listed.outcome is Outcome.CONFIRMED, "list after negatives")
        for item in listed.details.get("items") or []:
            _assert(item.get("title") != TITLE, "negative created task")
        scenarios["negatives"] = negatives
        executed += 1

        # R2: missing / partial / error calendar projections must not accept or create.
        calendar_path = analysis_home / "calendar-metadata.json"
        calendar_backup = calendar_path.read_text(encoding="utf-8")
        outbox_probe = Path(settings["runtime"]) / ".analysis" / "vikunja_outbox.json"
        provenance_negatives: dict[str, Any] = {}

        def _assert_no_accept_no_task(label: str, code: int) -> None:
            _assert(code == 422, f"{label} HTTP {code}")
            prop_now = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal["id"])
            _assert(prop_now["status"] != "accepted", f"{label} accepted status")
            _assert(prop_now.get("tracker") is None, f"{label} tracker leaked")
            if outbox_probe.exists():
                store = HermesOutboxStore(outbox_probe)
                _assert(store.get_by_proposal(proposal["id"]) is None, f"{label} outbox row")
            listed_now = client_api.list_project_tasks()
            _assert(listed_now.outcome is Outcome.CONFIRMED, f"{label} list")
            for item in listed_now.details.get("items") or []:
                _assert(item.get("title") != TITLE, f"{label} created task")

        calendar_path.unlink()
        missing_code = attempt(owner)
        _assert_no_accept_no_task("missing_calendar", missing_code)
        provenance_negatives["missing_calendar"] = missing_code

        def _write_calendar(status: str, *, title: str | None, starts: str | None, tz: str | None) -> None:
            record = {
                "title": title,
                "starts_at": starts,
                "ends_at": "2026-11-05T10:00:00+03:00" if starts else None,
                "timezone": tz,
                "verification_status": status,
                "checked_at": checked_at,
                "time_basis": "calendar",
                "matched_by": "meeting_and_activity_ids",
            }
            if status == "error":
                record["error_code"] = "calendar_lookup_failed"
            payload_cal = {
                "schema": "ea009-calendar/v1",
                "checked_at": checked_at,
                "meetings": {
                    mid: dict(record) if mid == "b" * 32 else {
                        "title": f"EA010-SYNTHETIC calendar {mid[:8]}",
                        "starts_at": CALENDAR_STARTS,
                        "ends_at": "2026-11-05T10:00:00+03:00",
                        "timezone": CALENDAR_TZ,
                        "verification_status": "verified",
                        "checked_at": checked_at,
                        "time_basis": "calendar",
                        "matched_by": "meeting_and_activity_ids",
                    }
                    for mid in (
                        "a" * 32,
                        "b" * 32,
                        "c" * 32,
                        "d" * 32,
                        "e" * 32,
                        "f" * 32,
                        "0" * 32,
                    )
                },
            }
            _write_private(calendar_path, json.dumps(payload_cal, ensure_ascii=False))

        _write_calendar(
            "partial",
            title="EA010-SYNTHETIC partial title",
            starts=CALENDAR_STARTS,
            tz=CALENDAR_TZ,
        )
        partial_code = attempt(owner)
        _assert_no_accept_no_task("partial_calendar", partial_code)
        provenance_negatives["partial_calendar"] = partial_code

        _write_calendar("error", title=None, starts=None, tz=None)
        error_code = attempt(owner)
        _assert_no_accept_no_task("error_calendar", error_code)
        provenance_negatives["error_calendar"] = error_code

        _write_private(calendar_path, calendar_backup)
        scenarios["provenance_negatives"] = provenance_negatives
        executed += 1

        # Legacy accepted must not auto-dispatch on snapshot/recover.
        snap = core.snapshot()
        legacy_after = next(p for p in snap["proposals"] if p["id"] == legacy["id"])
        _assert(legacy_after["status"] == "accepted", "legacy status")
        _assert(legacy_after.get("tracker") is None, "legacy must not gain tracker")
        _assert(sha256_file(legacy_files[0]) == legacy_sha_before, "legacy markdown mutated")
        recovered = core.recover_vikunja_outbox()
        _assert(recovered == [] or all(
            (getattr(r, "record", None) is None) or r.record.proposal_id != legacy["id"]
            for r in recovered
        ), "legacy must not be in vikunja outbox recover")
        scenarios["legacy_preserved"] = {
            "sha256_before": legacy_sha_before,
            "sha256_after": sha256_file(legacy_files[0]),
            "tracker": legacy_after.get("tracker"),
            "outbox_recover_count": len(recovered) if isinstance(recovered, list) else 0,
        }
        executed += 1

        # Happy path: one accept → one outbox/task/backlink.
        first = client.post(endpoint, json=payload, auth=owner, headers={"Origin": ORIGIN})
        _assert(first.status_code == 200, f"accept HTTP {first.status_code}: {first.text}")
        body = first.json()
        tracker = body["proposal"]["tracker"]
        _assert(tracker["state"] == "linked", f"tracker state: {tracker}")
        _assert(isinstance(tracker["task_id"], int), "task_id missing")
        _assert(isinstance(tracker["url"], str) and tracker["url"].endswith(f"/tasks/{tracker['task_id']}"), tracker)
        event_id = tracker["event_id"]
        synthetic_ids["event_id"] = event_id
        synthetic_ids["task_id"] = tracker["task_id"]
        # Confirm single Vikunja task via journal marker + navigable source link.
        journal = IntegrationJournal(journal_path)
        journal.open()
        try:
            mapped = journal.lookup_source(SOURCE_KIND, event_id)
            _assert(mapped is not None and mapped.receipt is not None, "source mapping/receipt missing")
            marker = mapped.marker
            _assert(_count_marker(client_api, marker) == 1, "create_count must be 1")
            synthetic_ids["operation_id"] = mapped.operation_id
            synthetic_ids["marker"] = marker
            read_back = client_api.read(int(tracker["task_id"]))
            _assert(read_back.outcome is Outcome.CONFIRMED and read_back.task, "task read-back failed")
            description = str((read_back.task or {}).get("description") or "")
            source_url = f"{ORIGIN}/#pt-proposal-{proposal['id']}"
            _assert(source_url in description, f"missing source URL in description: {description!r}")
            _assert(f"[Open proposal]({source_url})" in description, "missing markdown source link")
            _assert(
                f"Meeting title: {CALENDAR_TITLE}" in description,
                f"missing verified calendar title in description: {description!r}",
            )
            _assert(
                f"Meeting time: {CALENDAR_STARTS} ({CALENDAR_TZ})" in description,
                f"missing verified calendar time in description: {description!r}",
            )
            _assert("Meeting calendar: unavailable" not in description, "calendar marked unavailable")
            _assert("Meeting title: Synthetic s06" not in description, "inventory summary leaked as title")
            synthetic_ids["source_url"] = source_url
            synthetic_ids["calendar_title"] = CALENDAR_TITLE
            synthetic_ids["calendar_starts_at"] = CALENDAR_STARTS
        finally:
            journal.close()
        # Decision history preserved once.
        accepted_prop = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal["id"])
        _assert(accepted_prop["status"] == "accepted", "happy accept status")
        _assert(len(accepted_prop.get("decisions") or []) == 1, "happy accept decision count")
        snap_meeting = next(m for m in core.snapshot()["meetings"] if m["id"] == "b" * 32)
        _assert(
            (snap_meeting.get("calendar") or {}).get("title") == CALENDAR_TITLE,
            "snapshot calendar title missing",
        )
        scenarios["happy_accept"] = {
            "event_id": event_id,
            "task_id": tracker["task_id"],
            "url": tracker["url"],
            "state": tracker["state"],
            "source_url": synthetic_ids["source_url"],
            "decision_count": len(accepted_prop.get("decisions") or []),
            "calendar_title": CALENDAR_TITLE,
            "calendar_starts_at": CALENDAR_STARTS,
        }
        executed += 1

        # Identical replay returns same event/task; no second create.
        second = client.post(endpoint, json=payload, auth=owner, headers={"Origin": ORIGIN})
        _assert(second.status_code == 200, f"replay HTTP {second.status_code}: {second.text}")
        tracker2 = second.json()["proposal"]["tracker"]
        _assert(tracker2["event_id"] == event_id, "replay event_id drift")
        _assert(tracker2["task_id"] == tracker["task_id"], "replay task_id drift")
        _assert(_count_marker(client_api, synthetic_ids["marker"]) == 1, "replay duplicated create")
        # Old revision of identical accept still returns prior event (C2).
        old_rev = client.post(
            endpoint,
            json={**payload, "revision": 1},
            auth=owner,
            headers={"Origin": ORIGIN},
        )
        _assert(old_rev.status_code == 200, f"old-rev replay HTTP {old_rev.status_code}")
        _assert(old_rev.json()["proposal"]["tracker"]["event_id"] == event_id, "old-rev event drift")
        scenarios["replay"] = {
            "event_id": event_id,
            "task_id": tracker2["task_id"],
            "old_revision_ok": True,
        }
        executed += 1

        # Changed payload on accepted proposal → 409, no new task.
        conflict = client.post(
            endpoint,
            json={**payload, "title": TITLE + " changed"},
            auth=owner,
            headers={"Origin": ORIGIN},
        )
        _assert(conflict.status_code == 409, f"payload conflict HTTP {conflict.status_code}")
        _assert(_count_marker(client_api, synthetic_ids["marker"]) == 1, "conflict created task")
        scenarios["payload_conflict"] = {"http": conflict.status_code}
        executed += 1

        # R1: corrupt / wrong-schema / read-failing outbox on snapshot must report
        # unavailable (CoreError → router 503), not silently look like legacy.
        outbox_fault_path = Path(settings["runtime"]) / ".analysis" / "vikunja_outbox.json"
        _assert(outbox_fault_path.is_file(), "outbox missing before fault injection")
        outbox_fault_backup = outbox_fault_path.read_bytes()
        creates_before_fault = _count_marker(client_api, synthetic_ids["marker"])
        CoreError = plugin._module.CoreError

        def _acceptance_identity() -> dict[str, Any]:
            with core._lock():
                state = core._inventory(core._state())
                prop = state["proposals"][proposal["id"]]
                decisions = [
                    d
                    for d in (prop.get("decisions") or [])
                    if isinstance(d, dict) and d.get("event_id") == event_id
                ]
                return {
                    "status": prop.get("status"),
                    "event_id": decisions[0].get("event_id") if decisions else None,
                    "tracker_target": decisions[0].get("tracker_target") if decisions else None,
                }

        def _restore_outbox() -> None:
            if outbox_fault_path.exists():
                if outbox_fault_path.is_dir():
                    outbox_fault_path.rmdir()
                else:
                    outbox_fault_path.unlink()
            outbox_fault_path.write_bytes(outbox_fault_backup)
            os.chmod(outbox_fault_path, 0o600)

        def _expect_snapshot_code(code: str, label: str) -> str:
            raised = None
            try:
                core.snapshot()
            except Exception as exc:  # noqa: BLE001 — assert CoreError.code
                raised = exc
            _assert(raised is not None, f"{label}: snapshot did not raise")
            _assert(isinstance(raised, CoreError), f"{label}: expected CoreError, got {type(raised)}")
            _assert(getattr(raised, "code", None) == code, f"{label}: code={getattr(raised, 'code', None)}")
            identity = _acceptance_identity()
            _assert(identity["status"] == "accepted", f"{label}: lost accepted status")
            _assert(identity["event_id"] == event_id, f"{label}: lost event_id")
            _assert(identity["tracker_target"] == "vikunja", f"{label}: lost tracker_target")
            _assert(
                _count_marker(client_api, synthetic_ids["marker"]) == creates_before_fault,
                f"{label}: unexpected dispatch/create",
            )
            return code

        # Corrupt JSON object (review reproduction: truncated ledger).
        outbox_fault_path.write_text('{"schema":', encoding="utf-8")
        os.chmod(outbox_fault_path, 0o600)
        corrupt_code = _expect_snapshot_code("local_state_unreadable", "corrupt_json")
        snap_http = client.get(
            "/api/plugins/personal-tasks/snapshot?profile=default",
            auth=owner,
        )
        _assert(snap_http.status_code == 503, f"corrupt_json HTTP {snap_http.status_code}")
        _restore_outbox()

        # Wrong schema must not classify as missing/legacy tracker.
        outbox_fault_path.write_text(
            json.dumps({"schema": "not-ea010-outbox/v0", "by_proposal": {}, "by_event": {}}),
            encoding="utf-8",
        )
        os.chmod(outbox_fault_path, 0o600)
        schema_code = _expect_snapshot_code("local_state_schema_changed", "wrong_schema")
        _restore_outbox()

        # Read-failing ledger (path is a directory → OSError on read).
        outbox_fault_path.unlink()
        outbox_fault_path.mkdir(mode=0o700)
        read_fail_code = _expect_snapshot_code("local_state_unreadable", "read_failing")
        _restore_outbox()

        # R1: missing outbox file for a Vikunja-identified accept → unavailable, not legacy.
        outbox_fault_path.unlink()
        _assert(not outbox_fault_path.exists(), "outbox still present before missing-file case")
        missing_file_code = _expect_snapshot_code("local_state_unreadable", "missing_outbox_file")
        snap_missing_http = client.get(
            "/api/plugins/personal-tasks/snapshot?profile=default",
            auth=owner,
        )
        _assert(
            snap_missing_http.status_code == 503,
            f"missing_outbox_file HTTP {snap_missing_http.status_code}",
        )
        _restore_outbox()

        # R1: dropped by_event index must not project a healthy linked tracker.
        linked_before = json.loads(outbox_fault_path.read_text(encoding="utf-8"))
        historical_tracker = None
        for raw in (linked_before.get("by_event") or {}).values():
            if isinstance(raw, dict) and raw.get("event_id") == event_id:
                historical_tracker = {
                    "event_id": raw.get("event_id"),
                    "state": raw.get("state"),
                    "task_id": raw.get("task_id"),
                    "url": raw.get("url"),
                }
                break
        _assert(historical_tracker is not None, "missing historical linked tracker before index fault")
        damaged_index = dict(linked_before)
        damaged_index.pop("by_event", None)
        outbox_fault_path.write_text(json.dumps(damaged_index), encoding="utf-8")
        os.chmod(outbox_fault_path, 0o600)
        missing_index_code = _expect_snapshot_code("local_state_schema_changed", "missing_by_event_index")
        # deliver_vikunja_outbox must attach availability onto retained historical fields.
        delivered_missing_index = core.deliver_vikunja_outbox(
            {"id": proposal["id"], "status": "accepted", "tracker": dict(historical_tracker)}
        )
        missing_index_tracker = delivered_missing_index.get("tracker") or {}
        missing_index_avail = missing_index_tracker.get("availability") or {}
        _assert(
            missing_index_tracker.get("state") == historical_tracker.get("state"),
            f"missing-index lost historical state: {missing_index_tracker}",
        )
        _assert(
            missing_index_avail.get("status") == "unavailable",
            f"missing-index availability: {missing_index_tracker}",
        )
        _assert(
            missing_index_avail.get("reason") == "outbox_schema_mismatch",
            f"missing-index reason: {missing_index_avail}",
        )
        _assert(
            _count_marker(client_api, synthetic_ids["marker"]) == creates_before_fault,
            "missing-index fault created a task",
        )
        _restore_outbox()

        # R1: by_proposal retained but indexed event row removed → inconsistent indexes.
        inconsistent = json.loads(outbox_fault_path.read_text(encoding="utf-8"))
        inconsistent["by_event"] = {}
        outbox_fault_path.write_text(json.dumps(inconsistent), encoding="utf-8")
        os.chmod(outbox_fault_path, 0o600)
        missing_row_code = _expect_snapshot_code("local_state_unreadable", "missing_indexed_event_row")
        delivered_missing_row = core.deliver_vikunja_outbox(
            {"id": proposal["id"], "status": "accepted", "tracker": dict(historical_tracker)}
        )
        missing_row_avail = (delivered_missing_row.get("tracker") or {}).get("availability") or {}
        _assert(
            missing_row_avail.get("status") == "unavailable",
            f"missing-row availability: {delivered_missing_row}",
        )
        _assert(
            missing_row_avail.get("reason") == "outbox_index_inconsistent",
            f"missing-row reason: {missing_row_avail}",
        )
        _restore_outbox()

        # R1: bridge import failure must not drop tracker identity for Vikunja accepts.
        import builtins

        real_import = builtins.__import__

        def _boom(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
            if name == "vikunja_local.hermes_bridge" or name.startswith("vikunja_local.hermes_bridge"):
                raise ModuleNotFoundError(name)
            return real_import(name, globals, locals, fromlist, level)

        builtins.__import__ = _boom  # type: ignore[assignment]
        try:
            import_fail_code = _expect_snapshot_code("local_state_unreadable", "bridge_import_failure")
        finally:
            builtins.__import__ = real_import  # type: ignore[assignment]

        healed = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal["id"])
        healed_tracker = healed.get("tracker") or {}
        _assert(healed["status"] == "accepted", "healed snapshot lost accepted")
        _assert(healed_tracker.get("event_id") == event_id, "healed snapshot lost event_id")
        _assert(healed_tracker.get("state") == "linked", f"healed tracker: {healed_tracker}")
        _assert(
            _count_marker(client_api, synthetic_ids["marker"]) == creates_before_fault,
            "fault suite dispatched after restore",
        )
        scenarios["outbox_unavailable_snapshot"] = {
            "corrupt_json": corrupt_code,
            "wrong_schema": schema_code,
            "read_failing": read_fail_code,
            "missing_outbox_file": missing_file_code,
            "missing_by_event_index": missing_index_code,
            "missing_indexed_event_row": missing_row_code,
            "missing_index_availability": missing_index_avail.get("status"),
            "missing_row_availability": missing_row_avail.get("status"),
            "bridge_import_failure": import_fail_code,
            "http_503": snap_http.status_code,
            "missing_outbox_http_503": snap_missing_http.status_code,
            "event_id": event_id,
            "creates_unchanged": creates_before_fault,
            "healed_state": healed_tracker.get("state"),
        }
        executed += 1

    # Crash / recovery using bridge API against the durable outbox (real SIGKILL children).
    crash_dir = stand.run_dir / "crash"
    crash_dir.mkdir(mode=0o700)
    outbox_path = Path(settings["runtime"]) / ".analysis" / "vikunja_outbox.json"
    _assert(outbox_path.is_file(), "outbox file missing after accept")
    bridge = HermesBridge(BridgeConfig.from_mapping(bridge_cfg), HermesOutboxStore(outbox_path))
    child_env = {
        **os.environ,
        "EA010_WORKTREE": str(worktree),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(worktree),
    }

    def _write_crash_child(path: Path, body: str) -> None:
        path.write_text(body, encoding="utf-8")

    def _run_sigkill_child(child_py: Path, child_cfg_path: Path, ready: Path) -> int:
        if ready.exists():
            ready.unlink()
        proc = subprocess.Popen(
            [sys.executable, str(child_py), str(child_cfg_path)],
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if ready.exists() and ready.stat().st_size > 0:
                break
            if proc.poll() is not None and not ready.exists():
                out, err = proc.communicate(timeout=1)
                raise StandError(
                    f"crash child exited early rc={proc.returncode} out={out!r} err={err!r}"
                )
            time.sleep(0.05)
        else:
            proc.kill()
            raise StandError("timeout waiting for crash ready")
        rc = proc.wait(timeout=30)
        _assert(rc == -signal.SIGKILL, f"expected SIGKILL, rc={rc}")
        return rc

    crash_child_common = '''
import json, os, sys, signal, importlib.util
from pathlib import Path

cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
worktree = Path(os.environ["EA010_WORKTREE"])
sys.path.insert(0, str(worktree))
os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(Path(cfg["bridge_config_path"]))

from vikunja_local.hermes_bridge import BridgeConfig, HermesBridge, HermesOutboxStore

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

plugin = load(cfg["plugin_module"], Path(cfg["plugin_path"]))
plugin.CORE_PATH = Path(cfg["core_path"])
plugin._module = None
plugin.SETTINGS_PATH = Path(cfg["settings_path"])
core = plugin.core(cfg["settings"])
'''

    # Leave one accepted+pending row undelivered for rollback coverage.
    pending_accepted = core.decision(
        pending_prop_seed["id"],
        pending_prop_seed["revision"],
        "accept",
        title=pending_title_seed,
        due=None,
        tracker_target="vikunja",
        source_version=pending_prop_seed["version"],
        due_confirmed=True,
    )
    pending_tracker = pending_accepted.get("tracker") or {}
    pending_event = pending_tracker.get("event_id")
    _assert(pending_event, "pending rollback event missing")
    _assert(pending_tracker.get("state") == "pending", f"expected pending, got {pending_tracker}")
    pending_decision_count = len(
        next(p for p in core.snapshot()["proposals"] if p["id"] == pending_prop_seed["id"]).get("decisions") or []
    )
    _assert(pending_decision_count == 1, "pending decision count")
    outbox_count_before_rollback = len(HermesOutboxStore(outbox_path).list_outbox())
    synthetic_ids["pending_event_id"] = pending_event
    synthetic_ids["pending_proposal_id"] = pending_prop_seed["id"]
    scenarios["pending_before_rollback"] = {
        "event_id": pending_event,
        "state": pending_tracker.get("state"),
        "decision_count": pending_decision_count,
        "outbox_count": outbox_count_before_rollback,
    }
    executed += 1

    # R2/C2: by_event.title drift on an accepted pending row must not create a task.
    # Cross-index + acceptance digest fail closed before journal intent / Vikunja create.
    outbox_backup_drift = outbox_path.read_bytes()
    ledger_drift = json.loads(outbox_backup_drift.decode("utf-8"))
    pending_event_row = ledger_drift.get("by_event", {}).get(pending_event)
    pending_prop_row = ledger_drift.get("by_proposal", {}).get(pending_prop_seed["id"])
    _assert(isinstance(pending_event_row, dict), "pending by_event row missing for drift")
    _assert(isinstance(pending_prop_row, dict), "pending by_proposal row missing for drift")
    original_pending_title = pending_event_row.get("title")
    _assert(
        isinstance(original_pending_title, str) and original_pending_title == pending_title_seed,
        f"pending title mismatch before drift: {original_pending_title!r}",
    )
    altered_pending_title = f"{pending_title_seed}-unconfirmed-altered-title"
    pending_event_row["title"] = altered_pending_title
    # Leave by_proposal title and both payload_digest values untouched (review repro).
    outbox_path.write_text(json.dumps(ledger_drift), encoding="utf-8")
    os.chmod(outbox_path, 0o600)
    creates_before_payload_drift = _count_marker(client_api, synthetic_ids["marker"])
    # Also count any crash/pending markers that may not share the happy-path marker.
    total_tasks_before_drift = len(
        (client_api.list_project_tasks().details or {}).get("items") or []
    )
    drift_delivery = bridge.deliver(pending_event)
    _assert(
        not drift_delivery.ok,
        f"payload drift must refuse deliver, got {drift_delivery.outcome} {drift_delivery.details}",
    )
    _assert(
        drift_delivery.outcome is Outcome.VALIDATION_ERROR,
        f"payload drift outcome: {drift_delivery.outcome}",
    )
    _assert(
        (drift_delivery.details or {}).get("reason") == "outbox_index_inconsistent",
        f"payload drift reason: {drift_delivery.details}",
    )
    _assert(
        _count_marker(client_api, synthetic_ids["marker"]) == creates_before_payload_drift,
        "payload drift created a happy-path marker task",
    )
    total_tasks_after_drift = len(
        (client_api.list_project_tasks().details or {}).get("items") or []
    )
    _assert(
        total_tasks_after_drift == total_tasks_before_drift,
        f"payload drift changed task count {total_tasks_before_drift}->{total_tasks_after_drift}",
    )
    outbox_path.write_bytes(outbox_backup_drift)
    os.chmod(outbox_path, 0o600)
    restored_pending = HermesOutboxStore(outbox_path).get_by_event(pending_event)
    _assert(restored_pending is not None, "pending row missing after drift restore")
    assert restored_pending is not None
    _assert(restored_pending.state == "pending", f"pending state after restore: {restored_pending.state}")
    _assert(
        restored_pending.title == pending_title_seed,
        f"pending title after restore: {restored_pending.title!r}",
    )
    scenarios["accepted_payload_drift"] = {
        "event_id": pending_event,
        "original_title": original_pending_title,
        "altered_title": altered_pending_title,
        "outcome": drift_delivery.outcome.value,
        "reason": (drift_delivery.details or {}).get("reason"),
        "creates_unchanged": creates_before_payload_drift,
        "total_tasks_unchanged": total_tasks_before_drift,
        "restored_pending": True,
    }
    executed += 1

    # R1/C2/NFR-11: both indexes agree on drifted frozen_due.wire while due/digest stay.
    # Must refuse deliver and recover; never create with a different absolute due.
    dated_due = "2026-11-12"
    drifted_frozen_wire = "2026-12-31T23:59:59+03:00"
    dated_accepted = core.decision(
        dated_drift_prop_seed["id"],
        dated_drift_prop_seed["revision"],
        "accept",
        title=dated_drift_title_seed,
        due=dated_due,
        tracker_target="vikunja",
        source_version=dated_drift_prop_seed["version"],
        due_confirmed=True,
    )
    dated_tracker = dated_accepted.get("tracker") or {}
    dated_event = dated_tracker.get("event_id")
    _assert(dated_event, "dated frozen-drift event missing")
    _assert(dated_tracker.get("state") == "pending", f"dated drift state: {dated_tracker}")
    dated_row = HermesOutboxStore(outbox_path).get_by_event(dated_event)
    _assert(dated_row is not None, "dated drift outbox row missing")
    assert dated_row is not None
    _assert(dated_row.due == dated_due, f"dated drift due: {dated_row.due!r}")
    _assert(isinstance(dated_row.frozen_due, dict), "dated drift frozen_due missing")
    assert isinstance(dated_row.frozen_due, dict)
    original_dated_wire = dated_row.frozen_due.get("wire")
    _assert(
        original_dated_wire == "2026-11-12T23:59:59+03:00",
        f"expected MSK end-of-day wire, got {original_dated_wire!r}",
    )
    outbox_backup_frozen = outbox_path.read_bytes()
    ledger_frozen = json.loads(outbox_backup_frozen.decode("utf-8"))
    dated_event_row = ledger_frozen.get("by_event", {}).get(dated_event)
    dated_prop_row = ledger_frozen.get("by_proposal", {}).get(dated_drift_prop_seed["id"])
    _assert(isinstance(dated_event_row, dict), "dated by_event row missing")
    _assert(isinstance(dated_prop_row, dict), "dated by_proposal row missing")
    for drifted_row in (dated_event_row, dated_prop_row):
        frozen = drifted_row.get("frozen_due")
        _assert(isinstance(frozen, dict), "dated frozen_due object missing")
        assert isinstance(frozen, dict)
        _assert(frozen.get("expression") == dated_due, "dated expression mismatch before drift")
        _assert(frozen.get("wire") == original_dated_wire, "dated wire mismatch before drift")
        # Leave title/due/source_version/payload_digest and kind/expression untouched.
        frozen["wire"] = drifted_frozen_wire
    outbox_path.write_text(json.dumps(ledger_frozen), encoding="utf-8")
    os.chmod(outbox_path, 0o600)
    creates_before_frozen_drift = _count_marker(client_api, synthetic_ids["marker"])
    total_tasks_before_frozen = len(
        (client_api.list_project_tasks().details or {}).get("items") or []
    )
    frozen_deliver = bridge.deliver(dated_event)
    _assert(
        not frozen_deliver.ok,
        f"frozen wire drift must refuse deliver, got {frozen_deliver.outcome} {frozen_deliver.details}",
    )
    _assert(
        frozen_deliver.outcome is Outcome.VALIDATION_ERROR,
        f"frozen wire deliver outcome: {frozen_deliver.outcome}",
    )
    _assert(
        (frozen_deliver.details or {}).get("reason") == "outbox_accepted_payload_invalid",
        f"frozen wire deliver reason: {frozen_deliver.details}",
    )
    _assert(
        _count_marker(client_api, synthetic_ids["marker"]) == creates_before_frozen_drift,
        "frozen wire deliver created a happy-path marker task",
    )
    total_after_frozen_deliver = len(
        (client_api.list_project_tasks().details or {}).get("items") or []
    )
    _assert(
        total_after_frozen_deliver == total_tasks_before_frozen,
        f"frozen wire deliver changed task count {total_tasks_before_frozen}->{total_after_frozen_deliver}",
    )
    frozen_recover = bridge.recover(dated_event)
    _assert(
        not frozen_recover.ok,
        f"frozen wire drift must refuse recover, got {frozen_recover.outcome} {frozen_recover.details}",
    )
    _assert(
        frozen_recover.outcome is Outcome.VALIDATION_ERROR,
        f"frozen wire recover outcome: {frozen_recover.outcome}",
    )
    _assert(
        (frozen_recover.details or {}).get("reason") == "outbox_accepted_payload_invalid",
        f"frozen wire recover reason: {frozen_recover.details}",
    )
    total_after_frozen_recover = len(
        (client_api.list_project_tasks().details or {}).get("items") or []
    )
    _assert(
        total_after_frozen_recover == total_tasks_before_frozen,
        f"frozen wire recover changed task count {total_tasks_before_frozen}->{total_after_frozen_recover}",
    )
    outbox_path.write_bytes(outbox_backup_frozen)
    os.chmod(outbox_path, 0o600)
    restored_dated = HermesOutboxStore(outbox_path).get_by_event(dated_event)
    _assert(restored_dated is not None, "dated row missing after frozen-drift restore")
    assert restored_dated is not None
    _assert(restored_dated.state == "pending", f"dated state after restore: {restored_dated.state}")
    _assert(restored_dated.due == dated_due, f"dated due after restore: {restored_dated.due!r}")
    _assert(
        isinstance(restored_dated.frozen_due, dict)
        and restored_dated.frozen_due.get("wire") == original_dated_wire,
        f"dated wire after restore: {restored_dated.frozen_due!r}",
    )
    synthetic_ids["dated_frozen_drift_event_id"] = dated_event
    synthetic_ids["dated_frozen_drift_proposal_id"] = dated_drift_prop_seed["id"]
    scenarios["dated_frozen_wire_drift"] = {
        "event_id": dated_event,
        "accepted_due": dated_due,
        "original_wire": original_dated_wire,
        "drifted_wire": drifted_frozen_wire,
        "deliver_outcome": frozen_deliver.outcome.value,
        "deliver_reason": (frozen_deliver.details or {}).get("reason"),
        "recover_outcome": frozen_recover.outcome.value,
        "recover_reason": (frozen_recover.details or {}).get("reason"),
        "creates_unchanged": creates_before_frozen_drift,
        "total_tasks_unchanged": total_tasks_before_frozen,
        "restored_pending": True,
    }
    executed += 1

    # Real SIGKILL between durable state acceptance and outbox (commit protocol gap).
    gap_cfg = {
        "settings": settings,
        "plugin_path": str(plugin_path),
        "core_path": str(core_path),
        "bridge_config": bridge_cfg,
        "bridge_config_path": str(bridge_cfg_path),
        "settings_path": str(settings_path),
        "plugin_module": "ea010_s06_plugin_api_gap",
        "proposal_id": crash_gap_prop_seed["id"],
        "revision": crash_gap_prop_seed["revision"],
        "title": crash_gap_title_seed,
        "source_version": crash_gap_prop_seed["version"],
        "ready": str(crash_dir / "gap-ready.json"),
        "result": str(crash_dir / "gap-result.json"),
    }
    gap_cfg_path = crash_dir / "gap.json"
    gap_py = crash_dir / "crash_gap_child.py"
    _write_crash_child(
        gap_py,
        crash_child_common
        + '''
def crash(point):
    if point == "after_state_before_outbox":
        Path(cfg["ready"]).write_text(json.dumps({"point": point}), encoding="utf-8")
        os.kill(os.getpid(), signal.SIGKILL)

core._ea010_crash_hook = crash
try:
    core.decision(
        cfg["proposal_id"], cfg["revision"], "accept",
        title=cfg["title"], due=None, tracker_target="vikunja",
        source_version=cfg["source_version"], due_confirmed=True,
    )
except Exception as exc:
    Path(cfg["result"]).write_text(json.dumps({"phase":"error","error":str(exc)}), encoding="utf-8")
    raise
print("unreachable")
''',
    )
    gap_cfg_path.write_text(json.dumps(gap_cfg), encoding="utf-8")
    gap_rc = _run_sigkill_child(gap_py, gap_cfg_path, Path(gap_cfg["ready"]))
    # Interrupted commit must be recoverable: state accepted + outbox pending, one decision.
    gap_core = plugin.core(settings)
    gap_prop = next(p for p in gap_core.snapshot()["proposals"] if p["id"] == crash_gap_prop_seed["id"])
    # Completing commit / recover path via explicit recover_vikunja_outbox / decision replay.
    replay = gap_core.decision(
        crash_gap_prop_seed["id"],
        crash_gap_prop_seed["revision"],
        "accept",
        title=crash_gap_title_seed,
        due=None,
        tracker_target="vikunja",
        source_version=crash_gap_prop_seed["version"],
        due_confirmed=True,
    )
    gap_tracker = replay.get("tracker") or {}
    gap_event = gap_tracker.get("event_id")
    _assert(gap_event, "gap event missing after replay")
    gap_prop_after = next(p for p in gap_core.snapshot()["proposals"] if p["id"] == crash_gap_prop_seed["id"])
    _assert(gap_prop_after["status"] == "accepted", "gap status not accepted after repair")
    _assert(len(gap_prop_after.get("decisions") or []) == 1, "gap decision count drift")
    gap_row = HermesOutboxStore(outbox_path).get_by_event(gap_event)
    _assert(gap_row is not None, "gap outbox missing after repair")
    recovered_gap = bridge.recover(gap_event)
    if not recovered_gap.ok:
        recovered_gap = bridge.deliver(gap_event)
    _assert(recovered_gap.ok, f"gap recover failed: {recovered_gap.outcome} {recovered_gap.details}")
    assert recovered_gap.record is not None
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        mapped = journal.lookup_source(SOURCE_KIND, gap_event)
        _assert(mapped is not None and mapped.receipt is not None, "gap mapping/receipt missing")
        _assert(_count_marker(client_api, mapped.marker) == 1, "gap duplicate create")
        synthetic_ids["gap_event_id"] = gap_event
        synthetic_ids["gap_task_id"] = recovered_gap.record.task_id
        synthetic_ids["gap_operation_id"] = mapped.operation_id
        synthetic_ids["gap_marker"] = mapped.marker
    finally:
        journal.close()
    scenarios["crash_after_state_before_outbox"] = {
        "event_id": gap_event,
        "task_id": recovered_gap.record.task_id,
        "operation_id": synthetic_ids["gap_operation_id"],
        "decision_count": len(gap_prop_after.get("decisions") or []),
        "rc": gap_rc,
        "pre_repair_status": gap_prop.get("status"),
    }
    executed += 1

    # Real SIGKILL after acceptance before dispatch.
    crash_cfg = {
        "settings": settings,
        "plugin_path": str(plugin_path),
        "core_path": str(core_path),
        "bridge_config": bridge_cfg,
        "bridge_config_path": str(bridge_cfg_path),
        "settings_path": str(settings_path),
        "plugin_module": "ea010_s06_plugin_api_child",
        "proposal_id": crash_prop_seed["id"],
        "revision": crash_prop_seed["revision"],
        "title": crash_title_seed,
        "source_version": crash_prop_seed["version"],
        "ready": str(crash_dir / "ready.json"),
        "result": str(crash_dir / "result.json"),
        "crash_point": "before_dispatch",
    }
    crash_cfg_path = crash_dir / "child.json"
    crash_py = crash_dir / "crash_child.py"
    _write_crash_child(
        crash_py,
        crash_child_common
        + '''
def crash(point):
    if point == cfg["crash_point"]:
        Path(cfg["ready"]).write_text(json.dumps({"point": point}), encoding="utf-8")
        os.kill(os.getpid(), signal.SIGKILL)

accepted = core.decision(
    cfg["proposal_id"], cfg["revision"], "accept",
    title=cfg["title"], due=None, tracker_target="vikunja",
    source_version=cfg["source_version"], due_confirmed=True,
)
tracker = accepted.get("tracker") or {}
event_id = tracker.get("event_id")
Path(cfg["result"]).write_text(
    json.dumps({"phase": "accepted", "event_id": event_id, "tracker": tracker}),
    encoding="utf-8",
)
bridge = HermesBridge(
    BridgeConfig.from_mapping(cfg["bridge_config"]),
    HermesOutboxStore(core._vikunja_outbox_path()),
)
bridge._crash = crash
bridge.deliver(event_id)
print("unreachable")
''',
    )
    crash_cfg_path.write_text(json.dumps(crash_cfg), encoding="utf-8")
    rc = _run_sigkill_child(crash_py, crash_cfg_path, Path(crash_cfg["ready"]))
    accepted_payload = json.loads(Path(crash_cfg["result"]).read_text(encoding="utf-8"))
    crash_event = accepted_payload["event_id"]
    _assert(crash_event, "crash event_id missing")
    recovered_crash = bridge.recover(crash_event)
    if not recovered_crash.ok:
        recovered_crash = bridge.deliver(crash_event)
    _assert(recovered_crash.ok, f"crash recover failed: {recovered_crash.outcome} {recovered_crash.details}")
    assert recovered_crash.record is not None
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        mapped = journal.lookup_source(SOURCE_KIND, crash_event)
        _assert(mapped is not None, "crash mapping missing")
        assert mapped is not None
        marker = mapped.marker
        _assert(_count_marker(client_api, marker) == 1, "crash recover duplicate create")
        synthetic_ids["crash_event_id"] = crash_event
        synthetic_ids["crash_task_id"] = recovered_crash.record.task_id
        synthetic_ids["crash_operation_id"] = mapped.operation_id
        synthetic_ids["crash_marker"] = marker
    finally:
        journal.close()
    scenarios["crash_after_acceptance_before_dispatch"] = {
        "event_id": crash_event,
        "task_id": recovered_crash.record.task_id,
        "operation_id": synthetic_ids["crash_operation_id"],
        "rc": rc,
        "ready": json.loads(Path(crash_cfg["ready"]).read_text(encoding="utf-8")),
    }
    executed += 1

    # Real SIGKILL after possible HTTP create / before durable receipt.
    receipt_cfg = {
        **crash_cfg,
        "plugin_module": "ea010_s06_plugin_api_receipt",
        "proposal_id": crash_receipt_prop_seed["id"],
        "revision": crash_receipt_prop_seed["revision"],
        "title": crash_receipt_title_seed,
        "source_version": crash_receipt_prop_seed["version"],
        "ready": str(crash_dir / "receipt-ready.json"),
        "result": str(crash_dir / "receipt-result.json"),
        "crash_point": "after_commit_before_receipt",
    }
    receipt_cfg_path = crash_dir / "receipt.json"
    receipt_py = crash_dir / "crash_receipt_child.py"
    _write_crash_child(receipt_py, crash_py.read_text(encoding="utf-8"))
    receipt_cfg_path.write_text(json.dumps(receipt_cfg), encoding="utf-8")
    receipt_rc = _run_sigkill_child(receipt_py, receipt_cfg_path, Path(receipt_cfg["ready"]))
    receipt_payload = json.loads(Path(receipt_cfg["result"]).read_text(encoding="utf-8"))
    receipt_event = receipt_payload["event_id"]
    recovered_receipt = bridge.recover(receipt_event)
    if not recovered_receipt.ok:
        recovered_receipt = bridge.deliver(receipt_event)
    _assert(
        recovered_receipt.ok,
        f"receipt-gap recover failed: {recovered_receipt.outcome} {recovered_receipt.details}",
    )
    assert recovered_receipt.record is not None
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        mapped = journal.lookup_source(SOURCE_KIND, receipt_event)
        _assert(mapped is not None and mapped.receipt is not None, "receipt mapping missing")
        assert mapped is not None
        _assert(_count_marker(client_api, mapped.marker) == 1, "receipt-gap duplicate create")
        synthetic_ids["receipt_event_id"] = receipt_event
        synthetic_ids["receipt_task_id"] = recovered_receipt.record.task_id
        synthetic_ids["receipt_operation_id"] = mapped.operation_id
        synthetic_ids["receipt_marker"] = mapped.marker
    finally:
        journal.close()
    scenarios["crash_after_send_before_receipt"] = {
        "event_id": receipt_event,
        "task_id": recovered_receipt.record.task_id,
        "operation_id": synthetic_ids["receipt_operation_id"],
        "rc": receipt_rc,
    }
    executed += 1

    # Real SIGKILL after receipt before backlink.
    backlink_cfg = {
        **crash_cfg,
        "plugin_module": "ea010_s06_plugin_api_backlink",
        "proposal_id": crash_backlink_prop_seed["id"],
        "revision": crash_backlink_prop_seed["revision"],
        "title": crash_backlink_title_seed,
        "source_version": crash_backlink_prop_seed["version"],
        "ready": str(crash_dir / "backlink-ready.json"),
        "result": str(crash_dir / "backlink-result.json"),
        "crash_point": "after_receipt_before_backlink",
    }
    backlink_cfg_path = crash_dir / "backlink.json"
    backlink_py = crash_dir / "crash_backlink_child.py"
    _write_crash_child(backlink_py, crash_py.read_text(encoding="utf-8"))
    backlink_cfg_path.write_text(json.dumps(backlink_cfg), encoding="utf-8")
    backlink_rc = _run_sigkill_child(backlink_py, backlink_cfg_path, Path(backlink_cfg["ready"]))
    backlink_payload = json.loads(Path(backlink_cfg["result"]).read_text(encoding="utf-8"))
    backlink_event = backlink_payload["event_id"]
    recovered_backlink = bridge.recover(backlink_event)
    if not recovered_backlink.ok:
        recovered_backlink = bridge.deliver(backlink_event)
    _assert(
        recovered_backlink.ok,
        f"backlink-gap recover failed: {recovered_backlink.outcome} {recovered_backlink.details}",
    )
    assert recovered_backlink.record is not None
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        mapped = journal.lookup_source(SOURCE_KIND, backlink_event)
        _assert(mapped is not None and mapped.receipt is not None, "backlink mapping missing")
        assert mapped is not None
        _assert(_count_marker(client_api, mapped.marker) == 1, "backlink-gap duplicate create")
        synthetic_ids["backlink_event_id"] = backlink_event
        synthetic_ids["backlink_task_id"] = recovered_backlink.record.task_id
        synthetic_ids["backlink_operation_id"] = mapped.operation_id
        synthetic_ids["backlink_marker"] = mapped.marker
    finally:
        journal.close()
    scenarios["crash_after_receipt_before_backlink"] = {
        "event_id": backlink_event,
        "task_id": recovered_backlink.record.task_id,
        "operation_id": synthetic_ids["backlink_operation_id"],
        "rc": backlink_rc,
        "state": recovered_backlink.record.state,
    }
    executed += 1

    # Rollback/reapply while an actual pending row remains; then recover without second PUT.
    pending_row_before = HermesOutboxStore(outbox_path).get_by_event(pending_event)
    _assert(pending_row_before is not None and pending_row_before.state == "pending", "pending row lost before rollback")
    pending_before = outbox_path.read_bytes()
    rollback_tree = stand.run_dir / "hermes-rollback"
    if rollback_tree.exists():
        shutil.rmtree(rollback_tree)
    shutil.copytree(patch_info["patched_tree"], rollback_tree)
    rev = subprocess.run(
        ["patch", "-p1", "-R", "-i", str(worktree / "integration/hermes/backend.patch")],
        cwd=rollback_tree,
        capture_output=True,
        text=True,
    )
    _assert(rev.returncode == 0, f"rollback apply failed: {rev.stderr}")
    _assert(sha256_file(rollback_tree / "plugins/personal-tasks/dashboard/plugin_api.py") == FROM_API, "rollback api")
    _assert(sha256_file(rollback_tree / "scripts/ea_core.py") == FROM_CORE, "rollback core")
    _assert(outbox_path.read_bytes() == pending_before, "rollback lost outbox pending")
    reapply = subprocess.run(
        ["patch", "-p1", "-i", str(worktree / "integration/hermes/backend.patch")],
        cwd=rollback_tree,
        capture_output=True,
        text=True,
    )
    _assert(reapply.returncode == 0, f"reapply failed: {reapply.stderr}")
    _assert(outbox_path.read_bytes() == pending_before, "reapply mutated pending outbox")
    pending_row_after = HermesOutboxStore(outbox_path).get_by_event(pending_event)
    _assert(pending_row_after is not None and pending_row_after.state == "pending", "pending missing after reapply")
    pending_prop_after = next(p for p in core.snapshot()["proposals"] if p["id"] == pending_prop_seed["id"])
    _assert(len(pending_prop_after.get("decisions") or []) == 1, "pending decision count after rollback")
    recovered_pending = bridge.recover(pending_event)
    if not recovered_pending.ok:
        recovered_pending = bridge.deliver(pending_event)
    _assert(recovered_pending.ok, f"pending recover failed: {recovered_pending.outcome} {recovered_pending.details}")
    assert recovered_pending.record is not None
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        mapped = journal.lookup_source(SOURCE_KIND, pending_event)
        _assert(mapped is not None and mapped.receipt is not None, "pending mapping missing")
        assert mapped is not None
        _assert(_count_marker(client_api, mapped.marker) == 1, "pending recover duplicate create")
        synthetic_ids["pending_task_id"] = recovered_pending.record.task_id
        synthetic_ids["pending_operation_id"] = mapped.operation_id
        synthetic_ids["pending_marker"] = mapped.marker
    finally:
        journal.close()
    # Identical recover must not issue a second PUT/create.
    creates_before = _count_marker(client_api, synthetic_ids["pending_marker"])
    again = bridge.recover(pending_event)
    _assert(again.ok, "second pending recover failed")
    _assert(_count_marker(client_api, synthetic_ids["pending_marker"]) == creates_before, "second PUT observed")
    scenarios["rollback_preserves_pending"] = {
        "outbox_sha256_before_recover": hashlib.sha256(pending_before).hexdigest(),
        "reverse_ok": True,
        "reapply_ok": True,
        "event_id": pending_event,
        "task_id": recovered_pending.record.task_id,
        "operation_id": synthetic_ids["pending_operation_id"],
        "decision_count": len(pending_prop_after.get("decisions") or []),
        "outbox_count_before": outbox_count_before_rollback,
    }
    executed += 1

    # R1: already-linked happy-path task deleted — recover/deliver/sweep must not claim CONFIRMED.
    deleted_event = str(synthetic_ids["event_id"])
    deleted_task_id = int(synthetic_ids["task_id"])
    deleted_marker = str(synthetic_ids["marker"])
    deleted_url = str(scenarios["happy_accept"]["url"])
    creates_before_delete = _count_marker(client_api, deleted_marker)
    _assert(creates_before_delete == 1, "happy-path marker count before DELETE")
    synthetic_ids["linked_deleted_event_id"] = deleted_event
    synthetic_ids["linked_deleted_task_id"] = deleted_task_id
    synthetic_ids["linked_deleted_operation_id"] = synthetic_ids["operation_id"]
    synthetic_ids["linked_deleted_marker"] = deleted_marker
    del_http, _ = stand.api(
        f"tasks/{deleted_task_id}",
        "DELETE",
        token=stand._session_token,
    )
    _assert(del_http == 200, f"DELETE linked task HTTP {del_http}")
    gone = client_api.read(deleted_task_id)
    _assert(gone.http_status == 404, f"GET after DELETE expected 404, got {gone.http_status}")
    recovered_del = bridge.recover(deleted_event)
    _assert(not recovered_del.ok, f"recover after DELETE must not ok: {recovered_del.details}")
    _assert(
        recovered_del.outcome is not Outcome.CONFIRMED,
        f"recover after DELETE must not be confirmed: {recovered_del.outcome}",
    )
    _assert(
        (recovered_del.details or {}).get("reason") == "target_deleted_or_gone",
        f"recover reason: {recovered_del.details}",
    )
    assert recovered_del.record is not None
    _assert(recovered_del.record.state == "linked", "historical linked state lost")
    _assert(recovered_del.record.task_id == deleted_task_id, "historical task_id lost")
    _assert(recovered_del.record.url == deleted_url, "historical url lost")
    delivered_del = bridge.deliver(deleted_event)
    _assert(not delivered_del.ok, f"deliver after DELETE must not ok: {delivered_del.details}")
    _assert(
        delivered_del.outcome is not Outcome.CONFIRMED,
        f"deliver after DELETE must not be confirmed: {delivered_del.outcome}",
    )
    _assert(
        (delivered_del.details or {}).get("reason") == "target_deleted_or_gone",
        f"deliver reason: {delivered_del.details}",
    )
    sweep_del = bridge.recover_all_outbox()
    sweep_hit = [r for r in sweep_del if r.record and r.record.event_id == deleted_event]
    _assert(len(sweep_hit) == 1, "sweep missed deleted-linked row")
    _assert(not sweep_hit[0].ok, f"sweep after DELETE must not ok: {sweep_hit[0].details}")
    _assert(
        sweep_hit[0].outcome is not Outcome.CONFIRMED,
        f"sweep after DELETE must not be confirmed: {sweep_hit[0].outcome}",
    )
    # Core.deliver_vikunja_outbox must run live check and expose public availability.
    core_prop = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal["id"])
    core_prop = core._project_tracker(core_prop)
    # Snapshot projection after bridge.deliver must already surface unavailable.
    snap_tracker = (core_prop.get("tracker") or {})
    snap_avail = snap_tracker.get("availability") or {}
    _assert(snap_tracker.get("state") == "linked", f"snapshot lost historical linked: {snap_tracker}")
    _assert(snap_tracker.get("task_id") == deleted_task_id, f"snapshot lost task_id: {snap_tracker}")
    _assert(snap_avail.get("status") == "unavailable", f"snapshot missing unavailable: {snap_tracker}")
    _assert(
        snap_avail.get("reason") == "target_deleted_or_gone",
        f"snapshot reason: {snap_tracker}",
    )
    core_after = core.deliver_vikunja_outbox(core_prop)
    core_tracker = (core_after or {}).get("tracker") or {}
    core_avail = core_tracker.get("availability") or {}
    _assert(core_tracker.get("task_id") == deleted_task_id, "core lost historical task_id")
    _assert(core_tracker.get("state") == "linked", "core lost historical linked state")
    _assert(core_avail.get("status") == "unavailable", f"core public availability: {core_tracker}")
    _assert(core_avail.get("last_check_outcome") != "confirmed", f"core cached confirmed: {core_tracker}")
    _assert(
        core_avail.get("reason") == "target_deleted_or_gone",
        f"core public reason: {core_tracker}",
    )
    # Endpoint assertion: identical authorized POST must expose unavailable publicly.
    from fastapi.testclient import TestClient

    with TestClient(app, base_url=ORIGIN) as del_client:
        del_replay = del_client.post(
            endpoint,
            json=payload,
            auth=("s06-owner", owner_password),
            headers={"Origin": ORIGIN},
        )
    _assert(del_replay.status_code == 200, f"deleted replay HTTP {del_replay.status_code}: {del_replay.text}")
    public_tracker = ((del_replay.json().get("proposal") or {}).get("tracker") or {})
    public_avail = public_tracker.get("availability") or {}
    _assert(public_tracker.get("state") == "linked", f"endpoint lost historical linked: {public_tracker}")
    _assert(public_tracker.get("task_id") == deleted_task_id, f"endpoint lost task_id: {public_tracker}")
    _assert(public_tracker.get("url") == deleted_url, f"endpoint lost historical url: {public_tracker}")
    _assert(public_avail.get("status") == "unavailable", f"endpoint missing unavailable: {public_tracker}")
    _assert(
        public_avail.get("reason") == "target_deleted_or_gone",
        f"endpoint reason: {public_tracker}",
    )
    _assert(
        public_avail.get("last_check_outcome") != "confirmed",
        f"endpoint cached confirmed: {public_tracker}",
    )
    _assert(
        _count_marker(client_api, deleted_marker) == 0,
        "deleted-linked recreate observed",
    )
    scenarios["linked_then_deleted"] = {
        "event_id": deleted_event,
        "task_id": deleted_task_id,
        "delete_http": del_http,
        "get_http": gone.http_status,
        "recover_outcome": recovered_del.outcome.value,
        "deliver_outcome": delivered_del.outcome.value,
        "sweep_outcome": sweep_hit[0].outcome.value,
        "historical_state": recovered_del.record.state,
        "core_availability": core_avail.get("status"),
        "endpoint_http": del_replay.status_code,
        "endpoint_availability": public_avail.get("status"),
        "endpoint_reason": public_avail.get("reason"),
        "creates_after": _count_marker(client_api, deleted_marker),
    }
    executed += 1

    # R1: incompatible saved backlink vs receipt — explicit conflict, preserve link, no create.
    incompat_event = str(synthetic_ids["receipt_event_id"])
    incompat_receipt_id = int(synthetic_ids["receipt_task_id"])
    incompat_marker = str(synthetic_ids["receipt_marker"])
    incompat_proposal_id = crash_receipt_prop_seed["id"]
    foreign_id = 999999
    foreign_url = f"{origin_vikunja}/tasks/{foreign_id}"
    store = HermesOutboxStore(outbox_path)
    incompat_row = store.get_by_event(incompat_event)
    _assert(incompat_row is not None and incompat_row.state == "linked", "incompat baseline linked missing")
    assert incompat_row is not None
    _assert(incompat_row.task_id == incompat_receipt_id, "incompat baseline receipt id mismatch")
    incompat_row.task_id = foreign_id
    incompat_row.url = foreign_url
    store.upsert(incompat_row)
    creates_before_incompat = _count_marker(client_api, incompat_marker)
    recovered_incompat = bridge.recover(incompat_event)
    _assert(
        recovered_incompat.outcome is Outcome.CONFLICT,
        f"recover incompatible must conflict: {recovered_incompat.outcome} {recovered_incompat.details}",
    )
    _assert(not recovered_incompat.ok, "recover incompatible must not ok")
    assert recovered_incompat.record is not None
    _assert(recovered_incompat.record.state == "conflict", "recover must durable-conflict")
    _assert(recovered_incompat.record.task_id == foreign_id, "recover lost existing task_id")
    _assert(recovered_incompat.record.url == foreign_url, "recover lost existing url")
    _assert(
        (recovered_incompat.details or {}).get("reason") == "incompatible_existing_link",
        f"recover reason: {recovered_incompat.details}",
    )
    # Reset to linked foreign link so deliver also re-enters verify path.
    reset_row = store.get_by_event(incompat_event)
    _assert(reset_row is not None, "incompat row missing before deliver reset")
    assert reset_row is not None
    reset_row.state = "linked"
    reset_row.task_id = foreign_id
    reset_row.url = foreign_url
    reset_row.details = {
        key: value
        for key, value in (reset_row.details or {}).items()
        if key
        not in {
            "reason",
            "existing_task_id",
            "receipt_task_id",
            "last_check_outcome",
            "last_explicit_check",
            "last_check_path",
            "forbid_create",
            "api_outcome",
            "link_result",
        }
    }
    store.upsert(reset_row)
    delivered_incompat = bridge.deliver(incompat_event)
    _assert(
        delivered_incompat.outcome is Outcome.CONFLICT,
        f"deliver incompatible must conflict: {delivered_incompat.outcome} {delivered_incompat.details}",
    )
    _assert(not delivered_incompat.ok, "deliver incompatible must not ok")
    assert delivered_incompat.record is not None
    _assert(delivered_incompat.record.state == "conflict", "deliver must durable-conflict")
    _assert(delivered_incompat.record.task_id == foreign_id, "deliver lost existing task_id")
    _assert(delivered_incompat.record.url == foreign_url, "deliver lost existing url")
    sweep_incompat = bridge.recover_all_outbox()
    sweep_incompat_hit = [
        item for item in sweep_incompat if item.record and item.record.event_id == incompat_event
    ]
    _assert(len(sweep_incompat_hit) == 1, "sweep missed incompatible row")
    _assert(sweep_incompat_hit[0].outcome is Outcome.CONFLICT, "sweep must conflict")
    _assert(not sweep_incompat_hit[0].ok, "sweep incompatible must not ok")
    _assert(_count_marker(client_api, incompat_marker) == creates_before_incompat, "incompatible create")

    incompat_prop = next(p for p in core.snapshot()["proposals"] if p["id"] == incompat_proposal_id)
    incompat_prop = core._project_tracker(incompat_prop)
    snap_incompat = (incompat_prop.get("tracker") or {})
    snap_incompat_avail = snap_incompat.get("availability") or {}
    _assert(snap_incompat.get("state") == "conflict", f"snapshot state: {snap_incompat}")
    _assert(snap_incompat.get("task_id") == foreign_id, f"snapshot task_id: {snap_incompat}")
    _assert(snap_incompat.get("url") == foreign_url, f"snapshot url: {snap_incompat}")
    _assert(snap_incompat_avail.get("status") == "conflict", f"snapshot availability: {snap_incompat}")
    _assert(
        snap_incompat_avail.get("reason") == "incompatible_existing_link",
        f"snapshot reason: {snap_incompat}",
    )
    core_incompat = core.deliver_vikunja_outbox(incompat_prop)
    core_incompat_tracker = (core_incompat or {}).get("tracker") or {}
    core_incompat_avail = core_incompat_tracker.get("availability") or {}
    _assert(core_incompat_tracker.get("state") == "conflict", f"core state: {core_incompat_tracker}")
    _assert(core_incompat_tracker.get("task_id") == foreign_id, f"core task_id: {core_incompat_tracker}")
    _assert(core_incompat_avail.get("status") == "conflict", f"core availability: {core_incompat_tracker}")
    _assert(
        core_incompat_avail.get("last_check_outcome") == "conflict",
        f"core last_check: {core_incompat_tracker}",
    )
    receipt_prop_full = next(
        p for p in core.snapshot()["proposals"] if p["id"] == incompat_proposal_id
    )
    incompat_endpoint = (
        f"/api/plugins/personal-tasks/proposals/{incompat_proposal_id}/decision?profile=default"
    )
    incompat_payload = {
        "revision": int(receipt_prop_full.get("revision") or crash_receipt_prop_seed["revision"]),
        "action": "accept",
        "title": crash_receipt_title_seed,
        "due": None,
        "due_confirmed": True,
        "tracker_target": "vikunja",
        "source_version": crash_receipt_prop_seed["version"],
    }
    with TestClient(app, base_url=ORIGIN) as incompat_client:
        incompat_replay = incompat_client.post(
            incompat_endpoint,
            json=incompat_payload,
            auth=("s06-owner", owner_password),
            headers={"Origin": ORIGIN},
        )
    _assert(
        incompat_replay.status_code == 200,
        f"incompatible replay HTTP {incompat_replay.status_code}: {incompat_replay.text}",
    )
    public_incompat = ((incompat_replay.json().get("proposal") or {}).get("tracker") or {})
    public_incompat_avail = public_incompat.get("availability") or {}
    _assert(public_incompat.get("state") == "conflict", f"endpoint state: {public_incompat}")
    _assert(public_incompat.get("task_id") == foreign_id, f"endpoint task_id: {public_incompat}")
    _assert(public_incompat.get("url") == foreign_url, f"endpoint url: {public_incompat}")
    _assert(public_incompat_avail.get("status") == "conflict", f"endpoint availability: {public_incompat}")
    _assert(
        public_incompat_avail.get("reason") == "incompatible_existing_link",
        f"endpoint reason: {public_incompat}",
    )
    _assert(
        public_incompat_avail.get("last_check_outcome") == "conflict",
        f"endpoint last_check: {public_incompat}",
    )
    _assert(
        _count_marker(client_api, incompat_marker) == creates_before_incompat,
        "incompatible endpoint recreate observed",
    )
    scenarios["incompatible_existing_backlink"] = {
        "event_id": incompat_event,
        "receipt_task_id": incompat_receipt_id,
        "existing_task_id": foreign_id,
        "existing_url": foreign_url,
        "recover_outcome": recovered_incompat.outcome.value,
        "deliver_outcome": delivered_incompat.outcome.value,
        "sweep_outcome": sweep_incompat_hit[0].outcome.value,
        "durable_state": delivered_incompat.record.state,
        "core_availability": core_incompat_avail.get("status"),
        "endpoint_http": incompat_replay.status_code,
        "endpoint_availability": public_incompat_avail.get("status"),
        "endpoint_reason": public_incompat_avail.get("reason"),
        "creates_after": _count_marker(client_api, incompat_marker),
    }
    executed += 1

    # R1b: partial incompatible backlink (task_id set, url missing) must still conflict.
    # Linked-only incompatibility tests keep a URL and enter _verify_linked_outbox;
    # missing URL falls through the receipt-success branch that previously returned UNKNOWN.
    partial_foreign_id = 999
    partial_row = store.get_by_event(incompat_event)
    _assert(partial_row is not None, "partial incompat baseline missing")
    assert partial_row is not None
    partial_row.state = "unknown"
    partial_row.task_id = partial_foreign_id
    partial_row.url = None
    partial_row.details = {
        key: value
        for key, value in (partial_row.details or {}).items()
        if key
        not in {
            "reason",
            "existing_task_id",
            "receipt_task_id",
            "new_task_id",
            "last_check_outcome",
            "last_explicit_check",
            "last_check_path",
            "forbid_create",
            "api_outcome",
            "link_result",
        }
    }
    store.upsert(partial_row)
    creates_before_partial = _count_marker(client_api, incompat_marker)
    recovered_partial = bridge.recover(incompat_event)
    _assert(
        recovered_partial.outcome is Outcome.CONFLICT,
        f"recover partial incompat must conflict: {recovered_partial.outcome} {recovered_partial.details}",
    )
    _assert(not recovered_partial.ok, "recover partial incompat must not ok")
    assert recovered_partial.record is not None
    _assert(recovered_partial.record.state == "conflict", "recover partial must durable-conflict")
    _assert(recovered_partial.record.task_id == partial_foreign_id, "recover partial lost task_id")
    _assert(recovered_partial.record.url is None, "recover partial must preserve null url")
    _assert(
        (recovered_partial.details or {}).get("reason") == "incompatible_existing_link",
        f"recover partial reason: {recovered_partial.details}",
    )
    partial_public = recovered_partial.public_tracker() or {}
    partial_avail = partial_public.get("availability") or {}
    _assert(partial_public.get("state") == "conflict", f"partial public state: {partial_public}")
    _assert(partial_avail.get("status") == "conflict", f"partial availability: {partial_avail}")
    _assert(
        partial_avail.get("reason") == "incompatible_existing_link",
        f"partial availability reason: {partial_avail}",
    )
    reset_partial = store.get_by_event(incompat_event)
    _assert(reset_partial is not None, "partial row missing before deliver reset")
    assert reset_partial is not None
    reset_partial.state = "pending"
    reset_partial.task_id = partial_foreign_id
    reset_partial.url = None
    reset_partial.details = {
        key: value
        for key, value in (reset_partial.details or {}).items()
        if key
        not in {
            "reason",
            "existing_task_id",
            "receipt_task_id",
            "new_task_id",
            "last_check_outcome",
            "last_explicit_check",
            "last_check_path",
            "forbid_create",
            "api_outcome",
            "link_result",
        }
    }
    store.upsert(reset_partial)
    delivered_partial = bridge.deliver(incompat_event)
    _assert(
        delivered_partial.outcome is Outcome.CONFLICT,
        f"deliver partial incompat must conflict: {delivered_partial.outcome} {delivered_partial.details}",
    )
    _assert(not delivered_partial.ok, "deliver partial incompat must not ok")
    assert delivered_partial.record is not None
    _assert(delivered_partial.record.state == "conflict", "deliver partial must durable-conflict")
    _assert(delivered_partial.record.task_id == partial_foreign_id, "deliver partial lost task_id")
    _assert(delivered_partial.record.url is None, "deliver partial must preserve null url")
    _assert(
        (delivered_partial.details or {}).get("reason") == "incompatible_existing_link",
        f"deliver partial reason: {delivered_partial.details}",
    )
    _assert(
        _count_marker(client_api, incompat_marker) == creates_before_partial,
        "partial incompatible create observed",
    )
    scenarios["partial_incompatible_backlink_missing_url"] = {
        "event_id": incompat_event,
        "receipt_task_id": incompat_receipt_id,
        "existing_task_id": partial_foreign_id,
        "recover_outcome": recovered_partial.outcome.value,
        "deliver_outcome": delivered_partial.outcome.value,
        "durable_state": delivered_partial.record.state,
        "availability_status": partial_avail.get("status"),
        "availability_reason": partial_avail.get("reason"),
        "creates_after": _count_marker(client_api, incompat_marker),
    }
    executed += 1

    # R1c: linked URL-only mismatch (task_id matches receipt, URL points elsewhere).
    # Existing incompatible tests change both task_id and URL and miss this case.
    url_only_event = str(synthetic_ids["backlink_event_id"])
    url_only_receipt_id = int(synthetic_ids["backlink_task_id"])
    url_only_marker = str(synthetic_ids["backlink_marker"])
    url_only_proposal_id = crash_backlink_prop_seed["id"]
    expected_url = f"{origin_vikunja}/tasks/{url_only_receipt_id}"
    foreign_url_only = f"{origin_vikunja}/tasks/999999"
    _assert(foreign_url_only != expected_url, "url-only fixture must differ from admitted URL")
    url_only_row = store.get_by_event(url_only_event)
    _assert(
        url_only_row is not None and url_only_row.state == "linked",
        "url-only baseline linked missing",
    )
    assert url_only_row is not None
    _assert(url_only_row.task_id == url_only_receipt_id, "url-only baseline receipt id mismatch")
    _assert(url_only_row.url == expected_url, f"url-only baseline url: {url_only_row.url}")
    url_only_row.task_id = url_only_receipt_id
    url_only_row.url = foreign_url_only
    store.upsert(url_only_row)
    creates_before_url_only = _count_marker(client_api, url_only_marker)
    recovered_url_only = bridge.recover(url_only_event)
    _assert(
        recovered_url_only.outcome is Outcome.CONFLICT,
        f"recover url-only must conflict: {recovered_url_only.outcome} {recovered_url_only.details}",
    )
    _assert(not recovered_url_only.ok, "recover url-only must not ok")
    assert recovered_url_only.record is not None
    _assert(recovered_url_only.record.state == "conflict", "recover url-only must durable-conflict")
    _assert(recovered_url_only.record.task_id == url_only_receipt_id, "recover url-only lost task_id")
    _assert(recovered_url_only.record.url == foreign_url_only, "recover url-only lost foreign url")
    _assert(
        (recovered_url_only.details or {}).get("reason") == "incompatible_existing_link",
        f"recover url-only reason: {recovered_url_only.details}",
    )
    reset_url_only = store.get_by_event(url_only_event)
    _assert(reset_url_only is not None, "url-only row missing before deliver reset")
    assert reset_url_only is not None
    reset_url_only.state = "linked"
    reset_url_only.task_id = url_only_receipt_id
    reset_url_only.url = foreign_url_only
    reset_url_only.details = {
        key: value
        for key, value in (reset_url_only.details or {}).items()
        if key
        not in {
            "reason",
            "existing_task_id",
            "receipt_task_id",
            "last_check_outcome",
            "last_explicit_check",
            "last_check_path",
            "forbid_create",
            "api_outcome",
            "link_result",
        }
    }
    store.upsert(reset_url_only)
    delivered_url_only = bridge.deliver(url_only_event)
    _assert(
        delivered_url_only.outcome is Outcome.CONFLICT,
        f"deliver url-only must conflict: {delivered_url_only.outcome} {delivered_url_only.details}",
    )
    _assert(not delivered_url_only.ok, "deliver url-only must not ok")
    assert delivered_url_only.record is not None
    _assert(delivered_url_only.record.state == "conflict", "deliver url-only must durable-conflict")
    _assert(delivered_url_only.record.task_id == url_only_receipt_id, "deliver url-only lost task_id")
    _assert(delivered_url_only.record.url == foreign_url_only, "deliver url-only lost foreign url")
    sweep_url_only = bridge.recover_all_outbox()
    sweep_url_only_hit = [
        item for item in sweep_url_only if item.record and item.record.event_id == url_only_event
    ]
    _assert(len(sweep_url_only_hit) == 1, "sweep missed url-only row")
    _assert(sweep_url_only_hit[0].outcome is Outcome.CONFLICT, "sweep url-only must conflict")
    _assert(not sweep_url_only_hit[0].ok, "sweep url-only must not ok")
    _assert(
        _count_marker(client_api, url_only_marker) == creates_before_url_only,
        "url-only create observed",
    )

    url_only_prop = next(p for p in core.snapshot()["proposals"] if p["id"] == url_only_proposal_id)
    url_only_prop = core._project_tracker(url_only_prop)
    snap_url_only = (url_only_prop.get("tracker") or {})
    snap_url_only_avail = snap_url_only.get("availability") or {}
    _assert(snap_url_only.get("state") == "conflict", f"url-only snapshot state: {snap_url_only}")
    _assert(
        snap_url_only.get("task_id") == url_only_receipt_id,
        f"url-only snapshot task_id: {snap_url_only}",
    )
    _assert(snap_url_only.get("url") == foreign_url_only, f"url-only snapshot url: {snap_url_only}")
    _assert(
        snap_url_only_avail.get("status") == "conflict",
        f"url-only snapshot availability: {snap_url_only}",
    )
    _assert(
        snap_url_only_avail.get("reason") == "incompatible_existing_link",
        f"url-only snapshot reason: {snap_url_only}",
    )
    core_url_only = core.deliver_vikunja_outbox(url_only_prop)
    core_url_only_tracker = (core_url_only or {}).get("tracker") or {}
    core_url_only_avail = core_url_only_tracker.get("availability") or {}
    _assert(
        core_url_only_tracker.get("state") == "conflict",
        f"url-only core state: {core_url_only_tracker}",
    )
    _assert(
        core_url_only_tracker.get("task_id") == url_only_receipt_id,
        f"url-only core task_id: {core_url_only_tracker}",
    )
    _assert(
        core_url_only_tracker.get("url") == foreign_url_only,
        f"url-only core url: {core_url_only_tracker}",
    )
    _assert(
        core_url_only_avail.get("status") == "conflict",
        f"url-only core availability: {core_url_only_tracker}",
    )
    _assert(
        core_url_only_avail.get("last_check_outcome") == "conflict",
        f"url-only core last_check: {core_url_only_tracker}",
    )
    url_only_prop_full = next(
        p for p in core.snapshot()["proposals"] if p["id"] == url_only_proposal_id
    )
    url_only_endpoint = (
        f"/api/plugins/personal-tasks/proposals/{url_only_proposal_id}/decision?profile=default"
    )
    url_only_payload = {
        "revision": int(url_only_prop_full.get("revision") or crash_backlink_prop_seed["revision"]),
        "action": "accept",
        "title": crash_backlink_title_seed,
        "due": None,
        "due_confirmed": True,
        "tracker_target": "vikunja",
        "source_version": crash_backlink_prop_seed["version"],
    }
    with TestClient(app, base_url=ORIGIN) as url_only_client:
        url_only_replay = url_only_client.post(
            url_only_endpoint,
            json=url_only_payload,
            auth=("s06-owner", owner_password),
            headers={"Origin": ORIGIN},
        )
    _assert(
        url_only_replay.status_code == 200,
        f"url-only replay HTTP {url_only_replay.status_code}: {url_only_replay.text}",
    )
    public_url_only = ((url_only_replay.json().get("proposal") or {}).get("tracker") or {})
    public_url_only_avail = public_url_only.get("availability") or {}
    _assert(public_url_only.get("state") == "conflict", f"url-only endpoint state: {public_url_only}")
    _assert(
        public_url_only.get("task_id") == url_only_receipt_id,
        f"url-only endpoint task_id: {public_url_only}",
    )
    _assert(
        public_url_only.get("url") == foreign_url_only,
        f"url-only endpoint url: {public_url_only}",
    )
    _assert(
        public_url_only_avail.get("status") == "conflict",
        f"url-only endpoint availability: {public_url_only}",
    )
    _assert(
        public_url_only_avail.get("reason") == "incompatible_existing_link",
        f"url-only endpoint reason: {public_url_only}",
    )
    _assert(
        public_url_only_avail.get("last_check_outcome") == "conflict",
        f"url-only endpoint last_check: {public_url_only}",
    )
    _assert(
        _count_marker(client_api, url_only_marker) == creates_before_url_only,
        "url-only endpoint recreate observed",
    )
    scenarios["linked_url_only_mismatch"] = {
        "event_id": url_only_event,
        "receipt_task_id": url_only_receipt_id,
        "existing_task_id": url_only_receipt_id,
        "expected_url": expected_url,
        "existing_url": foreign_url_only,
        "recover_outcome": recovered_url_only.outcome.value,
        "deliver_outcome": delivered_url_only.outcome.value,
        "sweep_outcome": sweep_url_only_hit[0].outcome.value,
        "durable_state": delivered_url_only.record.state,
        "core_availability": core_url_only_avail.get("status"),
        "endpoint_http": url_only_replay.status_code,
        "endpoint_availability": public_url_only_avail.get("status"),
        "endpoint_reason": public_url_only_avail.get("reason"),
        "creates_after": _count_marker(client_api, url_only_marker),
    }
    executed += 1

    _assert(sha256_file(legacy_files[0]) == legacy_sha_before, "legacy markdown changed by end")
    scenarios["legacy_sha_final"] = legacy_sha_before

    return {
        "scenarios": scenarios,
        "scenario_names": sorted(scenarios),
        "executed_count": executed,
        "synthetic_ids": synthetic_ids,
        "source_files": patch_info,
        "fastapi": getattr(sys.modules.get("fastapi"), "__version__", "present"),
    }


def run_hermes_bridge_suite_inner(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            payload = run_hermes_bridge_scenarios(stand, worktree)
            result = {
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "synthetic_ids": payload["synthetic_ids"],
                "scenarios": payload["scenarios"],
                "scenario_names": payload["scenario_names"],
                "executed_count": payload["executed_count"],
                "source": {
                    **(stand.manifest.get("source") or {}),
                    **_source_digest(worktree),
                    **payload.get("source_files", {}),
                },
                "binary": stand.manifest.get("binary"),
                "argv": list(argv),
                "suite": "hermes-bridge",
                "mode": mode,
                "fastapi": payload.get("fastapi"),
            }
        result["cleanup"] = {"confirmed": stand.cleaned}
        if not stand.cleaned:
            raise StandError("hermes-bridge suite cleanup was not confirmed")
        if result["executed_count"] <= 0:
            raise StandError("hermes-bridge executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise


def run_hermes_bridge_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite hermes-bridge supports only isolated mode, got {mode!r}")
    # Real router import needs FastAPI from Hermes; re-exec under that interpreter when needed.
    if os.environ.get("EA010_HERMES_BRIDGE_INNER") != "1":
        try:
            import fastapi  # noqa: F401
        except ImportError:
            py = _hermes_python()
            env = {
                **os.environ,
                "EA010_HERMES_BRIDGE_INNER": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(worktree),
                "EA010_ISOLATED_STAND_PROFILE": os.environ.get(
                    "EA010_ISOLATED_STAND_PROFILE",
                    "/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json",
                ),
            }
            code = (
                "import json,sys; from pathlib import Path; "
                "from tests.support.hermes_bridge_suite import run_hermes_bridge_suite_inner; "
                "r=run_hermes_bridge_suite_inner(mode=sys.argv[1], worktree=Path(sys.argv[2]), "
                "argv=json.loads(sys.argv[3])); "
                "print('EA010_HERMES_BRIDGE_RESULT:'+json.dumps(r, sort_keys=True))"
            )
            proc = subprocess.run(
                [py, "-c", code, mode, str(worktree), json.dumps(list(argv))],
                capture_output=True,
                text=True,
                env=env,
                cwd=str(worktree),
            )
            if proc.returncode != 0:
                raise StandError(
                    "hermes-bridge inner suite failed under Hermes python: "
                    f"rc={proc.returncode}\nstdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}"
                )
            marker = "EA010_HERMES_BRIDGE_RESULT:"
            line = next((ln for ln in proc.stdout.splitlines() if ln.startswith(marker)), None)
            if line is None:
                raise StandError(f"inner suite produced no result marker; stdout={proc.stdout[-2000:]}")
            result = json.loads(line[len(marker) :])
            result["hermes_python"] = py
            result["reexec"] = True
            return result
    return run_hermes_bridge_suite_inner(mode=mode, worktree=worktree, argv=argv)
