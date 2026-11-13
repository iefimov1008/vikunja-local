"""Isolated Hermes UX suite (s07 / C5 0.2+0.3): UI patch + readiness/recover + Playwright."""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.server
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .stand import (
    STAND_SIGNAL_RECEIPT_ENV,
    IsolatedStand,
    StandError,
    allocate_component_signal_receipt,
    await_component_signal_receipt,
    load_profile,
    sha256_file,
    spawn_registered_component_proc,
    unregister_active_component_proc,
)

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

BASELINE_JS = Path(
    "/Users/efimovivan/Projects/cto-agent-environment/plugins/personal-tasks/dashboard/dist/index.js"
)
BASELINE_CSS = Path(
    "/Users/efimovivan/Projects/cto-agent-environment/plugins/personal-tasks/dashboard/dist/style.css"
)
BASELINE_API = Path(
    "/Users/efimovivan/Projects/cto-agent-environment/plugins/personal-tasks/dashboard/plugin_api.py"
)
BASELINE_CORE = Path("/Users/efimovivan/Projects/cto-agent-environment/scripts/ea_core.py")
FROM_JS = "bb0144e8bcb343880a334b342f74cdcbdf08a5d8996e96d5442b503a77b822e8"
FROM_API = "938dc648daee8f5484a76de6bac0ed86f79b28463c89a1d17c6af02e833b0f51"
FROM_CORE = "23deacba5cca06d314c5ec5717bbb3f6dd7f074974670324ab7f64baaecf10ef"
AFTER_S06_API = "c200627f38b4cea8b5f979f8fa1394def53082d72fcde66f6be10d40a1ec8c77"
AFTER_S06_CORE = "060a22948ac4154fbf2e5dd6c5699c2d887b32c7d312fd5f8ccc5982a7a11d49"
PLAYWRIGHT_MODULE = Path(
    "/Users/efimovivan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
)
CHROMIUM = Path(
    "/Users/efimovivan/Library/Caches/ms-playwright/chromium_headless_shell-1243/"
    "chrome-headless-shell-mac-arm64/chrome-headless-shell"
)
CHROMIUM_SHA = "a0bfe7b4da4787b66058477d696cd1d09065d25f06a548947722b9af77ee8282"
ESBUILD = Path("/Users/efimovivan/.hermes/hermes-agent/node_modules/.bin/esbuild")
REACT_ENTRY = Path("/Users/efimovivan/.hermes/hermes-agent/node_modules/react/index.js")
REACT_DOM_CLIENT = Path(
    "/Users/efimovivan/.hermes/hermes-agent/node_modules/react-dom/client.js"
)
WRITE_PERMS = {"tasks": ["create", "read_all", "read_one", "update"]}
OWNER = "s07-owner"
HERMES_ORIGIN = "http://127.0.0.1:0"  # set per run


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _hermes_python() -> str:
    override = os.environ.get("EA010_HERMES_PYTHON")
    if override:
        path = Path(override)
        _assert(path.is_file() and os.access(path, os.X_OK), f"EA010_HERMES_PYTHON unusable: {override}")
        return str(path)
    roots = [Path("/Users/efimovivan/.hermes/installs")]
    candidates: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        candidates.extend(root.glob("**/environments/*/venv/bin/python"))
        candidates.extend(root.glob("**/test-environment/*/venv/bin/python"))
    for cand in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
        probe = subprocess.run(
            [str(cand), "-c", "import fastapi, uvicorn, fastapi.testclient"],
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0:
            return str(cand)
    raise StandError(
        "Real Hermes router requires FastAPI; set EA010_HERMES_PYTHON to a Hermes venv python"
    )


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "integration" / "hermes" / "ui.patch",
        worktree / "integration" / "hermes" / "ui-readiness.patch",
        worktree / "integration" / "hermes" / "ui-manifest.json",
        worktree / "vikunja_local" / "hermes_bridge.py",
        worktree / "tests" / "test_hermes_ux.py",
        worktree / "tests" / "support" / "hermes_ux_suite.py",
        worktree / "tests" / "support" / "suites.py",
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


def _load_manifest(worktree: Path) -> dict[str, Any]:
    path = worktree / "integration" / "hermes" / "ui-manifest.json"
    _assert(path.is_file(), "ui-manifest.json missing")
    return json.loads(path.read_text(encoding="utf-8"))


def _verify_and_apply_patches(worktree: Path, dest: Path) -> dict[str, Any]:
    manifest = _load_manifest(worktree)
    ui_patch = worktree / "integration" / "hermes" / "ui.patch"
    readiness_patch = worktree / "integration" / "hermes" / "ui-readiness.patch"
    backend_patch = worktree / "integration" / "hermes" / "backend.patch"
    _assert(sha256_file(BASELINE_JS) == FROM_JS, "index.js baseline hash drift")
    _assert(sha256_file(BASELINE_API) == FROM_API, "plugin_api.py baseline hash drift")
    _assert(sha256_file(BASELINE_CORE) == FROM_CORE, "ea_core.py baseline hash drift")
    _assert(sha256_file(ui_patch) == manifest["patches"]["ui"]["sha256"], "ui.patch sha mismatch vs manifest")
    _assert(
        sha256_file(readiness_patch) == manifest["patches"]["ui_readiness"]["sha256"],
        "ui-readiness.patch sha mismatch vs manifest",
    )
    _assert(
        sha256_file(backend_patch) == manifest["baseline"]["plugin_api.py"]["s06_patch_sha256"],
        "s06 patch sha",
    )

    if dest.exists():
        shutil.rmtree(dest)
    (dest / "plugins/personal-tasks/dashboard/dist").mkdir(parents=True)
    (dest / "scripts").mkdir(parents=True)
    shutil.copy2(BASELINE_JS, dest / "plugins/personal-tasks/dashboard/dist/index.js")
    shutil.copy2(BASELINE_CSS, dest / "plugins/personal-tasks/dashboard/dist/style.css")
    shutil.copy2(BASELINE_API, dest / "plugins/personal-tasks/dashboard/plugin_api.py")
    shutil.copy2(BASELINE_CORE, dest / "scripts/ea_core.py")

    check_ui = subprocess.run(
        ["git", "apply", "--check", "--unidiff-zero", str(ui_patch)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(check_ui.returncode == 0, f"ui.patch --check failed: {check_ui.stderr}")
    apply_ui = subprocess.run(
        ["git", "apply", "--unidiff-zero", str(ui_patch)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(apply_ui.returncode == 0, f"ui.patch apply failed: {apply_ui.stderr}")
    to_js = sha256_file(dest / "plugins/personal-tasks/dashboard/dist/index.js")
    _assert(to_js == manifest["patches"]["ui"]["to_sha256"], f"ui to-hash {to_js}")

    apply_s06 = subprocess.run(
        ["patch", "-p1", "-i", str(backend_patch)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(apply_s06.returncode == 0, f"s06 backend apply failed: {apply_s06.stderr}")
    after_s06 = sha256_file(dest / "plugins/personal-tasks/dashboard/plugin_api.py")
    _assert(after_s06 == AFTER_S06_API, f"after s06 api hash {after_s06}")

    check_ready = subprocess.run(
        ["git", "apply", "--check", "--unidiff-zero", str(readiness_patch)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(check_ready.returncode == 0, f"ui-readiness --check failed: {check_ready.stderr}")
    apply_ready = subprocess.run(
        ["git", "apply", "--unidiff-zero", str(readiness_patch)],
        cwd=dest,
        capture_output=True,
        text=True,
    )
    _assert(apply_ready.returncode == 0, f"ui-readiness apply failed: {apply_ready.stderr}")
    to_api = sha256_file(dest / "plugins/personal-tasks/dashboard/plugin_api.py")
    _assert(to_api == manifest["patches"]["ui_readiness"]["to_sha256"], f"readiness to-hash {to_api}")
    to_core = sha256_file(dest / "scripts/ea_core.py")
    _assert(
        to_core == manifest["patches"]["ui_readiness"]["to_core_sha256"],
        f"readiness core to-hash {to_core}",
    )
    after_s06_core = manifest["baseline"]["ea_core.py"]["after_s06_sha256"]
    _assert(after_s06_core == AFTER_S06_CORE, "manifest after_s06 core drift")

    rev = dest.parent / (dest.name + "-reverse")
    if rev.exists():
        shutil.rmtree(rev)
    shutil.copytree(dest, rev)
    reverse_ready = subprocess.run(
        ["git", "apply", "--reverse", "--unidiff-zero", str(readiness_patch)],
        cwd=rev,
        capture_output=True,
        text=True,
    )
    _assert(reverse_ready.returncode == 0, f"readiness reverse failed: {reverse_ready.stderr}")
    _assert(
        sha256_file(rev / "plugins/personal-tasks/dashboard/plugin_api.py") == AFTER_S06_API,
        "readiness reverse did not restore s06 api",
    )
    _assert(
        sha256_file(rev / "scripts/ea_core.py") == AFTER_S06_CORE,
        "readiness reverse did not restore s06 core",
    )
    reverse_ui = subprocess.run(
        ["git", "apply", "--reverse", "--unidiff-zero", str(ui_patch)],
        cwd=rev,
        capture_output=True,
        text=True,
    )
    _assert(reverse_ui.returncode == 0, f"ui reverse failed: {reverse_ui.stderr}")
    _assert(
        sha256_file(rev / "plugins/personal-tasks/dashboard/dist/index.js") == FROM_JS,
        "ui reverse did not restore baseline js",
    )
    shutil.rmtree(rev)
    return {
        "to_js": to_js,
        "to_api": to_api,
        "to_core": to_core,
        "ui_patch_sha256": manifest["patches"]["ui"]["sha256"],
        "readiness_patch_sha256": manifest["patches"]["ui_readiness"]["sha256"],
        "reverse_ok": True,
        "patched_tree": str(dest),
    }


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _bundle_react(harness_dir: Path) -> Path:
    _assert(ESBUILD.is_file(), f"esbuild missing at {ESBUILD}")
    _assert(REACT_ENTRY.is_file(), "react module missing")
    _assert(REACT_DOM_CLIENT.is_file(), "react-dom/client missing")
    entry = harness_dir / "react-entry.js"
    entry.write_text(
        "import React from 'react';\n"
        "import {createRoot} from 'react-dom/client';\n"
        "window.React = React;\n"
        "window.ReactDOM = {createRoot};\n",
        encoding="utf-8",
    )
    out = harness_dir / "react-bundle.js"
    hermes_nm = "/Users/efimovivan/.hermes/hermes-agent/node_modules"
    cmd = [
        str(ESBUILD),
        str(entry),
        "--bundle",
        "--format=iife",
        "--platform=browser",
        f"--outfile={out}",
        f"--alias:react={hermes_nm}/react/index.js",
        f"--alias:react-dom={hermes_nm}/react-dom/index.js",
        f"--alias:react-dom/client={hermes_nm}/react-dom/client.js",
    ]
    env = {**os.environ, "NODE_PATH": hermes_nm}
    built = subprocess.run(cmd, capture_output=True, text=True, env=env)
    _assert(built.returncode == 0, f"esbuild failed: {built.stderr}")
    _assert(out.is_file() and out.stat().st_size > 1000, "react bundle empty")
    return out


def _load_module(name: str, path: Path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, path)
    _assert(spec is not None and spec.loader is not None, f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _store_token(stand: IsolatedStand, name: str, secret: str) -> Path:
    path = stand.run_dir / "secrets" / name
    path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path.parent, 0o700)
    path.write_text(secret, encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def _create_api_token(stand: IsolatedStand, *, title: str, permissions: dict[str, Any]) -> dict[str, Any]:
    token = stand.api(
        "tokens",
        "PUT",
        {
            "title": title,
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "permissions": permissions,
        },
        token=stand._session_token,
    )
    _assert(token[0] == 201, f"token create failed: {token}")
    return token[1]


class _TaskPageHandler(http.server.BaseHTTPRequestHandler):
    server_version = "EA010TaskSurface/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/tasks/"):
            task_id = parsed.path.rsplit("/", 1)[-1]
            body = (
                f"<!doctype html><html><body>"
                f"<h1 data-testid='task-page'>Synthetic task</h1>"
                f"<p data-testid='task-id'>{task_id}</p>"
                f"<p data-testid='route-label'>{self.server.route_label}</p>"  # type: ignore[attr-defined]
                f"</body></html>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path in ("/", ""):
            body = b"<!doctype html><html><body><h1 data-testid='vikunja-home'>Vikunja</h1></body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


def _start_task_surface(label: str) -> tuple[Any, str, int]:
    port = _free_port()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), _TaskPageHandler)
    server.route_label = label  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}", port


class _RedirectProbeHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.endswith("/tasks"):
            dest = self.server.redirect_to  # type: ignore[attr-defined]
            self.send_response(302)
            self.send_header("Location", dest)
            self.end_headers()
            self.server.saw_auth.append(self.headers.get("Authorization"))  # type: ignore[attr-defined]
            return
        if parsed.path == "/redirected":
            self.server.redirected_auth.append(self.headers.get("Authorization"))  # type: ignore[attr-defined]
            body = b"[]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if "/tasks" in parsed.path:
            body = b'{"not":"array"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


def _start_redirect_pair() -> dict[str, Any]:
    target_port = _free_port()
    source_port = _free_port()

    class Target(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            auth = self.headers.get("Authorization")
            self.server.auths.append(auth)  # type: ignore[attr-defined]
            body = b"[]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    target = http.server.ThreadingHTTPServer(("127.0.0.1", target_port), Target)
    target.auths = []  # type: ignore[attr-defined]
    threading.Thread(target=target.serve_forever, daemon=True).start()

    source = http.server.ThreadingHTTPServer(("127.0.0.1", source_port), _RedirectProbeHandler)
    source.redirect_to = f"http://127.0.0.1:{target_port}/redirected"  # type: ignore[attr-defined]
    source.saw_auth = []  # type: ignore[attr-defined]
    source.redirected_auth = []  # type: ignore[attr-defined]
    # Attach redirected handler on target path via second server already; override Location to target /redirected
    # Serve /redirected on target:
    class Target2(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            self.server.auths.append(self.headers.get("Authorization"))  # type: ignore[attr-defined]
            body = b"[]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    # Rebuild target with /redirected capturing auth
    target.shutdown()
    target.server_close()
    target = http.server.ThreadingHTTPServer(("127.0.0.1", target_port), Target2)
    target.auths = []  # type: ignore[attr-defined]
    threading.Thread(target=target.serve_forever, daemon=True).start()
    threading.Thread(target=source.serve_forever, daemon=True).start()
    return {
        "source": source,
        "target": target,
        "source_origin": f"http://127.0.0.1:{source_port}",
        "target_origin": f"http://127.0.0.1:{target_port}",
    }


def _node_helper_checks(patched_js: Path) -> dict[str, Any]:
    script = f"""
const ui = require({json.dumps(str(patched_js))});
const proposal = {{id:'prop-synth', meeting_id:'m', version:{json.dumps('a'*64)}, revision:1, status:'pending', text:'t'}};
const accept = ui.decisionPayload(proposal, 'accept', '', {{title:'T', due:null, due_confirmed:true}}, {{ready:true}});
if (accept.tracker_target !== 'vikunja') throw new Error('tracker_target');
if (accept.source_version !== proposal.version) throw new Error('source_version');
const delivered = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:7, url:'http://127.0.0.1/tasks/7'}}}});
if (delivered.kind !== 'delivered') throw new Error('delivered');
const unknown = ui.deliveryPresentation({{tracker:{{state:'unknown'}}}});
if (unknown.kind !== 'unknown' || unknown.action !== 'reconcile') throw new Error('unknown');
const done = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:7, url:'http://127.0.0.1/tasks/7', done_observation:{{done:true, checked_at:'t'}}}}}});
if (done.kind !== 'done_observed' || !done.label.includes('не актуальный статус')) throw new Error('done');
const deniedDone = ui.deliveryPresentation({{tracker:{{state:'linked', availability:{{status:'denied', outcome:'auth_denied', reason:'token'}}, done_observation:{{done:true, checked_at:'t'}}}}}});
if (deniedDone.kind !== 'denied' || deniedDone.action !== 'check_connection') throw new Error('denied+done');
if (!deniedDone.label.includes('не актуальный статус')) throw new Error('denied must keep observation');
const conflict = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:7, url:'http://127.0.0.1/tasks/7', availability:{{status:'conflict', outcome:'conflict', reason:'incompatible_existing_link'}}}}}});
if (conflict.kind !== 'conflict' || conflict.action !== 'reconcile') throw new Error('conflict must reconcile: ' + JSON.stringify(conflict));
const deleted = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:7, url:'http://127.0.0.1/tasks/7', availability:{{status:'unavailable', outcome:'unknown', reason:'target_deleted_or_gone'}}}}}});
if (deleted.kind !== 'deleted' || deleted.action !== null) throw new Error('deleted wire: ' + JSON.stringify(deleted));
const nomap = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:7, url:'http://127.0.0.1/tasks/7', availability:{{status:'unavailable', outcome:'unknown', reason:'no_source_mapping'}}}}}});
if (nomap.kind !== 'unknown' || nomap.action !== 'reconcile') throw new Error('no_source_mapping must not look delivered: ' + JSON.stringify(nomap));
const src = ui.proposalSourceUrl(proposal, {{origin:'http://127.0.0.1:18765'}});
if (src !== 'http://127.0.0.1:18765/#pt-proposal-prop-synth') throw new Error('source '+src);
console.log(JSON.stringify({{ok:true, accept, delivered:delivered.kind, unknown:unknown.kind, done:done.kind, deniedDone:deniedDone.kind, conflict:conflict.kind, deleted:deleted.kind, nomap:nomap.kind, source:src}}));
"""
    proc = subprocess.run(["node", "-e", script], capture_output=True, text=True)
    _assert(proc.returncode == 0, f"node helpers failed: {proc.stderr}\n{proc.stdout}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _run_readiness_via_snapshot(plugin: Any, *, auth_header: str, origin: str) -> dict[str, Any]:
    from fastapi import FastAPI, Request
    from fastapi.testclient import TestClient

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):  # noqa: ANN001
        if hmac.compare_digest(request.headers.get("authorization", ""), auth_header):
            request.state.session = SimpleNamespace(user_id=OWNER)
        return await call_next(request)

    app.include_router(plugin.router, prefix="/api/plugins/personal-tasks")
    with TestClient(app, base_url=origin) as client:
        resp = client.get(
            "/api/plugins/personal-tasks/snapshot?profile=default",
            headers={"Authorization": auth_header, "Origin": origin},
        )
        _assert(resp.status_code == 200, f"snapshot status {resp.status_code}: {resp.text}")
        payload = resp.json()
        _assert("meetings" in payload, "snapshot missing meetings")
        _assert("tracker" in payload, "snapshot missing tracker")
        return payload


def _run_readiness_scenarios(stand: IsolatedStand, plugin: Any, auth_header: str) -> dict[str, Any]:
    session = stand.create_synthetic_session() if not getattr(stand, "_session_project_id", None) else None
    # Prefer existing session from outer setup when present.
    project_id = int(session["project_id"]) if session else int(stand.synthetic_ids.get("project_id") or 0)
    if project_id <= 0:
        session = stand.create_synthetic_session()
        project_id = int(session["project_id"])

    token = _create_api_token(stand, title=stand.fixture_name("ux-read"), permissions=WRITE_PERMS)
    token_path = _store_token(stand, "ux-read.token", token["token"])
    vikunja_origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    browser_a = vikunja_origin

    # Writer (bridge) points at real Vikunja; UI profile only supplies browser_origin/timeout.
    bridge_ok = stand.run_dir / "bridge-ready.json"
    bridge_ok.write_text(
        json.dumps(
            {
                "journal_path": str(stand.run_dir / "journal" / "ready.sqlite3"),
                "api_origin": vikunja_origin,
                "project_id": project_id,
                "token_ref": {"kind": "file", "path": str(token_path)},
                "task_url_template": vikunja_origin + "/tasks/{id}",
                "hermes_origin": HERMES_ORIGIN,
            }
        ),
        encoding="utf-8",
    )
    os.chmod(bridge_ok, 0o600)
    (stand.run_dir / "journal").mkdir(mode=0o700, exist_ok=True)

    ui_profile = stand.run_dir / "ui-profile.json"
    ui_profile.write_text(
        json.dumps({"browser_origin": browser_a, "timeout_s": 2.0}),
        encoding="utf-8",
    )
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(bridge_ok)
    os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
    snap = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
    _assert(snap["tracker"].get("ready") is True, f"expected ready via snapshot: {snap['tracker']}")
    _assert(snap["tracker"].get("ui_url") == browser_a, f"ui_url {snap['tracker']}")

    # R2: writer unavailable must win over a separate successful synthetic API.
    decoy = _start_redirect_pair()
    try:
        bad_writer = stand.run_dir / "bridge-bad-writer.json"
        bad_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "bad.sqlite3"),
                    "api_origin": "http://127.0.0.1:1",
                    "project_id": 7,
                    "token_ref": {"kind": "file", "path": str(token_path)},
                    "task_url_template": "http://127.0.0.1:1/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(bad_writer, 0o600)
        # UI profile still naming a working decoy must not enable ready.
        ui_decoy = stand.run_dir / "ui-profile-decoy.json"
        ui_decoy.write_text(
            json.dumps(
                {
                    "browser_origin": browser_a,
                    "api_origin": decoy["source_origin"],
                    "project_id": 99,
                    "token_ref": {"kind": "file", "path": str(token_path)},
                    "timeout_s": 1.0,
                }
            ),
            encoding="utf-8",
        )
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(bad_writer)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_decoy)
        snap_bad = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert(snap_bad["tracker"].get("ready") is False, f"decoy must not ready: {snap_bad['tracker']}")

        # Missing writer config → ready false
        os.environ.pop("EA010_HERMES_BRIDGE_CONFIG", None)
        snap_missing = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert(snap_missing["tracker"].get("ready") is False, "missing writer must be not ready")
        _assert("meetings" in snap_missing, "meetings preserved when writer missing")

        # R1: redirect must not yield ready; Authorization must not land on other port.
        redirect_writer = stand.run_dir / "bridge-redirect.json"
        redirect_token = _store_token(stand, "redirect.token", "sentinel-bearer-ea010")
        redirect_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "redir.sqlite3"),
                    "api_origin": decoy["source_origin"],
                    "project_id": 1,
                    "token_ref": {"kind": "file", "path": str(redirect_token)},
                    "task_url_template": decoy["source_origin"] + "/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(redirect_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(redirect_writer)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
        snap_redir = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert(snap_redir["tracker"].get("ready") is False, f"redirect must fail closed: {snap_redir['tracker']}")
        _assert(
            all(a is None for a in decoy["target"].auths),  # type: ignore[index]
            f"Authorization forwarded cross-origin: {decoy['target'].auths}",  # type: ignore[index]
        )

        # Arbitrary JSON object on writer origin must not ready.
        obj_port = _free_port()

        class ObjHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

            def do_GET(self) -> None:  # noqa: N802
                body = b'{"items":[]}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        obj_server = http.server.ThreadingHTTPServer(("127.0.0.1", obj_port), ObjHandler)
        threading.Thread(target=obj_server.serve_forever, daemon=True).start()
        obj_writer = stand.run_dir / "bridge-object.json"
        obj_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "obj.sqlite3"),
                    "api_origin": f"http://127.0.0.1:{obj_port}",
                    "project_id": 3,
                    "token_ref": {"kind": "file", "path": str(token_path)},
                    "task_url_template": f"http://127.0.0.1:{obj_port}/tasks/{{id}}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(obj_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(obj_writer)
        snap_obj = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert(snap_obj["tracker"].get("ready") is False, f"object body must not ready: {snap_obj['tracker']}")
        obj_server.shutdown()
        obj_server.server_close()

        # R5: invalid browser_origin / non-finite timeout must not 503 the meetings snapshot.
        bad_ui = stand.run_dir / "ui-profile-bad.json"
        bad_ui.write_text(
            json.dumps(
                {
                    "browser_origin": "http://127.0.0.1:99999",
                    "timeout_s": "inf",
                }
            ),
            encoding="utf-8",
        )
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(bridge_ok)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(bad_ui)
        snap_err = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert("meetings" in snap_err, "meetings must survive invalid readiness config")
        _assert(snap_err["tracker"].get("ui_url") is None, "invalid browser_origin must yield null ui_url")
        # Writer remains healthy → ready may be true; the contract is no crash/503.
        _assert(isinstance(snap_err["tracker"].get("ready"), bool), "tracker.ready must be bool")
        # Direct helper containment for former ValueError/OverflowError inputs.
        surface = plugin._ui_tracker_surface()
        _assert(isinstance(surface.get("ready"), bool) and surface.get("ui_url") is None, f"helper surface {surface}")

        # Malformed API origin in writer also fails closed without 503.
        mal_writer = stand.run_dir / "bridge-malformed.json"
        mal_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "mal.sqlite3"),
                    "api_origin": "not a url",
                    "project_id": project_id,
                    "token_ref": {"kind": "file", "path": str(token_path)},
                    "task_url_template": vikunja_origin + "/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(mal_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(mal_writer)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
        snap_mal = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        _assert(snap_mal["tracker"].get("ready") is False, "malformed api_origin must not ready")
        _assert("meetings" in snap_mal, "meetings must survive malformed api_origin")

        # R2: non-loopback writer origin must fail closed with zero outbound requests.
        non_loop_hits: list[str] = []

        class NonLoopHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

            def do_GET(self) -> None:  # noqa: N802
                non_loop_hits.append(self.path)
                self.send_error(500)

        # Intercept urllib Request construction — even before DNS/connect.
        import urllib.request as _ur

        constructed_urls: list[str] = []
        real_request = _ur.Request

        class CountingRequest(_ur.Request):
            def __init__(self, url, data=None, headers=None, origin_req_host=None, unverifiable=False, method=None):  # noqa: ANN001
                constructed_urls.append(str(url))
                super().__init__(
                    url,
                    data=data,
                    headers={} if headers is None else headers,
                    origin_req_host=origin_req_host,
                    unverifiable=unverifiable,
                    method=method,
                )

        non_loop_writer = stand.run_dir / "bridge-nonloop.json"
        # Documentation-range host (TEST-NET-1); must never be probed.
        non_loop_origin = "http://192.0.2.10:3456"
        non_loop_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "nonloop.sqlite3"),
                    "api_origin": non_loop_origin,
                    "project_id": project_id,
                    "token_ref": {"kind": "file", "path": str(token_path)},
                    "task_url_template": non_loop_origin + "/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(non_loop_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(non_loop_writer)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
        _ur.Request = CountingRequest  # type: ignore[misc,assignment]
        try:
            snap_nonloop = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        finally:
            _ur.Request = real_request  # type: ignore[misc,assignment]
        _assert(snap_nonloop["tracker"].get("ready") is False, f"non-loopback must not ready: {snap_nonloop['tracker']}")
        _assert(
            not any("192.0.2.10" in u for u in constructed_urls),
            f"non-loopback must not construct Request: {constructed_urls}",
        )
        _assert(non_loop_hits == [], f"non-loopback must not hit listener: {non_loop_hits}")
        writer_nonloop = plugin._read_bridge_writer()
        _assert(writer_nonloop is None, f"writer must reject non-loopback: {writer_nonloop}")

        # R2: insecure token mode (0644) must fail before Authorization construction.
        insecure_token = stand.run_dir / "secrets" / "insecure-0644.token"
        insecure_token.write_text("synthetic-insecure-token", encoding="utf-8")
        os.chmod(insecure_token, 0o644)
        insecure_writer = stand.run_dir / "bridge-insecure-token.json"
        insecure_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "insecure.sqlite3"),
                    "api_origin": vikunja_origin,
                    "project_id": project_id,
                    "token_ref": {"kind": "file", "path": str(insecure_token)},
                    "task_url_template": vikunja_origin + "/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(insecure_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(insecure_writer)
        constructed_urls.clear()
        _ur.Request = CountingRequest  # type: ignore[misc,assignment]
        try:
            snap_insecure = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
        finally:
            _ur.Request = real_request  # type: ignore[misc,assignment]
        _assert(snap_insecure["tracker"].get("ready") is False, f"insecure token must not ready: {snap_insecure['tracker']}")
        _assert(constructed_urls == [], f"insecure token must not construct Request: {constructed_urls}")
        writer_insecure = plugin._read_bridge_writer()
        _assert(writer_insecure is None, f"writer must reject insecure token mode: {writer_insecure}")

        # NFR-02/03: env HTTP proxy must not receive Authorization or fabricate ready
        # when the configured writer is unreachable (ProxyHandler({}) on readiness).
        proxy_port = _free_port()
        proxy_hits: list[dict[str, Any]] = []

        class EnvProxyHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

            def do_GET(self) -> None:  # noqa: N802
                proxy_hits.append(
                    {
                        "path": self.path,
                        "authorization": self.headers.get("Authorization"),
                        "host": self.headers.get("Host"),
                    }
                )
                body = b"[]"
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_CONNECT(self) -> None:  # noqa: N802
                proxy_hits.append(
                    {
                        "path": self.path,
                        "authorization": self.headers.get("Authorization"),
                        "method": "CONNECT",
                    }
                )
                self.send_error(403, "CONNECT refused")

        proxy_server = http.server.ThreadingHTTPServer(("127.0.0.1", proxy_port), EnvProxyHandler)
        threading.Thread(target=proxy_server.serve_forever, daemon=True).start()
        proxy_origin = f"http://127.0.0.1:{proxy_port}"
        unreachable_writer = stand.run_dir / "bridge-unreachable-proxy.json"
        proxy_token = _store_token(stand, "proxy-sentinel.token", "sentinel-proxy-bearer-ea010")
        unreachable_writer.write_text(
            json.dumps(
                {
                    "journal_path": str(stand.run_dir / "journal" / "proxy.sqlite3"),
                    "api_origin": "http://127.0.0.1:1",
                    "project_id": 7,
                    "token_ref": {"kind": "file", "path": str(proxy_token)},
                    "task_url_template": "http://127.0.0.1:1/tasks/{id}",
                    "hermes_origin": HERMES_ORIGIN,
                }
            ),
            encoding="utf-8",
        )
        os.chmod(unreachable_writer, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(unreachable_writer)
        os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
        saved_proxy_env = {
            key: os.environ.get(key)
            for key in (
                "http_proxy",
                "https_proxy",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "all_proxy",
                "ALL_PROXY",
                "no_proxy",
                "NO_PROXY",
            )
        }
        try:
            for key in ("https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
                os.environ.pop(key, None)
            os.environ["http_proxy"] = proxy_origin
            # Direct probe must fail closed with zero proxy hits (writer down + proxy bait).
            direct_ready = plugin._probe_project_ready(
                "http://127.0.0.1:1",
                7,
                "sentinel-proxy-bearer-ea010",
                1.0,
            )
            _assert(direct_ready is False, f"unreachable writer via env proxy must be false: {direct_ready}")
            _assert(proxy_hits == [], f"readiness must not hit env proxy: {proxy_hits}")
            snap_proxy = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
            _assert(snap_proxy["tracker"].get("ready") is False, f"env proxy must not ready: {snap_proxy['tracker']}")
            _assert(proxy_hits == [], f"snapshot readiness must not hit env proxy: {proxy_hits}")
        finally:
            for key, value in saved_proxy_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            proxy_server.shutdown()
            proxy_server.server_close()

    finally:
        decoy["source"].shutdown()
        decoy["source"].server_close()
        decoy["target"].shutdown()
        decoy["target"].server_close()

    # C5 0.2: absolute readiness budget — slow body/headers must not overrun timeout_s.
    slow_timeout_s = 0.5
    slow_budget_cap_s = 0.8

    class SlowBodyHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            body = b"[" + (b" " * 18) + b"]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for byte in body:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.1)
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                return

    slow_body_port = _free_port()
    slow_body_server = http.server.ThreadingHTTPServer(("127.0.0.1", slow_body_port), SlowBodyHandler)
    threading.Thread(target=slow_body_server.serve_forever, daemon=True).start()
    slow_body_writer = stand.run_dir / "bridge-slow-body.json"
    slow_body_writer.write_text(
        json.dumps(
            {
                "journal_path": str(stand.run_dir / "journal" / "slow-body.sqlite3"),
                "api_origin": f"http://127.0.0.1:{slow_body_port}",
                "project_id": 1,
                "token_ref": {"kind": "file", "path": str(token_path)},
                "task_url_template": f"http://127.0.0.1:{slow_body_port}/tasks/{{id}}",
                "hermes_origin": HERMES_ORIGIN,
            }
        ),
        encoding="utf-8",
    )
    os.chmod(slow_body_writer, 0o600)
    slow_ui = stand.run_dir / "ui-profile-slow.json"
    slow_ui.write_text(
        json.dumps({"browser_origin": browser_a, "timeout_s": slow_timeout_s}),
        encoding="utf-8",
    )
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(slow_body_writer)
    os.environ["EA010_HERMES_UI_PROFILE"] = str(slow_ui)
    slow_body_start = time.monotonic()
    snap_slow_body = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
    slow_body_elapsed = time.monotonic() - slow_body_start
    _assert(snap_slow_body["tracker"].get("ready") is False, f"slow body must not ready: {snap_slow_body['tracker']}")
    _assert(
        slow_body_elapsed <= slow_budget_cap_s,
        f"slow body exceeded absolute budget: {slow_body_elapsed:.4f}s > {slow_budget_cap_s}",
    )
    _assert("meetings" in snap_slow_body, "meetings must survive slow body readiness")
    direct_slow_body_start = time.monotonic()
    direct_slow_body = plugin._probe_project_ready(
        f"http://127.0.0.1:{slow_body_port}",
        1,
        "synthetic-slow-body",
        slow_timeout_s,
    )
    direct_slow_body_elapsed = time.monotonic() - direct_slow_body_start
    _assert(direct_slow_body is False, f"direct slow body must be false: {direct_slow_body}")
    _assert(
        direct_slow_body_elapsed <= slow_budget_cap_s,
        f"direct slow body exceeded budget: {direct_slow_body_elapsed:.4f}s",
    )
    slow_body_server.shutdown()
    slow_body_server.server_close()

    class SlowHeaderHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def handle_one_request(self) -> None:  # noqa: N802
            try:
                self.raw_requestline = self.rfile.readline(65537)
                if not self.raw_requestline:
                    self.close_connection = True
                    return
                if not self.parse_request():
                    return
                chunks = [
                    b"HTTP/1.1 200 OK\r\n",
                    b"Content-Type: application/json\r\n",
                    b"Content-Length: 2\r\n",
                    b"X-Slow-1: a\r\n",
                    b"X-Slow-2: b\r\n",
                    b"X-Slow-3: c\r\n",
                    b"X-Slow-4: d\r\n",
                    b"X-Slow-5: e\r\n",
                    b"X-Slow-6: f\r\n",
                    b"X-Slow-7: g\r\n",
                    b"X-Slow-8: h\r\n",
                    b"\r\n",
                    b"[]",
                ]
                for idx, chunk in enumerate(chunks):
                    if idx:
                        time.sleep(0.15)
                    self.wfile.write(chunk)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                self.close_connection = True

    slow_header_port = _free_port()
    slow_header_server = http.server.ThreadingHTTPServer(("127.0.0.1", slow_header_port), SlowHeaderHandler)
    threading.Thread(target=slow_header_server.serve_forever, daemon=True).start()
    slow_header_writer = stand.run_dir / "bridge-slow-header.json"
    slow_header_writer.write_text(
        json.dumps(
            {
                "journal_path": str(stand.run_dir / "journal" / "slow-header.sqlite3"),
                "api_origin": f"http://127.0.0.1:{slow_header_port}",
                "project_id": 1,
                "token_ref": {"kind": "file", "path": str(token_path)},
                "task_url_template": f"http://127.0.0.1:{slow_header_port}/tasks/{{id}}",
                "hermes_origin": HERMES_ORIGIN,
            }
        ),
        encoding="utf-8",
    )
    os.chmod(slow_header_writer, 0o600)
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(slow_header_writer)
    os.environ["EA010_HERMES_UI_PROFILE"] = str(slow_ui)
    slow_header_start = time.monotonic()
    snap_slow_header = _run_readiness_via_snapshot(plugin, auth_header=auth_header, origin=HERMES_ORIGIN)
    slow_header_elapsed = time.monotonic() - slow_header_start
    _assert(
        snap_slow_header["tracker"].get("ready") is False,
        f"slow headers must not ready: {snap_slow_header['tracker']}",
    )
    _assert(
        slow_header_elapsed <= slow_budget_cap_s,
        f"slow headers exceeded absolute budget: {slow_header_elapsed:.4f}s > {slow_budget_cap_s}",
    )
    _assert("meetings" in snap_slow_header, "meetings must survive slow header readiness")
    direct_slow_header_start = time.monotonic()
    direct_slow_header = plugin._probe_project_ready(
        f"http://127.0.0.1:{slow_header_port}",
        1,
        "synthetic-slow-header",
        slow_timeout_s,
    )
    direct_slow_header_elapsed = time.monotonic() - direct_slow_header_start
    _assert(direct_slow_header is False, f"direct slow headers must be false: {direct_slow_header}")
    _assert(
        direct_slow_header_elapsed <= slow_budget_cap_s,
        f"direct slow headers exceeded budget: {direct_slow_header_elapsed:.4f}s",
    )
    slow_header_server.shutdown()
    slow_header_server.server_close()

    # Restore healthy writer for subsequent browser accept.
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(bridge_ok)
    os.environ["EA010_HERMES_UI_PROFILE"] = str(ui_profile)
    return {
        "ready_tracker": snap["tracker"],
        "bad_writer_ready": snap_bad["tracker"].get("ready"),
        "missing_writer_ready": snap_missing["tracker"].get("ready"),
        "redirect_ready": snap_redir["tracker"].get("ready"),
        "object_ready": snap_obj["tracker"].get("ready"),
        "invalid_profile_ready": snap_err["tracker"].get("ready"),
        "malformed_api_ready": snap_mal["tracker"].get("ready"),
        "nonloop_ready": snap_nonloop["tracker"].get("ready"),
        "insecure_token_ready": snap_insecure["tracker"].get("ready"),
        "env_proxy_ready": False,
        "env_proxy_hits": 0,
        "slow_body_ready": False,
        "slow_body_elapsed_s": round(slow_body_elapsed, 4),
        "slow_header_ready": False,
        "slow_header_elapsed_s": round(slow_header_elapsed, 4),
        "slow_timeout_s": slow_timeout_s,
        "synthetic_project_id": project_id,
        "browser_origin": browser_a,
        "bridge_config": str(bridge_ok),
        "ui_profile": str(ui_profile),
        "token_path": str(token_path),
        "vikunja_origin": vikunja_origin,
    }


def _seed_core(plugin: Any, settings: dict[str, Any], stand: IsolatedStand) -> dict[str, Any]:
    runtime = Path(settings["runtime"])
    core = plugin.core(settings)
    mid = "b" * 32
    legacy_mid = "a" * 32
    # Unbroken 313+ char title for I9/390px confirmation wrap (no soft hyphen points).
    title = "EA010-SYNTHETIC-" + ("я" * 297)
    legacy_title = stand.fixture_name("ux-legacy")
    text = f"# Synthetic fixture\nOwner: {title}\n"
    legacy_text = f"# Legacy\nOwner: {legacy_title}\n"
    name = f"transcript-{mid}.md"
    legacy_name = f"transcript-{legacy_mid}.md"
    (runtime / name).write_text(text, encoding="utf-8")
    os.chmod(runtime / name, 0o600)
    (runtime / legacy_name).write_text(legacy_text, encoding="utf-8")
    os.chmod(runtime / legacy_name, 0o600)
    version = hashlib.sha256(text.encode()).hexdigest()
    legacy_version = hashlib.sha256(legacy_text.encode()).hexdigest()
    registry = {
        "schema": 1,
        "scope": "owned_and_invited",
        "first_day": "2026-10-30",
        "last_complete_day": "2026-11-05",
        "meetings": {
            mid: {
                "first_seen": "2026-11-05T12:00:00+03:00",
                "status": "saved",
                "transcript": {"sha256": version, "file": name},
            },
            legacy_mid: {
                "first_seen": "2026-11-05T12:00:00+03:00",
                "status": "saved",
                "transcript": {"sha256": legacy_version, "file": legacy_name},
            },
        },
    }
    (runtime / ".collector-state.json").write_text(json.dumps(registry), encoding="utf-8")
    os.chmod(runtime / ".collector-state.json", 0o600)
    core.ingest(
        mid,
        version,
        {
            "summary": "Synthetic discussion context for UX",
            "decisions": [],
            "unresolved": [],
            "actions": [
                {
                    "text": title,
                    "kind": "self_commitment",
                    "owner": "Owner",
                    "speaker": "Owner",
                    "due_hint": "в пятницу",
                    "line_start": 2,
                    "line_end": 2,
                    "quote": f"Owner: {title}",
                }
            ],
        },
    )
    core.ingest(
        legacy_mid,
        legacy_version,
        {
            "summary": "Legacy",
            "decisions": [],
            "unresolved": [],
            "actions": [
                {
                    "text": legacy_title,
                    "kind": "self_commitment",
                    "owner": "Owner",
                    "speaker": "Owner",
                    "due_hint": None,
                    "line_start": 2,
                    "line_end": 2,
                    "quote": f"Owner: {legacy_title}",
                }
            ],
        },
    )
    legacy_prop = next(p for p in core.snapshot()["proposals"] if p["meeting_id"] == legacy_mid)
    core.decision(legacy_prop["id"], legacy_prop["revision"], "accept", title=legacy_title, due=None)

    checked_at = "2026-11-05T12:00:00+03:00"
    analysis_home = runtime / ".analysis"
    analysis_home.mkdir(mode=0o700, exist_ok=True)
    (analysis_home / "calendar-metadata.json").write_text(
        json.dumps(
            {
                "schema": "ea009-calendar/v1",
                "checked_at": checked_at,
                "meetings": {
                    mid: {
                        "title": "Synthetic meeting title",
                        "starts_at": "2026-11-05T11:00:00Z",
                        "ends_at": "2026-11-05T12:00:00Z",
                        "timezone": "Europe/Moscow",
                        "verification_status": "verified",
                        "checked_at": checked_at,
                        "time_basis": "calendar",
                        "matched_by": "meeting_and_activity_ids",
                    },
                    legacy_mid: {
                        "title": "Legacy meeting",
                        "starts_at": "2026-11-05T11:00:00Z",
                        "ends_at": "2026-11-05T12:00:00Z",
                        "timezone": "Europe/Moscow",
                        "verification_status": "verified",
                        "checked_at": checked_at,
                        "time_basis": "calendar",
                        "matched_by": "meeting_and_activity_ids",
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.chmod(analysis_home / "calendar-metadata.json", 0o600)

    proposal = next(p for p in core.snapshot()["proposals"] if p["meeting_id"] == mid)
    _assert("source_url" not in proposal, "production-shaped pending must omit invented source_url")
    tasks_dir = Path(settings["tasks"])
    legacy_files = list(tasks_dir.glob("*.md"))
    _assert(len(legacy_files) == 1, "legacy Markdown missing")
    return {
        "core": core,
        "proposal": proposal,
        "legacy_sha_before": sha256_file(legacy_files[0]),
        "legacy_file": str(legacy_files[0]),
        "meeting_id": mid,
        "version": proposal["version"],
    }


def _start_uvicorn(app: Any, host: str, port: int) -> tuple[Any, threading.Thread]:
    import uvicorn

    config = uvicorn.Config(app, host=host, port=port, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)
    else:
        raise StandError("uvicorn failed to bind")
    return server, thread


def _run_browser_and_accept(
    *,
    patched_tree: Path,
    stand: IsolatedStand,
    readiness: dict[str, Any],
    plugin: Any,
    auth_header: str,
    owner_password: str,
) -> dict[str, Any]:
    _assert(PLAYWRIGHT_MODULE.is_dir(), f"playwright module missing: {PLAYWRIGHT_MODULE}")
    _assert(CHROMIUM.is_file(), f"chromium missing: {CHROMIUM}")
    _assert(sha256_file(CHROMIUM) == CHROMIUM_SHA, "chromium sha mismatch")

    from fastapi import FastAPI, Request as FastAPIRequest
    from fastapi.responses import FileResponse, HTMLResponse

    settings = {
        "owner": OWNER,
        "runtime": str(stand.run_dir / "hermes-runtime"),
        "tasks": str(stand.run_dir / "hermes-tasks"),
        "policy": str(stand.run_dir / "policy.json"),
    }
    Path(settings["runtime"]).mkdir(mode=0o700, exist_ok=True)
    Path(settings["tasks"]).mkdir(mode=0o700, exist_ok=True)
    Path(settings["policy"]).write_text(
        json.dumps({"text_analysis": {"preferred_agent": "codex", "preferred_model": "fixture"}}),
        encoding="utf-8",
    )
    os.chmod(Path(settings["policy"]), 0o600)
    settings_path = stand.run_dir / "plugin-settings.json"
    settings_path.write_text(json.dumps(settings), encoding="utf-8")
    os.chmod(settings_path, 0o600)
    plugin.SETTINGS_PATH = settings_path
    plugin.CORE_PATH = Path(patched_tree) / "scripts/ea_core.py"
    plugin._module = None

    # Bridge already set by readiness; ensure journal path exists.
    journal_path = Path(json.loads(Path(readiness["bridge_config"]).read_text())["journal_path"])
    journal_path.parent.mkdir(mode=0o700, exist_ok=True)

    seeded = _seed_core(plugin, settings, stand)
    proposal = seeded["proposal"]
    proposal_id = proposal["id"]
    version = seeded["version"]
    meeting_id = seeded["meeting_id"]

    route_a = _start_task_surface("A")
    route_b = _start_task_surface("B")
    # Point task_url_template at route A; browser_origin also A home.
    bridge_raw = json.loads(Path(readiness["bridge_config"]).read_text(encoding="utf-8"))
    bridge_raw["task_url_template"] = route_a[1] + "/tasks/{id}"
    # Keep api_origin on real Vikunja writer.
    Path(readiness["bridge_config"]).write_text(json.dumps(bridge_raw), encoding="utf-8")
    os.chmod(Path(readiness["bridge_config"]), 0o600)
    ui_profile = {
        "browser_origin": route_a[1],
        "timeout_s": 2.0,
    }
    Path(readiness["ui_profile"]).write_text(json.dumps(ui_profile), encoding="utf-8")

    hermes_port = int(urlparse(HERMES_ORIGIN).port or 0)
    _assert(hermes_port > 0, "hermes port")
    # Bind exact HERMES_ORIGIN host/port for Origin checks.
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    request_log: list[dict[str, Any]] = []

    @app.middleware("http")
    async def authenticate(request: FastAPIRequest, call_next):  # noqa: ANN001
        request_log.append({"method": request.method, "path": request.url.path})
        if hmac.compare_digest(request.headers.get("authorization", ""), auth_header):
            request.state.session = SimpleNamespace(user_id=OWNER)
        return await call_next(request)

    harness_dir = stand.run_dir / "browser-harness"
    harness_dir.mkdir(parents=True, exist_ok=True)
    react_bundle = _bundle_react(harness_dir)
    ui_js = Path(patched_tree) / "plugins/personal-tasks/dashboard/dist/index.js"
    ui_css = Path(patched_tree) / "plugins/personal-tasks/dashboard/dist/style.css"

    app.include_router(plugin.router, prefix="/api/plugins/personal-tasks")

    @app.get("/")
    def index() -> HTMLResponse:
        html = f"""<!doctype html><html><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<link rel='stylesheet' href='/style.css'></head>
<body><div id='root'></div>
<script src='/react-bundle.js'></script>
<script>
window.__HERMES_PLUGIN_SDK__={{
  React: window.React,
  fetchJSON: async (url, opts={{}}) => {{
    const headers = Object.assign({{}}, (opts && opts.headers) || {{}}, {{
      Authorization: {json.dumps(auth_header)},
      Origin: {json.dumps(HERMES_ORIGIN)}
    }});
    const r = await fetch(url, Object.assign({{}}, opts, {{headers}}));
    if (!r.ok) throw new Error('http '+r.status);
    return r.json();
  }}
}};
window.__HERMES_PLUGINS__={{register(name,Comp){{window.__PT_COMP__=Comp;}}}};
</script>
<script src='/index.js'></script>
<script>window.ReactDOM.createRoot(document.getElementById('root')).render(window.React.createElement(window.__PT_COMP__));</script>
</body></html>"""
        return HTMLResponse(html)

    @app.get("/react-bundle.js")
    def react_js() -> FileResponse:
        return FileResponse(react_bundle, media_type="application/javascript")

    @app.get("/index.js")
    def index_js() -> FileResponse:
        return FileResponse(ui_js, media_type="application/javascript")

    @app.get("/style.css")
    def style_css() -> FileResponse:
        return FileResponse(ui_css, media_type="text/css")

    @app.post("/__fixture/mark-unknown")
    def mark_unknown() -> dict[str, Any]:
        """Flip durable outbox row to unknown for reconcile UI (no new accept/create)."""
        from fastapi import HTTPException
        from vikunja_local.hermes_bridge import HermesOutboxStore

        try:
            core = plugin.core(settings)
            live = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal_id)
            tracker = live.get("tracker") or {}
            event_id = tracker.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise HTTPException(500, f"missing event_id on proposal tracker: {tracker}")
            outbox_path = Path(core._vikunja_outbox_path())
            if not outbox_path.is_file():
                raise HTTPException(500, f"outbox missing at {outbox_path}; tracker={tracker}")
            store = HermesOutboxStore(outbox_path)

            def apply(data: dict[str, Any]) -> None:
                current = data["by_event"].get(event_id)
                if current is None:
                    raise KeyError(event_id)
                current["state"] = "unknown"
                current["task_id"] = None
                current["url"] = None
                data["by_proposal"][current["proposal_id"]] = current
                data["by_event"][event_id] = current

            store.mutate(apply)
            return {"ok": True, "event_id": event_id, "outbox": str(outbox_path)}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(500, f"mark-unknown failed: {type(exc).__name__}: {exc}") from exc

    server, thread = _start_uvicorn(app, "127.0.0.1", hermes_port)
    driver = harness_dir / "playwright_driver.mjs"
    result_path = harness_dir / "browser-result.json"
    driver.write_text(
        f"""
import {{ createRequire }} from 'node:module';
import {{ writeFileSync }} from 'node:fs';
const require = createRequire(import.meta.url);
const {{ chromium }} = require('playwright');

const origin = {json.dumps(HERMES_ORIGIN)};
const proposalId = {json.dumps(proposal_id)};
const meetingId = {json.dumps(meeting_id)};
const version = {json.dumps(version)};
const routeA = {json.dumps(route_a[1])};
const routeB = {json.dumps(route_b[1])};
const executablePath = {json.dumps(str(CHROMIUM))};
const resultPath = {json.dumps(str(result_path))};

function assert(cond, msg) {{ if (!cond) throw new Error(msg); }}

const browser = await chromium.launch({{ executablePath, headless: true, args: ['--no-sandbox'] }});
const page = await browser.newPage({{ viewport: {{ width: 390, height: 844 }} }});
const out = {{ ok: false }};
try {{
  assert(page.viewportSize().width === 390, 'viewport width');
  await page.goto(origin + '/', {{ waitUntil: 'networkidle' }});
  await page.waitForSelector('text=Встречи');

  // Tabs preserved.
  for (const label of ['Встречи', 'Новые предложения', 'Необработанные']) {{
    assert(await page.getByRole('button', {{ name: label }}).count() >= 1, 'missing tab ' + label);
  }}

  const personal = page.getByTestId('personal-tasks-link');
  assert(await personal.count() === 1, 'personal tasks link missing');
  assert(await personal.getAttribute('target') === '_blank', 'personal tasks must open new tab');
  const personalHref = (await personal.getAttribute('href')) || '';
  assert(personalHref.replace(/\\/$/, '') === routeA, 'ui_url ' + personalHref);

  // Navigate personal-tasks surface (route A home).
  const personalPage = await browser.newPage();
  await personalPage.goto(routeA + '/', {{ waitUntil: 'domcontentloaded' }});
  assert(await personalPage.getByTestId('vikunja-home').count() === 1, 'route A home missing');
  await personalPage.close();

  await page.getByRole('button', {{ name: 'Новые предложения' }}).click();
  await page.waitForSelector('#pt-proposal-' + proposalId);
  const cardText = await page.locator('#pt-proposal-' + proposalId).innerText();
  assert(cardText.includes('EA010-SYNTHETIC'), 'title missing');

  const addBtn = page.getByRole('button', {{ name: 'Добавить в мои задачи' }});
  await addBtn.focus();
  await addBtn.click();
  const dialog = page.locator('section.mt-confirm[role="dialog"]');
  await dialog.waitFor();
  const body = await dialog.innerText();
  for (const needle of ['Источник:', 'Synthetic meeting title', 'Обсуждали:', 'Срок из речи, не подтверждён', 'Принять и передать в Vikunja', 'Отмена', 'Открыть предложение']) {{
    assert(body.includes(needle), 'dialog missing ' + needle);
  }}
  const sourceLink = dialog.getByRole('link', {{ name: 'Открыть предложение ↗' }});
  assert(await sourceLink.count() === 1, 'source link missing for production-shaped proposal');
  const sourceHref = await sourceLink.getAttribute('href');
  assert((sourceHref || '').includes('#pt-proposal-' + proposalId), 'source deep-link ' + sourceHref);

  // I9 / UX §6: confirmation modal/title/context must not require horizontal scroll at 390px.
  const overflow = await page.evaluate(() => {{
    const measure = (el) => {{
      if (!el) return null;
      const cs = getComputedStyle(el);
      return {{
        clientWidth: el.clientWidth,
        scrollWidth: el.scrollWidth,
        overflowWrap: cs.overflowWrap || cs.wordWrap,
      }};
    }};
    return {{
      dialog: measure(document.querySelector('section.mt-confirm[role="dialog"]')),
      title: measure(document.querySelector('[data-testid="pt-confirm-title"]')),
      source: measure(document.querySelector('[data-testid="pt-confirm-source"]')),
      context: measure(document.querySelector('[data-testid="pt-confirm-context"]')),
    }};
  }});
  for (const [name, box] of Object.entries(overflow)) {{
    assert(box, 'missing overflow target ' + name);
    assert(box.scrollWidth <= box.clientWidth + 1, name + ' horizontal overflow ' + JSON.stringify(box));
  }}
  assert(overflow.title && overflow.title.overflowWrap === 'anywhere', 'title overflowWrap ' + JSON.stringify(overflow.title));
  out.dialog_overflow = overflow;

  // Focus lands in dialog; Escape returns focus to opener.
  const activeTag = await page.evaluate(() => document.activeElement && document.activeElement.tagName);
  assert(activeTag === 'INPUT' || activeTag === 'TEXTAREA' || activeTag === 'BUTTON', 'dialog focus ' + activeTag);
  await page.keyboard.press('Escape');
  await page.waitForSelector('text=Принять и передать в Vikunja', {{ state: 'detached' }});
  const focusedAgain = await page.evaluate(() => document.activeElement && (document.activeElement.innerText || document.activeElement.textContent || ''));
  assert((focusedAgain || '').includes('Добавить в мои задачи'), 'focus return failed: ' + focusedAgain);

  await addBtn.click();
  await dialog.waitFor();
  await dialog.getByRole('button', {{ name: 'Отмена' }}).click();

  await addBtn.click();
  await dialog.waitFor();
  await dialog.getByLabel('Подтверждаю отсутствие срока').check();
  await dialog.locator('input[type="date"]').fill('2026-11-14');
  const submit = dialog.getByRole('button', {{ name: 'Принять и передать в Vikunja' }});
  assert(!(await submit.isEnabled()), 'changing due must clear confirmation');
  await dialog.getByLabel('Подтверждаю выбранный срок').check();
  assert(await submit.isEnabled(), 'submit should enable');

  // Busy / double-click: first click starts write; second must not create another POST while busy.
  await Promise.all([
    page.waitForResponse(r => r.url().includes('/decision') && r.request().method() === 'POST'),
    submit.click(),
  ]);
  // Attempt second click quickly if still visible (may already be closed).
  if (await submit.count()) {{
    try {{ await submit.click({{ timeout: 200 }}); }} catch (_) {{}}
  }}
  await page.waitForSelector('text=Принять и передать в Vikunja', {{ state: 'detached' }});

  async function openMeetingProposals() {{
    await page.getByRole('button', {{ name: 'Встречи' }}).click();
    await page.locator('#pt-meeting-' + meetingId).getByRole('button', {{ name: 'Открыть саммари' }}).click();
    await page.getByRole('button', {{ name: 'Предложения', exact: true }}).click();
    await page.waitForSelector('#pt-proposal-' + proposalId);
  }}
  await openMeetingProposals();
  await page.waitForSelector('[data-delivery="delivered"], [data-delivery="accepted_pending"], [data-delivery="unknown"]', {{ timeout: 20000 }});
  // Prefer delivered; if pending briefly, refresh once.
  if (await page.locator('[data-delivery="delivered"]').count() === 0) {{
    await page.keyboard.press('Escape');
    await page.getByRole('button', {{ name: 'Обновить' }}).click();
    await page.waitForTimeout(500);
    await openMeetingProposals();
    await page.waitForSelector('[data-delivery="delivered"]', {{ timeout: 20000 }});
  }}
  assert(await page.getByRole('button', {{ name: 'Добавить в мои задачи' }}).count() === 0, 'accept button should disappear');
  const openLink = page.getByRole('link', {{ name: 'Открыть задачу ↗' }});
  assert(await openLink.count() >= 1, 'task link missing');
  const taskHref = await openLink.first().getAttribute('href');
  assert((taskHref || '').startsWith(routeA + '/tasks/'), 'task url route A ' + taskHref);
  const taskId = (taskHref || '').split('/').pop();
  assert(/^[0-9]+$/.test(taskId), 'synthetic task id ' + taskId);

  // Navigate real task link (route A) and matching ID on route B.
  const taskPageA = await browser.newPage();
  await taskPageA.goto(taskHref, {{ waitUntil: 'domcontentloaded' }});
  assert(await taskPageA.getByTestId('task-id').innerText() === taskId, 'route A id mismatch');
  assert(await taskPageA.getByTestId('route-label').innerText() === 'A', 'route A label');
  await taskPageA.close();
  const taskPageB = await browser.newPage();
  await taskPageB.goto(routeB + '/tasks/' + taskId, {{ waitUntil: 'domcontentloaded' }});
  assert(await taskPageB.getByTestId('task-id').innerText() === taskId, 'route B id mismatch');
  assert(await taskPageB.getByTestId('route-label').innerText() === 'B', 'route B label');
  await taskPageB.close();

  // Geometry at 390px: proposal card and prior confirmation dialog stay within viewport width.
  const box = await page.locator('#pt-proposal-' + proposalId).boundingBox();
  assert(box && box.width <= 390, 'proposal exceeds 390px: ' + JSON.stringify(box));
  assert(out.dialog_overflow && out.dialog_overflow.title, 'dialog overflow measurements missing');

  // Unknown via durable outbox mutation (authorized surface), not fabricated decision success.
  await page.keyboard.press('Escape');
  await page.waitForSelector('.mt-panel[role="dialog"]', {{ state: 'detached', timeout: 5000 }}).catch(() => {{}});
  const mark = await page.evaluate(async () => {{
    const r = await fetch('/__fixture/mark-unknown', {{ method: 'POST' }});
    const text = await r.text();
    let body = null;
    try {{ body = JSON.parse(text); }} catch (_) {{ body = text; }}
    return {{ status: r.status, body }};
  }});
  assert(mark.status === 200 && mark.body && mark.body.ok, 'mark-unknown failed ' + JSON.stringify(mark));
  await page.getByRole('button', {{ name: 'Обновить' }}).click();
  await page.waitForTimeout(400);
  await openMeetingProposals();
  await page.waitForSelector('[data-delivery="unknown"]');
  const recoverWait = page.waitForResponse(r => r.url().includes('/tracker/recover') && r.request().method() === 'POST');
  await page.getByRole('button', {{ name: 'Проверить передачу' }}).click();
  const recoverResp = await recoverWait;
  assert(recoverResp.ok(), 'recover HTTP ' + recoverResp.status());
  await page.waitForSelector('[data-delivery="delivered"]', {{ timeout: 20000 }});
  out.recovered_delivered = await page.locator('[data-delivery="delivered"]').count() >= 1;
  out.unknown_still = await page.locator('[data-delivery="unknown"]').count() >= 1;
  const openAfter = page.getByRole('link', {{ name: 'Открыть задачу ↗' }});
  assert(await openAfter.count() >= 1, 'task link missing after recover');
  const hrefAfter = await openAfter.first().getAttribute('href');
  assert(hrefAfter === taskHref, 'task href changed after recover: ' + hrefAfter);

  out.ok = true;
  out.proposal_id = proposalId;
  out.version = version;
  out.task_id = taskId;
  out.task_url_a = taskHref;
  out.task_url_b = routeB + '/tasks/' + taskId;
  out.viewport = page.viewportSize();
  out.proposal_box = box;
  out.source_href = sourceHref;
  writeFileSync(resultPath, JSON.stringify(out));
}} catch (err) {{
  writeFileSync(resultPath, JSON.stringify({{ ok: false, error: String(err), stack: err && err.stack }}));
  throw err;
}} finally {{
  await browser.close();
}}
""",
        encoding="utf-8",
    )

    try:
        proc = subprocess.run(
            ["node", str(driver)],
            capture_output=True,
            text=True,
            cwd=str(harness_dir),
            env={**os.environ, "NODE_PATH": str(PLAYWRIGHT_MODULE.parent)},
            timeout=180,
        )
        _assert(
            proc.returncode == 0,
            f"node playwright driver failed rc={proc.returncode}\nstdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}",
        )
        _assert(result_path.is_file(), "browser result missing")
        browser_result = json.loads(result_path.read_text(encoding="utf-8"))
        _assert(browser_result.get("ok") is True, f"browser result not ok: {browser_result}")

        # Decision posts must have gone through authorized backend (not fabricated 4242).
        decision_posts = [e for e in request_log if e["method"] == "POST" and "/decision" in e["path"]]
        _assert(len(decision_posts) == 1, f"expected one decision POST, got {decision_posts}")
        task_id = int(browser_result["task_id"])
        _assert(task_id != 4242, "fabricated task id 4242 is not admitted")

        # Legacy markdown unchanged.
        legacy_after = sha256_file(Path(seeded["legacy_file"]))
        _assert(legacy_after == seeded["legacy_sha_before"], "legacy Markdown mutated")

        # After mark-unknown + C5 0.3 recover, backlink restores without a new create.
        core = plugin.core(settings)
        live = next(p for p in core.snapshot()["proposals"] if p["id"] == proposal_id)
        tracker = live.get("tracker") or {}
        _assert(tracker.get("state") == "linked", f"expected linked after recover: {tracker}")
        _assert(isinstance(tracker.get("event_id"), str) and tracker["event_id"], "missing event_id")
        _assert(int(tracker.get("task_id")) == task_id, f"task_id drift after recover: {tracker}")
        _assert(browser_result.get("recovered_delivered") is True, "browser did not show delivered after recover")
        _assert(browser_result.get("unknown_still") is not True, "unknown must clear after successful recover")
        recover_posts = [e for e in request_log if e["method"] == "POST" and "/tracker/recover" in e["path"]]
        _assert(len(recover_posts) >= 1, f"expected recover POST, got {recover_posts}")

        recover_cases = _run_recover_api_cases(
            plugin=plugin,
            settings=settings,
            auth_header=auth_header,
            proposal_id=proposal_id,
            event_id=str(tracker["event_id"]),
            readiness=readiness,
            stand=stand,
        )

        return {
            "proposal_id": proposal_id,
            "meeting_id": meeting_id,
            "version": version,
            "task_id": task_id,
            "event_id": tracker["event_id"],
            "task_url_a": browser_result["task_url_a"],
            "task_url_b": browser_result["task_url_b"],
            "browser_result": browser_result,
            "legacy_sha_before": seeded["legacy_sha_before"],
            "legacy_sha_after": legacy_after,
            "decision_posts": len(decision_posts),
            "recover_posts": len(recover_posts),
            "request_log_tail": request_log[-20:],
            "recover_cases": recover_cases,
            "c5_0_3": True,
        }
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        route_a[0].shutdown()
        route_a[0].server_close()
        route_b[0].shutdown()
        route_b[0].server_close()


def _http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = Request(url, data=data, method=method, headers=headers or {})
    try:
        with urlopen(req, timeout=10) as resp:
            raw = resp.read()
            code = getattr(resp, "status", None) or resp.getcode()
    except Exception as exc:  # urllib.error.HTTPError is an HTTPResponse
        code = getattr(exc, "code", None)
        raw = getattr(exc, "read", lambda: b"")()
        if code is None:
            raise
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw else None
    except Exception:
        parsed = raw.decode("utf-8", errors="replace") if raw else None
    return int(code), parsed


def _seed_core_proposal_for_outbox(
    *,
    plugin: Any,
    stand: IsolatedStand,
    bridge_raw: dict[str, Any],
    proposal_id: str,
    event_id: str,
    outbox_src: Path,
    journal_path: Path,
    title: str,
    state_hint: str,
) -> Any:
    """Isolated Core with one accepted Vikunja proposal bound to a copied outbox row."""
    runtime = stand.run_dir / f"core-proj-{proposal_id}"
    tasks = stand.run_dir / f"core-proj-tasks-{proposal_id}"
    runtime.mkdir(mode=0o700, exist_ok=True)
    tasks.mkdir(mode=0o700, exist_ok=True)
    policy = stand.run_dir / f"core-proj-policy-{proposal_id}.json"
    policy.write_text(json.dumps({"text_analysis": {"preferred_agent": "codex"}}), encoding="utf-8")
    os.chmod(policy, 0o600)
    settings = {
        "owner": OWNER,
        "runtime": str(runtime),
        "tasks": str(tasks),
        "policy": str(policy),
    }
    mid = "b" * 32
    transcript_name = f"transcript-{mid}.md"
    transcript_text = f"# Synthetic\nOwner: {title}\n"
    (Path(runtime) / transcript_name).write_text(transcript_text, encoding="utf-8")
    os.chmod(Path(runtime) / transcript_name, 0o600)
    version = hashlib.sha256(transcript_text.encode()).hexdigest()
    (Path(runtime) / ".collector-state.json").write_text(
        json.dumps(
            {
                "schema": 1,
                "scope": "owned_and_invited",
                "first_day": "2026-10-30",
                "last_complete_day": "2026-11-05",
                "meetings": {
                    mid: {
                        "first_seen": "2026-11-05T12:00:00+03:00",
                        "status": "saved",
                        "transcript": {"sha256": version, "file": transcript_name},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    os.chmod(Path(runtime) / ".collector-state.json", 0o600)
    core = plugin.core(settings)
    # Ensure analysis home exists, then install outbox + accepted decision history.
    _ = core._state()
    outbox_dest = Path(core._vikunja_outbox_path())
    outbox_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(outbox_src, outbox_dest)
    os.chmod(outbox_dest, 0o600)
    with core._lock():
        s = core._state()
        s["proposals"][proposal_id] = {
            "id": proposal_id,
            "meeting_id": mid,
            "version": version,
            "revision": 2,
            "status": "accepted",
            "kind": "self_commitment",
            "text": title,
            "quote": f"Owner: {title}",
            "line_start": 2,
            "line_end": 2,
            "owner": "Owner",
            "speaker": "Owner",
            "decisions": [
                {
                    "action": "accept",
                    "at": "2026-11-12T10:00:00+00:00",
                    "note": "",
                    "source_version": version,
                    "task_id": None,
                    "due": None,
                    "tracker_target": "vikunja",
                    "event_id": event_id,
                }
            ],
        }
        core._save(s)
    cfg_path = stand.run_dir / f"core-proj-bridge-{proposal_id}.json"
    cfg_path.write_text(
        json.dumps(
            {
                **bridge_raw,
                "journal_path": str(journal_path),
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        ),
        encoding="utf-8",
    )
    os.chmod(cfg_path, 0o600)
    os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(cfg_path)
    projected = core.recover_vikunja_outbox(
        event_id=event_id,
        allow_first_http=False,
        proposal_id=proposal_id,
    )
    tracker = (projected or {}).get("tracker") or {}
    avail = tracker.get("availability") or {}
    _assert(isinstance(tracker.get("event_id"), str) and tracker["event_id"] == event_id, tracker)
    _assert(tracker.get("state") is not None, f"missing tracker state ({state_hint}): {tracker}")
    return projected, tracker, avail


def _run_core_recover_projections(
    *,
    plugin: Any,
    stand: IsolatedStand,
    bridge_raw: dict[str, Any],
    readiness: dict[str, Any],
    proposal_id: str,
    event_id: str,
    prep_event_id: str,
    prep_outbox: Path,
    prep_journal: Path,
    map_event_id: str,
    map_outbox: Path,
    map_journal: Path,
    prep_result: Any,
    map_result: Any,
) -> dict[str, Any]:
    """Core recover must surface bridge public_tracker for both no-first-PUT paths + linked miss."""
    prev_cfg = os.environ.get("EA010_HERMES_BRIDGE_CONFIG")
    try:
        # prepared_without_dispatch → pending/unknown + cannot_first_deliver reason.
        prep_projected, prep_tracker, prep_avail = _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw=bridge_raw,
            proposal_id="prop-nofirst-prep",
            event_id=prep_event_id,
            outbox_src=prep_outbox,
            journal_path=prep_journal,
            title="EA010-SYNTHETIC prepared",
            state_hint="pending",
        )
        _assert(prep_avail.get("reason") == "prepared_without_dispatch", f"core prep avail: {prep_tracker}")
        _assert(
            prep_avail.get("outcome") == "unknown" or prep_avail.get("status") == "unavailable",
            f"core prep outcome: {prep_tracker}",
        )
        _assert(prep_tracker.get("task_id") is None, f"core prep must not invent task_id: {prep_tracker}")
        # Bridge details carry cannot_first_deliver; public availability keeps reason.
        _assert(
            (prep_result.details or {}).get("cannot_first_deliver") is True,
            prep_result.details,
        )

        # no_source_mapping pending → same: diagnostic without delivered claim.
        map_projected, map_tracker, map_avail = _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw=bridge_raw,
            proposal_id="prop-nofirst-map",
            event_id=map_event_id,
            outbox_src=map_outbox,
            journal_path=map_journal,
            title="EA010-SYNTHETIC no-mapping",
            state_hint="unknown",
        )
        _assert(map_avail.get("reason") == "no_source_mapping", f"core map avail: {map_tracker}")
        _assert(
            map_avail.get("outcome") == "unknown" or map_avail.get("status") == "unavailable",
            f"core map outcome: {map_tracker}",
        )
        _assert(map_tracker.get("task_id") is None, f"core map must not invent task_id: {map_tracker}")
        _assert((map_result.details or {}).get("cannot_first_deliver") is True, map_result.details)

        # Linked historical identity with missing mapping: keep task_id/url, expose availability.
        live_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "hermes-runtime"),
                "tasks": str(stand.run_dir / "hermes-tasks"),
                "policy": str(stand.run_dir / "policy.json"),
            }
        )
        live = next(p for p in live_core.snapshot()["proposals"] if p["id"] == proposal_id)
        live_tracker = live.get("tracker") or {}
        _assert(live_tracker.get("state") == "linked", f"expected linked before nomap: {live_tracker}")
        hist_task = live_tracker.get("task_id")
        hist_url = live_tracker.get("url")
        _assert(hist_task is not None and hist_url, f"missing historical link: {live_tracker}")

        empty_journal = stand.run_dir / "recover-linked-nomap" / "journal.sqlite3"
        empty_journal.parent.mkdir(mode=0o700, exist_ok=True)
        from vikunja_local.journal import IntegrationJournal, new_issuer_id

        j = IntegrationJournal(empty_journal)
        j.open()
        try:
            j.ensure_issuer(new_issuer_id())
        finally:
            j.close()
        live_bridge_path = Path(readiness["bridge_config"])
        live_bridge = json.loads(live_bridge_path.read_text(encoding="utf-8"))
        live_bridge_path.write_text(
            json.dumps({**live_bridge, "journal_path": str(empty_journal)}),
            encoding="utf-8",
        )
        os.chmod(live_bridge_path, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(live_bridge_path)
        linked_projected = live_core.recover_vikunja_outbox(
            event_id=event_id,
            allow_first_http=False,
            proposal_id=proposal_id,
        )
        linked_tracker = (linked_projected or {}).get("tracker") or {}
        linked_avail = linked_tracker.get("availability") or {}
        _assert(linked_tracker.get("state") == "linked", f"linked identity lost: {linked_tracker}")
        _assert(linked_tracker.get("task_id") == hist_task, f"task_id lost: {linked_tracker}")
        _assert(linked_tracker.get("url") == hist_url, f"url lost: {linked_tracker}")
        _assert(
            linked_avail.get("reason") == "no_source_mapping",
            f"linked missing mapping must expose diagnostic: {linked_tracker}",
        )
        _assert(
            linked_avail.get("outcome") == "unknown" or linked_avail.get("status") == "unavailable",
            f"linked missing mapping must not look healthy: {linked_tracker}",
        )
        # R1: immediate Core.snapshot() must retain the failed-check diagnostic.
        snap_linked = next(p for p in live_core.snapshot()["proposals"] if p["id"] == proposal_id)
        snap_linked_avail = (snap_linked.get("tracker") or {}).get("availability") or {}
        _assert(
            snap_linked_avail.get("reason") == "no_source_mapping",
            f"snapshot dropped linked no_source_mapping after recover: {snap_linked.get('tracker')}",
        )
        _assert(
            snap_linked_avail.get("outcome") == "unknown"
            or snap_linked_avail.get("status") == "unavailable",
            f"snapshot must not look healthy after failed recover: {snap_linked.get('tracker')}",
        )

        # Same retention for pending prepared / no-mapping recover paths.
        prep_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-nofirst-prep"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-nofirst-prep"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-nofirst-prep.json"),
            }
        )
        snap_prep = next(p for p in prep_core.snapshot()["proposals"] if p["id"] == "prop-nofirst-prep")
        snap_prep_avail = (snap_prep.get("tracker") or {}).get("availability") or {}
        _assert(
            snap_prep_avail.get("reason") == "prepared_without_dispatch",
            f"snapshot dropped prepared_without_dispatch: {snap_prep.get('tracker')}",
        )
        map_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-nofirst-map"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-nofirst-map"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-nofirst-map.json"),
            }
        )
        snap_map = next(p for p in map_core.snapshot()["proposals"] if p["id"] == "prop-nofirst-map")
        snap_map_avail = (snap_map.get("tracker") or {}).get("availability") or {}
        _assert(
            snap_map_avail.get("reason") == "no_source_mapping",
            f"snapshot dropped pending no_source_mapping: {snap_map.get('tracker')}",
        )

        # Restore live bridge journal for any later observers.
        live_bridge_path.write_text(json.dumps(live_bridge), encoding="utf-8")
        os.chmod(live_bridge_path, 0o600)

        return {
            "prepared_without_dispatch": {
                "state": prep_tracker.get("state"),
                "availability": prep_avail,
                "snapshot_availability": snap_prep_avail,
            },
            "no_source_mapping": {
                "state": map_tracker.get("state"),
                "availability": map_avail,
                "snapshot_availability": snap_map_avail,
            },
            "linked_missing_mapping": {
                "state": linked_tracker.get("state"),
                "task_id": linked_tracker.get("task_id"),
                "url": linked_tracker.get("url"),
                "availability": linked_avail,
                "snapshot_availability": snap_linked_avail,
            },
        }
    finally:
        if prev_cfg is not None:
            os.environ["EA010_HERMES_BRIDGE_CONFIG"] = prev_cfg
        else:
            os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(Path(readiness["bridge_config"]))


def _run_recover_api_cases(
    *,
    plugin: Any,
    settings: dict[str, Any],
    auth_header: str,
    proposal_id: str,
    event_id: str,
    readiness: dict[str, Any],
    stand: IsolatedStand,
) -> dict[str, Any]:
    """C5 0.3: Origin/owner/wrong-event HTTP checks + no-first-PUT bridge branches."""
    from vikunja_local.hermes_bridge import (
        BridgeConfig,
        HermesBridge,
        HermesOutboxStore,
        meeting_user_portion,
    )
    from vikunja_local.journal import IntegrationJournal, new_issuer_id

    base = HERMES_ORIGIN.rstrip("/") + "/api/plugins/personal-tasks"
    recover_url = f"{base}/proposals/{proposal_id}/tracker/recover"
    owner_headers = {
        "Authorization": auth_header,
        "Origin": HERMES_ORIGIN,
        "Content-Type": "application/json",
    }

    # Already-linked recover is idempotent (live check, no new acceptance).
    ok_code, ok_body = _http_json(
        recover_url, method="POST", headers=owner_headers, body={"event_id": event_id}
    )
    _assert(ok_code == 200, f"linked recover expected 200, got {ok_code} {ok_body}")
    _assert(
        isinstance(ok_body, dict)
        and isinstance(ok_body.get("proposal"), dict)
        and (ok_body["proposal"].get("tracker") or {}).get("state") == "linked",
        f"linked recover projection: {ok_body}",
    )

    wrong_event = "00000000-0000-4000-8000-000000000099"
    bad_event_code, _ = _http_json(
        recover_url, method="POST", headers=owner_headers, body={"event_id": wrong_event}
    )
    _assert(bad_event_code == 409, f"wrong event expected 409, got {bad_event_code}")

    no_origin_headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json",
    }
    no_origin_code, _ = _http_json(
        recover_url, method="POST", headers=no_origin_headers, body={"event_id": event_id}
    )
    _assert(no_origin_code == 403, f"missing Origin expected 403, got {no_origin_code}")

    cross_headers = {
        **owner_headers,
        "Origin": "http://127.0.0.1:9",
        "sec-fetch-site": "cross-site",
    }
    cross_code, _ = _http_json(
        recover_url, method="POST", headers=cross_headers, body={"event_id": event_id}
    )
    _assert(cross_code == 403, f"cross-site expected 403, got {cross_code}")

    unauth_code, _ = _http_json(
        recover_url,
        method="POST",
        headers={"Origin": HERMES_ORIGIN, "Content-Type": "application/json"},
        body={"event_id": event_id},
    )
    _assert(unauth_code in {401, 403}, f"unauth expected 401/403, got {unauth_code}")

    # Bridge-level: allow_first_http=False must not first-deliver.
    from vikunja_local.api import Outcome, TaskResult

    class _FakeClient:
        def __init__(self, project_id: int) -> None:
            self.project_id = project_id
            self.create_calls = 0

        def create(self, **kwargs: Any) -> TaskResult:
            self.create_calls += 1
            raise AssertionError("UI recover must not create")

        def read(self, task_id: int) -> TaskResult:
            return TaskResult(outcome=Outcome.VALIDATION_ERROR, http_status=404, details={})

        def list_project_tasks(self) -> TaskResult:
            return TaskResult(
                outcome=Outcome.CONFIRMED,
                details={"items": [], "page_count": 1},
                http_status=200,
            )

        def find_by_marker(self, marker: str) -> TaskResult:
            return TaskResult(outcome=Outcome.EMPTY, details={"marker": marker})

    probe_root = stand.run_dir / "recover-nofirst"
    probe_root.mkdir(mode=0o700, exist_ok=True)
    journal_path = probe_root / "journal.sqlite3"
    outbox_path = probe_root / "outbox.json"
    bridge_raw = json.loads(Path(readiness["bridge_config"]).read_text(encoding="utf-8"))
    cfg = BridgeConfig.from_mapping(
        {
            **bridge_raw,
            "journal_path": str(journal_path),
            "task_url_template": bridge_raw.get("task_url_template")
            or "http://127.0.0.1/tasks/{id}",
        }
    )
    store = HermesOutboxStore(outbox_path)
    bridge = HermesBridge(cfg, store)
    fake = _FakeClient(int(bridge_raw["project_id"]))
    bridge._client = lambda: fake  # type: ignore[method-assign]
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        journal.ensure_issuer(new_issuer_id())
    finally:
        journal.close()

    def _portion(proposal_id: str, version: str) -> str:
        return meeting_user_portion(
            proposal_id=proposal_id,
            meeting_id="m-nofirst",
            meeting_title="Synthetic",
            meeting_when="2026-11-12T10:00:00+03:00 (Europe/Moscow)",
            quote="ea010-synth",
            version=version,
            source_url=f"{HERMES_ORIGIN}/#pt-proposal-{proposal_id}",
        )

    # prepared_without_dispatch: crash before HTTP then UI recover must not create.
    version_prep = "e" * 64
    row_prep = bridge.register_acceptance(
        proposal_id="prop-nofirst-prep",
        title="EA010-SYNTHETIC prepared",
        due=None,
        source_version=version_prep,
        user_portion=_portion("prop-nofirst-prep", version_prep),
    )

    def crash(point: str) -> None:
        if point == "before_http":
            raise RuntimeError("prepared_only")

    bridge._crash = crash  # type: ignore[attr-defined]
    try:
        try:
            bridge.deliver(row_prep.event_id)
        except RuntimeError:
            pass
    finally:
        bridge._crash = None  # type: ignore[attr-defined]
    _assert(fake.create_calls == 0, "crash-before-http must not create")
    prep_result = bridge.recover(row_prep.event_id, allow_first_http=False)
    _assert(not prep_result.ok, f"prepared UI recover must not confirm: {prep_result.details}")
    _assert(prep_result.details.get("cannot_first_deliver") is True, prep_result.details)
    _assert(prep_result.details.get("reason") == "prepared_without_dispatch", prep_result.details)
    _assert(fake.create_calls == 0, "UI recover must not PUT on prepared_without_dispatch")

    # no_source_mapping: copy durable outbox into a fresh journal with no source map.
    from vikunja_local.hermes_bridge import OUTBOX_UNKNOWN

    version_map = "f" * 64
    row_map = bridge.register_acceptance(
        proposal_id="prop-nofirst-map",
        title="EA010-SYNTHETIC no-mapping",
        due=None,
        source_version=version_map,
        user_portion=_portion("prop-nofirst-map", version_map),
    )

    def mark_unknown(data: dict[str, Any]) -> None:
        current = data["by_event"][row_map.event_id]
        current["state"] = OUTBOX_UNKNOWN
        current["task_id"] = None
        current["url"] = None
        current["operation_id"] = None
        data["by_proposal"][current["proposal_id"]] = current
        data["by_event"][row_map.event_id] = current

    store.mutate(mark_unknown)
    orphan_root = stand.run_dir / "recover-nofirst-orphan"
    orphan_root.mkdir(mode=0o700, exist_ok=True)
    orphan_journal = orphan_root / "journal.sqlite3"
    orphan_outbox = orphan_root / "outbox.json"
    shutil.copy2(outbox_path, orphan_outbox)
    orphan_cfg = BridgeConfig.from_mapping(
        {
            **bridge_raw,
            "journal_path": str(orphan_journal),
            "task_url_template": bridge_raw.get("task_url_template")
            or "http://127.0.0.1/tasks/{id}",
        }
    )
    orphan_store = HermesOutboxStore(orphan_outbox)
    orphan_bridge = HermesBridge(orphan_cfg, orphan_store)
    orphan_fake = _FakeClient(int(bridge_raw["project_id"]))
    orphan_bridge._client = lambda: orphan_fake  # type: ignore[method-assign]
    journal = IntegrationJournal(orphan_journal)
    journal.open()
    try:
        journal.ensure_issuer(new_issuer_id())
    finally:
        journal.close()
    map_result = orphan_bridge.recover(row_map.event_id, allow_first_http=False)
    _assert(not map_result.ok, f"no-mapping UI recover must not confirm: {map_result.details}")
    _assert(map_result.details.get("cannot_first_deliver") is True, map_result.details)
    _assert(map_result.details.get("reason") == "no_source_mapping", map_result.details)
    _assert(orphan_fake.create_calls == 0, "UI recover must not PUT on no_source_mapping")
    _assert(fake.create_calls == 0, "prepared probe create_calls must stay 0")

    # Core/router projection (R8): preserve public_tracker / cannot_first_deliver diagnostics.
    core_proj = _run_core_recover_projections(
        plugin=plugin,
        stand=stand,
        bridge_raw=bridge_raw,
        readiness=readiness,
        proposal_id=proposal_id,
        event_id=event_id,
        prep_event_id=row_prep.event_id,
        prep_outbox=outbox_path,
        prep_journal=journal_path,
        map_event_id=row_map.event_id,
        map_outbox=orphan_outbox,
        map_journal=orphan_journal,
        prep_result=prep_result,
        map_result=map_result,
    )

    persistence = _run_nofirst_resume_and_denied_snapshots(
        plugin=plugin,
        stand=stand,
        bridge_raw=bridge_raw,
        readiness=readiness,
    )

    return {
        "linked_recover_http": ok_code,
        "wrong_event": bad_event_code,
        "missing_origin": no_origin_code,
        "cross_site": cross_code,
        "unauth": unauth_code,
        "prepared_without_dispatch": {
            "create_calls": fake.create_calls,
            "details": dict(prep_result.details or {}),
        },
        "no_source_mapping": {
            "create_calls": orphan_fake.create_calls,
            "details": dict(map_result.details or {}),
        },
        "core_projections": core_proj,
        "persistence": persistence,
    }


def _run_nofirst_resume_and_denied_snapshots(
    *,
    plugin: Any,
    stand: IsolatedStand,
    bridge_raw: dict[str, Any],
    readiness: dict[str, Any],
) -> dict[str, Any]:
    """R1 clear diagnostics after successful resume; R2 retain denied/unavailable in Core.snapshot."""
    from vikunja_local.api import Outcome
    from vikunja_local.hermes_bridge import (
        BridgeConfig,
        HermesBridge,
        HermesOutboxStore,
        OUTBOX_UNKNOWN,
        meeting_user_portion,
        tracker_availability_projection,
    )
    from vikunja_local.journal import IntegrationJournal, new_issuer_id

    def _portion(proposal_id: str, version: str) -> str:
        return meeting_user_portion(
            proposal_id=proposal_id,
            meeting_id="m-persist",
            meeting_title="Synthetic persist",
            meeting_when="2026-11-12T10:00:00+03:00 (Europe/Moscow)",
            quote="ea010-synth-persist",
            version=version,
            source_url=f"{HERMES_ORIGIN}/#pt-proposal-{proposal_id}",
        )

    prev_cfg = os.environ.get("EA010_HERMES_BRIDGE_CONFIG")
    try:
        # --- R1 prepared_without_dispatch → nofirst → default recover → snapshot linked ---
        prep_root = stand.run_dir / "persist-prep-resume"
        prep_root.mkdir(mode=0o700, exist_ok=True)
        prep_journal = prep_root / "journal.sqlite3"
        prep_outbox = prep_root / "outbox.json"
        prep_cfg = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(prep_journal),
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        )
        prep_store = HermesOutboxStore(prep_outbox)
        prep_bridge = HermesBridge(prep_cfg, prep_store)
        journal = IntegrationJournal(prep_journal)
        journal.open()
        try:
            journal.ensure_issuer(new_issuer_id())
        finally:
            journal.close()
        version_prep = "a" * 64
        row_prep = prep_bridge.register_acceptance(
            proposal_id="prop-persist-prep",
            title="EA010-SYNTHETIC persist-prep",
            due=None,
            source_version=version_prep,
            user_portion=_portion("prop-persist-prep", version_prep),
        )

        def crash(point: str) -> None:
            if point == "before_http":
                raise RuntimeError("prepared_only_persist")

        prep_bridge._crash = crash  # type: ignore[attr-defined]
        try:
            try:
                prep_bridge.deliver(row_prep.event_id)
            except RuntimeError:
                pass
        finally:
            prep_bridge._crash = None  # type: ignore[attr-defined]
        nofirst_prep = prep_bridge.recover(row_prep.event_id, allow_first_http=False)
        _assert(
            nofirst_prep.details.get("reason") == "prepared_without_dispatch",
            nofirst_prep.details,
        )
        stored_prep = prep_store.get_by_event(row_prep.event_id)
        _assert(stored_prep is not None, "prep outbox missing after nofirst")
        _assert(
            (stored_prep.details or {}).get("last_check_outcome") == "unknown",
            stored_prep.details,
        )
        resumed_prep = prep_bridge.recover(row_prep.event_id)
        _assert(resumed_prep.ok, f"prep resume must confirm: {resumed_prep.details}")
        _assert(
            resumed_prep.record is not None and resumed_prep.record.state == "linked",
            resumed_prep.record,
        )
        stored_prep = prep_store.get_by_event(row_prep.event_id)
        _assert(stored_prep is not None, "prep outbox missing after resume")
        _assert(
            (stored_prep.details or {}).get("last_check_outcome") == "confirmed",
            f"prep resume must clear unknown diagnostic: {stored_prep.details}",
        )
        _assert(
            (stored_prep.details or {}).get("reason") != "prepared_without_dispatch",
            stored_prep.details,
        )
        _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw=bridge_raw,
            proposal_id="prop-persist-prep",
            event_id=row_prep.event_id,
            outbox_src=prep_outbox,
            journal_path=prep_journal,
            title="EA010-SYNTHETIC persist-prep",
            state_hint="linked",
        )
        # Seed helper calls recover(nofirst); re-seed snapshot from durable outbox only.
        prep_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-persist-prep"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-persist-prep"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-persist-prep.json"),
            }
        )
        # Re-copy resumed outbox so snapshot is not polluted by seed's nofirst recover.
        shutil.copy2(prep_outbox, Path(prep_core._vikunja_outbox_path()))
        os.chmod(Path(prep_core._vikunja_outbox_path()), 0o600)
        snap_prep = next(
            p for p in prep_core.snapshot()["proposals"] if p["id"] == "prop-persist-prep"
        )
        snap_prep_tracker = snap_prep.get("tracker") or {}
        snap_prep_avail = snap_prep_tracker.get("availability")
        _assert(snap_prep_tracker.get("state") == "linked", snap_prep_tracker)
        _assert(snap_prep_tracker.get("task_id") is not None, snap_prep_tracker)
        _assert(
            snap_prep_avail is None
            or snap_prep_avail.get("outcome") not in {"unknown", "prepared_without_dispatch"},
            f"snapshot must not keep prepared_without_dispatch after resume: {snap_prep_tracker}",
        )
        _assert(
            (snap_prep_avail or {}).get("reason") != "prepared_without_dispatch",
            snap_prep_tracker,
        )

        # --- R1 no_source_mapping pending → nofirst → default recover → snapshot linked ---
        map_root = stand.run_dir / "persist-map-resume"
        map_root.mkdir(mode=0o700, exist_ok=True)
        map_journal = map_root / "journal.sqlite3"
        map_outbox = map_root / "outbox.json"
        map_cfg = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(map_journal),
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        )
        map_store = HermesOutboxStore(map_outbox)
        map_bridge = HermesBridge(map_cfg, map_store)
        journal = IntegrationJournal(map_journal)
        journal.open()
        try:
            journal.ensure_issuer(new_issuer_id())
        finally:
            journal.close()
        version_map = "b" * 64
        row_map = map_bridge.register_acceptance(
            proposal_id="prop-persist-map",
            title="EA010-SYNTHETIC persist-map",
            due=None,
            source_version=version_map,
            user_portion=_portion("prop-persist-map", version_map),
        )
        nofirst_map = map_bridge.recover(row_map.event_id, allow_first_http=False)
        _assert(nofirst_map.details.get("reason") == "no_source_mapping", nofirst_map.details)
        stored_map = map_store.get_by_event(row_map.event_id)
        _assert(stored_map is not None, "map outbox missing after nofirst")
        _assert(
            (stored_map.details or {}).get("last_check_outcome") == "unknown",
            stored_map.details,
        )
        resumed_map = map_bridge.recover(row_map.event_id)
        _assert(resumed_map.ok, f"map resume must confirm: {resumed_map.details}")
        stored_map = map_store.get_by_event(row_map.event_id)
        _assert(stored_map is not None, "map outbox missing after resume")
        _assert(
            (stored_map.details or {}).get("last_check_outcome") == "confirmed",
            f"map resume must clear unknown diagnostic: {stored_map.details}",
        )
        _assert(
            (stored_map.details or {}).get("reason") != "no_source_mapping",
            stored_map.details,
        )
        _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw=bridge_raw,
            proposal_id="prop-persist-map",
            event_id=row_map.event_id,
            outbox_src=map_outbox,
            journal_path=map_journal,
            title="EA010-SYNTHETIC persist-map",
            state_hint="linked",
        )
        map_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-persist-map"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-persist-map"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-persist-map.json"),
            }
        )
        shutil.copy2(map_outbox, Path(map_core._vikunja_outbox_path()))
        os.chmod(Path(map_core._vikunja_outbox_path()), 0o600)
        snap_map = next(
            p for p in map_core.snapshot()["proposals"] if p["id"] == "prop-persist-map"
        )
        snap_map_tracker = snap_map.get("tracker") or {}
        snap_map_avail = snap_map_tracker.get("availability")
        _assert(snap_map_tracker.get("state") == "linked", snap_map_tracker)
        _assert(snap_map_tracker.get("task_id") is not None, snap_map_tracker)
        _assert(
            (snap_map_avail or {}).get("reason") != "no_source_mapping",
            f"snapshot must not keep no_source_mapping after resume: {snap_map_tracker}",
        )

        # --- R2 auth_denied: lost backlink + invalid token → recover → Core.snapshot ---
        denied_root = stand.run_dir / "persist-denied"
        denied_root.mkdir(mode=0o700, exist_ok=True)
        denied_journal = denied_root / "journal.sqlite3"
        denied_outbox = denied_root / "outbox.json"
        denied_cfg = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(denied_journal),
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        )
        denied_store = HermesOutboxStore(denied_outbox)
        denied_bridge = HermesBridge(denied_cfg, denied_store)
        journal = IntegrationJournal(denied_journal)
        journal.open()
        try:
            journal.ensure_issuer(new_issuer_id())
        finally:
            journal.close()
        version_denied = "c" * 64
        row_denied = denied_bridge.register_acceptance(
            proposal_id="prop-persist-denied",
            title="EA010-SYNTHETIC persist-denied",
            due=None,
            source_version=version_denied,
            user_portion=_portion("prop-persist-denied", version_denied),
        )
        delivered = denied_bridge.deliver(row_denied.event_id)
        _assert(delivered.ok, f"denied setup deliver failed: {delivered.details}")

        def lose_backlink(data: dict[str, Any]) -> None:
            current = data["by_event"][row_denied.event_id]
            current["state"] = OUTBOX_UNKNOWN
            current["task_id"] = None
            current["url"] = None
            data["by_proposal"][current["proposal_id"]] = current
            data["by_event"][row_denied.event_id] = current

        denied_store.mutate(lose_backlink)
        bad_token = denied_root / "invalid.token"
        bad_token.write_text("ea010-synthetic-invalid-token\n", encoding="utf-8")
        os.chmod(bad_token, 0o600)
        denied_cfg_bad = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(denied_journal),
                "token_ref": {"kind": "file", "path": str(bad_token)},
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        )
        denied_bridge_bad = HermesBridge(denied_cfg_bad, denied_store)
        denied_result = denied_bridge_bad.recover(row_denied.event_id, allow_first_http=False)
        _assert(
            denied_result.outcome is Outcome.AUTH_DENIED,
            f"expected auth_denied, got {denied_result.outcome} {denied_result.details}",
        )
        live_denied = denied_result.public_tracker() or {}
        live_denied_avail = live_denied.get("availability") or {}
        _assert(
            live_denied_avail.get("outcome") == "auth_denied"
            or live_denied_avail.get("status") == "denied",
            f"live recover missing denied availability: {live_denied}",
        )
        stored_denied = denied_store.get_by_event(row_denied.event_id)
        _assert(stored_denied is not None, "denied outbox missing")
        _assert(
            (stored_denied.details or {}).get("last_check_outcome") == "auth_denied",
            f"denied must persist last_check_outcome: {stored_denied.details}",
        )
        _assert(
            isinstance((stored_denied.details or {}).get("last_explicit_check"), str)
            and (stored_denied.details or {}).get("last_explicit_check"),
            stored_denied.details,
        )
        durable_denied = tracker_availability_projection(details=stored_denied.details)
        _assert(
            durable_denied is not None and durable_denied.get("outcome") == "auth_denied",
            f"durable projection missing denied: {durable_denied}",
        )
        _assert(durable_denied.get("status") == "denied", durable_denied)
        _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw={
                **bridge_raw,
                "token_ref": {"kind": "file", "path": str(bad_token)},
            },
            proposal_id="prop-persist-denied",
            event_id=row_denied.event_id,
            outbox_src=denied_outbox,
            journal_path=denied_journal,
            title="EA010-SYNTHETIC persist-denied",
            state_hint="unknown",
        )
        denied_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-persist-denied"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-persist-denied"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-persist-denied.json"),
            }
        )
        shutil.copy2(denied_outbox, Path(denied_core._vikunja_outbox_path()))
        os.chmod(Path(denied_core._vikunja_outbox_path()), 0o600)
        # Point Core recover at the bad-token bridge, then snapshot without re-copying availability.
        denied_bridge_path = denied_root / "bridge-denied.json"
        denied_bridge_path.write_text(
            json.dumps(
                {
                    **bridge_raw,
                    "journal_path": str(denied_journal),
                    "token_ref": {"kind": "file", "path": str(bad_token)},
                    "task_url_template": bridge_raw.get("task_url_template")
                    or "http://127.0.0.1/tasks/{id}",
                }
            ),
            encoding="utf-8",
        )
        os.chmod(denied_bridge_path, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(denied_bridge_path)
        denied_live = denied_core.recover_vikunja_outbox(
            event_id=row_denied.event_id,
            allow_first_http=False,
            proposal_id="prop-persist-denied",
        )
        denied_live_avail = ((denied_live or {}).get("tracker") or {}).get("availability") or {}
        _assert(
            denied_live_avail.get("outcome") == "auth_denied"
            or denied_live_avail.get("status") == "denied",
            f"Core recover missing denied: {denied_live}",
        )
        snap_denied = next(
            p for p in denied_core.snapshot()["proposals"] if p["id"] == "prop-persist-denied"
        )
        snap_denied_avail = ((snap_denied.get("tracker") or {}).get("availability")) or {}
        _assert(
            snap_denied_avail.get("outcome") == "auth_denied"
            or snap_denied_avail.get("status") == "denied",
            f"Core.snapshot dropped denied availability: {snap_denied.get('tracker')}",
        )

        # --- R2 dependency_unavailable: bad writer origin → recover → Core.snapshot ---
        unavail_root = stand.run_dir / "persist-unavail"
        unavail_root.mkdir(mode=0o700, exist_ok=True)
        unavail_journal = unavail_root / "journal.sqlite3"
        unavail_outbox = unavail_root / "outbox.json"
        # First deliver with good writer, then flip origin.
        good_cfg = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(unavail_journal),
                "task_url_template": bridge_raw.get("task_url_template")
                or "http://127.0.0.1/tasks/{id}",
            }
        )
        unavail_store = HermesOutboxStore(unavail_outbox)
        good_bridge = HermesBridge(good_cfg, unavail_store)
        journal = IntegrationJournal(unavail_journal)
        journal.open()
        try:
            journal.ensure_issuer(new_issuer_id())
        finally:
            journal.close()
        version_unavail = "d" * 64
        row_unavail = good_bridge.register_acceptance(
            proposal_id="prop-persist-unavail",
            title="EA010-SYNTHETIC persist-unavail",
            due=None,
            source_version=version_unavail,
            user_portion=_portion("prop-persist-unavail", version_unavail),
        )
        delivered_u = good_bridge.deliver(row_unavail.event_id)
        _assert(delivered_u.ok, f"unavail setup deliver failed: {delivered_u.details}")

        def lose_backlink_u(data: dict[str, Any]) -> None:
            current = data["by_event"][row_unavail.event_id]
            current["state"] = OUTBOX_UNKNOWN
            current["task_id"] = None
            current["url"] = None
            data["by_proposal"][current["proposal_id"]] = current
            data["by_event"][row_unavail.event_id] = current

        unavail_store.mutate(lose_backlink_u)
        unavail_cfg = BridgeConfig.from_mapping(
            {
                **bridge_raw,
                "journal_path": str(unavail_journal),
                "api_origin": "http://127.0.0.1:1",
                "task_url_template": "http://127.0.0.1:1/tasks/{id}",
            }
        )
        unavail_bridge = HermesBridge(unavail_cfg, unavail_store)
        unavail_result = unavail_bridge.recover(row_unavail.event_id, allow_first_http=False)
        _assert(
            unavail_result.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            f"expected dependency_unavailable, got {unavail_result.outcome} {unavail_result.details}",
        )
        stored_unavail = unavail_store.get_by_event(row_unavail.event_id)
        _assert(stored_unavail is not None, "unavail outbox missing")
        _assert(
            (stored_unavail.details or {}).get("last_check_outcome")
            == "dependency_unavailable",
            f"unavail must persist last_check_outcome: {stored_unavail.details}",
        )
        _seed_core_proposal_for_outbox(
            plugin=plugin,
            stand=stand,
            bridge_raw={
                **bridge_raw,
                "api_origin": "http://127.0.0.1:1",
                "task_url_template": "http://127.0.0.1:1/tasks/{id}",
            },
            proposal_id="prop-persist-unavail",
            event_id=row_unavail.event_id,
            outbox_src=unavail_outbox,
            journal_path=unavail_journal,
            title="EA010-SYNTHETIC persist-unavail",
            state_hint="unknown",
        )
        unavail_core = plugin.core(
            {
                "owner": OWNER,
                "runtime": str(stand.run_dir / "core-proj-prop-persist-unavail"),
                "tasks": str(stand.run_dir / "core-proj-tasks-prop-persist-unavail"),
                "policy": str(stand.run_dir / "core-proj-policy-prop-persist-unavail.json"),
            }
        )
        shutil.copy2(unavail_outbox, Path(unavail_core._vikunja_outbox_path()))
        os.chmod(Path(unavail_core._vikunja_outbox_path()), 0o600)
        unavail_bridge_path = unavail_root / "bridge-unavail.json"
        unavail_bridge_path.write_text(
            json.dumps(
                {
                    **bridge_raw,
                    "journal_path": str(unavail_journal),
                    "api_origin": "http://127.0.0.1:1",
                    "task_url_template": "http://127.0.0.1:1/tasks/{id}",
                }
            ),
            encoding="utf-8",
        )
        os.chmod(unavail_bridge_path, 0o600)
        os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(unavail_bridge_path)
        unavail_live = unavail_core.recover_vikunja_outbox(
            event_id=row_unavail.event_id,
            allow_first_http=False,
            proposal_id="prop-persist-unavail",
        )
        unavail_live_avail = ((unavail_live or {}).get("tracker") or {}).get("availability") or {}
        _assert(
            unavail_live_avail.get("outcome") == "dependency_unavailable"
            or unavail_live_avail.get("status") == "unavailable",
            f"Core recover missing unavailable: {unavail_live}",
        )
        snap_unavail = next(
            p for p in unavail_core.snapshot()["proposals"] if p["id"] == "prop-persist-unavail"
        )
        snap_unavail_avail = ((snap_unavail.get("tracker") or {}).get("availability")) or {}
        _assert(
            snap_unavail_avail.get("outcome") == "dependency_unavailable"
            or snap_unavail_avail.get("status") == "unavailable",
            f"Core.snapshot dropped unavailable: {snap_unavail.get('tracker')}",
        )

        return {
            "prepared_resume": {
                "task_id": snap_prep_tracker.get("task_id"),
                "snapshot_availability": snap_prep_avail,
                "last_check_outcome": (stored_prep.details or {}).get("last_check_outcome"),
            },
            "no_source_mapping_resume": {
                "task_id": snap_map_tracker.get("task_id"),
                "snapshot_availability": snap_map_avail,
                "last_check_outcome": (stored_map.details or {}).get("last_check_outcome"),
            },
            "auth_denied": {
                "live_availability": denied_live_avail,
                "snapshot_availability": snap_denied_avail,
                "last_check_outcome": (stored_denied.details or {}).get("last_check_outcome"),
            },
            "dependency_unavailable": {
                "live_availability": unavail_live_avail,
                "snapshot_availability": snap_unavail_avail,
                "last_check_outcome": (stored_unavail.details or {}).get("last_check_outcome"),
            },
        }
    finally:
        if prev_cfg is not None:
            os.environ["EA010_HERMES_BRIDGE_CONFIG"] = prev_cfg
        else:
            os.environ["EA010_HERMES_BRIDGE_CONFIG"] = str(Path(readiness["bridge_config"]))


def _run_recover_then_snapshot_browser(patched_js: Path, run_dir: Path, *, task_id: int, route_a: str) -> dict[str, Any]:
    """Browser: failed recover response then snapshot refresh must stay unknown (R1)."""
    import copy

    harness_dir = run_dir / "recover-snapshot-harness"
    harness_dir.mkdir(parents=True, exist_ok=True)
    react_bundle = _bundle_react(harness_dir)
    meeting_id = "a" * 32
    proposal_id = "prop-rs-" + uuid.uuid4().hex[:10]
    version = "d" * 64
    event_id = "11111111-1111-4111-8111-111111111111"
    task_url = f"{route_a}/tasks/{task_id}"
    failed_tracker = {
        "event_id": event_id,
        "state": "linked",
        "task_id": task_id,
        "url": task_url,
        "availability": {
            "status": "unavailable",
            "outcome": "unknown",
            "reason": "no_source_mapping",
            "last_check_outcome": "unknown",
        },
    }
    # Start as unknown so reconcile is available; recover returns failed diagnostic;
    # subsequent snapshot keeps the same availability (durable check stamp).
    state = {
        "phase": "initial",
        "snapshot": {
            "meetings": [
                {
                    "id": meeting_id,
                    "version": version,
                    "status": "saved",
                    "analysis_status": "complete",
                    "summary": "Synthetic recover-snapshot context",
                    "calendar": {
                        "title": "Synthetic recover-snapshot meeting",
                        "starts_at": "2026-11-05T11:00:00Z",
                        "ends_at": "2026-11-05T12:00:00Z",
                        "timezone": "Europe/Moscow",
                        "verification_status": "verified",
                        "time_basis": "calendar",
                    },
                }
            ],
            "proposals": [
                {
                    "id": proposal_id,
                    "meeting_id": meeting_id,
                    "version": version,
                    "revision": 5,
                    "status": "accepted",
                    "kind": "self_commitment",
                    "text": "EA010-SYNTHETIC recover-snapshot",
                    "quote": "Owner: EA010-SYNTHETIC recover-snapshot",
                    "line_start": 2,
                    "line_end": 2,
                    "owner": "Owner",
                    "speaker": "Owner",
                    "tracker": {
                        "event_id": event_id,
                        "state": "unknown",
                        "task_id": task_id,
                        "url": task_url,
                    },
                }
            ],
            "tasks": [],
            "errors": [],
            "coverage": {"meetings": 1, "analyzed": 1},
            "tracker": {"ready": True, "ui_url": route_a},
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
        "recover_proposal": {
            "id": proposal_id,
            "meeting_id": meeting_id,
            "version": version,
            "revision": 5,
            "status": "accepted",
            "kind": "self_commitment",
            "text": "EA010-SYNTHETIC recover-snapshot",
            "tracker": failed_tracker,
        },
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/":
                html = (
                    "<!doctype html><html><head><meta charset='utf-8'>"
                    "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                    "<link rel='stylesheet' href='/style.css'></head><body><div id='root'></div>"
                    "<script src='/react-bundle.js'></script><script>"
                    "window.__HERMES_PLUGIN_SDK__={React:window.React,fetchJSON:async(url,opts)=>{"
                    "const r=await fetch(url,opts); if(!r.ok) throw new Error('http '+r.status); return r.json();}};"
                    "window.__HERMES_PLUGINS__={register(n,C){window.__PT_COMP__=C;}};"
                    "</script><script src='/index.js'></script>"
                    "<script>window.ReactDOM.createRoot(document.getElementById('root')).render(window.React.createElement(window.__PT_COMP__));</script>"
                    "</body></html>"
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return
            static = {
                "/react-bundle.js": (self.server.react_bundle, "application/javascript"),  # type: ignore[attr-defined]
                "/index.js": (self.server.ui_js, "application/javascript"),  # type: ignore[attr-defined]
                "/style.css": (self.server.ui_css, "text/css"),  # type: ignore[attr-defined]
            }
            if parsed.path in static:
                path, ctype = static[parsed.path]
                data = Path(path).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if parsed.path == "/api/plugins/personal-tasks/snapshot":
                body = json.dumps(self.server.state["snapshot"]).encode()  # type: ignore[attr-defined]
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            parsed = urlparse(self.path)
            if parsed.path.endswith("/tracker/recover"):
                # After recover, durable snapshot retains failed availability (R1).
                snap = copy.deepcopy(self.server.state["snapshot"])  # type: ignore[attr-defined]
                snap["proposals"][0]["tracker"] = copy.deepcopy(failed_tracker)
                self.server.state["snapshot"] = snap  # type: ignore[attr-defined]
                self.server.state["phase"] = "after_recover"  # type: ignore[attr-defined]
                body = json.dumps({"proposal": self.server.state["recover_proposal"]}).encode()  # type: ignore[attr-defined]
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    port = _free_port()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.react_bundle = str(react_bundle)  # type: ignore[attr-defined]
    server.ui_js = str(patched_js)  # type: ignore[attr-defined]
    server.ui_css = str(BASELINE_CSS)  # type: ignore[attr-defined]
    server.state = state  # type: ignore[attr-defined]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{port}/"
    result_path = harness_dir / "recover-snapshot.json"
    driver = harness_dir / "recover-snapshot.mjs"
    driver.write_text(
        f"""
import {{ createRequire }} from 'node:module';
import {{ writeFileSync }} from 'node:fs';
const require = createRequire(import.meta.url);
const {{ chromium }} = require('playwright');
const assert = (cond, msg) => {{ if (!cond) throw new Error(msg); }};
const browser = await chromium.launch({{ executablePath: {json.dumps(str(CHROMIUM))}, headless: true, args: ['--no-sandbox'] }});
const page = await browser.newPage({{ viewport: {{ width: 390, height: 844 }} }});
const out = {{ ok: false }};
try {{
  await page.goto({json.dumps(origin)}, {{ waitUntil: 'networkidle' }});
  await page.getByRole('button', {{ name: 'Встречи' }}).click();
  await page.locator('#pt-meeting-{meeting_id}').getByRole('button', {{ name: 'Открыть саммари' }}).click();
  await page.getByRole('button', {{ name: 'Предложения', exact: true }}).click();
  await page.waitForSelector('[data-delivery="unknown"]');
  assert(await page.getByRole('button', {{ name: 'Проверить передачу' }}).count() >= 1, 'missing reconcile');
  const recoverWait = page.waitForResponse(r => r.url().includes('/tracker/recover') && r.request().method() === 'POST');
  const snapWait = page.waitForResponse(r => r.url().includes('/snapshot') && r.request().method() === 'GET');
  await page.getByRole('button', {{ name: 'Проверить передачу' }}).click();
  const recoverResp = await recoverWait;
  assert(recoverResp.ok(), 'recover HTTP ' + recoverResp.status());
  const recoverBody = await recoverResp.json();
  assert(recoverBody.proposal?.tracker?.availability?.reason === 'no_source_mapping', 'recover body missing diagnostic');
  await snapWait; // refresh after recover
  await page.waitForTimeout(300);
  await page.waitForSelector('[data-delivery="unknown"]', {{ timeout: 10000 }});
  assert(await page.locator('[data-delivery="delivered"]').count() === 0, 'failed recover must not show delivered after snapshot refresh');
  assert(await page.getByRole('button', {{ name: 'Проверить передачу' }}).count() >= 1, 'reconcile must remain after failed recover+snapshot');
  const label = await page.locator('[data-delivery="unknown"]').innerText();
  assert(label.includes('no_source_mapping') || label.includes('Проверяем'), 'label lost diagnostic: ' + label);
  out.ok = true;
  out.kind = 'unknown';
  out.after_phase = 'after_recover';
  writeFileSync({json.dumps(str(result_path))}, JSON.stringify(out));
}} catch (err) {{
  writeFileSync({json.dumps(str(result_path))}, JSON.stringify({{ ok: false, error: String(err), stack: err && err.stack }}));
  throw err;
}} finally {{
  await browser.close();
}}
""",
        encoding="utf-8",
    )
    try:
        proc = subprocess.run(
            ["node", str(driver)],
            capture_output=True,
            text=True,
            cwd=str(harness_dir),
            env={**os.environ, "NODE_PATH": str(PLAYWRIGHT_MODULE.parent)},
            timeout=90,
        )
        _assert(
            proc.returncode == 0,
            f"recover-then-snapshot browser failed: {proc.stderr[-2000:]}\n{proc.stdout[-2000:]}",
        )
        _assert(result_path.is_file(), "recover-snapshot result missing")
        body = json.loads(result_path.read_text(encoding="utf-8"))
        _assert(body.get("ok") is True, f"recover-snapshot result: {body}")
        return {"ok": True, "kind": body.get("kind"), "proposal_id": proposal_id}
    finally:
        server.shutdown()
        server.server_close()


def _run_presentation_browser(patched_js: Path, run_dir: Path, *, task_id: int, route_a: str) -> dict[str, Any]:
    """Browser DOM for denied/unavailable/deleted via production-shaped snapshot (no fabricated accept)."""
    harness_dir = run_dir / "presentation-harness"
    harness_dir.mkdir(parents=True, exist_ok=True)
    react_bundle = _bundle_react(harness_dir)
    meeting_id = "b" * 32
    proposal_id = "prop-" + uuid.uuid4().hex[:12]
    version = "c" * 64
    states = [
        {
            "name": "denied_with_done",
            "tracker": {
                "event_id": "00000000-0000-4000-8000-000000000010",
                "state": "linked",
                "task_id": task_id,
                "url": f"{route_a}/tasks/{task_id}",
                "availability": {"status": "denied", "outcome": "auth_denied", "reason": "token"},
                "done_observation": {"done": True, "checked_at": "2026-11-11T12:00:00Z"},
            },
            "expect": "denied",
            "action": "Проверить подключение",
            "needles": ["Нет доступа", "не актуальный статус"],
        },
        {
            "name": "unavailable",
            "tracker": {
                "event_id": "00000000-0000-4000-8000-000000000011",
                "state": "linked",
                "task_id": task_id,
                "url": None,
                "availability": {"status": "unavailable", "outcome": "dependency_unavailable", "reason": "down"},
            },
            "expect": "unavailable",
            "action": "Проверить подключение",
            "needles": ["недоступна"],
        },
        {
            "name": "deleted",
            "tracker": {
                "event_id": "00000000-0000-4000-8000-000000000012",
                "state": "linked",
                "task_id": task_id,
                "url": f"{route_a}/tasks/{task_id}",
                # Production wire from tracker_availability_projection after deleted target.
                "availability": {
                    "status": "unavailable",
                    "outcome": "unknown",
                    "reason": "target_deleted_or_gone",
                    "last_check_outcome": "unknown",
                },
            },
            "expect": "deleted",
            "action": None,
            "needles": ["Связанная задача недоступна"],
        },
        {
            "name": "conflict_reconcile",
            "tracker": {
                "event_id": "00000000-0000-4000-8000-000000000013",
                "state": "conflict",
                "task_id": task_id,
                "url": f"{route_a}/tasks/{task_id}",
                # Production wire: incompatible_existing_link → status=conflict (not deleted).
                "availability": {
                    "status": "conflict",
                    "outcome": "conflict",
                    "reason": "incompatible_existing_link",
                    "last_check_outcome": "conflict",
                },
            },
            "expect": "conflict",
            "action": "Проверить передачу",
            "needles": ["требует проверки", "incompatible_existing_link"],
        },
    ]

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/":
                html = (
                    "<!doctype html><html><head><meta charset='utf-8'>"
                    "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                    "<link rel='stylesheet' href='/style.css'></head><body><div id='root'></div>"
                    "<script src='/react-bundle.js'></script><script>"
                    "window.__HERMES_PLUGIN_SDK__={React:window.React,fetchJSON:async(url,opts)=>{"
                    "const r=await fetch(url,opts); if(!r.ok) throw new Error('http '+r.status); return r.json();}};"
                    "window.__HERMES_PLUGINS__={register(n,C){window.__PT_COMP__=C;}};"
                    "</script><script src='/index.js'></script>"
                    "<script>window.ReactDOM.createRoot(document.getElementById('root')).render(window.React.createElement(window.__PT_COMP__));</script>"
                    "</body></html>"
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return
            static = {
                "/react-bundle.js": (self.server.react_bundle, "application/javascript"),  # type: ignore[attr-defined]
                "/index.js": (self.server.ui_js, "application/javascript"),  # type: ignore[attr-defined]
                "/style.css": (self.server.ui_css, "text/css"),  # type: ignore[attr-defined]
            }
            if parsed.path in static:
                path, ctype = static[parsed.path]
                data = Path(path).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if parsed.path == "/api/plugins/personal-tasks/snapshot":
                body = json.dumps(self.server.snapshot).encode()  # type: ignore[attr-defined]
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    port = _free_port()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.react_bundle = str(react_bundle)  # type: ignore[attr-defined]
    server.ui_js = str(patched_js)  # type: ignore[attr-defined]
    server.ui_css = str(BASELINE_CSS)  # type: ignore[attr-defined]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{port}/"
    observed: list[str] = []
    try:
        for state in states:
            snapshot = {
                "meetings": [
                    {
                        "id": meeting_id,
                        "version": version,
                        "status": "saved",
                        "analysis_status": "complete",
                        "summary": "Synthetic discussion context for UX",
                        "calendar": {
                            "title": "Synthetic meeting title",
                            "starts_at": "2026-11-05T11:00:00Z",
                            "ends_at": "2026-11-05T12:00:00Z",
                            "timezone": "Europe/Moscow",
                            "verification_status": "verified",
                            "time_basis": "calendar",
                        },
                    }
                ],
                "proposals": [
                    {
                        "id": proposal_id,
                        "meeting_id": meeting_id,
                        "version": version,
                        "revision": 4,
                        "status": "accepted",
                        "kind": "self_commitment",
                        "text": "EA010-SYNTHETIC presentation",
                        "quote": "Owner: EA010-SYNTHETIC presentation",
                        "line_start": 2,
                        "line_end": 2,
                        "owner": "Owner",
                        "speaker": "Owner",
                        "tracker": state["tracker"],
                    }
                ],
                "tasks": [],
                "errors": [],
                "coverage": {"meetings": 1, "analyzed": 1},
                "tracker": {"ready": True, "ui_url": route_a},
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            server.snapshot = snapshot  # type: ignore[attr-defined]
            driver = harness_dir / f"present-{state['name']}.mjs"
            result_path = harness_dir / f"present-{state['name']}.json"
            driver.write_text(
                f"""
import {{ createRequire }} from 'node:module';
import {{ writeFileSync }} from 'node:fs';
const require = createRequire(import.meta.url);
const {{ chromium }} = require('playwright');
const browser = await chromium.launch({{ executablePath: {json.dumps(str(CHROMIUM))}, headless: true, args: ['--no-sandbox'] }});
const page = await browser.newPage({{ viewport: {{ width: 390, height: 844 }} }});
try {{
  await page.goto({json.dumps(origin)}, {{ waitUntil: 'networkidle' }});
  await page.getByRole('button', {{ name: 'Встречи' }}).click();
  await page.locator('#pt-meeting-{meeting_id}').getByRole('button', {{ name: 'Открыть саммари' }}).click();
  await page.getByRole('button', {{ name: 'Предложения', exact: true }}).click();
  await page.waitForSelector('[data-delivery="{state["expect"]}"]');
  const text = await page.locator('[data-delivery="{state["expect"]}"]').innerText();
  for (const needle of {json.dumps(state["needles"])}) {{
    if (!text.includes(needle)) throw new Error('missing ' + needle + ' in ' + text);
  }}
  const action = {json.dumps(state["action"])};
  if (action) {{
    if (await page.getByRole('button', {{ name: action }}).count() < 1) throw new Error('missing action ' + action);
  }} else {{
    if (await page.getByRole('button', {{ name: 'Проверить подключение' }}).count() !== 0) throw new Error('deleted must hide check_connection');
    if (await page.getByRole('button', {{ name: 'Проверить передачу' }}).count() !== 0) throw new Error('deleted must hide reconcile');
  }}
  writeFileSync({json.dumps(str(result_path))}, JSON.stringify({{ ok: true, kind: {json.dumps(state["expect"])} }}));
}} finally {{ await browser.close(); }}
""",
                encoding="utf-8",
            )
            proc = subprocess.run(
                ["node", str(driver)],
                capture_output=True,
                text=True,
                cwd=str(harness_dir),
                env={**os.environ, "NODE_PATH": str(PLAYWRIGHT_MODULE.parent)},
                timeout=90,
            )
            _assert(
                proc.returncode == 0,
                f"presentation {state['name']} failed: {proc.stderr[-2000:]}",
            )
            observed.append(state["expect"])
    finally:
        server.shutdown()
        server.server_close()
    return {"ok": True, "kinds": observed, "proposal_id": proposal_id}


def run_hermes_ux_suite_inner(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    global HERMES_ORIGIN
    worktree = worktree.resolve()
    profile = load_profile()
    source = _source_digest(worktree)
    stand = IsolatedStand(profile=profile, worktree=worktree)
    hermes_port = _free_port()
    HERMES_ORIGIN = f"http://127.0.0.1:{hermes_port}"
    owner_password = uuid.uuid4().hex
    auth_header = "Basic " + base64.b64encode(f"{OWNER}:{owner_password}".encode()).decode()
    try:
        with stand:
            patched_root = stand.run_dir / "hermes-ux-baseline"
            patches = _verify_and_apply_patches(worktree, patched_root)
            patched_js = Path(patches["patched_tree"]) / "plugins/personal-tasks/dashboard/dist/index.js"
            node_helpers = _node_helper_checks(patched_js)

            plugin_path = Path(patches["patched_tree"]) / "plugins/personal-tasks/dashboard/plugin_api.py"
            plugin = _load_module("ea010_s07_plugin_api", plugin_path)
            plugin.CORE_PATH = Path(patches["patched_tree"]) / "scripts/ea_core.py"
            plugin._module = None

            # Minimal settings for readiness snapshot (empty inventory OK).
            runtime = stand.run_dir / "hermes-runtime-ready"
            tasks_dir = stand.run_dir / "hermes-tasks-ready"
            runtime.mkdir(mode=0o700)
            tasks_dir.mkdir(mode=0o700)
            policy = stand.run_dir / "policy-ready.json"
            policy.write_text(json.dumps({"text_analysis": {"preferred_agent": "codex"}}), encoding="utf-8")
            os.chmod(policy, 0o600)
            settings_path = stand.run_dir / "plugin-settings-ready.json"
            settings_path.write_text(
                json.dumps(
                    {
                        "owner": OWNER,
                        "runtime": str(runtime),
                        "tasks": str(tasks_dir),
                        "policy": str(policy),
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(settings_path, 0o600)
            plugin.SETTINGS_PATH = settings_path

            readiness = _run_readiness_scenarios(stand, plugin, auth_header)
            browser = _run_browser_and_accept(
                patched_tree=Path(patches["patched_tree"]),
                stand=stand,
                readiness=readiness,
                plugin=plugin,
                auth_header=auth_header,
                owner_password=owner_password,
            )
            presentation = _run_presentation_browser(
                patched_js,
                stand.run_dir,
                task_id=int(browser["task_id"]),
                route_a=str(browser["task_url_a"]).rsplit("/tasks/", 1)[0],
            )
            recover_snapshot = _run_recover_then_snapshot_browser(
                patched_js,
                stand.run_dir,
                task_id=int(browser["task_id"]),
                route_a=str(browser["task_url_a"]).rsplit("/tasks/", 1)[0],
            )
            result = {
                "ok": True,
                "suite": "hermes-ux",
                "mode": mode,
                "argv": list(argv),
                "executed_count": 8,
                "source": {**(stand.manifest.get("source") or {}), **source, **patches},
                "synthetic_ids": {
                    "proposal_id": browser["proposal_id"],
                    "meeting_id": browser["meeting_id"],
                    "project_id": readiness["synthetic_project_id"],
                    "task_id": browser["task_id"],
                    "event_id": browser["event_id"],
                    "run_id": stand.run_id,
                },
                "run_id": stand.run_id,
                "details": {
                    "patches": patches,
                    "node_helpers": node_helpers,
                    "browser": browser,
                    "presentation": presentation,
                    "recover_snapshot": recover_snapshot,
                    "readiness": readiness,
                    "c5_0_3": {
                        "addendum_sha256": "c9662c397c88aba6608800bcd44ff4c1de12a4d4b3596b41bd8cee4a814006f4",
                        "allow_first_http": False,
                    },
                },
                "readiness": stand.readiness,
                "binary": stand.manifest.get("binary"),
            }
        result["cleanup"] = {"confirmed": stand.cleaned}
        if not stand.cleaned:
            raise StandError("hermes-ux suite cleanup was not confirmed")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
    finally:
        os.environ.pop("EA010_HERMES_UI_PROFILE", None)
        os.environ.pop("EA010_HERMES_BRIDGE_CONFIG", None)


def run_hermes_ux_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"hermes-ux supports only isolated mode, got {mode!r}")
    if os.environ.get("EA010_HERMES_UX_INNER") != "1":
        force_reexec = os.environ.get("EA010_FORCE_HERMES_REEXEC") == "1"
        try:
            if force_reexec:
                raise ImportError("EA010_FORCE_HERMES_REEXEC")
            import fastapi  # noqa: F401
            import uvicorn  # noqa: F401
        except ImportError:
            py = _hermes_python()
            profile = load_profile()
            # Durable receipt outside disposable stand: aggregate SIGTERM/SIGHUP
            # redelivers before in-process memory from the child can be read.
            signal_receipt = allocate_component_signal_receipt(profile, suite="hermes-ux")
            env = {
                **os.environ,
                "EA010_HERMES_UX_INNER": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(worktree),
                "EA010_ISOLATED_STAND_PROFILE": os.environ.get(
                    "EA010_ISOLATED_STAND_PROFILE",
                    "/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json",
                ),
                STAND_SIGNAL_RECEIPT_ENV: str(signal_receipt),
            }
            env.pop("EA010_FORCE_HERMES_REEXEC", None)
            code = (
                "import json,sys; from pathlib import Path; "
                "from tests.support.hermes_ux_suite import run_hermes_ux_suite_inner; "
                "r=run_hermes_ux_suite_inner(mode=sys.argv[1], worktree=Path(sys.argv[2]), "
                "argv=json.loads(sys.argv[3])); "
                "print('EA010_HERMES_UX_RESULT:'+json.dumps(r, sort_keys=True))"
            )
            # Spawn+register under blocked SIGTERM/SIGHUP so aggregate cancel
            # cannot miss this child between Popen acquisition and registration.
            proc = spawn_registered_component_proc(
                [py, "-c", code, mode, str(worktree), json.dumps(list(argv))],
                signal_receipt_path=signal_receipt,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                cwd=str(worktree),
            )
            try:
                stdout, stderr = proc.communicate()
            finally:
                unregister_active_component_proc(proc)
            if proc.returncode != 0:
                # Ordinary nonzero exits must carry structured durable evidence
                # (not only truncated receipt text) so suite-all can journal
                # run/IDs/cleanup/raw child rc.
                child_evidence = await_component_signal_receipt(signal_receipt, timeout=2.0)
                if isinstance(child_evidence, dict) and child_evidence.get("error"):
                    child_evidence = None
                evidence_note = ""
                if isinstance(child_evidence, dict):
                    try:
                        evidence_note = (
                            "\nsignal_receipt="
                            + json.dumps(child_evidence, sort_keys=True)[-2000:]
                        )
                    except Exception:
                        evidence_note = f"\nsignal_receipt_path={signal_receipt}"
                elif signal_receipt.is_file():
                    evidence_note = f"\nsignal_receipt_path={signal_receipt}"
                raise StandError(
                    "hermes-ux inner suite failed under Hermes python: "
                    f"rc={proc.returncode}\nstdout={(stdout or '')[-4000:]}\n"
                    f"stderr={(stderr or '')[-4000:]}"
                    f"{evidence_note}",
                    child_rc=int(proc.returncode),
                    child_evidence=child_evidence,
                    signal_receipt_path=signal_receipt,
                )
            if signal_receipt.exists():
                signal_receipt.unlink(missing_ok=True)
            marker = "EA010_HERMES_UX_RESULT:"
            line = next((ln for ln in (stdout or "").splitlines() if ln.startswith(marker)), None)
            if line is None:
                raise StandError(
                    f"inner suite produced no result marker; stdout={(stdout or '')[-2000:]}"
                )
            result = json.loads(line[len(marker) :])
            result["hermes_python"] = py
            result["reexec"] = True
            return result
    return run_hermes_ux_suite_inner(mode=mode, worktree=worktree, argv=argv)
