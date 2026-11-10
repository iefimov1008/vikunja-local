"""Isolated recovery suite: unified dispatcher, exact reconcile, crash/late-write."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from vikunja_local.api import (
    ApiError,
    Outcome,
    TokenRef,
    VikunjaClient,
    build_marker,
    extract_marker,
    new_operation_ids,
)
from vikunja_local.dates import MSK, confirm_date_only
from vikunja_local.dispatcher import IntegrationDispatcher
from vikunja_local.journal import (
    IntegrationJournal,
    OperationState,
    new_issuer_id,
    new_operation_id,
)
from vikunja_local.reconcile import reconcile_create_marker

from .stand import IsolatedStand, StandError, load_profile, sha256_file

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

ROOT = Path(__file__).resolve().parents[2]
WRITE_PERMS = {"tasks": ["create", "read_all", "read_one", "update"]}
READ_PERMS = {"tasks": ["read_all", "read_one"]}


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "vikunja_local" / "dispatcher.py",
        worktree / "vikunja_local" / "reconcile.py",
        worktree / "tests" / "test_recovery.py",
        worktree / "tests" / "support" / "recovery_suite.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(sha256_file(path).encode())
        digest.update(b"\0")
    return {
        "sha256": digest.hexdigest(),
        "head": subprocess.check_output(
            ["git", "-C", str(worktree), "rev-parse", "HEAD"], text=True
        ).strip(),
    }


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
        {
            "title": title,
            "expires_at": expires,
            "permissions": permissions,
        },
        token=stand._session_token,
    )
    if status != 201 or not isinstance(payload, dict) or "token" not in payload:
        raise StandError(f"API token create failed: HTTP {status}")
    return payload


def _client_for(
    stand: IsolatedStand,
    token_path: Path,
    project_id: int,
    *,
    origin: str | None = None,
    page_size: int = 2,
    http_timeout: float | None = None,
) -> VikunjaClient:
    api_origin = origin or f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    kwargs: dict[str, Any] = {
        "api_origin": api_origin,
        "project_id": project_id,
        "token_ref": TokenRef(kind="file", path=token_path),
        "page_size": page_size,
        "max_items_per_page": 50,
    }
    if http_timeout is not None:
        kwargs["http_timeout"] = http_timeout
    return VikunjaClient(**kwargs)


def _count_exact_marker(client: VikunjaClient, marker: str) -> int:
    listed = client.list_project_tasks()
    _assert(listed.outcome is Outcome.CONFIRMED, f"list for create_count failed: {listed.outcome}")
    count = 0
    for item in listed.details.get("items") or []:
        extracted = extract_marker(item.get("description"))
        if extracted and extracted["marker"] == marker:
            count += 1
    return count


def _journal_path(stand: IsolatedStand) -> Path:
    path = stand.run_dir / "journal" / "integration.sqlite3"
    path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path.parent, 0o700)
    return path


class _FaultProxyState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.create_seen = 0
        self.forwarded = 0
        self.upstream_status: int | None = None
        self.upstream_id: int | None = None
        self.client_timeout_observed = False
        self.delay_s = 12.0
        self.upstream_host = "127.0.0.1"
        self.upstream_port = 19345
        self.events: list[dict[str, Any]] = []


def _start_fault_proxy(
    *,
    listen_host: str,
    listen_port: int,
    upstream_host: str,
    upstream_port: int,
    delay_s: float,
) -> tuple[ThreadingHTTPServer, threading.Thread, _FaultProxyState]:
    state = _FaultProxyState()
    state.delay_s = delay_s
    state.upstream_host = upstream_host
    state.upstream_port = upstream_port

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: object) -> None:
            return

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return b""
            return self.rfile.read(length)

        def _proxy(self, method: str) -> None:
            body = self._read_body() if method in {"PUT", "POST", "PATCH"} else b""
            path = self.path
            is_create = method == "PUT" and "/projects/" in path and path.rstrip("/").endswith("/tasks")
            if is_create:
                with state.lock:
                    state.create_seen += 1
                    state.events.append({"event": "create_received", "t": time.monotonic()})
                # Hold before upstream so the client hits its HTTP deadline first.
                time.sleep(state.delay_s)
                with state.lock:
                    state.events.append({"event": "forwarding", "t": time.monotonic()})
            try:
                upstream = socket.create_connection((state.upstream_host, state.upstream_port), timeout=5)
            except OSError as exc:
                payload = json.dumps({"error": f"upstream_connect:{exc}"}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            try:
                req_lines = [f"{method} {path} HTTP/1.1\r\n".encode()]
                host_hdr = f"{state.upstream_host}:{state.upstream_port}"
                skip = {"host", "content-length", "connection", "transfer-encoding"}
                for key, value in self.headers.items():
                    if key.lower() in skip:
                        continue
                    req_lines.append(f"{key}: {value}\r\n".encode())
                req_lines.append(f"Host: {host_hdr}\r\n".encode())
                req_lines.append(f"Content-Length: {len(body)}\r\n".encode())
                req_lines.append(b"Connection: close\r\n\r\n")
                upstream.sendall(b"".join(req_lines) + body)
                chunks: list[bytes] = []
                while True:
                    block = upstream.recv(65536)
                    if not block:
                        break
                    chunks.append(block)
                raw = b"".join(chunks)
            finally:
                upstream.close()
            if is_create:
                with state.lock:
                    state.forwarded += 1
                    state.events.append({"event": "upstream_done", "t": time.monotonic(), "bytes": len(raw)})
                # Parse upstream status/id for evidence.
                try:
                    header, _, resp_body = raw.partition(b"\r\n\r\n")
                    status_line = header.split(b"\r\n", 1)[0].decode("latin1")
                    parts = status_line.split()
                    status = int(parts[1]) if len(parts) >= 2 else 0
                    parsed = json.loads(resp_body.decode("utf-8") or "null")
                    with state.lock:
                        state.upstream_status = status
                        if isinstance(parsed, dict) and isinstance(parsed.get("id"), int):
                            state.upstream_id = parsed["id"]
                except Exception:
                    pass
            # Return upstream bytes to the (likely already timed-out) client.
            self.wfile.write(raw)
            try:
                self.wfile.flush()
            except BrokenPipeError:
                with state.lock:
                    state.client_timeout_observed = True

        def do_GET(self) -> None:  # noqa: N802
            self._proxy("GET")

        def do_PUT(self) -> None:  # noqa: N802
            self._proxy("PUT")

        def do_POST(self) -> None:  # noqa: N802
            self._proxy("POST")

        def do_PATCH(self) -> None:  # noqa: N802
            self._proxy("PATCH")

        def do_DELETE(self) -> None:  # noqa: N802
            self._proxy("DELETE")

    # Fail closed if the fault port is already taken (do not kill the owner).
    probe = socket.socket()
    try:
        if probe.connect_ex((listen_host, listen_port)) == 0:
            raise StandError(f"fault proxy port {listen_port} already in use")
    finally:
        probe.close()

    server = ThreadingHTTPServer((listen_host, listen_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, state


def _stop_fault_proxy(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _child_script() -> str:
    return r"""
import json, os, signal, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
from vikunja_local.api import TokenRef, VikunjaClient
from vikunja_local.dispatcher import IntegrationDispatcher
from vikunja_local.journal import IntegrationJournal

mode = sys.argv[1]
cfg = json.loads(sys.argv[2])
ready = Path(cfg["ready"])
journal_path = Path(cfg["journal_path"])
crash_point = cfg.get("crash_point")
sync_gate = cfg.get("sync_gate")
sync_point = cfg.get("sync_point") or "before_http"

def crash(point: str) -> None:
    if sync_gate and point == sync_point:
        ready.write_text(json.dumps({"pid": os.getpid(), "point": point, "waiting": True}), encoding="utf-8")
        gate = Path(sync_gate)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if gate.exists():
                break
            time.sleep(0.02)
        else:
            raise SystemExit("sync_gate timeout")
        return
    if crash_point and point == crash_point:
        ready.write_text(json.dumps({"pid": os.getpid(), "point": point}), encoding="utf-8")
        os.kill(os.getpid(), signal.SIGKILL)

journal = IntegrationJournal(journal_path)
journal.open()
client = VikunjaClient(
    api_origin=cfg["api_origin"],
    project_id=int(cfg["project_id"]),
    token_ref=TokenRef(kind="file", path=Path(cfg["token_path"])),
    page_size=2,
)
dispatcher = IntegrationDispatcher(journal, client, link_callback=None, crash_hook=crash)

if mode == "create":
    if crash_point == "after_receipt_before_link":
        link_ready = Path(cfg["link_ready"])
        def link_cb(payload):
            crash("after_receipt_before_link")
            link_ready.write_text(json.dumps({"linked": True, **payload}), encoding="utf-8")
            return {"ok": True, "production_bridge": False}
        dispatcher.link_callback = link_cb
    result = dispatcher.execute_create(
        issuer_id=cfg["issuer_id"],
        operation_id=cfg["operation_id"],
        title=cfg["title"],
        user_portion=cfg.get("user_portion", ""),
    )
    ready.write_text(json.dumps({
        "pid": os.getpid(),
        "outcome": result.outcome.value,
        "vikunja_id": result.vikunja_id,
        "create_http_attempts": result.create_http_attempts,
        "linked": result.linked,
        "operation_state": None if result.operation is None else result.operation.state.value,
    }), encoding="utf-8")
elif mode == "recover":
    # Never re-arm crash hooks during recovery processes.
    dispatcher.crash_hook = None
    result = dispatcher.recover_create(cfg["issuer_id"], cfg["operation_id"])
    if cfg.get("link_ready") and result.ok and not result.linked:
        def link_cb(payload):
            Path(cfg["link_ready"]).write_text(json.dumps({"linked": True, **payload}), encoding="utf-8")
            return {"ok": True, "production_bridge": False}
        dispatcher.link_callback = link_cb
        result = dispatcher.recover_create(cfg["issuer_id"], cfg["operation_id"])
    ready.write_text(json.dumps({
        "pid": os.getpid(),
        "outcome": result.outcome.value,
        "vikunja_id": result.vikunja_id,
        "create_http_attempts": result.create_http_attempts,
        "linked": result.linked,
        "details": {k: v for k, v in (result.details or {}).items() if k != "task"},
    }), encoding="utf-8")
else:
    raise SystemExit(f"unknown mode {mode}")
journal.close()
"""


def _spawn_child(cfg: dict[str, Any], *, mode: str) -> subprocess.Popen[Any]:
    env = os.environ.copy()
    env["EA010_WORKTREE"] = str(ROOT)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.Popen(
        [sys.executable, "-c", _child_script(), mode, json.dumps(cfg)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_ready(path: Path, proc: subprocess.Popen[Any], timeout: float = 60.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size > 0:
            return json.loads(path.read_text(encoding="utf-8"))
        if proc.poll() is not None and not path.exists():
            out, err = proc.communicate(timeout=1)
            raise StandError(f"child exited early rc={proc.returncode} stdout={out!r} stderr={err!r}")
        time.sleep(0.05)
    raise StandError(f"timeout waiting for ready file {path}")


def _wait_proc(proc: subprocess.Popen[Any], timeout: float = 60.0) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise StandError("child process timeout")


def run_recovery_scenarios(stand: IsolatedStand) -> dict[str, Any]:
    session = stand.create_synthetic_session()
    project_id = int(session["project_id"])
    write_token = _create_api_token(
        stand,
        title=stand.fixture_name("recovery-write"),
        permissions=WRITE_PERMS,
    )
    read_token = _create_api_token(
        stand,
        title=stand.fixture_name("recovery-read"),
        permissions=READ_PERMS,
    )
    token_path = _store_token(stand, "recovery-write.token", write_token["token"])
    read_token_path = _store_token(stand, "recovery-read.token", read_token["token"])
    stand._token_secrets["recovery_write"] = write_token["token"]
    stand._token_secrets["recovery_read"] = read_token["token"]
    stand.manifest.setdefault("token_handles", []).append("recovery_write")
    stand.manifest.setdefault("token_handles", []).append("recovery_read")
    stand.synthetic_ids["recovery_write_token_id"] = write_token.get("id")
    stand.synthetic_ids["recovery_read_token_id"] = read_token.get("id")
    stand._save_secrets()
    stand._save_manifest()

    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    client = _client_for(stand, token_path, project_id, origin=origin, page_size=1)
    read_client = _client_for(stand, read_token_path, project_id, origin=origin, page_size=1)
    journal_path = _journal_path(stand)
    journal = IntegrationJournal(journal_path)
    journal.open()
    issuer = journal.ensure_issuer(new_issuer_id())

    link_log: list[dict[str, Any]] = []

    def link_cb(payload: dict[str, Any]) -> dict[str, Any]:
        entry = {"ok": True, "production_bridge": False, "vikunja_id": payload.get("vikunja_id")}
        link_log.append(dict(payload))
        return entry

    dispatcher = IntegrationDispatcher(journal, client, link_callback=link_cb)
    scenarios: dict[str, Any] = {}
    synthetic_ids: dict[str, Any] = {
        "project_id": project_id,
        "issuer_id": issuer,
        "recovery_write_token_id": write_token.get("id"),
        "recovery_read_token_id": read_token.get("id"),
    }

    # --- happy path create + controlled link callback ---
    op_ok = new_operation_id()
    title_ok = stand.fixture_name("recovery-ok")
    created = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=op_ok,
        title=title_ok,
        user_portion="recovery context",
    )
    _assert(created.ok and created.vikunja_id is not None, f"create failed: {created.outcome} {created.details}")
    _assert(created.linked is True, "controlled link callback must run")
    _assert(
        (created.link_result or {}).get("production_bridge") is False,
        "must not claim production Hermes bridge",
    )
    marker_ok = build_marker(issuer, op_ok)
    _assert(_count_exact_marker(client, marker_ok) == 1, "happy path create_count must be 1")
    synthetic_ids["happy_task_id"] = created.vikunja_id
    synthetic_ids["happy_operation_id"] = op_ok
    scenarios["happy_create_link"] = {
        "vikunja_id": created.vikunja_id,
        "create_http_attempts": created.create_http_attempts,
        "linked": created.linked,
        "production_bridge": False,
    }

    # --- replay same key: one ID ---
    replay = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=op_ok,
        title=title_ok,
        user_portion="recovery context",
    )
    _assert(replay.ok and replay.vikunja_id == created.vikunja_id, "replay must return same ID")
    _assert(_count_exact_marker(client, marker_ok) == 1, "replay must not create duplicate")
    scenarios["replay_same_key"] = {
        "vikunja_id": replay.vikunja_id,
        "create_http_attempts": replay.create_http_attempts,
    }

    # --- distinct intents with identical title → distinct IDs ---
    title_same = stand.fixture_name("same-title")
    op_a = new_operation_id()
    op_b = new_operation_id()
    a = dispatcher.execute_create(issuer_id=issuer, operation_id=op_a, title=title_same, user_portion="a")
    b = dispatcher.execute_create(issuer_id=issuer, operation_id=op_b, title=title_same, user_portion="b")
    _assert(a.ok and b.ok, f"distinct creates failed: {a.outcome}/{b.outcome}")
    _assert(a.vikunja_id != b.vikunja_id, "distinct intents must get distinct IDs")
    synthetic_ids["same_title_ids"] = [a.vikunja_id, b.vikunja_id]
    scenarios["distinct_same_title"] = {
        "title": title_same,
        "ids": [a.vikunja_id, b.vikunja_id],
        "markers": [build_marker(issuer, op_a), build_marker(issuer, op_b)],
    }

    # --- substring decoy must not exact-match ---
    decoy_op = new_operation_id()
    decoy_marker = build_marker(issuer, decoy_op)
    decoy_title = stand.fixture_name("decoy")
    # Plant a bait description that contains the marker as a substring suffix, not exact header.
    status, decoy_body = stand.api(
        f"projects/{project_id}/tasks",
        "PUT",
        {
            "title": decoy_title,
            "description": f"prefix-{decoy_marker}-suffix\nnot-an-exact-header",
        },
        token=write_token["token"],
    )
    _assert(status == 201, f"decoy create HTTP {status}")
    decoy_id = int(decoy_body["id"]) if isinstance(decoy_body, dict) else None
    synthetic_ids["decoy_task_id"] = decoy_id
    scanned = reconcile_create_marker(client, issuer_id=issuer, operation_id=decoy_op)
    _assert(scanned.outcome is Outcome.EMPTY, f"decoy must not exact-match, got {scanned.outcome}")
    # Real create for that op still allowed only via first dispatch (not yet dispatched).
    real = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=decoy_op,
        title=stand.fixture_name("decoy-real"),
        user_portion="real",
    )
    _assert(real.ok and real.vikunja_id != decoy_id, "exact create must ignore substring decoy")
    _assert(_count_exact_marker(client, decoy_marker) == 1, "exact marker create_count=1")
    scenarios["substring_decoy_ignored"] = {
        "decoy_id": decoy_id,
        "real_id": real.vikunja_id,
        "scan_before_create": scanned.outcome.value,
    }

    # --- multiple exact matches → conflict, no create ---
    multi_issuer, multi_op = new_operation_ids()
    multi_marker = build_marker(multi_issuer, multi_op)
    # Force two tasks with the same exact marker header (synthetic corruption).
    for idx in range(2):
        st, body = stand.api(
            f"projects/{project_id}/tasks",
            "PUT",
            {
                "title": stand.fixture_name(f"multi-{idx}"),
                "description": multi_marker + "\n" + f"u{idx}",
            },
            token=write_token["token"],
        )
        _assert(st == 201, f"multi seed HTTP {st}")
    journal2_path = stand.run_dir / "journal" / "multi.sqlite3"
    journal2 = IntegrationJournal(journal2_path)
    journal2.open()
    journal2.ensure_issuer(multi_issuer)
    journal2.prepare_intent(
        issuer_id=multi_issuer,
        operation_id=multi_op,
        op_type="create",
        project_id=project_id,
        payload={"title": "multi", "user_portion": ""},
    )
    with journal2.dispatch_critical_section() as section:
        section.begin_dispatch(multi_issuer, multi_op)
        section.mark_unknown(multi_issuer, multi_op, reason="synthetic_multi")
    disp2 = IntegrationDispatcher(journal2, client)
    multi_rec = disp2.recover_create(multi_issuer, multi_op)
    _assert(multi_rec.outcome is Outcome.CONFLICT, f"expected conflict, got {multi_rec.outcome}")
    _assert(multi_rec.details.get("forbid_create") is True or multi_rec.operation is not None, "conflict retained")
    after_multi = journal2.get_operation(multi_issuer, multi_op)
    _assert(after_multi is not None and after_multi.state is OperationState.CONFLICT, "durable conflict")
    # ensure no additional create for this marker via recover path
    count_multi = _count_exact_marker(client, multi_marker)
    _assert(count_multi == 2, f"conflict path must not create, count={count_multi}")
    journal2.close()
    scenarios["multiple_exact_conflict"] = {
        "marker": multi_marker,
        "count": count_multi,
        "outcome": multi_rec.outcome.value,
    }

    # --- zero after unknown / corrupted marker: keep unknown, forbid create ---
    zero_op = new_operation_id()
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=zero_op,
        op_type="create",
        project_id=project_id,
        payload={"title": stand.fixture_name("zero"), "user_portion": ""},
    )
    with journal.dispatch_critical_section() as section:
        section.begin_dispatch(issuer, zero_op)
        section.mark_unknown(issuer, zero_op, reason="synthetic_timeout_no_write")
    zero = dispatcher.recover_create(issuer, zero_op)
    _assert(zero.outcome is Outcome.UNKNOWN, f"zero reconcile must stay unknown, got {zero.outcome}")
    _assert(zero.details.get("forbid_create") is True, "zero must forbid create")
    zero_row = journal.get_operation(issuer, zero_op)
    _assert(zero_row is not None and zero_row.state is OperationState.UNKNOWN, "durable unknown")
    # Attempted execute_create must still reconcile, not mint a new task.
    zero_retry = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=zero_op,
        title=stand.fixture_name("zero"),
        user_portion="",
    )
    _assert(zero_retry.outcome is Outcome.UNKNOWN, "retry after zero must not create")
    _assert(_count_exact_marker(client, build_marker(issuer, zero_op)) == 0, "zero path create_count=0")
    scenarios["zero_after_unknown_forbids_create"] = {
        "operation_id": zero_op,
        "outcome": zero.outcome.value,
        "retry_outcome": zero_retry.outcome.value,
    }

    # --- confirmed create then delete/corrupt: recover ORIGINAL op, no fresh backlink ---
    corr_op = new_operation_id()
    links_before_corrupt = len(link_log)
    corr = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=corr_op,
        title=stand.fixture_name("corrupt-me"),
        user_portion="will-corrupt",
    )
    _assert(corr.ok and corr.vikunja_id is not None, "corrupt seed create failed")
    _assert(len(link_log) == links_before_corrupt + 1, "seed create must invoke link once")
    del_status, _ = stand.api(f"tasks/{corr.vikunja_id}", "DELETE", token=stand._session_token)
    _assert(del_status == 200, f"delete confirmed task HTTP {del_status}")
    synthetic_ids["corrupted_or_deleted_task_id"] = corr.vikunja_id
    deleted_rec = dispatcher.recover_create(issuer, corr_op)
    _assert(deleted_rec.outcome is Outcome.UNKNOWN, f"deleted confirmed must not stay confirmed, got {deleted_rec.outcome}")
    _assert(deleted_rec.linked is False, "deleted confirmed must not report linked")
    _assert(deleted_rec.details.get("callback_skipped") is True, "deleted confirmed must skip callback")
    _assert(deleted_rec.details.get("receipt_preserved") is True, "durable receipt must remain")
    _assert(len(link_log) == links_before_corrupt + 1, "recover after delete must not re-link")
    row_del = journal.get_operation(issuer, corr_op)
    _assert(row_del is not None and row_del.state is OperationState.CONFIRMED, "receipt history preserved")
    _assert(row_del.receipt is not None and int(row_del.receipt["vikunja_id"]) == corr.vikunja_id, "receipt id preserved")

    # Marker corruption on a still-present confirmed task: same original operation.
    corr2_op = new_operation_id()
    links_before_marker = len(link_log)
    corr2 = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=corr2_op,
        title=stand.fixture_name("corrupt-marker"),
        user_portion="will-corrupt-marker",
    )
    _assert(corr2.ok and corr2.vikunja_id is not None, "marker-corrupt seed create failed")
    # Raw stock write (not client.update): client.update rebuilds/preserves M.
    st, _ = stand.api(
        f"tasks/{corr2.vikunja_id}",
        "POST",
        {"description": "marker-removed-by-hand"},
        token=stand._session_token,
    )
    if st not in (200, 201):
        st, _ = stand.api(
            f"tasks/{corr2.vikunja_id}",
            "PATCH",
            {"description": "marker-removed-by-hand"},
            token=stand._session_token,
        )
        _assert(st in (200, 201, 204), f"marker corrupt raw patch HTTP {st}")
    marker_lost = dispatcher.recover_create(issuer, corr2_op)
    _assert(marker_lost.outcome is Outcome.UNKNOWN, f"corrupted marker must not confirm, got {marker_lost.outcome}")
    _assert(marker_lost.linked is False, "corrupted marker must not report linked")
    _assert(marker_lost.details.get("callback_skipped") is True, "corrupted marker must skip callback")
    _assert(len(link_log) == links_before_marker + 1, "recover after corruption must not re-link")
    scenarios["marker_corrupt_or_deleted"] = {
        "mode": "original_operation_after_delete_and_corrupt",
        "deleted_task_id": corr.vikunja_id,
        "deleted_outcome": deleted_rec.outcome.value,
        "corrupted_task_id": corr2.vikunja_id,
        "corrupted_outcome": marker_lost.outcome.value,
        "callback_invocations_after": len(link_log) - links_before_corrupt,
    }

    # --- unknown recurring complete: observe without re-sending / due shift ---
    rec_op = new_operation_id()
    due = confirm_date_only("2026-12-15", confirmed_at=datetime(2026, 11, 10, 10, 0, tzinfo=MSK))
    rec_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=rec_op,
        title=stand.fixture_name("recurring"),
        user_portion="recurring",
        due=due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(rec_create.ok and rec_create.vikunja_id is not None, "recurring create failed")
    rec_id = int(rec_create.vikunja_id)
    synthetic_ids["recurring_task_id"] = rec_id
    due_before = (rec_create.task or {}).get("due_date")
    complete_op = new_operation_id()
    # Prepare + begin_dispatch + mark unknown without PATCH (simulate lost response).
    pre = client.read(rec_id)
    _assert(pre.ok and pre.task is not None, "recurring preimage read failed")
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=complete_op,
        op_type="complete",
        project_id=project_id,
        target_id=rec_id,
        payload={
            "patch": {"done": True},
            "preimage": {
                "id": pre.task.get("id"),
                "due_date": pre.task.get("due_date"),
                "done": pre.task.get("done"),
                "description": pre.task.get("description"),
                "repeat_after": pre.task.get("repeat_after"),
                "repeat_mode": pre.task.get("repeat_mode"),
            },
        },
    )
    with journal.dispatch_critical_section() as section:
        section.begin_dispatch(issuer, complete_op)
        section.mark_unknown(issuer, complete_op, reason="simulated_lost_complete")
    before_obs = client.read(rec_id)
    _assert(before_obs.ok and before_obs.task is not None, "read before observe failed")
    due_mid = before_obs.task.get("due_date")
    observed = dispatcher.recover_unknown_mutation(issuer, complete_op)
    _assert(observed.outcome is Outcome.UNKNOWN, f"unknown recurring must stay unknown, got {observed.outcome}")
    _assert(observed.details.get("resend_forbidden") is True, "resend must be forbidden")
    after_obs = client.read(rec_id)
    _assert(after_obs.ok and after_obs.task is not None, "read after observe failed")
    _assert(after_obs.task.get("due_date") == due_mid, "observe must not shift recurring due")
    # Second recover still must not PATCH.
    observed2 = dispatcher.recover_unknown_mutation(issuer, complete_op)
    _assert(observed2.outcome is Outcome.UNKNOWN, "second observe must remain unknown")
    after2 = client.read(rec_id)
    _assert(after2.ok and after2.task is not None, "second after read failed")
    _assert(after2.task.get("due_date") == due_mid, "second observe must not shift due")
    scenarios["unknown_recurring_no_due_shift"] = {
        "task_id": rec_id,
        "due_before": due_before,
        "due_observed": due_mid,
        "due_after": after2.task.get("due_date"),
        "outcome": observed.outcome.value,
    }

    # --- F2: successful recurring complete persists full occurrence witness ---
    f2_wit_due = confirm_date_only("2026-12-20", confirmed_at=datetime(2026, 11, 10, 10, 0, tzinfo=MSK))
    f2_wit_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        title=stand.fixture_name("recurring-witness"),
        user_portion="recurring-witness",
        due=f2_wit_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(
        f2_wit_create.ok and f2_wit_create.vikunja_id is not None,
        "F2 witness recurring create failed",
    )
    f2_wit_id = int(f2_wit_create.vikunja_id)
    f2_wit_op = new_operation_id()
    f2_wit_complete = dispatcher.execute_complete(
        issuer_id=issuer,
        operation_id=f2_wit_op,
        task_id=f2_wit_id,
    )
    _assert(
        f2_wit_complete.ok,
        f"F2 witness recurring complete failed: {f2_wit_complete.outcome} {f2_wit_complete.details}",
    )
    f2_wit_row = journal.get_operation(issuer, f2_wit_op)
    _assert(
        f2_wit_row is not None and f2_wit_row.state is OperationState.CONFIRMED,
        "F2 witness must leave durable confirmed",
    )
    f2_wit_pre = (f2_wit_row.payload or {}).get("preimage") or {}
    for key in ("done_at", "start_date", "end_date", "reminders"):
        _assert(key in f2_wit_pre, f"F2 durable preimage missing {key}")
    f2_wit_effect = (f2_wit_row.receipt or {}).get("effect") or {}
    for key in ("preimage", "postimage", "occurrence"):
        _assert(key in f2_wit_effect, f"F2 receipt effect missing {key}")
        _assert("done_at" in f2_wit_effect[key], f"F2 effect.{key} missing done_at")
        _assert("reminders" in f2_wit_effect[key], f"F2 effect.{key} missing reminders")
    # Process-loss reopen must retain the same witness without reconstruction.
    journal.close()
    journal_reopen = IntegrationJournal(journal_path)
    journal_reopen.open()
    f2_wit_again = journal_reopen.get_operation(issuer, f2_wit_op)
    _assert(f2_wit_again is not None and f2_wit_again.receipt is not None, "F2 reopen missing receipt")
    f2_wit_again_effect = (f2_wit_again.receipt or {}).get("effect") or {}
    _assert(
        f2_wit_again_effect.get("occurrence") == f2_wit_effect.get("occurrence"),
        "F2 reopened occurrence must match",
    )
    _assert(
        f2_wit_again_effect.get("postimage") == f2_wit_effect.get("postimage"),
        "F2 reopened postimage must match",
    )
    _assert(
        "done_at" in ((f2_wit_again.payload or {}).get("preimage") or {}),
        "F2 reopened preimage must retain done_at",
    )
    journal = journal_reopen
    dispatcher = IntegrationDispatcher(journal, client, link_callback=link_cb)
    synthetic_ids["recurring_witness_task_id"] = f2_wit_id
    scenarios["recurring_complete_occurrence_witness"] = {
        "task_id": f2_wit_id,
        "operation_id": f2_wit_op,
        "preimage_keys": sorted(f2_wit_pre.keys()),
        "effect_keys": sorted(f2_wit_effect.keys()),
        "occurrence_keys": sorted((f2_wit_effect.get("occurrence") or {}).keys()),
        "reopened": True,
    }

    # --- fail-closed: revoked token / incomplete scan ---
    # Read-only incomplete: use a client with impossible page budget by closing journal writes.
    denied = client.update(rec_id, {"title": "should-fail-if-revoked-later"})
    # Journal unavailable: close and attempt create with new dispatcher pointing at closed journal.
    # Journal writes refused (quarantine): dispatcher must not create.
    q_path = stand.run_dir / "journal" / "quarantine.sqlite3"
    q_journal = IntegrationJournal(q_path)
    q_journal.open()
    q_issuer = q_journal.ensure_issuer(new_issuer_id())
    q_journal.quarantine()
    q_disp = IntegrationDispatcher(q_journal, client)
    q_op = new_operation_id()
    q_marker = build_marker(q_issuer, q_op)
    q_result = None
    q_exc = None
    try:
        q_result = q_disp.execute_create(
            issuer_id=q_issuer,
            operation_id=q_op,
            title=stand.fixture_name("quarantined-journal"),
        )
    except Exception as exc:  # noqa: BLE001
        q_exc = type(exc).__name__
    q_count = _count_exact_marker(client, q_marker)
    _assert(q_count == 0, f"quarantined journal must not create, count={q_count}")
    if q_exc is None:
        _assert(q_result is not None, "quarantine path must return a result")
        _assert(
            q_result.vikunja_id is None
            and q_result.outcome
            in {
                Outcome.DEPENDENCY_UNAVAILABLE,
                Outcome.UNKNOWN,
                Outcome.VALIDATION_ERROR,
                Outcome.CONFLICT,
            },
            f"quarantined journal must fail closed, got {q_result.outcome}",
        )
        q_outcome = q_result.outcome.value
    else:
        q_outcome = q_exc
    q_journal.close()
    scenarios["fail_closed_journal"] = {
        "closed_outcome": q_outcome,
        "raised": q_exc,
        "create_count": q_count,
    }

    # Auth/rights path: dispatcher-level read-only create/update (not scan-only).
    bad_path = _store_token(stand, "bad.token", "ea010-bad-" + uuid.uuid4().hex)
    bad_client = _client_for(stand, bad_path, project_id, origin=origin)
    auth_scan = reconcile_create_marker(bad_client, issuer_id=issuer, operation_id=new_operation_id())
    _assert(
        auth_scan.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED, Outcome.DEPENDENCY_UNAVAILABLE},
        f"bad token scan must fail closed, got {auth_scan.outcome}",
    )
    read_disp = IntegrationDispatcher(journal, read_client)
    ro_create_op = new_operation_id()
    ro_create = read_disp.execute_create(
        issuer_id=issuer,
        operation_id=ro_create_op,
        title=stand.fixture_name("readonly-create"),
        user_portion="ro",
    )
    _assert(
        ro_create.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED},
        f"read-only create must stay typed refusal, got {ro_create.outcome} {ro_create.details}",
    )
    _assert(ro_create.details.get("definitive_refusal") is True, "read-only create must mark definitive_refusal")
    _assert(ro_create.vikunja_id is None, "read-only create must not mint an ID")
    _assert(_count_exact_marker(client, build_marker(issuer, ro_create_op)) == 0, "read-only create must not write")
    ro_create_retry = read_disp.execute_create(
        issuer_id=issuer,
        operation_id=ro_create_op,
        title=stand.fixture_name("readonly-create"),
        user_portion="ro",
    )
    _assert(
        ro_create_retry.outcome == ro_create.outcome,
        f"read-only create retry must preserve refusal class, got {ro_create_retry.outcome}",
    )
    _assert(ro_create_retry.details.get("resend_forbidden") is True, "read-only create retry must forbid resend")
    _assert(_count_exact_marker(client, build_marker(issuer, ro_create_op)) == 0, "retry must not create")

    ro_target_op = new_operation_id()
    ro_target = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=ro_target_op,
        title=stand.fixture_name("readonly-update-target"),
        user_portion="rou",
    )
    _assert(ro_target.ok and ro_target.vikunja_id is not None, "read-only update target create failed")
    ro_target_id = int(ro_target.vikunja_id)
    synthetic_ids["readonly_update_target_id"] = ro_target_id
    title_before_ro = (client.read(ro_target_id).task or {}).get("title")
    ro_update_op = new_operation_id()
    ro_update = read_disp.execute_update(
        issuer_id=issuer,
        operation_id=ro_update_op,
        task_id=ro_target_id,
        patch={"title": stand.fixture_name("readonly-should-not-apply")},
    )
    _assert(
        ro_update.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED},
        f"read-only update must stay typed refusal, got {ro_update.outcome} {ro_update.details}",
    )
    _assert(ro_update.details.get("definitive_refusal") is True, "read-only update must mark definitive_refusal")
    title_after_ro = (client.read(ro_target_id).task or {}).get("title")
    _assert(title_after_ro == title_before_ro, "read-only update must not mutate title")
    ro_update_retry = read_disp.execute_update(
        issuer_id=issuer,
        operation_id=ro_update_op,
        task_id=ro_target_id,
        patch={"title": stand.fixture_name("readonly-should-not-apply")},
    )
    _assert(
        ro_update_retry.outcome == ro_update.outcome,
        f"read-only update retry must preserve refusal, got {ro_update_retry.outcome}",
    )
    _assert((client.read(ro_target_id).task or {}).get("title") == title_before_ro, "retry must not mutate")
    scenarios["fail_closed_rights"] = {
        "bad_token_scan": auth_scan.outcome.value,
        "readonly_create": ro_create.outcome.value,
        "readonly_create_retry": ro_create_retry.outcome.value,
        "readonly_update": ro_update.outcome.value,
        "readonly_update_retry": ro_update_retry.outcome.value,
        "readonly_update_target_id": ro_target_id,
    }

    # --- successful update/complete same-key replay keeps identity (R1) ---
    mut_create_op = new_operation_id()
    mut_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=mut_create_op,
        title=stand.fixture_name("mutation-target"),
        user_portion="mutation",
    )
    _assert(mut_create.ok and mut_create.vikunja_id is not None, "mutation target create failed")
    mut_id = int(mut_create.vikunja_id)
    synthetic_ids["mutation_replay_task_id"] = mut_id
    update_op = new_operation_id()
    upd1 = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=update_op,
        task_id=mut_id,
        patch={"title": stand.fixture_name("mutation-updated")},
    )
    _assert(upd1.ok, f"first update failed: {upd1.outcome} {upd1.details}")
    upd2 = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=update_op,
        task_id=mut_id,
        patch={"title": stand.fixture_name("mutation-updated")},
    )
    _assert(upd2.ok and upd2.vikunja_id == mut_id, f"update replay must confirm same ID, got {upd2.outcome}")
    _assert(upd2.details.get("path") == "mutation_replay", f"update replay path, got {upd2.details}")
    complete_replay_op = new_operation_id()
    cmp1 = dispatcher.execute_complete(
        issuer_id=issuer,
        operation_id=complete_replay_op,
        task_id=mut_id,
    )
    _assert(cmp1.ok, f"first complete failed: {cmp1.outcome}")
    cmp2 = dispatcher.execute_complete(
        issuer_id=issuer,
        operation_id=complete_replay_op,
        task_id=mut_id,
    )
    _assert(cmp2.ok and cmp2.vikunja_id == mut_id, f"complete replay must confirm, got {cmp2.outcome}")
    _assert(cmp2.details.get("path") == "mutation_replay", "complete replay path")
    # Unknown mutation retry must stay observe-only (no second PATCH).
    unknown_mut_op = new_operation_id()
    pre_u = client.read(mut_id)
    _assert(pre_u.ok and pre_u.task is not None, "unknown mutation preimage failed")
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=unknown_mut_op,
        op_type="update",
        project_id=project_id,
        target_id=mut_id,
        payload={
            "patch": {"title": "should-not-resend"},
            "preimage": {
                "id": pre_u.task.get("id"),
                "title": pre_u.task.get("title"),
                "due_date": pre_u.task.get("due_date"),
                "done": pre_u.task.get("done"),
                "description": pre_u.task.get("description"),
                "repeat_after": pre_u.task.get("repeat_after"),
                "repeat_mode": pre_u.task.get("repeat_mode"),
            },
        },
    )
    with journal.dispatch_critical_section() as section:
        section.begin_dispatch(issuer, unknown_mut_op)
        section.mark_unknown(issuer, unknown_mut_op, reason="lost_update")
    title_before_unknown = (client.read(mut_id).task or {}).get("title")
    unknown_retry = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=unknown_mut_op,
        task_id=mut_id,
        patch={"title": "should-not-resend"},
    )
    _assert(unknown_retry.outcome is Outcome.UNKNOWN, f"unknown update retry must stay unknown, got {unknown_retry.outcome}")
    _assert(unknown_retry.details.get("resend_forbidden") is True, "unknown update retry must forbid resend")
    title_after_unknown = (client.read(mut_id).task or {}).get("title")
    _assert(title_after_unknown == title_before_unknown, "unknown update retry must not PATCH again")
    scenarios["mutation_replay_stable_identity"] = {
        "task_id": mut_id,
        "update_replay": upd2.outcome.value,
        "complete_replay": cmp2.outcome.value,
        "unknown_retry": unknown_retry.outcome.value,
    }

    # --- F1: changed patch / source / project must not false-confirm replay ---
    f1_create_op = new_operation_id()
    f1_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=f1_create_op,
        title=stand.fixture_name("f1-envelope-target"),
        user_portion="f1",
    )
    _assert(f1_create.ok and f1_create.vikunja_id is not None, "F1 target create failed")
    f1_task_id = int(f1_create.vikunja_id)
    synthetic_ids["f1_envelope_task_id"] = f1_task_id
    f1_title_a = stand.fixture_name("f1-title-a")

    f1_patch_op = new_operation_id()
    f1_patch_first = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=f1_patch_op,
        task_id=f1_task_id,
        patch={"title": f1_title_a},
    )
    _assert(f1_patch_first.ok, f"F1 patch first update failed: {f1_patch_first.outcome}")
    f1_changed = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=f1_patch_op,
        task_id=f1_task_id,
        patch={"title": stand.fixture_name("f1-title-b")},
    )
    _assert(
        f1_changed.outcome is Outcome.CONFLICT,
        f"changed patch replay must conflict, got {f1_changed.outcome} {f1_changed.details}",
    )
    live_title = (client.read(f1_task_id).task or {}).get("title")
    _assert(live_title == f1_title_a, f"changed patch must not apply; live={live_title!r}")
    f1_patch_row = journal.get_operation(issuer, f1_patch_op)
    _assert(
        f1_patch_row is not None and f1_patch_row.state is OperationState.CONFLICT,
        f"F1 changed-patch must leave durable conflict, got {getattr(f1_patch_row, 'state', None)}",
    )

    f1_source_op = new_operation_id()
    f1_source_title = stand.fixture_name("f1-source-title")
    f1_source_first = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=f1_source_op,
        task_id=f1_task_id,
        patch={"title": f1_source_title},
        source_kind="meeting",
        source_key="synth-f1-meeting",
    )
    _assert(f1_source_first.ok, f"F1 source first update failed: {f1_source_first.outcome}")
    f1_source = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=f1_source_op,
        task_id=f1_task_id,
        patch={"title": f1_source_title},
        source_kind="meeting",
        source_key="synth-f1-meeting-OTHER",
    )
    _assert(
        f1_source.outcome is Outcome.CONFLICT,
        f"changed source replay must conflict, got {f1_source.outcome} {f1_source.details}",
    )
    f1_source_row = journal.get_operation(issuer, f1_source_op)
    _assert(
        f1_source_row is not None and f1_source_row.state is OperationState.CONFLICT,
        "F1 changed-source must leave durable conflict",
    )

    f1_proj_op = new_operation_id()
    f1_proj_title = stand.fixture_name("f1-project-title")
    f1_proj_first = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=f1_proj_op,
        task_id=f1_task_id,
        patch={"title": f1_proj_title},
    )
    _assert(f1_proj_first.ok, f"F1 project first update failed: {f1_proj_first.outcome}")
    st_f1p, f1_other_proj = stand.api(
        "projects",
        "PUT",
        {"title": stand.fixture_name("f1-other-project")},
        token=stand._session_token,
    )
    _assert(st_f1p == 201 and isinstance(f1_other_proj, dict), f"F1 other project HTTP {st_f1p}")
    f1_other_project_id = int(f1_other_proj["id"])
    synthetic_ids["f1_other_project_id"] = f1_other_project_id
    f1_foreign_client = _client_for(stand, token_path, f1_other_project_id, origin=origin, page_size=1)
    f1_foreign_disp = IntegrationDispatcher(journal, f1_foreign_client)
    f1_proj = f1_foreign_disp.execute_update(
        issuer_id=issuer,
        operation_id=f1_proj_op,
        task_id=f1_task_id,
        patch={"title": f1_proj_title},
    )
    _assert(
        f1_proj.outcome is Outcome.SCOPE_DENIED,
        f"wrong-project replay must be scope_denied, got {f1_proj.outcome} {f1_proj.details}",
    )
    f1_proj_row = journal.get_operation(issuer, f1_proj_op)
    _assert(
        f1_proj_row is not None
        and f1_proj_row.project_id == project_id
        and f1_proj_row.state is OperationState.CONFIRMED,
        "F1 wrong-project probe must preserve original confirmed intent",
    )
    scenarios["mutation_envelope_replay_guards"] = {
        "task_id": f1_task_id,
        "changed_patch": f1_changed.outcome.value,
        "changed_source": f1_source.outcome.value,
        "wrong_project": f1_proj.outcome.value,
        "live_title_after_changed_patch": live_title,
        "patch_durable_state": f1_patch_row.state.value,
        "source_durable_state": f1_source_row.state.value,
        "project_durable_state": f1_proj_row.state.value,
    }

    # --- R1: confirmed mutation replay after delete / marker loss / manual change ---
    r1_create_op = new_operation_id()
    r1_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=r1_create_op,
        title=stand.fixture_name("r1-mutation-target"),
        user_portion="r1",
    )
    _assert(r1_create.ok and r1_create.vikunja_id is not None, "R1 mutation target create failed")
    r1_task_id = int(r1_create.vikunja_id)
    synthetic_ids["r1_mutation_replay_task_id"] = r1_task_id
    r1_update_op = new_operation_id()
    r1_title = stand.fixture_name("r1-updated")
    r1_first = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r1_update_op,
        task_id=r1_task_id,
        patch={"title": r1_title},
    )
    _assert(r1_first.ok, f"R1 first update failed: {r1_first.outcome} {r1_first.details}")
    # Manual title change while marker/project remain: replay must return live state.
    r1_manual_title = stand.fixture_name("r1-manual-ui")
    r1_manual_write = client.update(r1_task_id, {"title": r1_manual_title})
    _assert(
        r1_manual_write.ok,
        f"R1 manual title update failed: {r1_manual_write.outcome} {r1_manual_write.details}",
    )
    r1_live = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r1_update_op,
        task_id=r1_task_id,
        patch={"title": r1_title},
    )
    _assert(r1_live.ok, f"R1 live replay must confirm, got {r1_live.outcome} {r1_live.details}")
    _assert(r1_live.details.get("path") == "mutation_replay", "R1 live replay path")
    _assert((r1_live.task or {}).get("title") == r1_manual_title, "R1 replay must surface current title")
    r1_row_ok = journal.get_operation(issuer, r1_update_op)
    _assert(r1_row_ok is not None and r1_row_ok.state is OperationState.CONFIRMED, "R1 receipt preserved after live replay")

    del_r1, _ = stand.api(f"tasks/{r1_task_id}", "DELETE", token=stand._session_token)
    _assert(del_r1 == 200, f"R1 delete mutation target HTTP {del_r1}")
    r1_deleted_replay = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r1_update_op,
        task_id=r1_task_id,
        patch={"title": r1_title},
    )
    _assert(
        r1_deleted_replay.outcome is Outcome.UNKNOWN,
        f"R1 deleted replay must be unknown, got {r1_deleted_replay.outcome} {r1_deleted_replay.details}",
    )
    _assert(r1_deleted_replay.details.get("reason") == "target_deleted_or_gone", "R1 deleted reason")
    _assert(r1_deleted_replay.details.get("receipt_preserved") is True, "R1 deleted must preserve receipt")
    _assert(r1_deleted_replay.details.get("resend_forbidden") is True, "R1 deleted must forbid resend")
    _assert(r1_deleted_replay.task is None, "R1 deleted replay must not invent a task body")
    r1_deleted_recover = dispatcher.recover_unknown_mutation(issuer, r1_update_op)
    _assert(
        r1_deleted_recover.outcome is Outcome.UNKNOWN,
        f"R1 deleted recover must be unknown, got {r1_deleted_recover.outcome}",
    )
    _assert(r1_deleted_recover.details.get("reason") == "target_deleted_or_gone", "R1 recover deleted reason")
    r1_row_del = journal.get_operation(issuer, r1_update_op)
    _assert(
        r1_row_del is not None and r1_row_del.state is OperationState.CONFIRMED and r1_row_del.receipt is not None,
        "R1 durable mutation receipt must survive deletion",
    )

    r1b_create_op = new_operation_id()
    r1b_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=r1b_create_op,
        title=stand.fixture_name("r1-marker-loss-target"),
        user_portion="r1b",
    )
    _assert(r1b_create.ok and r1b_create.vikunja_id is not None, "R1 marker-loss target create failed")
    r1b_id = int(r1b_create.vikunja_id)
    synthetic_ids["r1_marker_loss_task_id"] = r1b_id
    r1b_op = new_operation_id()
    r1b_title = stand.fixture_name("r1-marker-loss-updated")
    r1b_first = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r1b_op,
        task_id=r1b_id,
        patch={"title": r1b_title},
    )
    _assert(r1b_first.ok, f"R1 marker-loss first update failed: {r1b_first.outcome}")
    st_strip, _ = stand.api(
        f"tasks/{r1b_id}",
        "POST",
        {"description": "marker-removed-after-confirmed-mutation"},
        token=stand._session_token,
    )
    if st_strip not in (200, 201):
        st_strip, _ = stand.api(
            f"tasks/{r1b_id}",
            "PATCH",
            {"description": "marker-removed-after-confirmed-mutation"},
            token=stand._session_token,
        )
        _assert(st_strip in (200, 201, 204), f"R1 strip marker HTTP {st_strip}")
    r1b_replay = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r1b_op,
        task_id=r1b_id,
        patch={"title": r1b_title},
    )
    _assert(
        r1b_replay.outcome is Outcome.UNKNOWN,
        f"R1 marker-loss replay must be unknown, got {r1b_replay.outcome} {r1b_replay.details}",
    )
    _assert(r1b_replay.details.get("reason") == "marker_corrupted_or_removed", "R1 marker-loss reason")
    _assert(r1b_replay.details.get("resend_forbidden") is True, "R1 marker-loss must forbid resend")
    scenarios["confirmed_mutation_live_readback"] = {
        "deleted_task_id": r1_task_id,
        "deleted_replay": r1_deleted_replay.outcome.value,
        "deleted_recover": r1_deleted_recover.outcome.value,
        "manual_title_replay": r1_live.outcome.value,
        "manual_title_observed": (r1_live.task or {}).get("title"),
        "marker_loss_task_id": r1b_id,
        "marker_loss_replay": r1b_replay.outcome.value,
    }

    # --- R2: new update/complete after marker removal must not certify absent M ---
    r2_create_op = new_operation_id()
    r2_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=r2_create_op,
        title=stand.fixture_name("r2-marker-target"),
        user_portion="r2",
    )
    _assert(r2_create.ok and r2_create.vikunja_id is not None, "R2 marker target create failed")
    r2_id = int(r2_create.vikunja_id)
    synthetic_ids["r2_marker_removal_task_id"] = r2_id
    r2_marker = build_marker(issuer, r2_create_op)
    st_r2, _ = stand.api(
        f"tasks/{r2_id}",
        "POST",
        {"description": "marker-removed-before-new-update"},
        token=stand._session_token,
    )
    if st_r2 not in (200, 201):
        st_r2, _ = stand.api(
            f"tasks/{r2_id}",
            "PATCH",
            {"description": "marker-removed-before-new-update"},
            token=stand._session_token,
        )
        _assert(st_r2 in (200, 201, 204), f"R2 strip marker HTTP {st_r2}")
    live_r2 = client.read(r2_id)
    _assert(live_r2.ok and live_r2.task is not None, "R2 live read after strip failed")
    _assert(extract_marker(live_r2.task.get("description")) is None, "R2 live marker must be gone")
    title_before_r2 = live_r2.task.get("title")
    r2_update_op = new_operation_id()
    r2_update = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r2_update_op,
        task_id=r2_id,
        patch={"title": stand.fixture_name("r2-should-not-apply")},
    )
    _assert(
        r2_update.outcome is Outcome.UNKNOWN,
        f"R2 update after marker loss must be unknown, got {r2_update.outcome} {r2_update.details}",
    )
    _assert(r2_update.details.get("reason") == "marker_missing_on_known_target", "R2 update reason")
    _assert((client.read(r2_id).task or {}).get("title") == title_before_r2, "R2 must not PATCH title")
    r2_update_row = journal.get_operation(issuer, r2_update_op)
    _assert(
        r2_update_row is not None and r2_update_row.state is OperationState.UNKNOWN,
        f"R2 update must leave durable unknown, got {getattr(r2_update_row, 'state', None)}",
    )
    r2_update_retry = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r2_update_op,
        task_id=r2_id,
        patch={"title": stand.fixture_name("r2-should-not-apply")},
    )
    _assert(r2_update_retry.outcome is not Outcome.CONFIRMED, "R2 update retry must not confirm")
    _assert((client.read(r2_id).task or {}).get("title") == title_before_r2, "R2 retry must not PATCH")
    r2_complete_op = new_operation_id()
    r2_complete = dispatcher.execute_complete(
        issuer_id=issuer,
        operation_id=r2_complete_op,
        task_id=r2_id,
    )
    # F2 / C1 §6: the prior unknown update on this target blocks a new key until
    # explicit resolution; do not admit another occurrence command.
    _assert(
        r2_complete.outcome is Outcome.CONFLICT,
        f"R2 complete after unresolved marker-loss update must conflict, got {r2_complete.outcome}",
    )
    _assert(
        r2_complete.details.get("reason") == "unresolved_target_mutation",
        f"R2 complete must cite unresolved peer, got {r2_complete.details}",
    )
    _assert(
        r2_complete.details.get("blocking_operation_id") == r2_update_op,
        "R2 complete must name the unknown update as blocker",
    )
    _assert((client.read(r2_id).task or {}).get("done") is not True, "R2 must not complete after marker loss")

    # Genuinely unmarked manual target remains supported.
    st_manual_put, manual_task = stand.api(
        f"projects/{project_id}/tasks",
        "PUT",
        {
            "title": stand.fixture_name("r2-unmarked-manual"),
            "description": "plain manual notes without EA010 header",
        },
        token=stand._session_token,
    )
    _assert(st_manual_put == 201 and isinstance(manual_task, dict), f"R2 unmarked create HTTP {st_manual_put}")
    r2_manual_id = int(manual_task["id"])
    synthetic_ids["r2_unmarked_manual_task_id"] = r2_manual_id
    r2_manual_op = new_operation_id()
    r2_manual_title = stand.fixture_name("r2-unmarked-updated")
    r2_manual = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=r2_manual_op,
        task_id=r2_manual_id,
        patch={"title": r2_manual_title},
    )
    _assert(r2_manual.ok, f"R2 unmarked manual update must confirm, got {r2_manual.outcome} {r2_manual.details}")
    _assert((client.read(r2_manual_id).task or {}).get("title") == r2_manual_title, "R2 unmarked title applied")
    r2_manual_row = journal.get_operation(issuer, r2_manual_op)
    _assert(
        r2_manual_row is not None
        and r2_manual_row.state is OperationState.CONFIRMED
        and (r2_manual_row.marker or "") == "",
        "R2 unmarked receipt must keep empty marker",
    )
    scenarios["mutation_marker_removal_guards"] = {
        "known_marker": r2_marker,
        "stripped_task_id": r2_id,
        "update_outcome": r2_update.outcome.value,
        "complete_outcome": r2_complete.outcome.value,
        "unmarked_task_id": r2_manual_id,
        "unmarked_outcome": r2_manual.outcome.value,
        "unmarked_marker": r2_manual_row.marker,
    }

    # --- scope-drift recovery: foreign-project marker must fail closed (R3) ---
    st_proj, other_proj = stand.api(
        "projects",
        "PUT",
        {"title": stand.fixture_name("other-project")},
        token=stand._session_token,
    )
    _assert(st_proj == 201 and isinstance(other_proj, dict), f"other project create HTTP {st_proj}")
    other_project_id = int(other_proj["id"])
    synthetic_ids["scope_drift_other_project_id"] = other_project_id
    drift_op = new_operation_id()
    drift_marker = build_marker(issuer, drift_op)
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=drift_op,
        op_type="create",
        project_id=project_id,
        payload={"title": stand.fixture_name("scope-drift"), "user_portion": ""},
    )
    with journal.dispatch_critical_section() as section:
        section.begin_dispatch(issuer, drift_op)
        section.mark_unknown(issuer, drift_op, reason="scope_drift_probe")
    st_plant, planted = stand.api(
        f"projects/{other_project_id}/tasks",
        "PUT",
        {
            "title": stand.fixture_name("planted-foreign"),
            "description": drift_marker + "\nforeign",
        },
        token=stand._session_token,
    )
    _assert(st_plant == 201 and isinstance(planted, dict), f"plant foreign marker HTTP {st_plant}")
    planted_id = int(planted["id"])
    synthetic_ids["scope_drift_planted_task_id"] = planted_id
    foreign_client = _client_for(stand, token_path, other_project_id, origin=origin, page_size=1)
    foreign_disp = IntegrationDispatcher(journal, foreign_client)
    drift_rec = foreign_disp.recover_create(issuer, drift_op)
    _assert(
        drift_rec.outcome is Outcome.SCOPE_DENIED,
        f"scope drift must fail closed, got {drift_rec.outcome} {drift_rec.details}",
    )
    _assert(drift_rec.linked is False, "scope drift must not link")
    drift_row = journal.get_operation(issuer, drift_op)
    _assert(drift_row is not None and drift_row.receipt is None, "scope drift must not write receipt")
    _assert(drift_row.state is not OperationState.CONFIRMED, "scope drift must not confirm")
    _assert(drift_row.project_id == project_id, "intent project must stay immutable")
    scenarios["scope_drift_recovery"] = {
        "intent_project_id": project_id,
        "foreign_project_id": other_project_id,
        "planted_task_id": planted_id,
        "outcome": drift_rec.outcome.value,
        "receipt": drift_row.receipt,
    }

    # --- concurrent first-dispatch claim race: both callers share one ID (R4) ---
    journal.close()
    race_op = new_operation_id()
    race_title = stand.fixture_name("concurrent-first-dispatch")
    race_gate = stand.run_dir / "concurrent-first-dispatch.gate"
    race_ready_a = stand.run_dir / "concurrent-first-a.json"
    race_ready_b = stand.run_dir / "concurrent-first-b.json"
    for p in (race_gate, race_ready_a, race_ready_b):
        if p.exists():
            p.unlink()
    race_cfg = {
        "journal_path": str(journal_path),
        "api_origin": origin,
        "project_id": project_id,
        "token_path": str(token_path),
        "issuer_id": issuer,
        "operation_id": race_op,
        "title": race_title,
        "user_portion": "race",
        "crash_point": None,
        "sync_gate": str(race_gate),
        "sync_point": "before_http",
    }
    pa = _spawn_child(dict(race_cfg, ready=str(race_ready_a)), mode="create")
    pb = _spawn_child(dict(race_cfg, ready=str(race_ready_b)), mode="create")
    wait_a = _wait_ready(race_ready_a, pa, timeout=60)
    wait_b = _wait_ready(race_ready_b, pb, timeout=60)
    _assert(wait_a.get("point") == "before_http" and wait_b.get("point") == "before_http", "both must sync before_http")
    race_gate.write_text("go", encoding="utf-8")
    # Children overwrite ready files with final results after release.
    deadline = time.monotonic() + 60
    out_a = wait_a
    out_b = wait_b
    while time.monotonic() < deadline:
        if race_ready_a.exists() and race_ready_b.exists():
            try:
                out_a = json.loads(race_ready_a.read_text(encoding="utf-8"))
                out_b = json.loads(race_ready_b.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                time.sleep(0.05)
                continue
            if out_a.get("outcome") and out_b.get("outcome") and not out_a.get("waiting") and not out_b.get("waiting"):
                break
        if pa.poll() is not None and pb.poll() is not None:
            break
        time.sleep(0.05)
    _wait_proc(pa)
    _wait_proc(pb)
    out_a = json.loads(race_ready_a.read_text(encoding="utf-8"))
    out_b = json.loads(race_ready_b.read_text(encoding="utf-8"))
    marker_race = build_marker(issuer, race_op)
    race_count = _count_exact_marker(client, marker_race)
    _assert(race_count == 1, f"concurrent first-dispatch create_count must be 1, got {race_count}")
    _assert(out_a.get("outcome") == "confirmed" and out_b.get("outcome") == "confirmed", f"both must confirm, got {out_a} {out_b}")
    _assert(out_a.get("vikunja_id") == out_b.get("vikunja_id") and out_a.get("vikunja_id") is not None, "both must share one ID")
    journal.open()
    race_row = journal.get_operation(issuer, race_op)
    _assert(race_row is not None and race_row.state is OperationState.CONFIRMED, "race must leave durable confirmed")
    scenarios["concurrent_first_dispatch"] = {
        "operation_id": race_op,
        "vikunja_id": out_a.get("vikunja_id"),
        "create_count": race_count,
        "outcomes": [out_a.get("outcome"), out_b.get("outcome")],
        "operation_states": [out_a.get("operation_state"), out_b.get("operation_state")],
    }
    synthetic_ids["concurrent_first_dispatch_task_id"] = out_a.get("vikunja_id")

    # --- overlapping identical first-call: pause after prepare/admission (F1/R1) ---
    overlap_kinds: dict[str, Any] = {}
    for kind in ("update", "complete"):
        seed = dispatcher.execute_create(
            issuer_id=issuer,
            operation_id=new_operation_id(),
            title=stand.fixture_name(f"overlap-{kind}-seed"),
            user_portion="overlap",
        )
        _assert(seed.ok and seed.vikunja_id is not None, f"overlap {kind} seed create failed")
        task_id = int(seed.vikunja_id)
        op = new_operation_id()
        entered = threading.Event()
        release = threading.Event()
        results: dict[str, Any] = {}
        client_b = _client_for(stand, token_path, project_id, origin=origin, page_size=1)

        def pause_after_prepare(
            point: str,
            _entered=entered,
            _release=release,
        ) -> None:
            if point != "before_mutation_dispatch_lock":
                return
            _entered.set()
            _assert(_release.wait(30), "overlap release timed out")

        journal_b = IntegrationJournal(journal_path)
        journal_b.open()
        disp_b = IntegrationDispatcher(journal_b, client_b, crash_hook=pause_after_prepare)

        def invoke(disp: IntegrationDispatcher) -> Any:
            if kind == "update":
                return disp.execute_update(
                    issuer_id=issuer,
                    operation_id=op,
                    task_id=task_id,
                    patch={"title": stand.fixture_name(f"overlap-{kind}-title")},
                )
            return disp.execute_complete(
                issuer_id=issuer,
                operation_id=op,
                task_id=task_id,
            )

        def second() -> None:
            try:
                results["b"] = invoke(disp_b)
            except BaseException as exc:  # noqa: BLE001 — surface in assertion
                results["error"] = repr(exc)

        thread = threading.Thread(target=second)
        thread.start()
        _assert(
            entered.wait(30),
            f"overlap {kind} second caller never reached post-prepare barrier",
        )
        try:
            results["a"] = invoke(dispatcher)
        finally:
            release.set()
        thread.join(timeout=60)
        _assert(not thread.is_alive(), f"overlap {kind} second caller hung")
        _assert("error" not in results, f"overlap {kind} second caller raised: {results.get('error')}")
        first = results["a"]
        second_result = results["b"]
        row = journal.get_operation(issuer, op)
        _assert(first.ok and second_result.ok, f"overlap {kind} must confirm both: {first.outcome} {second_result.outcome}")
        _assert(
            row is not None and row.state is OperationState.CONFIRMED and row.receipt is not None,
            f"overlap {kind} durable state must stay confirmed with receipt",
        )
        events = [e.get("event_type") for e in journal.history(issuer, op)]
        _assert(events.count("dispatch_started") == 1, f"overlap {kind} must have one dispatch_started")
        _assert("conflict" not in events, f"overlap {kind} must not record conflict")
        overlap_kinds[kind] = {
            "operation_id": op,
            "task_id": task_id,
            "first": first.outcome.value,
            "second": second_result.outcome.value,
            "durable_state": row.state.value,
            "receipt_preserved": True,
            "dispatch_started_count": events.count("dispatch_started"),
            "pause_point": "before_mutation_dispatch_lock",
        }
        synthetic_ids[f"overlap_{kind}_task_id"] = task_id
        journal_b.close()
    scenarios["overlapping_first_mutation"] = overlap_kinds

    # --- F1: distinct operations on one target; drifted agreed snapshot → conflict ---
    f1_distinct_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        title=stand.fixture_name("f1-distinct-A"),
        user_portion="f1-distinct",
    )
    _assert(f1_distinct_create.ok and f1_distinct_create.vikunja_id is not None, "F1 distinct create failed")
    f1_distinct_id = int(f1_distinct_create.vikunja_id)
    synthetic_ids["f1_distinct_ops_task_id"] = f1_distinct_id
    outer_op = new_operation_id()
    pre_outer = client.read(f1_distinct_id)
    _assert(pre_outer.ok and pre_outer.task is not None, "F1 distinct preimage read failed")
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=outer_op,
        op_type="update",
        project_id=project_id,
        target_id=f1_distinct_id,
        payload={
            "patch": {"title": stand.fixture_name("f1-distinct-C")},
            "preimage": {
                "id": pre_outer.task.get("id"),
                "title": pre_outer.task.get("title"),
                "due_date": pre_outer.task.get("due_date"),
                "done": pre_outer.task.get("done"),
                "description": pre_outer.task.get("description"),
                "repeat_after": pre_outer.task.get("repeat_after"),
                "repeat_mode": pre_outer.task.get("repeat_mode"),
            },
        },
    )
    peer_title = stand.fixture_name("f1-distinct-B")
    peer = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        task_id=f1_distinct_id,
        patch={"title": peer_title},
    )
    _assert(peer.ok, f"F1 distinct peer update failed: {peer.outcome} {peer.details}")
    outer_title = stand.fixture_name("f1-distinct-C")
    outer = dispatcher.execute_update(
        issuer_id=issuer,
        operation_id=outer_op,
        task_id=f1_distinct_id,
        patch={"title": outer_title},
    )
    _assert(
        outer.outcome is Outcome.CONFLICT,
        f"F1 distinct outer must conflict on drifted preimage, got {outer.outcome} {outer.details}",
    )
    _assert(outer.details.get("reason") == "preimage_drift_before_patch", "F1 distinct reason")
    live_f1 = client.read(f1_distinct_id)
    _assert(live_f1.ok and live_f1.task is not None, "F1 distinct live read failed")
    _assert(live_f1.task.get("title") == peer_title, "F1 distinct must not overwrite peer title")
    outer_row = journal.get_operation(issuer, outer_op)
    _assert(
        outer_row is not None and outer_row.state is OperationState.CONFLICT,
        "F1 distinct outer must leave durable conflict",
    )
    scenarios["distinct_ops_preimage_conflict"] = {
        "task_id": f1_distinct_id,
        "peer_title": peer_title,
        "outer_outcome": outer.outcome.value,
        "live_title": live_f1.task.get("title"),
        "durable_state": outer_row.state.value,
    }

    # --- F2: lost response (PATCH applied) + new key must not advance again ---
    f2_due = confirm_date_only("2026-12-15", confirmed_at=datetime(2026, 11, 10, 10, 0, tzinfo=MSK))
    f2_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        title=stand.fixture_name("f2-lost-response"),
        user_portion="f2-lost",
        due=f2_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(f2_create.ok and f2_create.vikunja_id is not None, "F2 lost-response create failed")
    f2_id = int(f2_create.vikunja_id)
    synthetic_ids["f2_lost_response_task_id"] = f2_id
    f2_op1 = new_operation_id()
    pre_f2 = client.read(f2_id)
    _assert(pre_f2.ok and pre_f2.task is not None, "F2 preimage read failed")
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=f2_op1,
        op_type="complete",
        project_id=project_id,
        target_id=f2_id,
        payload={
            "patch": {"done": True},
            "preimage": {
                "id": pre_f2.task.get("id"),
                "title": pre_f2.task.get("title"),
                "due_date": pre_f2.task.get("due_date"),
                "done": pre_f2.task.get("done"),
                "description": pre_f2.task.get("description"),
                "repeat_after": pre_f2.task.get("repeat_after"),
                "repeat_mode": pre_f2.task.get("repeat_mode"),
            },
        },
    )
    with journal.dispatch_critical_section() as section:
        section.begin_dispatch(issuer, f2_op1)
        # Real stock PATCH succeeds; response is "lost" — no receipt, durable unknown.
        patched = client.complete(f2_id)
        _assert(patched.ok and patched.task is not None, f"F2 stock complete failed: {patched.outcome}")
        section.mark_unknown(
            issuer,
            f2_op1,
            reason="simulated_dropped_complete_response",
            details={"mutation_sent": True, "resend_forbidden": True},
        )
    due_after_loss = (client.read(f2_id).task or {}).get("due_date")
    _assert(due_after_loss not in {None, "0001-01-01T00:00:00Z"}, "F2 expected advanced due after PATCH")
    f2_second = dispatcher.execute_complete(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        task_id=f2_id,
    )
    _assert(
        f2_second.outcome is Outcome.CONFLICT,
        f"F2 new key must refuse while op1 unknown, got {f2_second.outcome} {f2_second.details}",
    )
    _assert(f2_second.details.get("reason") == "unresolved_target_mutation", "F2 admission reason")
    _assert(f2_second.details.get("blocking_operation_id") == f2_op1, "F2 must name blocking op")
    due_after_refuse = (client.read(f2_id).task or {}).get("due_date")
    _assert(due_after_refuse == due_after_loss, "F2 new key must not advance recurring due again")
    f2_row = journal.get_operation(issuer, f2_op1)
    _assert(f2_row is not None and f2_row.state is OperationState.UNKNOWN, "F2 op1 must stay unknown")
    scenarios["lost_response_new_key_refused"] = {
        "task_id": f2_id,
        "operation_1": f2_op1,
        "due_after_loss": due_after_loss,
        "second_outcome": f2_second.outcome.value,
        "due_after_refuse": due_after_refuse,
        "operation_1_state": f2_row.state.value,
    }

    # --- P1: response-loss → incompatible same-key → new key must not PATCH again ---
    f2c_due = confirm_date_only(
        "2026-12-16", confirmed_at=datetime(2026, 11, 10, 10, 0, tzinfo=MSK)
    )
    f2c_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        title=stand.fixture_name("f2-conflict-fence"),
        user_portion="f2-conflict-fence",
        due=f2c_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(
        f2c_create.ok and f2c_create.vikunja_id is not None,
        "F2 conflict-fence create failed",
    )
    f2c_id = int(f2c_create.vikunja_id)
    synthetic_ids["f2_conflict_fence_task_id"] = f2c_id
    f2c_op1 = new_operation_id()
    pre_f2c = client.read(f2c_id)
    _assert(pre_f2c.ok and pre_f2c.task is not None, "F2 conflict-fence preimage read failed")
    journal.prepare_intent(
        issuer_id=issuer,
        operation_id=f2c_op1,
        op_type="complete",
        project_id=project_id,
        target_id=f2c_id,
        payload={
            "patch": {"done": True},
            "preimage": {
                "id": pre_f2c.task.get("id"),
                "title": pre_f2c.task.get("title"),
                "due_date": pre_f2c.task.get("due_date"),
                "done": pre_f2c.task.get("done"),
                "description": pre_f2c.task.get("description"),
                "repeat_after": pre_f2c.task.get("repeat_after"),
                "repeat_mode": pre_f2c.task.get("repeat_mode"),
            },
        },
        source_kind="meeting",
        source_key="EA010-SYNTHETIC-f2c-meeting",
    )
    f2c_complete_calls = {"n": 0}
    real_complete_f2c = client.complete

    def _counting_complete(task_id: int, _real=real_complete_f2c, **kwargs: Any) -> Any:
        f2c_complete_calls["n"] += 1
        return _real(task_id, **kwargs)

    client.complete = _counting_complete  # type: ignore[method-assign]
    try:
        with journal.dispatch_critical_section() as section:
            section.begin_dispatch(issuer, f2c_op1)
            patched_f2c = client.complete(f2c_id)
            _assert(
                patched_f2c.ok and patched_f2c.task is not None,
                f"F2 conflict-fence stock complete failed: {patched_f2c.outcome}",
            )
            section.mark_unknown(
                issuer,
                f2c_op1,
                reason="simulated_dropped_complete_response",
                details={"mutation_sent": True, "resend_forbidden": True},
            )
        due_after_f2c_loss = (client.read(f2c_id).task or {}).get("due_date")
        _assert(
            due_after_f2c_loss not in {None, "0001-01-01T00:00:00Z"},
            "F2 conflict-fence expected advanced due after PATCH",
        )
        _assert(f2c_complete_calls["n"] == 1, "F2 conflict-fence expected exactly one loss PATCH")
        f2c_incompatible = dispatcher.execute_complete(
            issuer_id=issuer,
            operation_id=f2c_op1,
            task_id=f2c_id,
            source_kind="meeting",
            source_key="EA010-SYNTHETIC-f2c-meeting-OTHER",
        )
        _assert(
            f2c_incompatible.outcome is Outcome.CONFLICT,
            f"F2 conflict-fence same-key source mismatch must conflict, got "
            f"{f2c_incompatible.outcome} {f2c_incompatible.details}",
        )
        f2c_row = journal.get_operation(issuer, f2c_op1)
        _assert(
            f2c_row is not None and f2c_row.state is OperationState.CONFLICT,
            "F2 conflict-fence op1 must be durable conflict after incompatible replay",
        )
        _assert(
            f2c_complete_calls["n"] == 1,
            "F2 conflict-fence incompatible replay must not PATCH",
        )
        f2c_new_key = dispatcher.execute_complete(
            issuer_id=issuer,
            operation_id=new_operation_id(),
            task_id=f2c_id,
        )
        _assert(
            f2c_new_key.outcome is Outcome.CONFLICT,
            f"F2 conflict-fence new key must refuse while op1 unresolved conflict, got "
            f"{f2c_new_key.outcome} {f2c_new_key.details}",
        )
        _assert(
            f2c_new_key.details.get("reason") == "unresolved_target_mutation",
            f"F2 conflict-fence admission reason, got {f2c_new_key.details}",
        )
        _assert(
            f2c_new_key.details.get("blocking_operation_id") == f2c_op1,
            "F2 conflict-fence must name blocking conflicted op",
        )
        _assert(
            f2c_new_key.details.get("blocking_state") == OperationState.CONFLICT.value,
            "F2 conflict-fence blocking_state must remain conflict",
        )
        due_after_f2c_refuse = (client.read(f2c_id).task or {}).get("due_date")
        _assert(
            due_after_f2c_refuse == due_after_f2c_loss,
            "F2 conflict-fence new key must not advance recurring due again",
        )
        _assert(
            f2c_complete_calls["n"] == 1,
            "F2 conflict-fence must not send additional complete after loss",
        )
    finally:
        client.complete = real_complete_f2c  # type: ignore[method-assign]
    scenarios["lost_response_conflict_new_key_refused"] = {
        "task_id": f2c_id,
        "operation_1": f2c_op1,
        "due_after_loss": due_after_f2c_loss,
        "incompatible_outcome": f2c_incompatible.outcome.value,
        "operation_1_state": f2c_row.state.value,
        "new_key_outcome": f2c_new_key.outcome.value,
        "due_after_refuse": due_after_f2c_refuse,
        "blocking_state": f2c_new_key.details.get("blocking_state"),
        "actual_complete_calls": f2c_complete_calls["n"],
    }

    # --- F3: ApiError on client mutation after durable begin_dispatch ---
    f3_create = dispatcher.execute_create(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        title=stand.fixture_name("f3-second-get"),
        user_portion="f3",
    )
    _assert(f3_create.ok and f3_create.vikunja_id is not None, "F3 create failed")
    f3_id = int(f3_create.vikunja_id)
    synthetic_ids["f3_second_get_task_id"] = f3_id
    f3_op = new_operation_id()
    f3_title_before = (client.read(f3_id).task or {}).get("title")
    real_update = client.update

    def _boom_update(task_id: int, patch: Any, _real=real_update, **kwargs: Any) -> Any:
        raise ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic second-GET transport failure",
            details={"phase": "preimage", "synthetic": True, "task_id": task_id},
        )

    client.update = _boom_update  # type: ignore[method-assign]
    try:
        f3_result = dispatcher.execute_update(
            issuer_id=issuer,
            operation_id=f3_op,
            task_id=f3_id,
            patch={"title": stand.fixture_name("f3-should-not-apply")},
        )
    finally:
        client.update = real_update  # type: ignore[method-assign]
    _assert(
        f3_result.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
        f"F3 must return typed dependency_unavailable, got {f3_result.outcome} {f3_result.details}",
    )
    _assert(f3_result.details.get("resend_forbidden") is True, "F3 must forbid resend")
    _assert(f3_result.details.get("mutation_sent") is False, "F3 must record mutation_sent=false")
    _assert(f3_result.details.get("api_error") is True, "F3 must mark api_error")
    live_f3 = client.read(f3_id)
    _assert(live_f3.ok and live_f3.task is not None, "F3 live read failed")
    _assert(live_f3.task.get("title") == f3_title_before, "F3 must not PATCH after transport fault")
    f3_row = journal.get_operation(issuer, f3_op)
    _assert(f3_row is not None and f3_row.state is OperationState.UNKNOWN, "F3 must leave durable unknown")
    f3_events = [e.get("event_type") for e in journal.history(issuer, f3_op)]
    _assert("dispatch_started" in f3_events, "F3 must have durable dispatch_started")
    _assert("unknown" in f3_events, "F3 must have durable unknown")
    scenarios["dispatch_second_get_api_error"] = {
        "task_id": f3_id,
        "operation_id": f3_op,
        "outcome": f3_result.outcome.value,
        "mutation_sent": f3_result.details.get("mutation_sent"),
        "durable_state": f3_row.state.value,
        "events": f3_events,
        "live_title": live_f3.task.get("title"),
    }

    # --- SIGKILL after claim before HTTP; two-process recover without new create ---
    kill_op = new_operation_id()
    kill_title = stand.fixture_name("kill-before-http")
    kill_ready = stand.run_dir / "kill-before-http.json"
    if kill_ready.exists():
        kill_ready.unlink()
    cfg = {
        "ready": str(kill_ready),
        "journal_path": str(journal_path),
        "api_origin": origin,
        "project_id": project_id,
        "token_path": str(token_path),
        "issuer_id": issuer,
        "operation_id": kill_op,
        "title": kill_title,
        "user_portion": "kill-before-http",
        "crash_point": "after_dispatch_claim_before_http",
    }
    # Parent must not hold the journal lock across the child crash window.
    journal.close()
    proc = _spawn_child(cfg, mode="create")
    ready_payload = _wait_ready(kill_ready, proc, timeout=30)
    rc = _wait_proc(proc)
    _assert(rc == -signal.SIGKILL, f"expected SIGKILL before HTTP, rc={rc}")
    _assert(ready_payload.get("point") == "after_dispatch_claim_before_http", "crash point mismatch")
    # Two recovery processes.
    r1_ready = stand.run_dir / "recover1.json"
    r2_ready = stand.run_dir / "recover2.json"
    for p in (r1_ready, r2_ready):
        if p.exists():
            p.unlink()
    cfg_r = dict(cfg)
    cfg_r["crash_point"] = None
    cfg1 = dict(cfg_r, ready=str(r1_ready))
    cfg2 = dict(cfg_r, ready=str(r2_ready))
    p1 = _spawn_child(cfg1, mode="recover")
    p2 = _spawn_child(cfg2, mode="recover")
    out1 = _wait_ready(r1_ready, p1, timeout=60)
    out2 = _wait_ready(r2_ready, p2, timeout=60)
    _wait_proc(p1)
    _wait_proc(p2)
    marker_kill = build_marker(issuer, kill_op)
    # Reopen journal for later scenarios.
    journal.open()
    kill_count = _count_exact_marker(client, marker_kill)
    # Before HTTP: zero matches expected; both recoveries stay unknown; no create.
    _assert(kill_count == 0, f"pre-HTTP crash must not create, count={kill_count}")
    _assert(out1.get("outcome") == "unknown" and out2.get("outcome") == "unknown", "both recoveries unknown")
    scenarios["sigkill_before_http_two_recover"] = {
        "child_rc": rc,
        "create_count": kill_count,
        "recover_outcomes": [out1.get("outcome"), out2.get("outcome")],
        "operation_id": kill_op,
    }
    synthetic_ids["sigkill_before_http_operation_id"] = kill_op

    # --- SIGKILL after commit before receipt ---
    journal.close()
    commit_op = new_operation_id()
    commit_ready = stand.run_dir / "kill-after-commit.json"
    if commit_ready.exists():
        commit_ready.unlink()
    cfg_c = {
        "ready": str(commit_ready),
        "journal_path": str(journal_path),
        "api_origin": origin,
        "project_id": project_id,
        "token_path": str(token_path),
        "issuer_id": issuer,
        "operation_id": commit_op,
        "title": stand.fixture_name("kill-after-commit"),
        "user_portion": "after-commit",
        "crash_point": "after_commit_before_receipt",
    }
    proc_c = _spawn_child(cfg_c, mode="create")
    ready_c = _wait_ready(commit_ready, proc_c, timeout=60)
    rc_c = _wait_proc(proc_c)
    _assert(rc_c == -signal.SIGKILL, f"expected SIGKILL after commit, rc={rc_c}")
    marker_c = build_marker(issuer, commit_op)
    # Task should exist in Vikunja; receipt missing until recover.
    # Small settle wait for stock commit visibility.
    deadline = time.monotonic() + 10
    count_c = 0
    while time.monotonic() < deadline:
        # Need a client; journal closed but API fine.
        count_c = _count_exact_marker(client, marker_c)
        if count_c >= 1:
            break
        time.sleep(0.2)
    _assert(count_c == 1, f"after-commit crash must leave exactly one task, count={count_c}")
    r1c = stand.run_dir / "recover-commit-1.json"
    r2c = stand.run_dir / "recover-commit-2.json"
    for p in (r1c, r2c):
        if p.exists():
            p.unlink()
    cfg_cr = dict(cfg_c, crash_point=None)
    p1c = _spawn_child(dict(cfg_cr, ready=str(r1c)), mode="recover")
    p2c = _spawn_child(dict(cfg_cr, ready=str(r2c)), mode="recover")
    o1c = _wait_ready(r1c, p1c, timeout=60)
    o2c = _wait_ready(r2c, p2c, timeout=60)
    _wait_proc(p1c)
    _wait_proc(p2c)
    ids = {o1c.get("vikunja_id"), o2c.get("vikunja_id")}
    _assert(None not in ids and len(ids) == 1, f"both recovers must share one ID, got {ids}")
    _assert(o1c.get("outcome") == "confirmed" and o2c.get("outcome") == "confirmed", "recover must confirm")
    _assert(_count_exact_marker(client, marker_c) == 1, "recover must not create second task")
    journal.open()
    row_c = journal.get_operation(issuer, commit_op)
    _assert(row_c is not None and row_c.state is OperationState.CONFIRMED, "receipt durable after recover")
    scenarios["sigkill_after_commit_before_receipt"] = {
        "child_rc": rc_c,
        "vikunja_id": next(iter(ids)),
        "create_count": _count_exact_marker(client, marker_c),
        "point": ready_c.get("point"),
    }
    synthetic_ids["sigkill_after_commit_task_id"] = next(iter(ids))

    # --- SIGKILL after receipt before link; recover completes link without create ---
    journal.close()
    link_op = new_operation_id()
    link_ready_file = stand.run_dir / "link-callback.json"
    kill_link_ready = stand.run_dir / "kill-after-receipt.json"
    for p in (link_ready_file, kill_link_ready):
        if p.exists():
            p.unlink()
    cfg_l = {
        "ready": str(kill_link_ready),
        "link_ready": str(link_ready_file),
        "journal_path": str(journal_path),
        "api_origin": origin,
        "project_id": project_id,
        "token_path": str(token_path),
        "issuer_id": issuer,
        "operation_id": link_op,
        "title": stand.fixture_name("kill-after-receipt"),
        "user_portion": "after-receipt",
        "crash_point": "after_receipt_before_link",
    }
    proc_l = _spawn_child(cfg_l, mode="create")
    ready_l = _wait_ready(kill_link_ready, proc_l, timeout=60)
    rc_l = _wait_proc(proc_l)
    _assert(rc_l == -signal.SIGKILL, f"expected SIGKILL after receipt, rc={rc_l}")
    marker_l = build_marker(issuer, link_op)
    _assert(_count_exact_marker(client, marker_l) == 1, "receipt crash create_count=1")
    # Recover with link callback in two processes.
    r1l = stand.run_dir / "recover-link-1.json"
    r2l = stand.run_dir / "recover-link-2.json"
    for p in (r1l, r2l):
        if p.exists():
            p.unlink()
    if link_ready_file.exists():
        link_ready_file.unlink()
    cfg_lr = dict(cfg_l, crash_point="after_receipt_before_link")
    # recover mode re-attaches link callback when confirmed but unlinked.
    p1l = _spawn_child(dict(cfg_lr, ready=str(r1l)), mode="recover")
    p2l = _spawn_child(dict(cfg_lr, ready=str(r2l)), mode="recover")
    o1l = _wait_ready(r1l, p1l, timeout=60)
    o2l = _wait_ready(r2l, p2l, timeout=60)
    _wait_proc(p1l)
    _wait_proc(p2l)
    _assert(o1l.get("vikunja_id") == o2l.get("vikunja_id"), "link recover same ID")
    _assert(_count_exact_marker(client, marker_l) == 1, "link recover no new create")
    _assert(link_ready_file.exists() or o1l.get("linked") or o2l.get("linked"), "link callback completed")
    journal.open()
    scenarios["sigkill_after_receipt_before_link"] = {
        "child_rc": rc_l,
        "vikunja_id": o1l.get("vikunja_id"),
        "create_count": 1,
        "linked_flags": [o1l.get("linked"), o2l.get("linked")],
        "point": ready_l.get("point"),
    }
    synthetic_ids["sigkill_after_receipt_task_id"] = o1l.get("vikunja_id")

    # --- late-write fault proxy: timeout → intermediate zero → late ID, create_count=1 ---
    journal.close()
    proxy_server = None
    proxy_thread = None
    try:
        fault_port = int(stand.profile.ports["fault_proxy"])
        proxy_server, proxy_thread, proxy_state = _start_fault_proxy(
            listen_host=stand.profile.listen_host,
            listen_port=fault_port,
            upstream_host=stand.profile.listen_host,
            upstream_port=int(stand.profile.ports["vikunja"]),
            delay_s=12.0,
        )
        proxy_origin = f"http://{stand.profile.listen_host}:{fault_port}"
        late_journal_path = stand.run_dir / "journal" / "late.sqlite3"
        late_journal = IntegrationJournal(late_journal_path)
        late_journal.open()
        late_issuer = late_journal.ensure_issuer(new_issuer_id())
        late_client = _client_for(
            stand,
            token_path,
            project_id,
            origin=proxy_origin,
            page_size=1,
            http_timeout=3.0,
        )
        # Direct client for intermediate/exact scans (bypass proxy delay on GET).
        direct_client = _client_for(stand, token_path, project_id, origin=origin, page_size=1)
        late_op = new_operation_id()
        late_disp = IntegrationDispatcher(late_journal, late_client)
        late_marker = build_marker(late_issuer, late_op)

        late_result_box: dict[str, Any] = {}

        def _late_worker() -> None:
            late_result_box["result"] = late_disp.execute_create(
                issuer_id=late_issuer,
                operation_id=late_op,
                title=stand.fixture_name("late-write"),
                user_portion="late",
            )

        worker = threading.Thread(target=_late_worker, daemon=True)
        worker.start()
        # Wait until proxy has received create, then scan for intermediate zero via direct API.
        deadline = time.monotonic() + 20
        mid_zero = None
        while time.monotonic() < deadline:
            with proxy_state.lock:
                seen = proxy_state.create_seen
                forwarded = proxy_state.forwarded
            if seen >= 1 and forwarded == 0:
                mid_zero = _count_exact_marker(direct_client, late_marker)
                break
            time.sleep(0.05)
        _assert(mid_zero == 0, f"expected intermediate zero, got {mid_zero}")
        worker.join(timeout=60)
        _assert(not worker.is_alive(), "late-write worker did not finish")
        late_result = late_result_box.get("result")
        _assert(late_result is not None, "late-write missing result")
        # Client timed out; dispatcher should reconcile to the late ID (or retry recover).
        if not late_result.ok:
            late_result = late_disp.recover_create(late_issuer, late_op)
        # Wait for upstream commit if needed.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not late_result.ok:
            time.sleep(0.5)
            late_result = late_disp.recover_create(late_issuer, late_op)
        _assert(late_result.ok and late_result.vikunja_id is not None, f"late reconcile failed: {late_result.outcome} {late_result.details}")
        create_count = _count_exact_marker(direct_client, late_marker)
        _assert(create_count == 1, f"late-write create_count must be 1, got {create_count}")
        with proxy_state.lock:
            proxy_evidence = {
                "create_seen": proxy_state.create_seen,
                "forwarded": proxy_state.forwarded,
                "upstream_status": proxy_state.upstream_status,
                "upstream_id": proxy_state.upstream_id,
                "events": list(proxy_state.events),
            }
        _assert(proxy_evidence["create_seen"] == 1, "proxy must see one create")
        _assert(proxy_evidence["forwarded"] == 1, "proxy must forward once")
        scenarios["late_write_fault_proxy"] = {
            "timeout_path": True,
            "intermediate_zero": mid_zero,
            "vikunja_id": late_result.vikunja_id,
            "create_count": create_count,
            "proxy": {
                "create_seen": proxy_evidence["create_seen"],
                "forwarded": proxy_evidence["forwarded"],
                "upstream_status": proxy_evidence["upstream_status"],
                "upstream_id": proxy_evidence["upstream_id"],
            },
            "outcome": late_result.outcome.value,
        }
        synthetic_ids["late_write_task_id"] = late_result.vikunja_id
        late_journal.close()
    finally:
        if proxy_server is not None and proxy_thread is not None:
            _stop_fault_proxy(proxy_server, proxy_thread)

    journal.close()

    # Revoke suite tokens
    for handle, token_payload in (
        ("recovery_write", write_token),
        ("recovery_read", read_token),
    ):
        token_id = token_payload.get("id")
        if not (stand._session_token and token_id is not None):
            continue
        del_status, _ = stand.api(f"tokens/{token_id}", "DELETE", token=stand._session_token)
        _assert(del_status == 200, f"{handle} revoke failed HTTP {del_status}")
        denied_status, _ = stand.api("tasks", token=token_payload["token"])
        _assert(
            denied_status in (401, 403),
            f"revoked {handle} still authorized HTTP {denied_status}",
        )
        stand._token_secrets.pop(handle, None)
        if handle in stand.manifest.get("token_handles", []):
            stand.manifest["token_handles"] = [
                h for h in stand.manifest["token_handles"] if h != handle
            ]
    stand._save_manifest()
    stand._save_secrets()

    return {
        "scenarios": scenarios,
        "synthetic_ids": synthetic_ids,
        "executed_count": len(scenarios),
        "scenario_names": sorted(scenarios),
        "link_callback_invocations": len(link_log),
    }


def run_recovery_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite recovery supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            payload = run_recovery_scenarios(stand)
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
                },
                "binary": stand.manifest.get("binary"),
                "argv": list(argv),
                "suite": "recovery",
                "mode": mode,
                "link_callback_invocations": payload.get("link_callback_invocations"),
            }
        result["cleanup"] = {"confirmed": stand.cleaned}
        if not stand.cleaned:
            raise StandError("Recovery suite cleanup was not confirmed")
        if result["executed_count"] <= 0:
            raise StandError("Recovery suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
