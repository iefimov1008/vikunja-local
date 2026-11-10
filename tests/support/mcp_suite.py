"""Isolated MCP stdio suite for s05 (C3 0.1). Mutations only via dispatcher."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from vikunja_local.api import Outcome, TokenRef, VikunjaClient, extract_marker
from vikunja_local.journal import IntegrationJournal
from vikunja_local.mcp import COMMAND_META_KEY, PROTOCOL_VERSION, SOURCE_KIND

from .stand import IsolatedStand, StandError, load_profile, sha256_file

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

WRITE_PERMS = {"tasks": ["create", "read_all", "read_one", "update"]}
READ_PERMS = {"tasks": ["read_all", "read_one"]}


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


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


def _journal_path(stand: IsolatedStand) -> Path:
    path = stand.run_dir / "journal" / "integration.sqlite3"
    path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path.parent, 0o700)
    return path


def _source_digest(worktree: Path) -> dict[str, str]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    diff = subprocess.run(
        ["git", "diff", "HEAD"],
        cwd=worktree,
        capture_output=True,
        check=False,
    )
    untracked = subprocess.run(
        ["git", "ls-files", "-o", "--exclude-standard"],
        cwd=worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    material = bytearray(diff.stdout or b"")
    paths: list[str] = []
    for rel in (untracked.stdout or "").splitlines():
        path = worktree / rel
        if not path.is_file() or rel.startswith("docs/"):
            continue
        paths.append(rel)
        material.extend(rel.encode())
        material.extend(b"\0")
        material.extend(path.read_bytes())
        material.extend(b"\0")
    from .stand import sha256_bytes

    return {
        "worktree": str(worktree),
        "head": head.stdout.strip() if head.returncode == 0 else "unknown",
        "diff_sha256": sha256_bytes(bytes(material)),
        "untracked_implementation_files": paths,
    }


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _confirmed_at() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


class TrustedHostSession:
    """Synthetic trusted host: durable command_ref precedes the first RPC."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(mode=0o700, exist_ok=True)
        if not self.path.exists():
            self._write({})

    def _read(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, data: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        os.chmod(tmp, 0o600)
        tmp.replace(self.path)

    def remember(self, *, tool: str, arguments: dict[str, Any], command_ref: str | None = None) -> str:
        ref = command_ref or _new_uuid()
        confirmed = _confirmed_at()
        data = self._read()
        entry = {
            "tool": tool,
            "arguments": arguments,
            "command_ref": ref,
            "confirmed_at": confirmed,
        }
        existing = data.get(ref)
        if existing is not None:
            if existing.get("tool") != tool or existing.get("arguments") != arguments:
                raise StandError("trusted host refused incompatible command_ref replay")
            return ref
        data[ref] = entry
        self._write(data)
        return ref

    def meta_for(self, command_ref: str) -> dict[str, Any]:
        entry = self._read().get(command_ref)
        if entry is None:
            raise StandError(f"unknown command_ref {command_ref}")
        return {
            COMMAND_META_KEY: {
                "command_ref": command_ref,
                "confirmed_at": entry["confirmed_at"],
            }
        }


class McpSubprocess:
    """Framed NDJSON exchange with ea010_mcp.py."""

    def __init__(
        self,
        worktree: Path,
        config_path: Path,
        *,
        env: dict[str, str] | None = None,
    ):
        self.worktree = worktree
        self.config_path = config_path
        self.extra_env = dict(env or {})
        self.proc: subprocess.Popen[bytes] | None = None
        self.stderr_chunks: list[bytes] = []
        self._rpc_id = 0

    def start(self) -> None:
        script = self.worktree / "scripts" / "ea010_mcp.py"
        env = {
            "PATH": "/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(self.worktree),
        }
        env.update(self.extra_env)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", str(script), "--config", str(self.config_path)],
            cwd=str(self.worktree),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )

    def stop(self) -> dict[str, Any]:
        if self.proc is None:
            return {"returncode": None, "stderr": ""}
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            stdout_tail, stderr = self.proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            stdout_tail, stderr = self.proc.communicate(timeout=5)
        if stderr:
            self.stderr_chunks.append(stderr)
        if stdout_tail:
            # Any leftover stdout after EOF is unexpected for a clean session.
            pass
        code = self.proc.returncode
        self.proc = None
        return {
            "returncode": code,
            "stderr": b"".join(self.stderr_chunks).decode("utf-8", errors="replace"),
        }

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    def write_raw(self, message: dict[str, Any]) -> None:
        if self.proc is None or self.proc.stdin is None:
            raise StandError("MCP subprocess is not running")
        line = json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
        self.proc.stdin.write(line)
        self.proc.stdin.flush()

    def read_response(self, *, label: str) -> dict[str, Any]:
        if self.proc is None or self.proc.stdout is None:
            raise StandError("MCP subprocess is not running")
        raw = self.proc.stdout.readline()
        if not raw:
            raise StandError(f"MCP subprocess EOF while waiting for {label}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise StandError(f"non-JSON stdout line: {raw!r}") from exc
        if not isinstance(payload, dict):
            raise StandError("stdout JSON must be an object")
        return payload

    def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        rpc_id: Any = None,
        wait: bool = True,
    ) -> dict[str, Any] | None:
        if self.proc is None or self.proc.stdin is None or self.proc.stdout is None:
            raise StandError("MCP subprocess is not running")
        msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if rpc_id is None:
            rpc_id = self._next_id()
        msg["id"] = rpc_id
        if params is not None:
            msg["params"] = params
        self.write_raw(msg)
        if not wait:
            return None
        return self.read_response(label=method)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        if self.proc is None or self.proc.stdin is None:
            raise StandError("MCP subprocess is not running")
        msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        line = json.dumps(msg, separators=(",", ":")).encode("utf-8") + b"\n"
        self.proc.stdin.write(line)
        self.proc.stdin.flush()

    def handshake(self) -> dict[str, Any]:
        init = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "ea010-mcp-suite", "version": "1.0.0"},
            },
        )
        _assert("result" in init, f"initialize failed: {init}")
        _assert(init["result"]["protocolVersion"] == PROTOCOL_VERSION, "protocol mismatch")
        self.notify("notifications/initialized")
        ping = self.request("ping")
        _assert(ping.get("result") == {}, f"ping failed: {ping}")
        return init

    def tools_list(self) -> list[dict[str, Any]]:
        resp = self.request("tools/list", {})
        _assert("result" in resp, f"tools/list failed: {resp}")
        tools = resp["result"].get("tools")
        _assert(isinstance(tools, list), "tools/list missing tools")
        return tools

    def call(
        self,
        name: str,
        arguments: dict[str, Any],
        *,
        meta: dict[str, Any] | None = None,
        rpc_id: Any = None,
        wait: bool = True,
    ) -> dict[str, Any] | None:
        params: dict[str, Any] = {"name": name, "arguments": arguments}
        if meta is not None:
            params["_meta"] = meta
        resp = self.request("tools/call", params, rpc_id=rpc_id, wait=wait)
        if not wait:
            return None
        assert resp is not None
        _assert("result" in resp, f"tools/call {name} protocol error: {resp}")
        result = resp["result"]
        structured = result.get("structuredContent")
        _assert(isinstance(structured, dict), f"missing structuredContent for {name}")
        return structured

    def wait_exit(self, *, timeout: float = 30.0) -> int:
        if self.proc is None:
            raise StandError("MCP subprocess is not running")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self.proc.poll()
            if code is not None:
                try:
                    _, stderr = self.proc.communicate(timeout=1)
                except Exception:
                    stderr = b""
                if stderr:
                    self.stderr_chunks.append(stderr)
                rc = int(code)
                self.proc = None
                return rc
            time.sleep(0.05)
        self.proc.kill()
        self.proc.communicate(timeout=5)
        self.proc = None
        raise StandError("MCP subprocess did not exit after injected fault")


def _write_config(
    stand: IsolatedStand,
    *,
    name: str,
    token_path: Path,
    journal_path: Path,
    project_id: int,
    profile: str,
    trusted_write_client: bool,
    browser_origin: str | None,
    issuer_id: str | None = None,
    api_origin: str | None = None,
) -> Path:
    origin = api_origin or f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "api_origin": origin,
        "project_id": project_id,
        "token_ref": {"kind": "file", "path": str(token_path)},
        "journal_path": str(journal_path),
        "page_size": 2,
        "max_items_per_page": 50,
        "profile": profile,
        "browser_origin": browser_origin,
        "issuer_id": issuer_id,
        "trusted_write_client": trusted_write_client,
    }
    path = stand.run_dir / name
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def _count_marker(client: VikunjaClient, marker: str) -> int:
    listed = client.list_project_tasks()
    _assert(listed.outcome is Outcome.CONFIRMED, f"list failed: {listed.outcome}")
    count = 0
    for item in listed.details.get("items") or []:
        extracted = extract_marker(item.get("description"))
        if extracted and extracted["marker"] == marker:
            count += 1
    return count


def run_mcp_scenarios(stand: IsolatedStand, worktree: Path) -> dict[str, Any]:
    session = stand.create_synthetic_session()
    project_id = int(session["project_id"])
    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    # Synthetic browser origin for verified link assembly only (not a live device URL claim).
    browser_origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"

    write_tok = _create_api_token(stand, title=stand.fixture_name("mcp-write"), permissions=WRITE_PERMS)
    read_tok = _create_api_token(stand, title=stand.fixture_name("mcp-read"), permissions=READ_PERMS)
    write_path = _store_token(stand, "mcp-write.token", write_tok["token"])
    read_path = _store_token(stand, "mcp-read.token", read_tok["token"])
    stand._token_secrets["mcp_write"] = write_tok["token"]
    stand._token_secrets["mcp_read"] = read_tok["token"]
    stand.manifest.setdefault("token_handles", []).append("mcp_write")
    stand.manifest.setdefault("token_handles", []).append("mcp_read")
    stand.synthetic_ids["mcp_write_token_id"] = write_tok.get("id")
    stand.synthetic_ids["mcp_read_token_id"] = read_tok.get("id")
    stand._save_secrets()
    stand._save_manifest()
    journal_path = _journal_path(stand)
    host = TrustedHostSession(stand.run_dir / "trusted-host.json")

    (stand.run_dir / "journal").mkdir(mode=0o700, exist_ok=True)
    write_cfg = _write_config(
        stand,
        name="mcp-write.json",
        token_path=write_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=browser_origin,
    )
    readonly_cfg = _write_config(
        stand,
        name="mcp-readonly.json",
        token_path=read_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="read_only",
        trusted_write_client=False,
        browser_origin=None,
    )
    # Same write profile but without trusted host binding — mutations must fail closed.
    untrusted_cfg = _write_config(
        stand,
        name="mcp-untrusted.json",
        token_path=write_path,
        journal_path=stand.run_dir / "journal" / "untrusted.sqlite3",
        project_id=project_id,
        profile="write",
        trusted_write_client=False,
        browser_origin=None,
    )

    client = VikunjaClient(
        api_origin=origin,
        project_id=project_id,
        token_ref=TokenRef(kind="file", path=write_path),
        page_size=2,
    )

    synthetic_ids: dict[str, Any] = {}
    scenarios: dict[str, Any] = {}
    executed = 0

    # --- 1) lifecycle + tools/list on write profile ---
    mcp = McpSubprocess(worktree, write_cfg)
    mcp.start()
    try:
        mcp.handshake()
        tools = mcp.tools_list()
        names = sorted(t["name"] for t in tools)
        _assert(
            names == ["ea010_complete", "ea010_create", "ea010_find", "ea010_reconcile", "ea010_update"],
            f"unexpected write tools: {names}",
        )
        scenarios["lifecycle_write_tools"] = {"tools": names}
        executed += 1

        # Mutation without command context must not write.
        denied = mcp.call("ea010_create", {"title": stand.fixture_name("no-meta")})
        _assert(denied["status"] == "denied", f"expected denied without meta, got {denied}")
        _assert(denied.get("reason") == "missing_command_context", f"reason={denied.get('reason')}")
        scenarios["create_without_command_context"] = {"status": denied["status"], "reason": denied.get("reason")}
        executed += 1

        # Create with durable command_ref.
        create_args = {
            "title": stand.fixture_name("mcp-create"),
            "description": "synthetic mcp create",
            "due": {"kind": "date", "value": "2026-12-01"},
        }
        create_ref = host.remember(tool="ea010_create", arguments=create_args)
        created = mcp.call("ea010_create", create_args, meta=host.meta_for(create_ref), rpc_id=1001)
        _assert(created["status"] == "confirmed", f"create failed: {created}")
        _assert(created["task"] is not None and created["task"]["id"] >= 1, "create missing task id")
        _assert(created["operation_ref"] and UUID_OK(created["operation_ref"]), "missing operation_ref")
        task_id = int(created["task"]["id"])
        op_ref = created["operation_ref"]
        synthetic_ids["create_task_id"] = task_id
        synthetic_ids["create_operation_ref"] = op_ref
        synthetic_ids["create_command_ref"] = create_ref
        url = created["task"]["url"]
        _assert(isinstance(url, str) and url.endswith(f"/tasks/{task_id}"), f"bad url {url}")
        _assert(created["link_state"] == "verified", "link_state must be verified with browser_origin")
        parsed = urlparse(url)
        _assert(parsed.scheme == "http" and parsed.hostname == stand.profile.listen_host, "url origin mismatch")

        # Journal source mapping exists before/with confirmation.
        journal = IntegrationJournal(journal_path)
        journal.open()
        mapped = journal.lookup_source(SOURCE_KIND, create_ref)
        _assert(mapped is not None and mapped.operation_id == op_ref, "source mapping missing")
        marker = mapped.marker
        create_count = _count_marker(client, marker)
        _assert(create_count == 1, f"expected one create HTTP effect, got {create_count}")
        journal.close()
        scenarios["trusted_create"] = {
            "task_id": task_id,
            "operation_ref": op_ref,
            "command_ref": create_ref,
            "create_count": create_count,
            "url": url,
        }
        executed += 1

        # Same process, different JSON-RPC id, same command_ref → same task id, still one marker.
        replay = mcp.call("ea010_create", create_args, meta=host.meta_for(create_ref), rpc_id=1002)
        _assert(replay["status"] == "confirmed", f"replay failed: {replay}")
        _assert(int(replay["task"]["id"]) == task_id, "replay id drift")
        _assert(replay["operation_ref"] == op_ref, "operation_ref drift on replay")
        _assert(_count_marker(client, marker) == 1, "replay must not create second task")
        scenarios["same_process_repeat"] = {"task_id": task_id, "operation_ref": op_ref}
        executed += 1

        # Find by id and by operation_ref.
        found_id = mcp.call("ea010_find", {"task_id": task_id})
        _assert(found_id["status"] == "confirmed" and found_id["task"]["id"] == task_id, f"find id: {found_id}")
        found_ref = mcp.call("ea010_find", {"operation_ref": op_ref})
        _assert(found_ref["status"] == "confirmed" and found_ref["task"]["id"] == task_id, f"find ref: {found_ref}")
        missing = mcp.call("ea010_find", {"operation_ref": _new_uuid()})
        _assert(missing["status"] == "not_found", f"unknown ref must be not_found: {missing}")
        scenarios["find_paths"] = {
            "by_id": found_id["status"],
            "by_ref": found_ref["status"],
            "unknown_ref": missing["status"],
        }
        executed += 1

        # Update: edit title, set due datetime, then clear due, with omission preserving description.
        upd_title = {
            "task_id": task_id,
            "patch": {"title": stand.fixture_name("mcp-updated")},
        }
        upd_ref = host.remember(tool="ea010_update", arguments=upd_title)
        updated = mcp.call("ea010_update", upd_title, meta=host.meta_for(upd_ref))
        _assert(updated["status"] == "confirmed", f"update title failed: {updated}")
        _assert(updated["task"]["title"] == upd_title["patch"]["title"], "title not updated")

        upd_due = {
            "task_id": task_id,
            "patch": {"due": {"kind": "datetime", "value": "2026-12-15T12:00:00+03:00"}},
        }
        due_ref = host.remember(tool="ea010_update", arguments=upd_due)
        due_set = mcp.call("ea010_update", upd_due, meta=host.meta_for(due_ref))
        _assert(due_set["status"] == "confirmed", f"due set failed: {due_set}")
        _assert(due_set["task"]["due_at"] is not None, "due_at missing after set")

        clear_due = {"task_id": task_id, "patch": {"due": None}}
        clear_ref = host.remember(tool="ea010_update", arguments=clear_due)
        cleared = mcp.call("ea010_update", clear_due, meta=host.meta_for(clear_ref))
        _assert(cleared["status"] == "confirmed", f"due clear failed: {cleared}")
        _assert(cleared["task"]["due_at"] is None, "due_at must be null after clear")
        scenarios["update_due_edit_clear"] = {
            "title_task_id": updated["task"]["id"],
            "due_at_after_set": due_set["task"]["due_at"],
            "due_at_after_clear": cleared["task"]["due_at"],
        }
        executed += 1

        # Invalid schema must not mutate.
        before = client.read(task_id)
        _assert(before.ok and before.task is not None, "pre-invalid read failed")
        bad = mcp.call(
            "ea010_update",
            {"task_id": task_id, "patch": {"done": True}},
            meta=host.meta_for(host.remember(tool="ea010_update", arguments={"task_id": task_id, "patch": {"done": True}})),
        )
        # done is not in C3 update patch schema → invalid_input
        _assert(bad["status"] == "invalid_input", f"done via update patch must be invalid: {bad}")
        after = client.read(task_id)
        _assert(after.ok and after.task is not None, "post-invalid read failed")
        _assert(after.task.get("done") is False, "invalid patch mutated done")
        _assert(after.task.get("title") == before.task.get("title"), "invalid patch mutated title")
        scenarios["invalid_schema_no_mutation"] = {"status": bad["status"], "done": after.task.get("done")}
        executed += 1

        # Seed an unmarked recurring task via stock API (no second journal while MCP holds DB).
        st_rec, rec_payload = stand.api(
            f"projects/{project_id}/tasks",
            "PUT",
            {
                "title": stand.fixture_name("mcp-recurring"),
                "description": "synthetic recurring",
                "repeat_after": 86400,
                "repeat_mode": 0,
            },
            token=stand._session_token,
        )
        _assert(st_rec == 201 and isinstance(rec_payload, dict), f"recurring seed HTTP {st_rec}")
        rec_id = int(rec_payload["id"])
        synthetic_ids["recurring_task_id"] = rec_id
        pre_rec = client.read(rec_id)
        _assert(pre_rec.ok and pre_rec.task is not None, "recurring preimage failed")
        due_before = pre_rec.task.get("due_date")
        complete_args = {"task_id": rec_id}
        complete_ref = host.remember(tool="ea010_complete", arguments=complete_args)
        completed = mcp.call("ea010_complete", complete_args, meta=host.meta_for(complete_ref))
        _assert(completed["status"] == "confirmed", f"complete failed: {completed}")
        # Recurring may remain done=false with advanced due; still one confirmed complete.
        post_rec = client.read(rec_id)
        _assert(post_rec.ok and post_rec.task is not None, "recurring post read failed")
        scenarios["recurring_complete"] = {
            "task_id": rec_id,
            "done": post_rec.task.get("done"),
            "due_before": due_before,
            "due_after": post_rec.task.get("due_date"),
            "operation_ref": completed["operation_ref"],
        }
        executed += 1
    finally:
        stop1 = mcp.stop()

    # --- 2) reconnect / new subprocess / same command_ref ---
    mcp2 = McpSubprocess(worktree, write_cfg)
    mcp2.start()
    try:
        mcp2.handshake()
        # Intentionally start RPC ids at a different range.
        mcp2._rpc_id = 5000
        again = mcp2.call("ea010_create", create_args, meta=host.meta_for(create_ref), rpc_id=5001)
        _assert(again["status"] == "confirmed", f"reconnect create failed: {again}")
        _assert(int(again["task"]["id"]) == task_id, "reconnect id drift")
        _assert(again["operation_ref"] == op_ref, "reconnect operation_ref drift")
        _assert(_count_marker(client, marker) == 1, "reconnect must not create second task")
        scenarios["subprocess_reconnect_repeat"] = {
            "task_id": again["task"]["id"],
            "operation_ref": again["operation_ref"],
            "create_count": 1,
            "prior_stop_rc": stop1.get("returncode"),
        }
        executed += 1

        # Read-only reconcile of a confirmed op returns confirmed without new PUT.
        recon = mcp2.call("ea010_reconcile", {"operation_ref": op_ref})
        _assert(recon["status"] == "confirmed", f"reconcile confirmed failed: {recon}")
        _assert(int(recon["task"]["id"]) == task_id, "reconcile id drift")
        scenarios["reconcile_confirmed"] = {"status": recon["status"], "task_id": recon["task"]["id"]}
        executed += 1
    finally:
        mcp2.stop()

    # --- 3) read-only profile lists only find/reconcile ---
    mcp_ro = McpSubprocess(worktree, readonly_cfg)
    mcp_ro.start()
    try:
        mcp_ro.handshake()
        ro_tools = sorted(t["name"] for t in mcp_ro.tools_list())
        _assert(ro_tools == ["ea010_find", "ea010_reconcile"], f"read-only tools: {ro_tools}")
        # Protocol-level unknown tool for create.
        raw = mcp_ro.request(
            "tools/call",
            {"name": "ea010_create", "arguments": {"title": "x"}},
        )
        _assert("error" in raw, f"read-only must reject create tool: {raw}")
        found = mcp_ro.call("ea010_find", {"task_id": task_id})
        # Read token can still read the project task.
        _assert(found["status"] == "confirmed", f"read-only find failed: {found}")
        _assert(found["task"]["url"] is None and found["link_state"] == "unavailable", "readonly url must be unavailable")
        scenarios["read_only_profile"] = {
            "tools": ro_tools,
            "create_rejected": True,
            "find_status": found["status"],
            "link_state": found["link_state"],
        }
        executed += 1
    finally:
        mcp_ro.stop()

    # --- 4) untrusted write profile: no mutation ---
    mcp_u = McpSubprocess(worktree, untrusted_cfg)
    mcp_u.start()
    try:
        mcp_u.handshake()
        # Even with forged meta, accepts_write_commands is false.
        forged_meta = {
            COMMAND_META_KEY: {"command_ref": _new_uuid(), "confirmed_at": _confirmed_at()}
        }
        blocked = mcp_u.call(
            "ea010_create",
            {"title": stand.fixture_name("untrusted")},
            meta=forged_meta,
        )
        _assert(blocked["status"] == "denied", f"untrusted create must deny: {blocked}")
        scenarios["untrusted_write_denied"] = {"status": blocked["status"], "reason": blocked.get("reason")}
        executed += 1
    finally:
        mcp_u.stop()

    # --- 5) foreign project / auth denied do not mutate personal object ---
    other = stand.api(
        "projects",
        "PUT",
        {"title": stand.fixture_name("other-project"), "description": ""},
        token=stand._session_token,
    )
    _assert(other[0] in {201, 200} and isinstance(other[1], dict), f"other project failed: {other[0]}")
    other_id = int(other[1]["id"])
    synthetic_ids["other_project_id"] = other_id
    foreign_cfg = _write_config(
        stand,
        name="mcp-foreign.json",
        token_path=write_path,
        journal_path=stand.run_dir / "journal" / "foreign.sqlite3",
        project_id=other_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=None,
    )
    personal_before = client.read(task_id)
    _assert(personal_before.ok and personal_before.task is not None, "personal pre-foreign read failed")
    mcp_f = McpSubprocess(worktree, foreign_cfg)
    mcp_f.start()
    try:
        mcp_f.handshake()
        # Attempt update against foreign-configured project using personal task id → scope/denied/not_found.
        f_args = {"task_id": task_id, "patch": {"title": stand.fixture_name("foreign-hijack")}}
        f_ref = host.remember(tool="ea010_update", arguments=f_args)
        foreign_try = mcp_f.call("ea010_update", f_args, meta=host.meta_for(f_ref))
        _assert(
            foreign_try["status"] in {"denied", "not_found", "unknown", "invalid_input", "conflict"},
            f"foreign update unexpected success: {foreign_try}",
        )
        personal_after = client.read(task_id)
        _assert(personal_after.ok and personal_after.task is not None, "personal post-foreign read failed")
        _assert(
            personal_after.task.get("title") == personal_before.task.get("title"),
            "foreign-scoped call mutated personal task",
        )
        scenarios["foreign_project_no_mutation"] = {
            "status": foreign_try["status"],
            "personal_title": personal_after.task.get("title"),
        }
        executed += 1
    finally:
        mcp_f.stop()

    # --- 6) revoked/read-only token create ---
    ro_write_cfg = _write_config(
        stand,
        name="mcp-readonly-write.json",
        token_path=read_path,
        journal_path=stand.run_dir / "journal" / "readonly-write.sqlite3",
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=None,
    )
    mcp_rw = McpSubprocess(worktree, ro_write_cfg)
    mcp_rw.start()
    try:
        mcp_rw.handshake()
        ro_args = {"title": stand.fixture_name("ro-token-create")}
        ro_ref = host.remember(tool="ea010_create", arguments=ro_args)
        ro_create = mcp_rw.call("ea010_create", ro_args, meta=host.meta_for(ro_ref))
        _assert(ro_create["status"] == "denied", f"read token create must deny: {ro_create}")
        scenarios["read_token_create_denied"] = {
            "status": ro_create["status"],
            "reason": ro_create.get("reason"),
        }
        executed += 1
    finally:
        mcp_rw.stop()

    # --- 7) C3 acceptance #2: real before-send / after-write response loss on framed MCP ---
    def _crash_create(*, crash_point: str, title_suffix: str) -> dict[str, Any]:
        ready = stand.run_dir / f"crash-ready-{title_suffix}.json"
        if ready.exists():
            ready.unlink()
        args = {
            "title": stand.fixture_name(f"mcp-lost-{title_suffix}"),
            "description": f"lost-response {title_suffix}",
        }
        ref = host.remember(tool="ea010_create", arguments=args)
        env = {
            "EA010_MCP_TEST_CRASH_POINT": crash_point,
            "EA010_MCP_TEST_CRASH_READY": str(ready),
        }
        crashing = McpSubprocess(worktree, write_cfg, env=env)
        crashing.start()
        try:
            crashing.handshake()
            crashing.call(
                "ea010_create",
                args,
                meta=host.meta_for(ref),
                rpc_id=9100,
                wait=False,
            )
            crashing.wait_exit(timeout=45)
        finally:
            if crashing.proc is not None:
                crashing.proc.kill()
                crashing.proc.communicate(timeout=5)
                crashing.proc = None
        _assert(ready.is_file(), f"{title_suffix}: crash barrier ready file missing")
        ready_payload = json.loads(ready.read_text(encoding="utf-8"))
        _assert(ready_payload.get("point") == crash_point, f"{title_suffix}: wrong crash point")
        journal = IntegrationJournal(journal_path)
        journal.open()
        try:
            mapped = journal.lookup_source(SOURCE_KIND, ref)
            _assert(mapped is not None, f"{title_suffix}: source mapping missing after crash")
            assert mapped is not None
            op_ref = mapped.operation_id
        finally:
            journal.close()
        return {
            "args": args,
            "ref": ref,
            "operation_ref": op_ref,
            "ready": ready_payload,
        }

    def _title_count(title: str) -> int:
        listed = client.list_project_tasks()
        _assert(listed.outcome is Outcome.CONFIRMED, f"list failed while counting {title!r}")
        return sum(
            1
            for item in (listed.details.get("items") or [])
            if isinstance(item, dict) and item.get("title") == title
        )

    # 7a) before-send: claim durable, no HTTP create; replay stays unknown (no PUT).
    before = _crash_create(
        crash_point="after_dispatch_claim_before_http",
        title_suffix="before-send",
    )
    _assert(_title_count(before["args"]["title"]) == 0, "before-send must not create task")
    before_op = before["operation_ref"]
    synthetic_ids["lost_before_send_operation_ref"] = before_op

    mcp_before_replay = McpSubprocess(worktree, write_cfg)
    mcp_before_replay.start()
    try:
        mcp_before_replay.handshake()
        before_replay = mcp_before_replay.call(
            "ea010_create",
            before["args"],
            meta=host.meta_for(before["ref"]),
            rpc_id=9101,
        )
        _assert(before_replay is not None, "before-send replay missing result")
        assert before_replay is not None
        _assert(
            before_replay["status"] == "unknown",
            f"before-send replay must be unknown: {before_replay}",
        )
        _assert(before_replay.get("operation_ref") == before_op, "before-send op_ref drift")
        _assert(_title_count(before["args"]["title"]) == 0, "before-send replay must not PUT")
        recon_before_write = mcp_before_replay.call(
            "ea010_reconcile", {"operation_ref": before_op}
        )
        assert recon_before_write is not None
        _assert(
            recon_before_write["status"] == "unknown",
            f"before-send reconcile must stay unknown: {recon_before_write}",
        )
        # Read-only reconcile after disconnect.
        ro_before = McpSubprocess(worktree, readonly_cfg)
        ro_before.start()
        try:
            ro_before.handshake()
            recon_before = ro_before.call("ea010_reconcile", {"operation_ref": before_op})
            assert recon_before is not None
            _assert(
                recon_before["status"] == "unknown",
                f"before-send readonly reconcile: {recon_before}",
            )
            find_before = ro_before.call("ea010_find", {"operation_ref": before_op})
            assert find_before is not None
            _assert(
                find_before["status"] == "unknown",
                f"before-send readonly find: {find_before}",
            )
        finally:
            ro_before.stop()
        scenarios["lost_response_before_send"] = {
            "crash_point": "after_dispatch_claim_before_http",
            "operation_ref": before_op,
            "replay_status": before_replay["status"],
            "reconcile_status": recon_before_write["status"],
            "readonly_reconcile": recon_before["status"],
            "create_count": 0,
        }
        executed += 1
    finally:
        mcp_before_replay.stop()

    # 7b) after-write: HTTP create observed, receipt not yet durable; response lost.
    after = _crash_create(
        crash_point="after_commit_before_receipt",
        title_suffix="after-write",
    )
    _assert(_title_count(after["args"]["title"]) == 1, "after-write expected exactly one task")
    listed_after = client.list_project_tasks()
    after_matches = [
        item
        for item in (listed_after.details.get("items") or [])
        if isinstance(item, dict) and item.get("title") == after["args"]["title"]
    ]
    after_task_live = int(after_matches[0]["id"])
    after_op = after["operation_ref"]

    mcp_after_replay = McpSubprocess(worktree, write_cfg)
    mcp_after_replay.start()
    try:
        mcp_after_replay.handshake()
        after_replay = mcp_after_replay.call(
            "ea010_create",
            after["args"],
            meta=host.meta_for(after["ref"]),
            rpc_id=9201,
        )
        _assert(after_replay is not None, "after-write replay missing result")
        assert after_replay is not None
        if after_replay["status"] == "confirmed":
            after_task = int(after_replay["task"]["id"])
            after_op = after_replay["operation_ref"]
            _assert(after_task == after_task_live, "after-write replay id drift")
        else:
            _assert(
                after_replay["status"] == "unknown",
                f"after-write replay unexpected: {after_replay}",
            )
            after_op = after_replay.get("operation_ref") or after_op
            _assert(isinstance(after_op, str) and UUID_OK(after_op), "after-write unknown needs op_ref")
            recon = mcp_after_replay.call("ea010_reconcile", {"operation_ref": after_op})
            assert recon is not None
            _assert(recon["status"] == "confirmed", f"after-write reconcile failed: {recon}")
            after_task = int(recon["task"]["id"])
            _assert(after_task == after_task_live, "after-write reconcile id drift")
        _assert(_title_count(after["args"]["title"]) == 1, "after-write must not second PUT")
        synthetic_ids["lost_after_write_task_id"] = after_task
        synthetic_ids["lost_after_write_operation_ref"] = after_op
        # Read-only reconciliation after disconnect.
        ro_after = McpSubprocess(worktree, readonly_cfg)
        ro_after.start()
        try:
            ro_after.handshake()
            recon_after = ro_after.call("ea010_reconcile", {"operation_ref": after_op})
            assert recon_after is not None
            _assert(
                recon_after["status"] == "confirmed",
                f"after-write readonly reconcile: {recon_after}",
            )
            _assert(int(recon_after["task"]["id"]) == after_task, "after-write readonly id drift")
        finally:
            ro_after.stop()
        scenarios["lost_response_after_write"] = {
            "crash_point": "after_commit_before_receipt",
            "task_id": after_task,
            "operation_ref": after_op,
            "replay_status": after_replay["status"],
            "readonly_reconcile": recon_after["status"],
            "create_count": 1,
        }
        executed += 1
    finally:
        mcp_after_replay.stop()

    # Keep a confirmed-then-kill reconnect as identity proof, not as lost-response evidence.
    mcp3 = McpSubprocess(worktree, write_cfg)
    mcp3.start()
    try:
        mcp3.handshake()
        confirmed_args = {
            "title": stand.fixture_name("mcp-confirmed-reconnect"),
            "description": "confirmed-then-kill synthetic",
        }
        confirmed_ref = host.remember(tool="ea010_create", arguments=confirmed_args)
        confirmed = mcp3.call(
            "ea010_create",
            confirmed_args,
            meta=host.meta_for(confirmed_ref),
            rpc_id=9001,
        )
        assert confirmed is not None
        _assert(confirmed["status"] == "confirmed", f"confirmed reconnect prep failed: {confirmed}")
        confirmed_task = int(confirmed["task"]["id"])
        confirmed_op = confirmed["operation_ref"]
        synthetic_ids["confirmed_reconnect_task_id"] = confirmed_task
        synthetic_ids["confirmed_reconnect_operation_ref"] = confirmed_op
    finally:
        if mcp3.proc is not None:
            mcp3.proc.kill()
            mcp3.proc.communicate(timeout=5)
            mcp3.proc = None

    mcp4 = McpSubprocess(worktree, write_cfg)
    mcp4.start()
    try:
        mcp4.handshake()
        confirmed_replay = mcp4.call(
            "ea010_create",
            confirmed_args,
            meta=host.meta_for(confirmed_ref),
            rpc_id=1,
        )
        assert confirmed_replay is not None
        _assert(confirmed_replay["status"] == "confirmed", f"confirmed replay failed: {confirmed_replay}")
        _assert(int(confirmed_replay["task"]["id"]) == confirmed_task, "confirmed replay id drift")
        scenarios["confirmed_replay_after_kill"] = {
            "task_id": confirmed_task,
            "operation_ref": confirmed_op,
            "replay_task_id": confirmed_replay["task"]["id"],
            "note": "not counted as lost-response proof",
        }
        executed += 1
    finally:
        mcp4.stop()

    # 7c) Framed invalid JSON-RPC ids must not mutate (incl. unpaired surrogate \\ud800).
    mcp_bad_id = McpSubprocess(worktree, write_cfg)
    mcp_bad_id.start()
    try:
        mcp_bad_id.handshake()
        listed_ids_before = client.list_project_tasks()
        _assert(listed_ids_before.outcome is Outcome.CONFIRMED, "list before bad-id failed")
        count_before = len(listed_ids_before.details.get("items") or [])
        for bad_id in ({"bad": "id"}, ["array-id"], True, "\ud800"):
            bad_args = {"title": stand.fixture_name(f"bad-id-{type(bad_id).__name__}")}
            bad_ref = host.remember(tool="ea010_create", arguments=bad_args)
            resp = mcp_bad_id.request(
                "tools/call",
                {
                    "name": "ea010_create",
                    "arguments": bad_args,
                    "_meta": host.meta_for(bad_ref),
                },
                rpc_id=bad_id,
            )
            assert resp is not None
            _assert("error" in resp, f"invalid id must be protocol error: {resp}")
            _assert(resp["error"]["code"] == -32600, f"expected -32600, got {resp}")
            _assert(resp.get("id") is None, "invalid id response must use null id")
        listed_ids_after = client.list_project_tasks()
        _assert(listed_ids_after.outcome is Outcome.CONFIRMED, "list after bad-id failed")
        _assert(
            len(listed_ids_after.details.get("items") or []) == count_before,
            "invalid JSON-RPC id must not mutate",
        )
        # Process must still answer a later valid request (no encode crash / exit 1).
        alive = mcp_bad_id.request("ping")
        _assert(alive.get("result") == {}, f"server must survive surrogate id: {alive}")
        scenarios["invalid_jsonrpc_id_no_mutation"] = {
            "ids": ["object", "array", "boolean", "unpaired_surrogate"],
            "create_count_delta": 0,
            "survived_after_surrogate": True,
        }
        executed += 1
    finally:
        stop_bad = mcp_bad_id.stop()
        _assert(stop_bad.get("returncode") == 0, f"bad-id session exit: {stop_bad}")

    # 7c2) Huge integer JSON-RPC id (Py3.15 digit limit) must not kill the subprocess.
    mcp_huge = McpSubprocess(worktree, write_cfg)
    mcp_huge.start()
    try:
        mcp_huge.handshake()
        if mcp_huge.proc is None or mcp_huge.proc.stdin is None or mcp_huge.proc.stdout is None:
            raise StandError("MCP subprocess is not running for huge-id probe")
        huge_frame = b'{"jsonrpc":"2.0","id":' + (b"9" * 5000) + b',"method":"ping"}\n'
        _assert(len(huge_frame) < 256 * 1024, "huge-id frame must stay under C3 256 KiB")
        mcp_huge.proc.stdin.write(huge_frame)
        mcp_huge.proc.stdin.flush()
        parse_raw = mcp_huge.proc.stdout.readline()
        _assert(parse_raw, "EOF after huge integer id frame")
        parse_resp = json.loads(parse_raw.decode("utf-8"))
        _assert("error" in parse_resp, f"huge id must be parse error: {parse_resp}")
        _assert(parse_resp["error"]["code"] == -32700, f"expected -32700, got {parse_resp}")
        _assert(parse_resp.get("id") is None, "parse error id must be null")
        alive_huge = mcp_huge.request("ping")
        _assert(alive_huge.get("result") == {}, f"server must survive huge int id: {alive_huge}")
        scenarios["huge_integer_id_parse_keeps_session"] = {
            "frame_bytes": len(huge_frame),
            "parse_code": -32700,
            "later_ping_ok": True,
        }
        executed += 1
    finally:
        stop_huge = mcp_huge.stop()
        _assert(stop_huge.get("returncode") == 0, f"huge-id session exit: {stop_huge}")
        _assert(
            "ValueError" not in (stop_huge.get("stderr") or ""),
            f"huge-id must not leak ValueError on stderr: {stop_huge.get('stderr')}",
        )

    # 7d) Real loopback connection refusal on find (by id and by operation_ref).
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind((stand.profile.listen_host, 0))
    refused_port = int(probe.getsockname()[1])
    probe.close()
    refused_origin = f"http://{stand.profile.listen_host}:{refused_port}"
    # Reuse the live write journal so find-by-ref still resolves the durable operation.
    refused_cfg = _write_config(
        stand,
        name="mcp-refused.json",
        token_path=write_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=None,
        api_origin=refused_origin,
    )
    mcp_refused = McpSubprocess(worktree, refused_cfg)
    mcp_refused.start()
    try:
        mcp_refused.handshake()
        by_id = mcp_refused.call("ea010_find", {"task_id": task_id})
        _assert(by_id["status"] == "unavailable", f"refused find-by-id: {by_id}")
        _assert(by_id.get("reason") == "transport", f"refused find-by-id reason: {by_id}")
        _assert(by_id.get("operation_ref") is None, "find-by-id unavailable must keep null op_ref")
        by_ref = mcp_refused.call("ea010_find", {"operation_ref": op_ref})
        _assert(by_ref["status"] == "unavailable", f"refused find-by-ref: {by_ref}")
        _assert(by_ref.get("reason") == "transport", f"refused find-by-ref reason: {by_ref}")
        _assert(by_ref.get("operation_ref") == op_ref, "find-by-ref must preserve operation_ref")
        scenarios["refused_connection_find"] = {
            "by_id_status": by_id["status"],
            "by_id_reason": by_id.get("reason"),
            "by_ref_status": by_ref["status"],
            "by_ref_reason": by_ref.get("reason"),
            "by_ref_operation_ref": by_ref.get("operation_ref"),
            "refused_origin": refused_origin,
        }
        executed += 1
    finally:
        mcp_refused.stop()

    # 7e) Real deleted linked task: find-by-ref must stay unknown (not invalid_input).
    mcp_del = McpSubprocess(worktree, write_cfg)
    mcp_del.start()
    try:
        mcp_del.handshake()
        del_args = {"title": stand.fixture_name("mcp-deleted-linked")}
        del_ref = host.remember(tool="ea010_create", arguments=del_args)
        deleted_created = mcp_del.call("ea010_create", del_args, meta=host.meta_for(del_ref))
        _assert(deleted_created["status"] == "confirmed", f"deleted-linked create: {deleted_created}")
        deleted_task_id = int(deleted_created["task"]["id"])
        deleted_op = deleted_created["operation_ref"]
        synthetic_ids["deleted_linked_task_id"] = deleted_task_id
        synthetic_ids["deleted_linked_operation_ref"] = deleted_op
        del_http, _ = stand.api(
            f"tasks/{deleted_task_id}",
            "DELETE",
            token=stand._session_token,
        )
        _assert(del_http == 200, f"DELETE linked task HTTP {del_http}")
        del_by_id = mcp_del.call("ea010_find", {"task_id": deleted_task_id})
        del_by_ref = mcp_del.call("ea010_find", {"operation_ref": deleted_op})
        del_recon = mcp_del.call("ea010_reconcile", {"operation_ref": deleted_op})
        _assert(del_by_id["status"] == "not_found", f"deleted find-by-id: {del_by_id}")
        _assert(del_by_ref["status"] == "unknown", f"deleted find-by-ref: {del_by_ref}")
        _assert(del_by_ref.get("operation_ref") == deleted_op, "deleted find-by-ref op_ref drift")
        _assert(
            del_by_ref.get("reason") != "invalid_argument",
            f"deleted find-by-ref must not blame arguments: {del_by_ref}",
        )
        _assert(del_recon["status"] == "unknown", f"deleted reconcile: {del_recon}")
        journal_del = IntegrationJournal(journal_path)
        journal_del.open()
        try:
            mapped_del = journal_del.lookup_source(SOURCE_KIND, del_ref)
            _assert(mapped_del is not None, "deleted linked source mapping missing")
            assert mapped_del is not None
            del_row = journal_del.get_operation(mapped_del.issuer_id, deleted_op)
            _assert(del_row is not None and del_row.receipt is not None, "receipt must be preserved")
        finally:
            journal_del.close()
        scenarios["deleted_linked_task_unknown"] = {
            "task_id": deleted_task_id,
            "operation_ref": deleted_op,
            "delete_http": del_http,
            "find_by_id": del_by_id["status"],
            "find_by_ref": del_by_ref["status"],
            "reconcile": del_recon["status"],
            "receipt_preserved": True,
        }
        executed += 1
    finally:
        mcp_del.stop()

    # 7f) Confirmed update/complete then API unavailable: durable receipt ⇒ unknown.
    mcp_mut = McpSubprocess(worktree, write_cfg)
    mcp_mut.start()
    try:
        mcp_mut.handshake()
        base_args = {"title": stand.fixture_name("mcp-post-send-base")}
        base_ref = host.remember(tool="ea010_create", arguments=base_args)
        base = mcp_mut.call("ea010_create", base_args, meta=host.meta_for(base_ref))
        _assert(base["status"] == "confirmed", f"post-send base create: {base}")
        base_task = int(base["task"]["id"])
        upd_args = {
            "task_id": base_task,
            "patch": {"title": stand.fixture_name("mcp-post-send-updated")},
        }
        upd_ref = host.remember(tool="ea010_update", arguments=upd_args)
        updated = mcp_mut.call("ea010_update", upd_args, meta=host.meta_for(upd_ref))
        _assert(updated["status"] == "confirmed", f"post-send update: {updated}")
        update_op = updated["operation_ref"]
        complete_args = {"task_id": base_task}
        complete_ref = host.remember(tool="ea010_complete", arguments=complete_args)
        completed = mcp_mut.call(
            "ea010_complete", complete_args, meta=host.meta_for(complete_ref)
        )
        _assert(completed["status"] == "confirmed", f"post-send complete: {completed}")
        complete_op = completed["operation_ref"]
        synthetic_ids["post_send_update_operation_ref"] = update_op
        synthetic_ids["post_send_complete_operation_ref"] = complete_op
        synthetic_ids["post_send_task_id"] = base_task
    finally:
        mcp_mut.stop()

    post_send_cfg = _write_config(
        stand,
        name="mcp-post-send-unavailable.json",
        token_path=write_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=None,
        api_origin=refused_origin,
    )
    mcp_post = McpSubprocess(worktree, post_send_cfg)
    mcp_post.start()
    try:
        mcp_post.handshake()
        upd_recon = mcp_post.call("ea010_reconcile", {"operation_ref": update_op})
        complete_recon = mcp_post.call("ea010_reconcile", {"operation_ref": complete_op})
        _assert(
            upd_recon["status"] == "unknown",
            f"post-send update reconcile must be unknown: {upd_recon}",
        )
        _assert(
            complete_recon["status"] == "unknown",
            f"post-send complete reconcile must be unknown: {complete_recon}",
        )
        scenarios["post_send_unavailable_reconcile"] = {
            "update_operation_ref": update_op,
            "complete_operation_ref": complete_op,
            "update_reconcile": upd_recon["status"],
            "complete_reconcile": complete_recon["status"],
            "refused_origin": refused_origin,
        }
        executed += 1
    finally:
        mcp_post.stop()

    # 7g) Unknown update (lost before receipt) then deleted target: reconcile stays unknown.
    mcp_unk_base = McpSubprocess(worktree, write_cfg)
    mcp_unk_base.start()
    try:
        mcp_unk_base.handshake()
        unk_base_args = {"title": stand.fixture_name("mcp-unknown-del-base")}
        unk_base_ref = host.remember(tool="ea010_create", arguments=unk_base_args)
        unk_base = mcp_unk_base.call(
            "ea010_create", unk_base_args, meta=host.meta_for(unk_base_ref)
        )
        _assert(unk_base["status"] == "confirmed", f"unknown-del base create: {unk_base}")
        unk_task_id = int(unk_base["task"]["id"])
    finally:
        mcp_unk_base.stop()

    unk_upd_args = {
        "task_id": unk_task_id,
        "patch": {"title": stand.fixture_name("mcp-unknown-del-updated")},
    }
    unk_upd_ref = host.remember(tool="ea010_update", arguments=unk_upd_args)
    unk_ready = stand.run_dir / "crash-ready-unknown-del-update.json"
    if unk_ready.exists():
        unk_ready.unlink()
    unk_crash_env = {
        "EA010_MCP_TEST_CRASH_POINT": "after_mutation_commit_before_receipt",
        "EA010_MCP_TEST_CRASH_READY": str(unk_ready),
    }
    mcp_unk_crash = McpSubprocess(worktree, write_cfg, env=unk_crash_env)
    mcp_unk_crash.start()
    try:
        mcp_unk_crash.handshake()
        mcp_unk_crash.call(
            "ea010_update",
            unk_upd_args,
            meta=host.meta_for(unk_upd_ref),
            rpc_id=9200,
            wait=False,
        )
        mcp_unk_crash.wait_exit(timeout=45)
    finally:
        if mcp_unk_crash.proc is not None:
            mcp_unk_crash.proc.kill()
            mcp_unk_crash.proc.communicate(timeout=5)
            mcp_unk_crash.proc = None
    _assert(unk_ready.is_file(), "unknown-del update: crash barrier ready file missing")
    journal_unk = IntegrationJournal(journal_path)
    journal_unk.open()
    try:
        mapped_unk = journal_unk.lookup_source(SOURCE_KIND, unk_upd_ref)
        _assert(mapped_unk is not None, "unknown-del update: source mapping missing")
        assert mapped_unk is not None
        unk_op = mapped_unk.operation_id
        unk_row = journal_unk.get_operation(mapped_unk.issuer_id, unk_op)
        _assert(unk_row is not None, "unknown-del update: operation row missing")
        assert unk_row is not None
        _assert(unk_row.receipt is None, "unknown-del update must lack receipt")
    finally:
        journal_unk.close()
    synthetic_ids["unknown_deleted_update_operation_ref"] = unk_op
    synthetic_ids["unknown_deleted_update_task_id"] = unk_task_id

    del_http_unk, _ = stand.api(
        f"tasks/{unk_task_id}",
        "DELETE",
        token=stand._session_token,
    )
    _assert(del_http_unk == 200, f"DELETE unknown-del target HTTP {del_http_unk}")

    mcp_unk_recon = McpSubprocess(worktree, write_cfg)
    mcp_unk_recon.start()
    try:
        mcp_unk_recon.handshake()
        unk_recon = mcp_unk_recon.call("ea010_reconcile", {"operation_ref": unk_op})
        _assert(
            unk_recon["status"] == "unknown",
            f"unknown-del reconcile must be unknown not invalid_input: {unk_recon}",
        )
        _assert(unk_recon.get("operation_ref") == unk_op, "unknown-del op_ref drift")
        _assert(
            unk_recon.get("reason") != "invalid_argument",
            f"unknown-del must not blame arguments: {unk_recon}",
        )
        scenarios["unknown_update_deleted_target"] = {
            "task_id": unk_task_id,
            "operation_ref": unk_op,
            "delete_http": del_http_unk,
            "reconcile": unk_recon["status"],
            "reason": unk_recon.get("reason"),
            "crash_point": "after_mutation_commit_before_receipt",
        }
        executed += 1
    finally:
        mcp_unk_recon.stop()

    # Incompatible payload on same command_ref must conflict and not create a second task.
    mcp5 = McpSubprocess(worktree, write_cfg)
    mcp5.start()
    try:
        mcp5.handshake()
        bad_replay_args = dict(create_args)
        bad_replay_args["title"] = stand.fixture_name("incompatible-title")
        # Host storage would normally refuse; call MCP directly with same ref to prove server fence.
        conflict = mcp5.call(
            "ea010_create",
            bad_replay_args,
            meta={
                COMMAND_META_KEY: {
                    "command_ref": create_ref,
                    "confirmed_at": host.meta_for(create_ref)[COMMAND_META_KEY]["confirmed_at"],
                }
            },
        )
        _assert(conflict["status"] == "conflict", f"incompatible payload must conflict: {conflict}")
        _assert(_count_marker(client, marker) == 1, "conflict must not create second task")
        scenarios["incompatible_payload_conflict"] = {"status": conflict["status"], "create_count": 1}
        executed += 1
    finally:
        mcp5.stop()

    _assert(executed > 0, "mcp suite executed_count must be positive")
    return {
        "executed_count": executed,
        "synthetic_ids": synthetic_ids,
        "scenarios": scenarios,
        "scenario_names": sorted(scenarios),
        "project_id": project_id,
        "config_example_sha256": sha256_file(worktree / "config" / "mcp.example.json"),
        "mcp_module_sha256": sha256_file(worktree / "vikunja_local" / "mcp.py"),
    }


def UUID_OK(value: str) -> bool:
    import re

    return bool(
        re.fullmatch(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            value,
        )
    )


def run_mcp_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite mcp supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            payload = run_mcp_scenarios(stand, worktree)
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
                "suite": "mcp",
                "mode": mode,
                "config_example_sha256": payload.get("config_example_sha256"),
                "mcp_module_sha256": payload.get("mcp_module_sha256"),
            }
        result["cleanup"] = {"confirmed": stand.cleaned}
        if not stand.cleaned:
            raise StandError("MCP suite cleanup was not confirmed")
        if result["executed_count"] <= 0:
            raise StandError("MCP suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
