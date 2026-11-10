"""Unit tests for MCP stdio adapter (s05 / C3 0.1)."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from unittest.mock import patch

from vikunja_local.api import ApiError, Outcome, TaskResult, TokenRef
from vikunja_local.dispatcher import DispatchResult, IntegrationDispatcher
from vikunja_local.journal import (
    DeliveryState,
    IntegrationJournal,
    JournalError,
    OperationState,
    new_issuer_id,
)
from vikunja_local.mcp import (
    COMMAND_META_KEY,
    JSONRPC_INVALID_REQUEST,
    MAX_SAFE_INT,
    PROTOCOL_VERSION,
    SOURCE_KIND,
    McpConfig,
    McpError,
    McpServer,
    _tool_definitions,
    load_config,
    map_dispatch_to_wire,
)


class FakeClient:
    def __init__(self, project_id: int = 2):
        self.project_id = project_id
        self.tasks: dict[int, dict[str, Any]] = {}
        self.next_id = 1
        self.create_calls = 0
        self.update_calls = 0
        self.complete_calls = 0
        self.fail_create_with: Outcome | None = None
        self.fail_create_mutation_sent: bool = False
        self.fail_create_http_status: int | None = None
        self.raise_read_with: ApiError | None = None

    def create(self, **kwargs: Any) -> TaskResult:
        self.create_calls += 1
        if self.fail_create_with is not None:
            if self.fail_create_http_status is not None:
                http_status = self.fail_create_http_status
            elif self.fail_create_with is Outcome.SCOPE_DENIED:
                http_status = 403
            elif self.fail_create_with is Outcome.VALIDATION_ERROR:
                http_status = 400
            else:
                http_status = 401
            return TaskResult(
                outcome=self.fail_create_with,
                mutation_sent=self.fail_create_mutation_sent,
                http_status=http_status,
                details={"phase": "create", "synthetic": True, "http_status": http_status},
            )
        from vikunja_local.api import build_create_description

        task_id = self.next_id
        self.next_id += 1
        description = build_create_description(
            kwargs["issuer_id"], kwargs["operation_id"], kwargs.get("user_portion", "")
        )
        due = None
        if kwargs.get("due") is not None:
            due = kwargs["due"].wire_value()
        task = {
            "id": task_id,
            "project_id": self.project_id,
            "title": kwargs["title"],
            "description": description,
            "due_date": due or "0001-01-01T00:00:00Z",
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        self.tasks[task_id] = task
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=dict(task),
            http_status=200,
            mutation_sent=True,
            details={"create_http": 201, "task_id": task_id},
        )

    def read(self, task_id: int, **kwargs: Any) -> TaskResult:
        if self.raise_read_with is not None:
            raise self.raise_read_with
        task = self.tasks.get(task_id)
        if task is None:
            # Mirror stock VikunjaClient: HTTP 404 → VALIDATION_ERROR (not EMPTY).
            return TaskResult(outcome=Outcome.VALIDATION_ERROR, http_status=404)
        return TaskResult(outcome=Outcome.CONFIRMED, task=dict(task), http_status=200)

    def update(self, task_id: int, patch: dict[str, Any]) -> TaskResult:
        self.update_calls += 1
        task = self.tasks.get(task_id)
        if task is None:
            return TaskResult(outcome=Outcome.EMPTY, http_status=404)
        updated = dict(task)
        if "title" in patch:
            updated["title"] = patch["title"]
        if "description" in patch:
            # Keep marker line if present.
            desc = updated.get("description") or ""
            if "\n" in desc:
                marker, _ = desc.split("\n", 1)
                updated["description"] = marker + "\n" + patch["description"]
            else:
                updated["description"] = patch["description"]
        if "due_date" in patch:
            updated["due_date"] = (
                "0001-01-01T00:00:00Z" if patch["due_date"] is None else patch["due_date"]
            )
        self.tasks[task_id] = updated
        return TaskResult(outcome=Outcome.CONFIRMED, task=dict(updated), http_status=200, mutation_sent=True)

    def complete(self, task_id: int) -> TaskResult:
        self.complete_calls += 1
        task = self.tasks.get(task_id)
        if task is None:
            return TaskResult(outcome=Outcome.EMPTY, http_status=404)
        updated = dict(task)
        updated["done"] = True
        self.tasks[task_id] = updated
        return TaskResult(outcome=Outcome.CONFIRMED, task=dict(updated), http_status=200, mutation_sent=True)

    def list_project_tasks(self) -> TaskResult:
        items = list(self.tasks.values())
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=None,
            http_status=200,
            details={"items": items},
        )


def _meta(command_ref: str, confirmed_at: str | None = None) -> dict[str, Any]:
    return {
        COMMAND_META_KEY: {
            "command_ref": command_ref,
            "confirmed_at": confirmed_at
            or datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    }


class McpUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.journal_path = self.root / "journal.sqlite3"
        self.token_path = self.root / "token"
        self.token_path.write_text("synthetic-token", encoding="utf-8")
        self.client = FakeClient()
        self.journal = IntegrationJournal(self.journal_path)
        self.journal.open()
        self.issuer = self.journal.ensure_issuer(new_issuer_id())
        self.dispatcher = IntegrationDispatcher(self.journal, self.client)  # type: ignore[arg-type]
        self.config = McpConfig(
            schema_version=1,
            api_origin="http://127.0.0.1:19345",
            project_id=2,
            token_ref=TokenRef(kind="file", path=self.token_path),
            journal_path=self.journal_path,
            page_size=2,
            max_items_per_page=50,
            profile="write",
            browser_origin="http://127.0.0.1:19345",
            issuer_id=self.issuer,
            trusted_write_client=True,
        )
        # Reuse already-open journal/dispatcher; avoid double-open via McpServer.__init__.
        self.server = object.__new__(McpServer)
        self.server.config = self.config
        self.server.journal = self.journal
        self.server.issuer_id = self.issuer
        self.server.client = self.client  # type: ignore[assignment]
        self.server.dispatcher = self.dispatcher
        self.server._initialize_succeeded = False
        self.server._initialized = False
        self.server._closed = False
        self.server._active_operation_ref = None
        self.server._tools = _tool_definitions(write_enabled=True)

    def tearDown(self) -> None:
        try:
            self.journal.close()
        except Exception:
            pass
        self.tmp.cleanup()

    def _init(self) -> None:
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "unit", "version": "0"},
                },
            }
        )
        self.assertIsNotNone(resp)
        assert resp is not None
        self.assertIn("result", resp)
        self.assertIsNone(
            self.server.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
        )

    def test_tools_list_and_create_repeat_stable(self) -> None:
        self._init()
        listed = self.server.handle_message({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        assert listed is not None
        names = sorted(t["name"] for t in listed["result"]["tools"])
        self.assertEqual(
            names,
            ["ea010_complete", "ea010_create", "ea010_find", "ea010_reconcile", "ea010_update"],
        )
        command_ref = str(uuid.uuid4())
        args = {"title": "EA010-SYNTHETIC-mcp-unit", "description": "unit"}
        first = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {"name": "ea010_create", "arguments": args, "_meta": _meta(command_ref)},
            }
        )
        assert first is not None
        payload = first["result"]["structuredContent"]
        self.assertEqual(payload["status"], "confirmed")
        task_id = payload["task"]["id"]
        op_ref = payload["operation_ref"]
        self.assertEqual(self.client.create_calls, 1)
        mapped = self.journal.lookup_source(SOURCE_KIND, command_ref)
        self.assertIsNotNone(mapped)
        assert mapped is not None
        self.assertEqual(mapped.operation_id, op_ref)

        second = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 99,
                "method": "tools/call",
                "params": {"name": "ea010_create", "arguments": args, "_meta": _meta(command_ref)},
            }
        )
        assert second is not None
        payload2 = second["result"]["structuredContent"]
        self.assertEqual(payload2["task"]["id"], task_id)
        self.assertEqual(payload2["operation_ref"], op_ref)
        self.assertEqual(self.client.create_calls, 1)

    def test_missing_command_context_denies_without_http(self) -> None:
        self._init()
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "ea010_create", "arguments": {"title": "x"}},
            }
        )
        assert resp is not None
        payload = resp["result"]["structuredContent"]
        self.assertEqual(payload["status"], "denied")
        self.assertEqual(payload["reason"], "missing_command_context")
        self.assertEqual(self.client.create_calls, 0)

    def test_find_unknown_ref_not_found(self) -> None:
        self._init()
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "ea010_find", "arguments": {"operation_ref": str(uuid.uuid4())}},
            }
        )
        assert resp is not None
        payload = resp["result"]["structuredContent"]
        self.assertEqual(payload["status"], "not_found")
        self.assertFalse(resp["result"]["isError"])

    def test_update_clear_due_and_complete(self) -> None:
        self._init()
        create_ref = str(uuid.uuid4())
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {
                        "title": "EA010-SYNTHETIC-due",
                        "due": {"kind": "date", "value": "2026-12-01"},
                    },
                    "_meta": _meta(create_ref),
                },
            }
        )
        assert created is not None
        task_id = created["result"]["structuredContent"]["task"]["id"]
        clear_ref = str(uuid.uuid4())
        cleared = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {"task_id": task_id, "patch": {"due": None}},
                    "_meta": _meta(clear_ref),
                },
            }
        )
        assert cleared is not None
        self.assertEqual(cleared["result"]["structuredContent"]["status"], "confirmed")
        self.assertIsNone(cleared["result"]["structuredContent"]["task"]["due_at"])
        complete_ref = str(uuid.uuid4())
        completed = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "ea010_complete",
                    "arguments": {"task_id": task_id},
                    "_meta": _meta(complete_ref),
                },
            }
        )
        assert completed is not None
        self.assertEqual(completed["result"]["structuredContent"]["status"], "confirmed")
        self.assertTrue(completed["result"]["structuredContent"]["task"]["done"])
        self.assertEqual(self.client.complete_calls, 1)

    def test_read_only_tools_list(self) -> None:
        self.config = McpConfig(
            schema_version=1,
            api_origin="http://127.0.0.1:19345",
            project_id=2,
            token_ref=TokenRef(kind="file", path=self.token_path),
            journal_path=self.journal_path,
            page_size=2,
            max_items_per_page=50,
            profile="read_only",
            browser_origin=None,
            issuer_id=self.issuer,
            trusted_write_client=False,
        )
        self.server.config = self.config
        from vikunja_local.mcp import _tool_definitions

        self.server._tools = _tool_definitions(write_enabled=False)
        self._init()
        listed = self.server.handle_message({"jsonrpc": "2.0", "id": 8, "method": "tools/list", "params": {}})
        assert listed is not None
        names = sorted(t["name"] for t in listed["result"]["tools"])
        self.assertEqual(names, ["ea010_find", "ea010_reconcile"])

    def test_serve_rejects_batch_and_oversized_line(self) -> None:
        self._init()
        # Batch array
        stdin = io.BytesIO(b'[{"jsonrpc":"2.0","id":1,"method":"ping"}]\n')
        stdout = io.BytesIO()
        stderr = io.BytesIO()
        # Use a fresh server state already initialized.
        code = self.server.serve(stdin=stdin, stdout=stdout, stderr=stderr)
        self.assertEqual(code, 0)
        out = stdout.getvalue().decode("utf-8")
        self.assertIn("-32600", out)

    def test_load_config_example_ignores_comments(self) -> None:
        example = Path(__file__).resolve().parents[1] / "config" / "mcp.example.json"
        # Example uses placeholder absolute paths; load_config requires files only at runtime
        # for token/journal existence? Currently only absolute path check — token need not exist yet.
        cfg = load_config(example)
        self.assertEqual(cfg.schema_version, 1)
        self.assertEqual(cfg.token_ref.kind, "file")
        self.assertEqual(cfg.max_items_per_page, 50)
        self.assertEqual(cfg.page_size, 2)
        self.assertEqual(cfg.profile, "read_only")
        self.assertFalse(cfg.trusted_write_client)
        self.assertIsNone(cfg.browser_origin)

    def test_load_config_c1_schema_requires_version_token_ref_and_cap(self) -> None:
        """R1: C1 §9 closed settings; unversioned token_file replacement is rejected."""
        base = {
            "schema_version": 1,
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_ref": {"kind": "file", "path": str(self.token_path)},
            "journal_path": str(self.journal_path),
            "page_size": 2,
            "max_items_per_page": 7,
            "profile": "read_only",
            "browser_origin": None,
            "trusted_write_client": False,
        }
        ok = self.root / "c1-ok.json"
        ok.write_text(json.dumps(base), encoding="utf-8")
        cfg = load_config(ok)
        self.assertEqual(cfg.schema_version, 1)
        self.assertEqual(cfg.token_ref.path, self.token_path)
        self.assertEqual(cfg.max_items_per_page, 7)
        self.assertEqual(cfg.page_size, 2)

        # Propagate configured cap into VikunjaClient (not silent default 50).
        server = McpServer(cfg)
        try:
            self.assertEqual(server.client.max_items_per_page, 7)
            self.assertEqual(server.client.page_size, 2)
        finally:
            server.close()

        missing_version = dict(base)
        del missing_version["schema_version"]
        path = self.root / "missing-version.json"
        path.write_text(json.dumps(missing_version), encoding="utf-8")
        with self.assertRaises(McpError) as raised:
            load_config(path)
        self.assertIn("schema_version", str(raised.exception))

        bad_version = {**base, "schema_version": 2}
        path = self.root / "bad-version.json"
        path.write_text(json.dumps(bad_version), encoding="utf-8")
        with self.assertRaises(McpError):
            load_config(path)

        missing_cap = dict(base)
        del missing_cap["max_items_per_page"]
        path = self.root / "missing-cap.json"
        path.write_text(json.dumps(missing_cap), encoding="utf-8")
        with self.assertRaises(McpError) as raised:
            load_config(path)
        self.assertIn("max_items_per_page", str(raised.exception))

        # Legacy unversioned token_file replacement must fail closed as unknown.
        legacy = {
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_file": str(self.token_path),
            "journal_path": str(self.journal_path),
            "page_size": 2,
            "profile": "read_only",
            "trusted_write_client": False,
        }
        path = self.root / "legacy-token-file.json"
        path.write_text(json.dumps(legacy), encoding="utf-8")
        with self.assertRaises(McpError) as raised:
            load_config(path)
        self.assertIn("unknown config fields", str(raised.exception))
        self.assertIn("token_file", str(raised.exception))

        oversized_page = {**base, "page_size": 8, "max_items_per_page": 7}
        path = self.root / "oversized-page.json"
        path.write_text(json.dumps(oversized_page), encoding="utf-8")
        with self.assertRaises(McpError):
            load_config(path)

    def test_scope_denied_maps_to_denied(self) -> None:
        self._init()
        self.client.fail_create_with = Outcome.SCOPE_DENIED
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-denied"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert resp is not None
        payload = resp["result"]["structuredContent"]
        self.assertEqual(payload["status"], "denied")
        self.assertTrue(resp["result"]["isError"])

    def test_create_http_400_validation_refusal_first_replay_reconcile(self) -> None:
        """R1: definitive HTTP-400 create refusal → invalid_input, not unknown.

        Real McpServer + IntegrationDispatcher with a synthetic HTTP-400
        TaskResult: first create, same-command replay, and reconcile must all
        stay invalid_input/invalid_argument with one create attempt and zero
        objects. send_evidence must not blanket-remap VALIDATION_ERROR.
        """
        self._init()
        self.client.fail_create_with = Outcome.VALIDATION_ERROR
        self.client.fail_create_mutation_sent = True
        self.client.fail_create_http_status = 400
        command_ref = str(uuid.uuid4())
        args = {"title": "EA010-SYNTHETIC-http400-refusal"}
        first = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": args,
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert first is not None
        first_payload = first["result"]["structuredContent"]
        self.assertEqual(first_payload["status"], "invalid_input")
        self.assertEqual(first_payload.get("reason"), "invalid_argument")
        self.assertTrue(first["result"]["isError"])
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(len(self.client.tasks), 0)
        op_ref = first_payload["operation_ref"]
        self.assertIsInstance(op_ref, str)
        row = self.journal.get_operation(self.issuer, op_ref)
        assert row is not None
        self.assertIs(row.state, OperationState.UNKNOWN)
        self.assertIsNone(row.receipt)
        history = self.journal.history(self.issuer, op_ref)
        self.assertTrue(
            any(
                (event.get("detail") or {}).get("definitive_refusal") is True
                for event in history
            ),
            "durable history must retain definitive_refusal",
        )

        replay = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 13,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": args,
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert replay is not None
        replay_payload = replay["result"]["structuredContent"]
        self.assertEqual(replay_payload["status"], "invalid_input")
        self.assertEqual(replay_payload.get("reason"), "invalid_argument")
        self.assertEqual(replay_payload["operation_ref"], op_ref)
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(len(self.client.tasks), 0)

        reconciled = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 14,
                "method": "tools/call",
                "params": {"name": "ea010_reconcile", "arguments": {"operation_ref": op_ref}},
            }
        )
        assert reconciled is not None
        recon_payload = reconciled["result"]["structuredContent"]
        self.assertEqual(recon_payload["status"], "invalid_input")
        self.assertEqual(recon_payload.get("reason"), "invalid_argument")
        self.assertEqual(recon_payload["operation_ref"], op_ref)
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(len(self.client.tasks), 0)

    def test_validation_error_send_evidence_without_definitive_stays_unknown(self) -> None:
        """R1: missing/invalid GET after uncertain send stays unknown, not invalid_input."""
        created = self.dispatcher.execute_create(
            issuer_id=self.issuer,
            operation_id=str(uuid.uuid4()),
            title="EA010-SYNTHETIC-post-send-missing",
            user_portion="",
            due=None,
            source_kind=SOURCE_KIND,
            source_key=str(uuid.uuid4()),
        )
        self.assertTrue(created.ok)
        assert created.operation is not None
        # Fabricate post-send recover-shaped VALIDATION_ERROR without definitive_refusal
        # (stock 404 after possible write — not a typed API argument refusal).
        missing_shaped = DispatchResult(
            outcome=Outcome.VALIDATION_ERROR,
            operation=created.operation,
            create_http_attempts=1,
            details={
                "path": "mutation_unresolved",
                "phase": "unknown_mutation_read",
                "reason": "object_unreadable_or_gone",
                "http_status": 404,
            },
        )
        history = self.journal.history(self.issuer, created.operation.operation_id)
        payload = map_dispatch_to_wire(self.config, missing_shaped, history=history)
        self.assertEqual(payload["status"], "unknown")
        self.assertEqual(payload.get("reason"), "insufficient_evidence")
        self.assertNotEqual(payload["status"], "invalid_input")

        refused = DispatchResult(
            outcome=Outcome.VALIDATION_ERROR,
            operation=created.operation,
            create_http_attempts=1,
            details={
                "path": "definitive_create_refusal",
                "definitive_refusal": True,
                "api_outcome": "validation_error",
                "mutation_sent": True,
                "resend_forbidden": True,
                "forbid_create": True,
            },
        )
        refused_payload = map_dispatch_to_wire(self.config, refused, history=history)
        self.assertEqual(refused_payload["status"], "invalid_input")
        self.assertEqual(refused_payload.get("reason"), "invalid_argument")

    def test_find_schema_declares_root_properties(self) -> None:
        tools = {t["name"]: t for t in _tool_definitions(write_enabled=False)}
        schema = tools["ea010_find"]["inputSchema"]
        self.assertEqual(schema.get("additionalProperties"), False)
        props = schema.get("properties") or {}
        self.assertIn("task_id", props)
        self.assertIn("operation_ref", props)
        # Draft 2020-12 semantics for this published shape:
        # root declarations + oneOf exclusivity (both keys → two matches → fail).
        def accepts(args: dict[str, Any]) -> bool:
            if any(k not in props for k in args):
                return False
            matches = 0
            for branch in schema["oneOf"]:
                required = branch.get("required") or []
                if all(k in args for k in required):
                    matches += 1
            return matches == 1

        self.assertTrue(accepts({"task_id": 1}))
        self.assertTrue(accepts({"operation_ref": str(uuid.uuid4())}))
        self.assertFalse(accepts({}))
        self.assertFalse(accepts({"task_id": 1, "operation_ref": str(uuid.uuid4())}))
        self.assertFalse(accepts({"task_id": 1, "extra": True}))

    def test_trusted_write_client_requires_json_boolean(self) -> None:
        base = {
            "schema_version": 1,
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_ref": {"kind": "file", "path": str(self.token_path)},
            "journal_path": str(self.journal_path),
            "page_size": 2,
            "max_items_per_page": 50,
            "profile": "write",
            "browser_origin": None,
        }
        for bad in ("false", 0, 1, [], {}, "true"):
            path = self.root / f"bad-trusted-{hash(str(bad))}.json"
            path.write_text(json.dumps({**base, "trusted_write_client": bad}), encoding="utf-8")
            with self.assertRaises(McpError):
                load_config(path)
        path_ok = self.root / "ok-trusted.json"
        path_ok.write_text(json.dumps({**base, "trusted_write_client": False}), encoding="utf-8")
        cfg = load_config(path_ok)
        self.assertFalse(cfg.accepts_write_commands)

    def test_browser_origin_rejects_non_origin_urls(self) -> None:
        base = {
            "schema_version": 1,
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_ref": {"kind": "file", "path": str(self.token_path)},
            "journal_path": str(self.journal_path),
            "page_size": 2,
            "max_items_per_page": 50,
            "profile": "read_only",
            "trusted_write_client": False,
        }
        for bad in (
            "javascript:alert(1)//",
            "http://user:pass@127.0.0.1:19345",
            "http://127.0.0.1:19345/tasks",
            "http://127.0.0.1:19345/?q=1",
            "http://127.0.0.1:19345/#frag",
            "ftp://127.0.0.1:19345",
        ):
            path = self.root / f"bad-origin-{abs(hash(bad))}.json"
            path.write_text(json.dumps({**base, "browser_origin": bad}), encoding="utf-8")
            with self.assertRaises(McpError):
                load_config(path)

    def test_invalid_jsonrpc_id_rejects_before_mutation(self) -> None:
        self._init()
        before = self.client.create_calls
        for bad_id in ({"bad": "id"}, ["x"], True, False, "\ud800"):
            resp = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": bad_id,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_create",
                        "arguments": {"title": "EA010-SYNTHETIC-bad-id"},
                        "_meta": _meta(str(uuid.uuid4())),
                    },
                }
            )
            assert resp is not None
            self.assertEqual(resp["error"]["code"], JSONRPC_INVALID_REQUEST)
            self.assertIsNone(resp.get("id"))
            self.assertEqual(self.client.create_calls, before)

    def test_find_maps_api_error_to_unavailable_tool_result(self) -> None:
        self._init()
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection refused",
            details={"phase": "connect", "reason": "Connection refused"},
        )
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 40,
                "method": "tools/call",
                "params": {"name": "ea010_find", "arguments": {"task_id": 1}},
            }
        )
        assert resp is not None
        self.assertNotIn("error", resp)
        structured = resp["result"]["structuredContent"]
        self.assertEqual(structured["status"], "unavailable")
        self.assertEqual(structured["reason"], "transport")
        self.assertTrue(resp["result"]["isError"])
        self.assertIsNone(structured["operation_ref"])

    def test_find_by_ref_preserves_operation_ref_on_api_error(self) -> None:
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 41,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-find-ref-transport"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        op_ref = created["result"]["structuredContent"]["operation_ref"]
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection refused",
            details={"phase": "connect", "reason": "Connection refused"},
        )
        resp = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 42,
                "method": "tools/call",
                "params": {
                    "name": "ea010_find",
                    "arguments": {"operation_ref": op_ref},
                },
            }
        )
        assert resp is not None
        structured = resp["result"]["structuredContent"]
        self.assertEqual(structured["status"], "unavailable")
        self.assertEqual(structured["reason"], "transport")
        self.assertEqual(structured["operation_ref"], op_ref)
        self.assertTrue(resp["result"]["isError"])

    def test_stdio_surrogate_id_rejects_without_mutation(self) -> None:
        """Framed stdio path: JSON \\ud800 id must not admit create or crash encode."""
        stdin = io.BytesIO()
        stdout = io.BytesIO()
        stderr = io.BytesIO()

        def _line(obj: dict[str, Any]) -> bytes:
            return json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n"

        stdin.write(
            _line(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "unit", "version": "1"},
                    },
                }
            )
        )
        stdin.write(_line({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        # ensure_ascii=True emits a valid UTF-8 escape that json.loads turns into U+D800.
        stdin.write(
            _line(
                {
                    "jsonrpc": "2.0",
                    "id": "\ud800",
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_create",
                        "arguments": {"title": "EA010-SYNTHETIC-surrogate-id"},
                        "_meta": _meta(str(uuid.uuid4())),
                    },
                }
            )
        )
        stdin.seek(0)
        before = self.client.create_calls
        rc = self.server.serve(stdin=stdin, stdout=stdout, stderr=stderr)
        self.assertEqual(rc, 0)
        self.assertEqual(self.client.create_calls, before)
        lines = [json.loads(line) for line in stdout.getvalue().decode("utf-8").splitlines() if line]
        self.assertGreaterEqual(len(lines), 2)
        init_resp, bad_resp = lines[0], lines[-1]
        self.assertIn("result", init_resp)
        self.assertEqual(bad_resp["error"]["code"], JSONRPC_INVALID_REQUEST)
        self.assertIsNone(bad_resp.get("id"))

    def test_lifecycle_requires_initialize_before_initialized(self) -> None:
        # Premature initialized must not enable tools.
        self.assertIsNone(
            self.server.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
        )
        self.assertFalse(self.server._initialized)
        blocked = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 20,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-premature"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert blocked is not None
        self.assertIn("error", blocked)
        self.assertEqual(self.client.create_calls, 0)
        # Missing required initialize fields.
        missing = self.server.handle_message(
            {"jsonrpc": "2.0", "id": 21, "method": "initialize", "params": {}}
        )
        assert missing is not None
        self.assertIn("error", missing)
        self.assertFalse(self.server._initialize_succeeded)

    def test_wire_mapping_ambiguity_and_unavailable(self) -> None:
        cases = [
            (
                DispatchResult(
                    outcome=Outcome.CONFLICT,
                    details={"reason": "multiple_exact_matches", "ids": [1, 2]},
                ),
                "ambiguity",
                "insufficient_evidence",
            ),
            (
                DispatchResult(
                    outcome=Outcome.CONFLICT,
                    details={"reason": "preimage_drift_before_patch"},
                ),
                "conflict",
                "immutable_conflict",
            ),
            (
                DispatchResult(
                    outcome=Outcome.DEPENDENCY_UNAVAILABLE,
                    details={"phase": "preimage", "mutation_sent": False},
                ),
                "unavailable",
                "transport",
            ),
            (
                DispatchResult(
                    outcome=Outcome.DEPENDENCY_UNAVAILABLE,
                    create_http_attempts=1,
                    details={"phase": "dispatch", "mutation_sent": True},
                ),
                "unknown",
                "insufficient_evidence",
            ),
        ]
        for result, status, reason in cases:
            with self.subTest(status=status, reason=reason):
                payload = map_dispatch_to_wire(self.config, result)
                self.assertEqual(payload["status"], status)
                self.assertEqual(payload.get("reason"), reason)
                if status == "unavailable" and result.operation is None:
                    self.assertIsNone(payload["operation_ref"])

    def test_update_response_loss_then_unavailable_reads_is_unknown(self) -> None:
        """R1: durable dispatch claim → dependency_unavailable maps to unknown."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 50,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-update-loss-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = created["result"]["structuredContent"]["task"]["id"]
        original_update = self.client.update

        def lose_response(tid: int, patch: dict[str, Any]) -> TaskResult:
            original_update(tid, patch)
            self.client.raise_read_with = ApiError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "synthetic connection lost after write",
            )
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                mutation_sent=True,
                details={"phase": "patch", "synthetic_response_lost": True},
            )

        self.client.update = lose_response  # type: ignore[method-assign]
        updated = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 51,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {
                        "task_id": task_id,
                        "patch": {"title": "EA010-SYNTHETIC-update-loss-written"},
                    },
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert updated is not None
        payload = updated["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unknown")
        self.assertEqual(payload.get("reason"), "insufficient_evidence")
        op_ref = payload["operation_ref"]
        self.assertIsInstance(op_ref, str)
        row = self.journal.get_operation(self.issuer, op_ref)
        assert row is not None
        self.assertIs(row.delivery_state, DeliveryState.UNKNOWN)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-update-loss-written")
        reconciled = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 52,
                "method": "tools/call",
                "params": {"name": "ea010_reconcile", "arguments": {"operation_ref": op_ref}},
            }
        )
        assert reconciled is not None
        self.assertEqual(reconciled["result"]["structuredContent"]["status"], "unknown")

    def test_complete_response_loss_then_unavailable_reads_is_unknown(self) -> None:
        """R1: complete after possible send stays unknown when later reads fail."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 53,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-complete-loss-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = created["result"]["structuredContent"]["task"]["id"]
        original_complete = self.client.complete

        def lose_complete(tid: int) -> TaskResult:
            original_complete(tid)
            self.client.raise_read_with = ApiError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "synthetic connection lost after complete",
            )
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                mutation_sent=True,
                details={"phase": "complete", "synthetic_response_lost": True},
            )

        self.client.complete = lose_complete  # type: ignore[method-assign]
        completed = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 54,
                "method": "tools/call",
                "params": {
                    "name": "ea010_complete",
                    "arguments": {"task_id": task_id},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert completed is not None
        payload = completed["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unknown")
        op_ref = payload["operation_ref"]
        reconciled = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 55,
                "method": "tools/call",
                "params": {"name": "ea010_reconcile", "arguments": {"operation_ref": op_ref}},
            }
        )
        assert reconciled is not None
        self.assertEqual(reconciled["result"]["structuredContent"]["status"], "unknown")
        self.assertTrue(self.client.tasks[task_id]["done"])

    def test_journal_error_before_send_is_unavailable_storage(self) -> None:
        """R2: JournalError at lookup boundary is a tool result, not -32603."""
        self._init()
        fault = JournalError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic storage unavailable",
            storage_kind="unavailable",
        )
        known_ref = str(uuid.uuid4())
        with patch.object(self.journal, "lookup_source", side_effect=fault), patch.object(
            self.journal, "get_operation", side_effect=fault
        ):
            create = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 60,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_create",
                        "arguments": {"title": "EA010-SYNTHETIC-storage"},
                        "_meta": _meta(str(uuid.uuid4())),
                    },
                }
            )
            find = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 61,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_find",
                        "arguments": {"operation_ref": known_ref},
                    },
                }
            )
            reconcile = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 62,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_reconcile",
                        "arguments": {"operation_ref": known_ref},
                    },
                }
            )
        assert create is not None and find is not None and reconcile is not None
        for resp in (create, find, reconcile):
            self.assertNotIn("error", resp)
            structured = resp["result"]["structuredContent"]
            self.assertEqual(structured["status"], "unavailable")
            self.assertEqual(structured["reason"], "storage")
            self.assertTrue(resp["result"]["isError"])
        self.assertIsNone(create["result"]["structuredContent"]["operation_ref"])
        self.assertEqual(find["result"]["structuredContent"]["operation_ref"], known_ref)
        self.assertEqual(reconcile["result"]["structuredContent"]["operation_ref"], known_ref)
        self.assertEqual(self.client.create_calls, 0)
        self.assertEqual(self.client.update_calls, 0)
        self.assertEqual(self.client.complete_calls, 0)

    def test_deleted_linked_task_find_by_ref_is_unknown(self) -> None:
        """R3: stock 404 on a receipt-linked task must not become invalid_input."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 70,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-deleted-linked"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        structured = created["result"]["structuredContent"]
        self.assertEqual(structured["status"], "confirmed")
        task_id = structured["task"]["id"]
        op_ref = structured["operation_ref"]
        del self.client.tasks[task_id]
        by_id = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 71,
                "method": "tools/call",
                "params": {"name": "ea010_find", "arguments": {"task_id": task_id}},
            }
        )
        by_ref = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 72,
                "method": "tools/call",
                "params": {"name": "ea010_find", "arguments": {"operation_ref": op_ref}},
            }
        )
        reconcile = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 73,
                "method": "tools/call",
                "params": {"name": "ea010_reconcile", "arguments": {"operation_ref": op_ref}},
            }
        )
        assert by_id is not None and by_ref is not None and reconcile is not None
        self.assertEqual(by_id["result"]["structuredContent"]["status"], "not_found")
        self.assertEqual(by_ref["result"]["structuredContent"]["status"], "unknown")
        self.assertEqual(by_ref["result"]["structuredContent"]["operation_ref"], op_ref)
        self.assertNotEqual(by_ref["result"]["structuredContent"].get("reason"), "invalid_argument")
        self.assertEqual(reconcile["result"]["structuredContent"]["status"], "unknown")
        row = self.journal.get_operation(self.issuer, op_ref)
        assert row is not None
        self.assertIsNotNone(row.receipt)
        self.assertIs(row.state, OperationState.CONFIRMED)

    def test_durable_delivery_history_maps_unavailable_to_unknown(self) -> None:
        """R1: recover result without mutation_sent still uses durable delivery history."""
        created = self.dispatcher.execute_create(
            issuer_id=self.issuer,
            operation_id=str(uuid.uuid4()),
            title="EA010-SYNTHETIC-durable-send",
            user_portion="",
            due=None,
            source_kind=SOURCE_KIND,
            source_key=str(uuid.uuid4()),
        )
        self.assertTrue(created.ok)
        assert created.operation is not None and created.vikunja_id is not None
        # Fabricate a post-send recover-shaped result without transient mutation_sent.
        recover_shaped = DispatchResult(
            outcome=Outcome.DEPENDENCY_UNAVAILABLE,
            operation=created.operation,
            details={"path": "mutation_unresolved", "phase": "unknown_mutation_read"},
        )
        history = self.journal.history(self.issuer, created.operation.operation_id)
        payload = map_dispatch_to_wire(self.config, recover_shaped, history=history)
        self.assertEqual(payload["status"], "unknown")
        self.assertEqual(payload["operation_ref"], created.operation.operation_id)

    def test_invalid_due_calendar_and_fractional_no_mutation(self) -> None:
        self._init()
        before = self.client.create_calls
        bad_date = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {
                        "title": "EA010-SYNTHETIC-bad-date",
                        "due": {"kind": "date", "value": "2026-02-30"},
                    },
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert bad_date is not None
        self.assertEqual(bad_date["result"]["structuredContent"]["status"], "invalid_input")
        self.assertEqual(self.client.create_calls, before)
        # Create one task, then invalid fractional update.
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 31,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-frac-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = created["result"]["structuredContent"]["task"]["id"]
        updates_before = self.client.update_calls
        frac = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 32,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {
                        "task_id": task_id,
                        "patch": {
                            "due": {
                                "kind": "datetime",
                                "value": "2026-12-01T12:00:00.5+03:00",
                            }
                        },
                    },
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert frac is not None
        self.assertEqual(frac["result"]["structuredContent"]["status"], "invalid_input")
        self.assertEqual(self.client.update_calls, updates_before)

    def test_post_send_storage_fault_preserves_operation_ref_as_unknown(self) -> None:
        """R1: JournalError after a successful write keeps operation_ref and unknown."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 80,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-storage-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        base = created["result"]["structuredContent"]
        self.assertEqual(base["status"], "confirmed")
        task_id = int(base["task"]["id"])
        fault = JournalError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic storage unavailable",
            storage_kind="unavailable",
        )
        state = {"fail_reads": False}
        real_get = self.journal.get_operation
        real_receipt = self.journal.record_receipt

        def gated_receipt(*args: Any, **kwargs: Any) -> Any:
            state["fail_reads"] = True
            raise fault

        def gated_get(*args: Any, **kwargs: Any) -> Any:
            if state["fail_reads"]:
                raise fault
            return real_get(*args, **kwargs)

        update_ref = str(uuid.uuid4())
        with patch.object(self.journal, "record_receipt", side_effect=gated_receipt), patch.object(
            self.journal, "get_operation", side_effect=gated_get
        ):
            updated = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 81,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_update",
                        "arguments": {
                            "task_id": task_id,
                            "patch": {"title": "EA010-SYNTHETIC-storage-written"},
                        },
                        "_meta": _meta(update_ref),
                    },
                }
            )
        assert updated is not None
        self.assertNotIn("error", updated)
        payload = updated["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unknown")
        self.assertIn(payload.get("reason"), {"storage", "insufficient_evidence"})
        self.assertIsNotNone(payload.get("operation_ref"))
        self.assertNotEqual(payload["status"], "unavailable")
        self.assertEqual(self.client.update_calls, 1)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-storage-written")
        mapped = self.journal.lookup_source(SOURCE_KIND, update_ref)
        assert mapped is not None
        self.assertEqual(payload["operation_ref"], mapped.operation_id)
        row = self.journal.get_operation(self.issuer, mapped.operation_id)
        assert row is not None
        self.assertIn(
            row.delivery_state,
            {DeliveryState.DISPATCH_STARTED, DeliveryState.UNKNOWN},
        )

    def test_unknown_update_deleted_target_reconcile_is_unknown(self) -> None:
        """R2: deleted target of unknown update/complete must not become invalid_input."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 90,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-unknown-del-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        fault = JournalError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic lost response before receipt",
            storage_kind="unavailable",
        )
        update_ref = str(uuid.uuid4())
        with patch.object(self.journal, "record_receipt", side_effect=fault):
            lost = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 91,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_update",
                        "arguments": {
                            "task_id": task_id,
                            "patch": {"title": "EA010-SYNTHETIC-unknown-del-updated"},
                        },
                        "_meta": _meta(update_ref),
                    },
                }
            )
        assert lost is not None
        lost_payload = lost["result"]["structuredContent"]
        self.assertEqual(lost_payload["status"], "unknown")
        op_ref = lost_payload["operation_ref"]
        self.assertIsNotNone(op_ref)
        row = self.journal.get_operation(self.issuer, op_ref)
        assert row is not None
        self.assertIsNone(row.receipt)
        self.assertIn(
            row.delivery_state,
            {DeliveryState.DISPATCH_STARTED, DeliveryState.UNKNOWN},
        )

        del self.client.tasks[task_id]

        # Restart adapter instance on the same durable journal (reconnect identity).
        restarted = object.__new__(McpServer)
        restarted.config = self.config
        restarted.journal = self.journal
        restarted.issuer_id = self.issuer
        restarted.client = self.client  # type: ignore[assignment]
        restarted.dispatcher = IntegrationDispatcher(self.journal, self.client)  # type: ignore[arg-type]
        restarted._initialize_succeeded = False
        restarted._initialized = False
        restarted._closed = False
        restarted._active_operation_ref = None
        restarted._tools = _tool_definitions(write_enabled=True)
        restarted.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 92,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "unit-restart", "version": "0"},
                },
            }
        )
        restarted.handle_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
        reconciled = restarted.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 93,
                "method": "tools/call",
                "params": {"name": "ea010_reconcile", "arguments": {"operation_ref": op_ref}},
            }
        )
        assert reconciled is not None
        payload = reconciled["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unknown")
        self.assertEqual(payload.get("operation_ref"), op_ref)
        self.assertEqual(payload.get("reason"), "insufficient_evidence")
        self.assertNotEqual(payload.get("reason"), "invalid_argument")
        self.assertNotEqual(payload["status"], "invalid_input")

    def test_create_prepare_intent_storage_failure_keeps_null_operation_ref(self) -> None:
        """R1: prepare_intent DEPENDENCY_UNAVAILABLE must not invent operation_ref."""
        self._init()
        command_ref = str(uuid.uuid4())
        fault = JournalError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic prepare_intent storage unavailable",
            storage_kind="unavailable",
        )
        before = self.client.create_calls
        with patch.object(self.journal, "prepare_intent", side_effect=fault):
            created = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 100,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_create",
                        "arguments": {"title": "EA010-SYNTHETIC-prepare-fail"},
                        "_meta": _meta(command_ref),
                    },
                }
            )
        assert created is not None
        self.assertNotIn("error", created)
        payload = created["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload.get("reason"), "storage")
        self.assertIsNone(payload["operation_ref"])
        self.assertEqual(self.client.create_calls, before)
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))
        # Replay must not advertise a durable ref that never existed.
        with patch.object(self.journal, "prepare_intent", side_effect=fault):
            again = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": 101,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_create",
                        "arguments": {"title": "EA010-SYNTHETIC-prepare-fail"},
                        "_meta": _meta(command_ref),
                    },
                }
            )
        assert again is not None
        again_payload = again["result"]["structuredContent"]
        self.assertEqual(again_payload["status"], "unavailable")
        self.assertIsNone(again_payload["operation_ref"])

    def test_update_preimage_transport_failure_keeps_null_operation_ref(self) -> None:
        """R1: initial update preimage GET failure must keep null operation_ref."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 110,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-preimage-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        command_ref = str(uuid.uuid4())
        updates_before = self.client.update_calls
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection refused on preimage GET",
            details={"phase": "connect", "reason": "Connection refused"},
        )
        updated = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 111,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {
                        "task_id": task_id,
                        "patch": {"title": "EA010-SYNTHETIC-preimage-fail"},
                    },
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert updated is not None
        payload = updated["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload.get("reason"), "transport")
        self.assertIsNone(payload["operation_ref"])
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))
        # find/reconcile of a fabricated UUID would be not_found; no durable mapping.
        missing_ref = str(uuid.uuid4())
        by_ref = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 112,
                "method": "tools/call",
                "params": {
                    "name": "ea010_find",
                    "arguments": {"operation_ref": missing_ref},
                },
            }
        )
        assert by_ref is not None
        self.assertEqual(by_ref["result"]["structuredContent"]["status"], "not_found")

    def test_complete_preimage_transport_failure_keeps_null_operation_ref(self) -> None:
        """R1: initial complete preimage GET failure must keep null operation_ref."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 120,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-complete-preimage-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        command_ref = str(uuid.uuid4())
        completes_before = self.client.complete_calls
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection refused on complete preimage GET",
            details={"phase": "connect", "reason": "Connection refused"},
        )
        completed = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 121,
                "method": "tools/call",
                "params": {
                    "name": "ea010_complete",
                    "arguments": {"task_id": task_id},
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert completed is not None
        payload = completed["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload.get("reason"), "transport")
        self.assertIsNone(payload["operation_ref"])
        self.assertEqual(self.client.complete_calls, completes_before)
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))

    def test_update_preimage_auth_denied_keeps_null_operation_ref(self) -> None:
        """R2: update preimage 401 must not advertise an unpersisted operation_ref."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 130,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-update-auth-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        command_ref = str(uuid.uuid4())
        updates_before = self.client.update_calls
        self.client.raise_read_with = ApiError(
            Outcome.AUTH_DENIED,
            "unauthorized preimage GET",
            details={"phase": "preimage", "http_status": 401},
        )
        refs: list[Any] = []
        for req_id in (131, 132):
            updated = self.server.handle_message(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "tools/call",
                    "params": {
                        "name": "ea010_update",
                        "arguments": {
                            "task_id": task_id,
                            "patch": {"title": "EA010-SYNTHETIC-update-auth-denied"},
                        },
                        "_meta": _meta(command_ref),
                    },
                }
            )
            assert updated is not None
            payload = updated["result"]["structuredContent"]
            self.assertEqual(payload["status"], "denied")
            self.assertEqual(payload.get("reason"), "authorization")
            self.assertIsNone(payload["operation_ref"])
            refs.append(payload["operation_ref"])
        self.assertEqual(refs, [None, None])
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))
        missing = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 133,
                "method": "tools/call",
                "params": {
                    "name": "ea010_reconcile",
                    "arguments": {"operation_ref": str(uuid.uuid4())},
                },
            }
        )
        assert missing is not None
        self.assertEqual(missing["result"]["structuredContent"]["status"], "not_found")

    def test_update_preimage_scope_denied_keeps_null_operation_ref(self) -> None:
        """R2: update preimage project-scope refusal keeps null operation_ref."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 140,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-update-scope-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        command_ref = str(uuid.uuid4())
        updates_before = self.client.update_calls
        self.client.raise_read_with = ApiError(
            Outcome.SCOPE_DENIED,
            "project scope refused on preimage GET",
            details={"phase": "preimage", "http_status": 403, "reason": "project_scope"},
        )
        updated = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 141,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {
                        "task_id": task_id,
                        "patch": {"title": "EA010-SYNTHETIC-update-scope-denied"},
                    },
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert updated is not None
        payload = updated["result"]["structuredContent"]
        self.assertEqual(payload["status"], "denied")
        self.assertEqual(payload.get("reason"), "project_scope")
        self.assertIsNone(payload["operation_ref"])
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))

    def test_complete_preimage_auth_and_scope_denied_keep_null_operation_ref(self) -> None:
        """R2: complete preimage 401/403 refusals must keep null operation_ref."""
        self._init()
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 150,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-complete-auth-base"},
                    "_meta": _meta(str(uuid.uuid4())),
                },
            }
        )
        assert created is not None
        task_id = int(created["result"]["structuredContent"]["task"]["id"])
        completes_before = self.client.complete_calls
        cases = [
            (
                151,
                ApiError(
                    Outcome.AUTH_DENIED,
                    "unauthorized complete preimage GET",
                    details={"phase": "preimage", "http_status": 401},
                ),
                "authorization",
            ),
            (
                152,
                ApiError(
                    Outcome.SCOPE_DENIED,
                    "project scope refused on complete preimage GET",
                    details={"phase": "preimage", "http_status": 403, "reason": "project_scope"},
                ),
                "project_scope",
            ),
        ]
        for req_id, fault, reason in cases:
            with self.subTest(reason=reason):
                command_ref = str(uuid.uuid4())
                self.client.raise_read_with = fault
                completed = self.server.handle_message(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "method": "tools/call",
                        "params": {
                            "name": "ea010_complete",
                            "arguments": {"task_id": task_id},
                            "_meta": _meta(command_ref),
                        },
                    }
                )
                assert completed is not None
                payload = completed["result"]["structuredContent"]
                self.assertEqual(payload["status"], "denied")
                self.assertEqual(payload.get("reason"), reason)
                self.assertIsNone(payload["operation_ref"])
                self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref))
        self.assertEqual(self.client.complete_calls, completes_before)

    def test_update_and_complete_missing_target_preimage_keep_null_operation_ref(self) -> None:
        """R2: missing-target preimage refusals must not invent operation_ref."""
        self._init()
        missing_id = 999_001
        command_ref_update = str(uuid.uuid4())
        command_ref_complete = str(uuid.uuid4())
        updates_before = self.client.update_calls
        completes_before = self.client.complete_calls
        updated = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 160,
                "method": "tools/call",
                "params": {
                    "name": "ea010_update",
                    "arguments": {
                        "task_id": missing_id,
                        "patch": {"title": "EA010-SYNTHETIC-missing-target"},
                    },
                    "_meta": _meta(command_ref_update),
                },
            }
        )
        assert updated is not None
        update_payload = updated["result"]["structuredContent"]
        self.assertEqual(update_payload["status"], "invalid_input")
        self.assertEqual(update_payload.get("reason"), "invalid_argument")
        self.assertIsNone(update_payload["operation_ref"])
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref_update))
        self.assertEqual(self.client.update_calls, updates_before)

        completed = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 161,
                "method": "tools/call",
                "params": {
                    "name": "ea010_complete",
                    "arguments": {"task_id": missing_id},
                    "_meta": _meta(command_ref_complete),
                },
            }
        )
        assert completed is not None
        complete_payload = completed["result"]["structuredContent"]
        self.assertEqual(complete_payload["status"], "invalid_input")
        self.assertEqual(complete_payload.get("reason"), "invalid_argument")
        self.assertIsNone(complete_payload["operation_ref"])
        self.assertIsNone(self.journal.lookup_source(SOURCE_KIND, command_ref_complete))
        self.assertEqual(self.client.complete_calls, completes_before)

    def test_stdio_huge_integer_id_parse_error_keeps_session(self) -> None:
        """R2: 5000-digit JSON id must not terminate the NDJSON session (Py3.15)."""
        from vikunja_local.mcp import JSONRPC_PARSE

        stdin = io.BytesIO()
        stdout = io.BytesIO()
        stderr = io.BytesIO()

        def _line(obj: dict[str, Any] | str) -> bytes:
            if isinstance(obj, str):
                return obj.encode("utf-8") + b"\n"
            return json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n"

        stdin.write(
            _line(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "unit", "version": "1"},
                    },
                }
            )
        )
        stdin.write(_line({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        # Valid JSON syntax; decoder raises ValueError on integer digit limit.
        huge_id = "9" * 5000
        huge_frame = f'{{"jsonrpc":"2.0","id":{huge_id},"method":"ping"}}'
        self.assertLess(len(huge_frame.encode("utf-8")), 256 * 1024)
        stdin.write(_line(huge_frame))
        stdin.write(_line({"jsonrpc": "2.0", "id": 2, "method": "ping"}))
        stdin.seek(0)
        rc = self.server.serve(stdin=stdin, stdout=stdout, stderr=stderr)
        self.assertEqual(rc, 0)
        lines = [
            json.loads(line) for line in stdout.getvalue().decode("utf-8").splitlines() if line
        ]
        self.assertGreaterEqual(len(lines), 3)
        self.assertIn("result", lines[0])  # initialize
        parse_err = lines[1]
        self.assertEqual(parse_err["error"]["code"], JSONRPC_PARSE)
        self.assertIsNone(parse_err.get("id"))
        self.assertEqual(lines[2].get("result"), {})  # later valid ping

    def test_ea010_mcp_subprocess_huge_integer_id_survives(self) -> None:
        """R2 regression: real ea010_mcp.py subprocess must not exit on huge int id."""
        import subprocess
        import sys

        from vikunja_local.mcp import JSONRPC_PARSE
        from vikunja_local.journal import new_issuer_id

        worktree = Path(__file__).resolve().parents[1]
        script = worktree / "scripts" / "ea010_mcp.py"
        cfg_dir = self.root / "subprocess-huge-id"
        cfg_dir.mkdir()
        token = cfg_dir / "token"
        token.write_text("synthetic-token", encoding="utf-8")
        journal = cfg_dir / "journal.sqlite3"
        cfg_path = cfg_dir / "mcp.json"
        cfg_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "api_origin": "http://127.0.0.1:1",
                    "project_id": 1,
                    "token_ref": {"kind": "file", "path": str(token)},
                    "journal_path": str(journal),
                    "page_size": 2,
                    "max_items_per_page": 50,
                    "profile": "read_only",
                    "browser_origin": None,
                    "issuer_id": new_issuer_id(),
                    "trusted_write_client": False,
                }
            ),
            encoding="utf-8",
        )
        env = {
            "PATH": "/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(worktree),
        }
        proc = subprocess.Popen(
            [sys.executable, "-I", str(script), "--config", str(cfg_path)],
            cwd=str(worktree),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        assert proc.stdin is not None and proc.stdout is not None

        def send(obj: dict[str, Any] | str) -> None:
            if isinstance(obj, str):
                raw = obj.encode("utf-8") + b"\n"
            else:
                raw = json.dumps(obj, separators=(",", ":")).encode("utf-8") + b"\n"
            proc.stdin.write(raw)
            proc.stdin.flush()

        def recv() -> dict[str, Any]:
            line = proc.stdout.readline()
            self.assertTrue(line, "subprocess exited before response")
            return json.loads(line.decode("utf-8"))

        try:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "subprocess-unit", "version": "0"},
                    },
                }
            )
            init = recv()
            self.assertIn("result", init)
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})
            huge_frame = '{"jsonrpc":"2.0","id":' + ("9" * 5000) + ',"method":"ping"}'
            self.assertLess(len(huge_frame.encode("utf-8")), 256 * 1024)
            send(huge_frame)
            parse_err = recv()
            self.assertEqual(parse_err["error"]["code"], JSONRPC_PARSE)
            self.assertIsNone(parse_err.get("id"))
            send({"jsonrpc": "2.0", "id": 2, "method": "ping"})
            ping = recv()
            self.assertEqual(ping.get("result"), {})
        finally:
            if proc.stdin:
                proc.stdin.close()
            try:
                _out, err = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                _out, err = proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, 0, err.decode("utf-8", errors="replace"))
            self.assertNotIn("ValueError", err.decode("utf-8", errors="replace"))

    def test_advertised_id_schemas_include_safe_int_bounds(self) -> None:
        """R1: input/output ID schemas advertise 1..MAX_SAFE_INT."""
        tools = {t["name"]: t for t in _tool_definitions(write_enabled=True)}
        expected = {"type": "integer", "minimum": 1, "maximum": MAX_SAFE_INT}
        self.assertEqual(tools["ea010_update"]["inputSchema"]["properties"]["task_id"], expected)
        self.assertEqual(tools["ea010_complete"]["inputSchema"]["properties"]["task_id"], expected)
        self.assertEqual(tools["ea010_find"]["inputSchema"]["properties"]["task_id"], expected)
        out_id = tools["ea010_create"]["outputSchema"]["properties"]["task"]["oneOf"][1]["properties"][
            "id"
        ]
        self.assertEqual(out_id, expected)

    def test_create_max_safe_int_id_confirmed(self) -> None:
        """R1: dependency ID at MAX_SAFE_INT remains confirmed with verified URL."""
        self._init()
        self.client.next_id = MAX_SAFE_INT
        command_ref = str(uuid.uuid4())
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 200,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-max-safe-int"},
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert created is not None
        payload = created["result"]["structuredContent"]
        self.assertEqual(payload["status"], "confirmed")
        self.assertFalse(created["result"]["isError"])
        self.assertEqual(payload["task"]["id"], MAX_SAFE_INT)
        self.assertEqual(
            payload["task"]["url"],
            f"{self.config.browser_origin}/tasks/{MAX_SAFE_INT}",
        )
        self.assertEqual(payload["link_state"], "verified")
        self.assertIsNotNone(payload["operation_ref"])

        found = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 201,
                "method": "tools/call",
                "params": {
                    "name": "ea010_find",
                    "arguments": {"task_id": MAX_SAFE_INT},
                },
            }
        )
        assert found is not None
        find_payload = found["result"]["structuredContent"]
        self.assertEqual(find_payload["status"], "confirmed")
        self.assertEqual(find_payload["task"]["id"], MAX_SAFE_INT)

    def test_create_beyond_max_safe_int_is_unknown_keeps_operation_ref(self) -> None:
        """R1: unrepresentable dependency ID must not confirm; retain operation_ref."""
        self._init()
        oversized = MAX_SAFE_INT + 1
        self.client.next_id = oversized
        command_ref = str(uuid.uuid4())
        created = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 202,
                "method": "tools/call",
                "params": {
                    "name": "ea010_create",
                    "arguments": {"title": "EA010-SYNTHETIC-beyond-max-safe-int"},
                    "_meta": _meta(command_ref),
                },
            }
        )
        assert created is not None
        payload = created["result"]["structuredContent"]
        self.assertEqual(payload["status"], "unknown")
        self.assertTrue(created["result"]["isError"])
        self.assertIsNone(payload["task"])
        self.assertEqual(payload["link_state"], "not_applicable")
        self.assertEqual(payload.get("reason"), "insufficient_evidence")
        op_ref = payload["operation_ref"]
        self.assertIsNotNone(op_ref)
        mapped = self.journal.lookup_source(SOURCE_KIND, command_ref)
        self.assertIsNotNone(mapped)
        assert mapped is not None
        self.assertEqual(mapped.operation_id, op_ref)

        # Oversized IDs are rejected at the input boundary, not silently confirmed.
        find_by_id = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 203,
                "method": "tools/call",
                "params": {
                    "name": "ea010_find",
                    "arguments": {"task_id": oversized},
                },
            }
        )
        assert find_by_id is not None
        find_payload = find_by_id["result"]["structuredContent"]
        self.assertEqual(find_payload["status"], "invalid_input")
        self.assertEqual(find_payload.get("reason"), "invalid_argument")

        # Find by durable ref keeps unknown / operation_ref without emitting the ID.
        find_by_ref = self.server.handle_message(
            {
                "jsonrpc": "2.0",
                "id": 204,
                "method": "tools/call",
                "params": {
                    "name": "ea010_find",
                    "arguments": {"operation_ref": op_ref},
                },
            }
        )
        assert find_by_ref is not None
        ref_payload = find_by_ref["result"]["structuredContent"]
        self.assertEqual(ref_payload["status"], "unknown")
        self.assertEqual(ref_payload["operation_ref"], op_ref)
        self.assertIsNone(ref_payload["task"])


if __name__ == "__main__":
    unittest.main()
