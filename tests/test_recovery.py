"""Unit tests for exact reconcile and unified dispatcher (s04)."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from typing import Any

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local.api import ApiError, Outcome, TaskResult, build_marker, new_operation_ids
from vikunja_local.dispatcher import DispatchResult, IntegrationDispatcher
from vikunja_local.journal import IntegrationJournal, OperationState, new_issuer_id, new_operation_id
from vikunja_local.reconcile import (
    apply_create_reconcile_to_journal,
    exact_marker_scan,
    reconcile_create_marker,
    reconcile_unknown_mutation,
)


class FakeClient:
    def __init__(self, project_id: int = 2):
        self.project_id = project_id
        self.tasks: dict[int, dict[str, Any]] = {}
        self.next_id = 1
        self.create_calls = 0
        self.complete_calls = 0
        self.update_calls = 0
        self.fail_create_with: Outcome | None = None
        self.fail_create_mutation_sent: bool = True
        self.fail_update_with: Outcome | None = None
        self.fail_update_mutation_sent: bool = False
        self.fail_read_with: Outcome | None = None
        self.raise_read_with: ApiError | None = None
        self.raise_update_with: ApiError | None = None
        self.scan_override: TaskResult | None = None
        self.read_calls = 0
        self.on_find_by_marker: Any | None = None

    def create(self, **kwargs: Any) -> TaskResult:
        self.create_calls += 1
        if self.fail_create_with is not None:
            return TaskResult(
                outcome=self.fail_create_with,
                mutation_sent=self.fail_create_mutation_sent,
                http_status=403 if self.fail_create_with is Outcome.SCOPE_DENIED else 401,
                details={"phase": "create", "synthetic": True},
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
        if kwargs.get("extra_fields"):
            task.update(dict(kwargs["extra_fields"]))
        self.tasks[task_id] = task
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=dict(task),
            http_status=200,
            mutation_sent=True,
            details={"create_http": 201, "task_id": task_id},
        )

    def read(self, task_id: int, **_kwargs: Any) -> TaskResult:
        self.read_calls += 1
        if self.raise_read_with is not None:
            raise self.raise_read_with
        if self.fail_read_with is not None:
            return TaskResult(
                outcome=self.fail_read_with,
                http_status=401 if self.fail_read_with is Outcome.AUTH_DENIED else 403,
                details={"phase": "read", "synthetic": True},
            )
        task = self.tasks.get(task_id)
        if task is None:
            # Stock maps missing tasks to HTTP 404 → VALIDATION_ERROR.
            return TaskResult(
                outcome=Outcome.VALIDATION_ERROR,
                http_status=404,
                details={"missing": True},
            )
        return TaskResult(outcome=Outcome.CONFIRMED, task=dict(task), http_status=200)

    def list_project_tasks(self) -> TaskResult:
        if self.scan_override is not None:
            return self.scan_override
        items = [dict(t) for t in sorted(self.tasks.values(), key=lambda x: x["id"])]
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            http_status=200,
            details={"items": items, "total": len(items), "pages_read": 1},
        )

    def find_by_marker(self, marker: str) -> TaskResult:
        if self.on_find_by_marker is not None:
            self.on_find_by_marker(marker)
        if self.scan_override is not None:
            return self.scan_override
        listed = self.list_project_tasks()
        matches = []
        for item in listed.details.get("items") or []:
            from vikunja_local.api import extract_marker

            extracted = extract_marker(item.get("description"))
            if extracted and extracted["marker"] == marker:
                matches.append(item)
        if not matches:
            return TaskResult(outcome=Outcome.EMPTY, details={"marker": marker})
        if len(matches) > 1:
            return TaskResult(
                outcome=Outcome.CONFLICT,
                details={"marker": marker, "ids": [m["id"] for m in matches]},
            )
        return self.read(int(matches[0]["id"]))

    def update(self, task_id: int, patch: dict[str, Any]) -> TaskResult:
        # Mirror stock client: pre-PATCH read can raise ApiError before mutation_sent.
        if self.raise_update_with is not None:
            raise self.raise_update_with
        self.update_calls += 1
        if self.fail_update_with is not None:
            return TaskResult(
                outcome=self.fail_update_with,
                mutation_sent=self.fail_update_mutation_sent,
                http_status=403 if self.fail_update_with is Outcome.SCOPE_DENIED else 401,
                details={"phase": "patch", "synthetic": True},
            )
        task = self.tasks.get(task_id)
        if task is None:
            return TaskResult(outcome=Outcome.UNKNOWN, mutation_sent=False, details={"missing": True})
        task = dict(task)
        task.update(patch)
        self.tasks[task_id] = task
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=dict(task),
            mutation_sent=True,
            http_status=200,
            details={"patch_http": 200},
        )

    def complete(self, task_id: int) -> TaskResult:
        if self.raise_update_with is not None:
            raise self.raise_update_with
        self.complete_calls += 1
        return self.update(task_id, {"done": True})


class ReconcileUnitTests(unittest.TestCase):
    def test_exact_scan_ignores_substring_decoy(self) -> None:
        client = FakeClient()
        issuer, op = new_operation_ids()
        marker = build_marker(issuer, op)
        client.tasks[1] = {
            "id": 1,
            "project_id": 2,
            "title": "decoy",
            "description": f"xx{marker}yy\nnope",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
        }
        result = reconcile_create_marker(client, issuer_id=issuer, operation_id=op)
        self.assertIs(result.outcome, Outcome.EMPTY)
        self.assertTrue(result.details.get("forbid_create"))

    def test_multiple_exact_is_conflict(self) -> None:
        client = FakeClient()
        issuer, op = new_operation_ids()
        marker = build_marker(issuer, op)
        for i in (1, 2):
            client.tasks[i] = {
                "id": i,
                "project_id": 2,
                "title": f"t{i}",
                "description": marker + f"\nu{i}",
                "due_date": "0001-01-01T00:00:00Z",
                "done": False,
            }
        result = reconcile_create_marker(client, issuer_id=issuer, operation_id=op)
        self.assertIs(result.outcome, Outcome.CONFLICT)
        self.assertEqual(sorted(result.matches), [1, 2])

    def test_incomplete_scan_fail_closed(self) -> None:
        client = FakeClient()
        client.scan_override = TaskResult(
            outcome=Outcome.INCOMPLETE_SCAN,
            details={"reason": "page"},
        )
        issuer, op = new_operation_ids()
        result = exact_marker_scan(client, build_marker(issuer, op))
        self.assertIs(result.outcome, Outcome.INCOMPLETE_SCAN)

    def test_unknown_mutation_does_not_claim_causality(self) -> None:
        client = FakeClient()
        client.tasks[7] = {
            "id": 7,
            "project_id": 2,
            "title": "x",
            "description": "plain",
            "due_date": "2026-12-01T20:59:59Z",
            "done": False,
            "repeat_after": 86400,
            "repeat_mode": 0,
        }
        result = reconcile_unknown_mutation(
            client,
            task_id=7,
            expected_marker=None,
            project_id=2,
            preimage={"due_date": "2026-12-01T20:59:59Z", "done": False},
        )
        self.assertIs(result.outcome, Outcome.CONFIRMED)
        self.assertIs(result.details.get("causal_proof"), False)
        self.assertEqual(client.complete_calls, 0)
        self.assertEqual(client.update_calls, 0)


class DispatcherUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="ea010-recovery-unit-")
        self.journal_path = Path(self._tmpdir.name) / "journal.sqlite3"
        self.journal = IntegrationJournal(self.journal_path)
        self.journal.open()
        self.issuer = self.journal.ensure_issuer(new_issuer_id())
        self.client = FakeClient(project_id=2)

    def tearDown(self) -> None:
        self.journal.close()
        self._tmpdir.cleanup()

    def test_create_receipt_and_controlled_link(self) -> None:
        links: list[dict[str, Any]] = []

        def link_cb(payload: dict[str, Any]) -> dict[str, Any]:
            links.append(dict(payload))
            return {"ok": True, "production_bridge": False}

        disp = IntegrationDispatcher(self.journal, self.client, link_callback=link_cb)
        op = new_operation_id()
        result = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-unit",
            user_portion="ctx",
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.create_http_attempts, 1)
        self.assertTrue(result.linked)
        self.assertIs(result.link_result.get("production_bridge"), False)
        self.assertEqual(len(links), 1)
        replay = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-unit",
            user_portion="ctx",
        )
        self.assertEqual(replay.vikunja_id, result.vikunja_id)
        self.assertEqual(self.client.create_calls, 1)

    def test_uncertain_create_reconciles_single_match(self) -> None:
        disp = IntegrationDispatcher(self.journal, self.client)
        op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "t", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)
            section.mark_unknown(self.issuer, op, reason="timeout")
        marker = build_marker(self.issuer, op)
        self.client.tasks[9] = {
            "id": 9,
            "project_id": 2,
            "title": "t",
            "description": marker + "\n",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
        }
        recovered = disp.recover_create(self.issuer, op)
        self.assertTrue(recovered.ok)
        self.assertEqual(recovered.vikunja_id, 9)
        self.assertEqual(self.client.create_calls, 0)
        row = self.journal.get_operation(self.issuer, op)
        self.assertIsNotNone(row)
        self.assertIs(row.state, OperationState.CONFIRMED)

    def test_zero_match_forbids_create_on_retry(self) -> None:
        disp = IntegrationDispatcher(self.journal, self.client)
        op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "z", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)
            section.mark_unknown(self.issuer, op, reason="timeout")
        recovered = disp.recover_create(self.issuer, op)
        self.assertIs(recovered.outcome, Outcome.UNKNOWN)
        self.assertTrue(recovered.details.get("forbid_create"))
        retry = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="z",
            user_portion="",
        )
        self.assertIs(retry.outcome, Outcome.UNKNOWN)
        self.assertEqual(self.client.create_calls, 0)

    def test_apply_conflict_to_journal(self) -> None:
        client = FakeClient()
        issuer = self.issuer
        op = new_operation_id()
        marker = build_marker(issuer, op)
        for i in (1, 2):
            client.tasks[i] = {
                "id": i,
                "project_id": 2,
                "title": f"t{i}",
                "description": marker + f"\n{i}",
                "due_date": "0001-01-01T00:00:00Z",
                "done": False,
            }
        self.journal.prepare_intent(
            issuer_id=issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "t", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(issuer, op)
            section.mark_unknown(issuer, op, reason="x")
        result = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=issuer,
            operation_id=op,
            client=client,
        )
        self.assertIs(result.outcome, Outcome.CONFLICT)
        row = self.journal.get_operation(issuer, op)
        self.assertIs(row.state, OperationState.CONFLICT)

    def test_unknown_recurring_observe_does_not_complete(self) -> None:
        disp = IntegrationDispatcher(self.journal, self.client)
        # Seed a create receipt so complete intent can resolve marker.
        create_op = new_operation_id()
        create_marker = build_marker(self.issuer, create_op)
        self.client.tasks[3] = {
            "id": 3,
            "project_id": 2,
            "title": "r",
            "description": create_marker + "\n",
            "due_date": "2026-12-01T20:59:59Z",
            "done": False,
            "repeat_after": 86400,
            "repeat_mode": 0,
        }
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=create_op,
            op_type="create",
            project_id=2,
            payload={"title": "r", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, create_op)
            section.record_receipt(
                self.issuer,
                create_op,
                vikunja_id=3,
                project_id=2,
                marker=create_marker,
                effect={"source": "seed"},
            )
        complete_op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=complete_op,
            op_type="complete",
            project_id=2,
            target_id=3,
            payload={
                "patch": {"done": True},
                "preimage": {
                    "id": 3,
                    "due_date": "2026-12-01T20:59:59Z",
                    "done": False,
                },
            },
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, complete_op)
            section.mark_unknown(self.issuer, complete_op, reason="lost")
        due_before = self.client.tasks[3]["due_date"]
        observed = disp.recover_unknown_mutation(self.issuer, complete_op)
        self.assertIs(observed.outcome, Outcome.UNKNOWN)
        self.assertTrue(observed.details.get("resend_forbidden"))
        self.assertEqual(self.client.complete_calls, 0)
        self.assertEqual(self.client.tasks[3]["due_date"], due_before)

    def test_auth_denied_scan_fail_closed(self) -> None:
        self.client.scan_override = TaskResult(outcome=Outcome.AUTH_DENIED, http_status=401)
        result = reconcile_create_marker(
            self.client,
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
        )
        self.assertIs(result.outcome, Outcome.AUTH_DENIED)

    def test_successful_update_and_complete_replay_reuse_preimage(self) -> None:
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-mut",
            user_portion="m",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-mut-2"},
        )
        self.assertTrue(first.ok)
        self.assertEqual(self.client.update_calls, 1)
        replay = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-mut-2"},
        )
        self.assertTrue(replay.ok)
        self.assertEqual(replay.vikunja_id, task_id)
        self.assertEqual(replay.details.get("path"), "mutation_replay")
        self.assertEqual(self.client.update_calls, 1)
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)

        complete_op = new_operation_id()
        done1 = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=complete_op,
            task_id=task_id,
        )
        self.assertTrue(done1.ok)
        calls_after_first = self.client.complete_calls + self.client.update_calls
        done2 = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=complete_op,
            task_id=task_id,
        )
        self.assertTrue(done2.ok)
        self.assertEqual(done2.details.get("path"), "mutation_replay")
        self.assertEqual(
            self.client.complete_calls + self.client.update_calls,
            calls_after_first,
        )

    def test_unknown_update_complete_replay_is_read_only(self) -> None:
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        marker = build_marker(self.issuer, create_op)
        self.client.tasks[11] = {
            "id": 11,
            "project_id": 2,
            "title": "u",
            "description": marker + "\n",
            "due_date": "2026-12-01T20:59:59Z",
            "done": False,
            "repeat_after": 86400,
            "repeat_mode": 0,
        }
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=create_op,
            op_type="create",
            project_id=2,
            payload={"title": "u", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, create_op)
            section.record_receipt(
                self.issuer,
                create_op,
                vikunja_id=11,
                project_id=2,
                marker=marker,
                effect={"source": "seed"},
            )
        complete_op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=complete_op,
            op_type="complete",
            project_id=2,
            target_id=11,
            payload={
                "patch": {"done": True},
                "preimage": {
                    "id": 11,
                    "title": "u",
                    "due_date": "2026-12-01T20:59:59Z",
                    "done": False,
                    "description": marker + "\n",
                    "repeat_after": 86400,
                    "repeat_mode": 0,
                },
            },
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, complete_op)
            section.mark_unknown(self.issuer, complete_op, reason="lost")
        # Simulate late successful stock mutation that changed due/done.
        self.client.tasks[11]["done"] = False
        self.client.tasks[11]["due_date"] = "2026-12-02T20:59:59Z"
        due_before = self.client.tasks[11]["due_date"]
        retry = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=complete_op,
            task_id=11,
        )
        self.assertIs(retry.outcome, Outcome.UNKNOWN)
        self.assertTrue(retry.details.get("resend_forbidden"))
        self.assertEqual(self.client.complete_calls, 0)
        self.assertEqual(self.client.tasks[11]["due_date"], due_before)

    def test_recover_confirmed_create_validates_before_link(self) -> None:
        links: list[dict[str, Any]] = []

        def link_cb(payload: dict[str, Any]) -> dict[str, Any]:
            links.append(dict(payload))
            return {"ok": True}

        disp = IntegrationDispatcher(self.journal, self.client, link_callback=link_cb)
        op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-del",
            user_portion="d",
        )
        self.assertTrue(created.ok)
        self.assertEqual(len(links), 1)
        del self.client.tasks[int(created.vikunja_id)]
        recovered = disp.recover_create(self.issuer, op)
        self.assertIs(recovered.outcome, Outcome.UNKNOWN)
        self.assertFalse(recovered.linked)
        self.assertTrue(recovered.details.get("callback_skipped"))
        self.assertTrue(recovered.details.get("receipt_preserved"))
        self.assertEqual(len(links), 1)
        row = self.journal.get_operation(self.issuer, op)
        self.assertIs(row.state, OperationState.CONFIRMED)

    def test_recover_create_refuses_non_create_in_all_states(self) -> None:
        """R1: kind guard must precede confirmed-receipt shortcuts for update/complete."""
        links: list[dict[str, Any]] = []

        def link_cb(payload: dict[str, Any]) -> dict[str, Any]:
            links.append(dict(payload))
            return {"ok": True}

        disp = IntegrationDispatcher(self.journal, self.client, link_callback=link_cb)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-kind-guard",
            user_portion="kg",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        create_callbacks = len(links)

        # Prepared update: never enter create recovery / callback.
        prepared_op = new_operation_id()
        prepared = self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=prepared_op,
            op_type="update",
            project_id=2,
            target_id=task_id,
            payload={"patch": {"title": "EA010-SYNTHETIC-prepared"}, "preimage": {"title": "x"}},
        )
        self.assertIs(prepared.state, OperationState.PREPARED)
        refused_prepared = disp.recover_create(self.issuer, prepared_op)
        self.assertIs(refused_prepared.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(
            refused_prepared.details.get("reason"),
            "recover_create_requires_create_op",
        )
        self.assertFalse(refused_prepared.linked)
        self.assertEqual(len(links), create_callbacks)
        prepared_helper = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=prepared_op,
            client=self.client,
        )
        self.assertIs(prepared_helper.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(prepared_helper.details.get("reason"), "not_a_create")
        self.assertEqual(prepared_helper.details.get("op_type"), "update")

        # Unknown complete: same typed refusal, no callback.
        unknown_op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=unknown_op,
            op_type="complete",
            project_id=2,
            target_id=task_id,
            payload={"patch": {"done": True}, "preimage": {"done": False}},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, unknown_op)
            section.mark_unknown(self.issuer, unknown_op, reason="timeout")
        refused_unknown = disp.recover_create(self.issuer, unknown_op)
        self.assertIs(refused_unknown.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(
            refused_unknown.details.get("reason"),
            "recover_create_requires_create_op",
        )
        self.assertFalse(refused_unknown.linked)
        self.assertEqual(len(links), create_callbacks)
        unknown_helper = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=unknown_op,
            client=self.client,
        )
        self.assertIs(unknown_helper.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(unknown_helper.details.get("reason"), "not_a_create")
        self.assertEqual(unknown_helper.details.get("op_type"), "complete")

        # Confirmed update/complete on a fresh target: the prior unknown on task_id
        # must keep blocking new keys there (F2), so kind-guard coverage uses a
        # separate synthetic object without unresolved peers.
        confirmed_seed = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            title="EA010-SYNTHETIC-kind-guard-confirmed",
            user_portion="kind-guard-2",
        )
        self.assertTrue(confirmed_seed.ok)
        confirmed_task_id = int(confirmed_seed.vikunja_id)
        update_op = new_operation_id()
        updated = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=confirmed_task_id,
            patch={"title": "EA010-SYNTHETIC-kind-guard-2"},
        )
        self.assertTrue(updated.ok)
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertEqual(row.op_type, "update")
        links.clear()
        refused_confirmed = disp.recover_create(self.issuer, update_op)
        self.assertIs(refused_confirmed.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(
            refused_confirmed.details.get("reason"),
            "recover_create_requires_create_op",
        )
        self.assertFalse(refused_confirmed.linked)
        self.assertEqual(len(links), 0)
        confirmed_helper = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=update_op,
            client=self.client,
        )
        self.assertIs(confirmed_helper.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(confirmed_helper.details.get("reason"), "not_a_create")
        self.assertEqual(confirmed_helper.details.get("op_type"), "update")
        self.assertEqual(len(links), 0)

        # Confirmed complete: same boundary.
        complete_op = new_operation_id()
        completed = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=complete_op,
            task_id=confirmed_task_id,
        )
        self.assertTrue(completed.ok)
        links.clear()
        refused_complete = disp.recover_create(self.issuer, complete_op)
        self.assertIs(refused_complete.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(
            refused_complete.details.get("reason"),
            "recover_create_requires_create_op",
        )
        self.assertFalse(refused_complete.linked)
        self.assertEqual(len(links), 0)
        complete_helper = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=complete_op,
            client=self.client,
        )
        self.assertIs(complete_helper.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(complete_helper.details.get("reason"), "not_a_create")
        self.assertEqual(complete_helper.details.get("op_type"), "complete")
        self.assertEqual(self.client.create_calls, 2)

    def test_scope_drift_reconcile_fail_closed(self) -> None:
        op = new_operation_id()
        marker = build_marker(self.issuer, op)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "scope", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)
            section.mark_unknown(self.issuer, op, reason="scope")
        foreign = FakeClient(project_id=3)
        foreign.tasks[4] = {
            "id": 4,
            "project_id": 3,
            "title": "planted",
            "description": marker + "\n",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
        }
        result = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=op,
            client=foreign,
        )
        self.assertIs(result.outcome, Outcome.SCOPE_DENIED)
        row = self.journal.get_operation(self.issuer, op)
        self.assertIsNone(row.receipt)
        self.assertIsNot(row.state, OperationState.CONFIRMED)
        self.assertEqual(row.project_id, 2)

    def test_claim_race_recovers_confirmed_create(self) -> None:
        """Loser of begin_dispatch must recover the winner's receipt, not return unknown."""
        import threading

        barrier = threading.Barrier(2)
        results: list[DispatchResult] = []
        lock = threading.Lock()

        class RacingJournal:
            """Wrap real journal; pause both callers after eligibility, before claim."""

            def __init__(self, inner: IntegrationJournal):
                self._inner = inner

            def __getattr__(self, name: str) -> Any:
                return getattr(self._inner, name)

            def dispatch_critical_section(self):
                barrier.wait(timeout=5)
                return self._inner.dispatch_critical_section()

        racing = RacingJournal(self.journal)
        client = self.client

        def worker() -> None:
            disp = IntegrationDispatcher(racing, client)  # type: ignore[arg-type]
            op = shared_op
            result = disp.execute_create(
                issuer_id=self.issuer,
                operation_id=op,
                title="EA010-SYNTHETIC-race",
                user_portion="race",
            )
            with lock:
                results.append(result)

        shared_op = new_operation_id()
        # Prepare once so both workers race only on claim/HTTP.
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=shared_op,
            op_type="create",
            project_id=2,
            payload={"title": "EA010-SYNTHETIC-race", "user_portion": "race"},
        )
        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.ok for r in results), [r.outcome for r in results])
        ids = {r.vikunja_id for r in results}
        self.assertEqual(len(ids), 1)
        self.assertEqual(client.create_calls, 1)
        row = self.journal.get_operation(self.issuer, shared_op)
        self.assertIs(row.state, OperationState.CONFIRMED)

    def test_recover_create_reuses_receipt_committed_during_exact_scan(self) -> None:
        """P2: recovery overlapping first receipt persistence must reuse, not CONFLICT.

        Ordering: recover loads DISPATCH_STARTED → exact scan → original writer
        commits create_read_back → recover would otherwise record_receipt with
        source=exact_reconcile and hit incompatible receipt.
        """
        op = new_operation_id()
        marker = build_marker(self.issuer, op)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "EA010-SYNTHETIC-overlap-receipt", "user_portion": "or"},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)

        task = {
            "id": 1,
            "project_id": 2,
            "title": "EA010-SYNTHETIC-overlap-receipt",
            "description": marker + "\nor",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
        }
        self.client.tasks[1] = task
        self.client.next_id = 2
        self.client.create_calls = 1  # original HTTP create already happened

        committed: list[bool] = []

        def commit_original_receipt(_marker: str) -> None:
            if committed:
                return
            committed.append(True)
            with self.journal.dispatch_critical_section() as section:
                section.record_receipt(
                    self.issuer,
                    op,
                    vikunja_id=1,
                    project_id=2,
                    marker=marker,
                    effect={
                        "title": task["title"],
                        "due_date": task["due_date"],
                        "done": False,
                        "create_http": 201,
                        "source": "create_read_back",
                    },
                )

        self.client.on_find_by_marker = commit_original_receipt
        disp = IntegrationDispatcher(self.journal, self.client)
        recovered = disp.recover_create(self.issuer, op)
        self.assertIs(recovered.outcome, Outcome.CONFIRMED, recovered.details)
        self.assertEqual(recovered.vikunja_id, 1)
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(recovered.details.get("reason"), "reused_confirmed_receipt")
        self.assertTrue(recovered.details.get("receipt_preserved"))
        self.assertEqual(recovered.details.get("effect_source"), "create_read_back")
        self.assertTrue(recovered.details.get("live_validated"))
        self.assertEqual(recovered.details.get("path"), "recover_exact_one")
        row = self.journal.get_operation(self.issuer, op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertEqual(int((row.receipt or {})["vikunja_id"]), 1)
        self.assertEqual((row.receipt or {}).get("effect", {}).get("source"), "create_read_back")

    def test_recover_confirmed_create_transport_api_error(self) -> None:
        """P2: confirmed recover_create must return DispatchResult on ApiError, keep receipt."""
        disp = IntegrationDispatcher(self.journal, self.client)
        op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-transport-confirmed",
            user_portion="tc",
        )
        self.assertTrue(created.ok)
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection refused",
            details={"phase": "connect", "reason": "Connection refused"},
        )
        recovered = disp.recover_create(self.issuer, op)
        self.assertIs(recovered.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(recovered.details.get("receipt_preserved"))
        self.assertTrue(recovered.details.get("resend_forbidden"))
        self.assertTrue(recovered.details.get("api_error"))
        row = self.journal.get_operation(self.issuer, op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertIsNotNone(row.receipt)

    def test_unknown_mutation_transport_api_error(self) -> None:
        """P2: unknown mutation GET ApiError stays typed; no resend."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-transport-mut",
            user_portion="tm",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=update_op,
            op_type="update",
            project_id=2,
            target_id=task_id,
            payload={
                "patch": {"title": "EA010-SYNTHETIC-transport-mut-2"},
                "preimage": {"title": "EA010-SYNTHETIC-transport-mut", "done": False},
            },
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, update_op)
            section.mark_unknown(self.issuer, update_op, reason="timeout")
        title_before = self.client.tasks[task_id]["title"]
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "connection reset",
            details={"phase": "transfer"},
        )
        recovered = disp.recover_unknown_mutation(self.issuer, update_op)
        self.assertIs(recovered.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(recovered.details.get("resend_forbidden"))
        self.assertEqual(self.client.update_calls, 0)
        self.assertEqual(self.client.tasks[task_id]["title"], title_before)

    def test_exact_scan_final_get_transport_api_error(self) -> None:
        """P2: ApiError on exact-scan final matching-ID read-back must not raise."""
        op = new_operation_id()
        marker = build_marker(self.issuer, op)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "EA010-SYNTHETIC-scan-get", "user_portion": "sg"},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)
            section.mark_unknown(self.issuer, op, reason="timeout")
        self.client.tasks[9] = {
            "id": 9,
            "project_id": 2,
            "title": "EA010-SYNTHETIC-scan-get",
            "description": marker + "\nsg",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
        }
        self.client.raise_read_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "API returned non-JSON body",
            details={"phase": "read_back"},
        )
        disp = IntegrationDispatcher(self.journal, self.client)
        recovered = disp.recover_create(self.issuer, op)
        self.assertIs(recovered.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(recovered.details.get("forbid_create"))
        self.assertTrue(recovered.details.get("resend_forbidden") or recovered.details.get("api_error"))
        self.assertEqual(self.client.create_calls, 0)
        row = self.journal.get_operation(self.issuer, op)
        self.assertIsNone(row.receipt)
        self.assertIsNot(row.state, OperationState.CONFIRMED)

    def test_changed_patch_replay_conflicts_instead_of_false_confirm(self) -> None:
        """F1: same op key with a different patch must not CONFIRMED-replay the prior receipt."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-A",
            user_portion="f1",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-A"},
        )
        self.assertTrue(first.ok)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-A")
        changed = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-B"},
        )
        self.assertIs(changed.outcome, Outcome.CONFLICT)
        self.assertFalse(changed.ok)
        self.assertEqual(self.client.update_calls, 1)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-A")
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFLICT)

    def test_changed_source_replay_conflicts(self) -> None:
        """F1: source identity is part of the immutable envelope on mutation replay."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-src",
            user_portion="f1s",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-src-2"},
            source_kind="meeting",
            source_key="synth-meeting-1",
        )
        self.assertTrue(first.ok)
        changed = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-src-2"},
            source_kind="meeting",
            source_key="synth-meeting-OTHER",
        )
        self.assertIs(changed.outcome, Outcome.CONFLICT)
        self.assertEqual(self.client.update_calls, 1)

    def test_wrong_project_replay_is_scope_denied(self) -> None:
        """F1: confirmed mutation must not replay against a drifted client project."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-proj",
            user_portion="f1p",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-proj-2"},
        )
        self.assertTrue(first.ok)
        foreign = FakeClient(project_id=999)
        foreign.tasks = dict(self.client.tasks)
        foreign_disp = IntegrationDispatcher(self.journal, foreign)
        replay = foreign_disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-proj-2"},
        )
        self.assertIs(replay.outcome, Outcome.SCOPE_DENIED)
        self.assertFalse(replay.ok)
        self.assertEqual(foreign.update_calls, 0)
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertEqual(row.project_id, 2)

    def test_confirmed_mutation_replay_after_delete_is_unknown(self) -> None:
        """R1: durable update receipt must not report current success after deletion."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-del-mut",
            user_portion="r1",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-del-mut-2"},
        )
        self.assertTrue(first.ok)
        del self.client.tasks[task_id]
        calls_before = self.client.update_calls
        replay = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-del-mut-2"},
        )
        self.assertIs(replay.outcome, Outcome.UNKNOWN)
        self.assertEqual(replay.details.get("reason"), "target_deleted_or_gone")
        self.assertTrue(replay.details.get("receipt_preserved"))
        self.assertTrue(replay.details.get("resend_forbidden"))
        self.assertIsNone(replay.task)
        self.assertEqual(self.client.update_calls, calls_before)
        recovered = disp.recover_unknown_mutation(self.issuer, update_op)
        self.assertIs(recovered.outcome, Outcome.UNKNOWN)
        self.assertEqual(recovered.details.get("reason"), "target_deleted_or_gone")
        self.assertEqual(self.client.update_calls, calls_before)
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertIsNotNone(row.receipt)

    def test_confirmed_mutation_replay_revoked_access(self) -> None:
        """R1: revoked/denied read-back must surface auth, never silent confirmed."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-rev-mut",
            user_portion="r1a",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-rev-mut-2"},
        )
        self.assertTrue(first.ok)
        self.client.fail_read_with = Outcome.AUTH_DENIED
        calls_before = self.client.update_calls
        replay = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-rev-mut-2"},
        )
        self.assertIs(replay.outcome, Outcome.AUTH_DENIED)
        self.assertTrue(replay.details.get("receipt_preserved"))
        self.assertEqual(self.client.update_calls, calls_before)

    def test_confirmed_mutation_replay_marker_loss(self) -> None:
        """R1: confirmed mutation replay after marker removal stays unknown."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-ml-mut",
            user_portion="r1m",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-ml-mut-2"},
        )
        self.assertTrue(first.ok)
        self.client.tasks[task_id]["description"] = "marker-removed"
        calls_before = self.client.update_calls
        replay = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-ml-mut-2"},
        )
        self.assertIs(replay.outcome, Outcome.UNKNOWN)
        self.assertEqual(replay.details.get("reason"), "marker_corrupted_or_removed")
        self.assertEqual(self.client.update_calls, calls_before)
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)

    def test_confirmed_mutation_replay_reflects_manual_title(self) -> None:
        """R1: successful replay returns current Vikunja state after manual edits."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-manual",
            user_portion="r1c",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-manual-2"},
        )
        self.assertTrue(first.ok)
        self.client.tasks[task_id]["title"] = "EA010-SYNTHETIC-manual-UI"
        replay = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-manual-2"},
        )
        self.assertTrue(replay.ok)
        self.assertEqual(replay.details.get("path"), "mutation_replay")
        self.assertEqual((replay.task or {}).get("title"), "EA010-SYNTHETIC-manual-UI")

    def test_update_refuses_when_known_marker_removed(self) -> None:
        """R2: nonempty receipt marker + empty live marker must not dispatch PATCH."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-strip",
            user_portion="r2",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        self.client.tasks[task_id]["description"] = "no-marker-left"
        calls_before = self.client.update_calls
        update_op = new_operation_id()
        result = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-should-not-apply"},
        )
        self.assertIs(result.outcome, Outcome.UNKNOWN)
        self.assertEqual(result.details.get("reason"), "marker_missing_on_known_target")
        self.assertEqual(self.client.update_calls, calls_before)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-strip")
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIsNotNone(row)
        self.assertIs(row.state, OperationState.UNKNOWN)
        retry = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-should-not-apply"},
        )
        self.assertIsNot(retry.outcome, Outcome.CONFIRMED)
        self.assertEqual(self.client.update_calls, calls_before)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-strip")

    def test_complete_refuses_when_known_marker_removed(self) -> None:
        """R2: complete also refuses when the verified target marker was stripped."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-strip-c",
            user_portion="r2c",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        self.client.tasks[task_id]["description"] = ""
        calls_before = self.client.complete_calls + self.client.update_calls
        complete_op = new_operation_id()
        result = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=complete_op,
            task_id=task_id,
        )
        self.assertIs(result.outcome, Outcome.UNKNOWN)
        self.assertEqual(result.details.get("reason"), "marker_missing_on_known_target")
        self.assertEqual(
            self.client.complete_calls + self.client.update_calls,
            calls_before,
        )
        self.assertFalse(self.client.tasks[task_id].get("done"))

    def test_unmarked_manual_target_update_still_allowed(self) -> None:
        """R2: genuinely unmarked manual targets remain supported."""
        disp = IntegrationDispatcher(self.journal, self.client)
        task_id = 42
        self.client.tasks[task_id] = {
            "id": task_id,
            "project_id": 2,
            "title": "manual-unmarked",
            "description": "plain notes",
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        update_op = new_operation_id()
        result = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "manual-unmarked-2"},
        )
        self.assertTrue(result.ok)
        self.assertEqual(self.client.tasks[task_id]["title"], "manual-unmarked-2")
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertEqual((row.receipt or {}).get("marker") or "", "")

    def test_overlapping_first_mutation_reuses_peer_preimage(self) -> None:
        """R1/F1: pause after prepare/admission; peer confirm must not be downgraded."""
        import threading

        for kind in ("update", "complete"):
            with self.subTest(kind=kind):
                client = FakeClient()
                journal = IntegrationJournal(Path(self._tmpdir.name) / f"overlap-{kind}.sqlite3")
                journal.open()
                self.addCleanup(journal.close)
                issuer = journal.ensure_issuer(new_issuer_id())
                disp_a = IntegrationDispatcher(journal, client)
                seed = disp_a.execute_create(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    title=f"EA010-SYNTHETIC-overlap-{kind}",
                    user_portion="ov",
                )
                self.assertTrue(seed.ok)
                task_id = int(seed.vikunja_id)
                op = new_operation_id()
                entered = threading.Event()
                release = threading.Event()
                results: dict[str, DispatchResult] = {}

                client_b = FakeClient()
                client_b.tasks = client.tasks
                client_b.next_id = client.next_id

                def pause_after_prepare(point: str) -> None:
                    if point != "before_mutation_dispatch_lock":
                        return
                    entered.set()
                    self.assertTrue(release.wait(5), "release timed out")

                disp_b = IntegrationDispatcher(
                    journal, client_b, crash_hook=pause_after_prepare
                )

                def second() -> None:
                    if kind == "update":
                        results["b"] = disp_b.execute_update(
                            issuer_id=issuer,
                            operation_id=op,
                            task_id=task_id,
                            patch={"title": f"EA010-SYNTHETIC-overlap-{kind}-2"},
                        )
                    else:
                        results["b"] = disp_b.execute_complete(
                            issuer_id=issuer,
                            operation_id=op,
                            task_id=task_id,
                        )

                thread = threading.Thread(target=second)
                thread.start()
                self.assertTrue(entered.wait(5), "second caller never reached post-prepare barrier")
                try:
                    if kind == "update":
                        results["a"] = disp_a.execute_update(
                            issuer_id=issuer,
                            operation_id=op,
                            task_id=task_id,
                            patch={"title": f"EA010-SYNTHETIC-overlap-{kind}-2"},
                        )
                    else:
                        results["a"] = disp_a.execute_complete(
                            issuer_id=issuer,
                            operation_id=op,
                            task_id=task_id,
                        )
                finally:
                    release.set()
                thread.join(timeout=10)
                self.assertFalse(thread.is_alive())
                self.assertIs(results["a"].outcome, Outcome.CONFIRMED, results["a"].details)
                self.assertIs(results["b"].outcome, Outcome.CONFIRMED, results["b"].details)
                row = journal.get_operation(issuer, op)
                self.assertIs(row.state, OperationState.CONFIRMED)
                self.assertIsNotNone(row.receipt)
                events = [e.get("event_type") for e in journal.history(issuer, op)]
                self.assertEqual(events.count("dispatch_started"), 1)
                self.assertNotIn("conflict", events)
                if kind == "update":
                    self.assertEqual(client.update_calls + client_b.update_calls, 1)
                else:
                    self.assertEqual(client.complete_calls + client_b.complete_calls, 1)

    def test_recover_unknown_to_confirmed_foreign_client_skips_callback(self) -> None:
        """R2: unknown→confirmed race must not backlink through a foreign client shortcut."""
        op = new_operation_id()
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "EA010-SYNTHETIC-scope-race", "user_portion": ""},
        )
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, op)
            section.mark_unknown(self.issuer, op, reason="original_writer_lost")
        self.client.create(
            title="EA010-SYNTHETIC-scope-race",
            issuer_id=self.issuer,
            operation_id=op,
            user_portion="",
        )
        base_journal = self.journal
        issuer = self.issuer
        good = IntegrationDispatcher(base_journal, self.client)
        hook_state = {"once": True}

        class HookJournal:
            def __getattr__(self, name: str) -> Any:
                return getattr(base_journal, name)

            def get_operation(self, *args: Any, **kwargs: Any) -> Any:
                snapshot = base_journal.get_operation(*args, **kwargs)
                if hook_state["once"]:
                    hook_state["once"] = False
                    peer = good.recover_create(issuer, op)
                    assert peer.ok, peer.details
                return snapshot

        links: list[dict[str, Any]] = []
        foreign = FakeClient(project_id=999)
        bad = IntegrationDispatcher(
            HookJournal(),  # type: ignore[arg-type]
            foreign,
            link_callback=lambda p: links.append(dict(p)) or {"ok": True},
        )
        result = bad.recover_create(self.issuer, op)
        self.assertIs(result.outcome, Outcome.SCOPE_DENIED, result.details)
        self.assertFalse(result.linked)
        self.assertEqual(len(links), 0)
        self.assertEqual(foreign.read_calls, 0)
        helper = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=self.issuer,
            operation_id=op,
            client=foreign,
        )
        self.assertIs(helper.outcome, Outcome.SCOPE_DENIED)
        self.assertEqual(helper.details.get("reason"), "client_project_mismatch")
        row = self.journal.get_operation(self.issuer, op)
        self.assertIs(row.state, OperationState.CONFIRMED)
        self.assertEqual(row.project_id, 2)

    def test_definitive_create_refusal_preserved(self) -> None:
        """F2: SCOPE_DENIED create must stay typed; empty reconcile must not erase it."""
        self.client.fail_create_with = Outcome.SCOPE_DENIED
        self.client.fail_create_mutation_sent = False
        disp = IntegrationDispatcher(self.journal, self.client)
        op = new_operation_id()
        first = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-denied",
            user_portion="f2",
        )
        self.assertIs(first.outcome, Outcome.SCOPE_DENIED)
        self.assertTrue(first.details.get("definitive_refusal"))
        self.assertTrue(first.details.get("resend_forbidden"))
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(len(self.client.tasks), 0)
        # Clear the synthetic refusal so a mistaken reconcile path would succeed empty.
        self.client.fail_create_with = None
        retry = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=op,
            title="EA010-SYNTHETIC-denied",
            user_portion="f2",
        )
        self.assertIs(retry.outcome, Outcome.SCOPE_DENIED)
        self.assertTrue(retry.details.get("definitive_refusal"))
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(len(self.client.tasks), 0)
        row = self.journal.get_operation(self.issuer, op)
        self.assertIs(row.state, OperationState.UNKNOWN)
        self.assertIsNone(row.receipt)

    def test_definitive_update_refusal_preserved(self) -> None:
        """F2: read-only/auth update refusal stays AUTH_DENIED across recovery."""
        disp = IntegrationDispatcher(self.journal, self.client)
        create_op = new_operation_id()
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=create_op,
            title="EA010-SYNTHETIC-ro",
            user_portion="f2u",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        title_before = self.client.tasks[task_id]["title"]
        self.client.fail_update_with = Outcome.AUTH_DENIED
        self.client.fail_update_mutation_sent = False
        update_op = new_operation_id()
        first = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-should-not-apply"},
        )
        self.assertIs(first.outcome, Outcome.AUTH_DENIED)
        self.assertTrue(first.details.get("definitive_refusal"))
        self.assertEqual(self.client.tasks[task_id]["title"], title_before)
        self.client.fail_update_with = None
        retry = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-should-not-apply"},
        )
        self.assertIs(retry.outcome, Outcome.AUTH_DENIED)
        self.assertTrue(retry.details.get("resend_forbidden"))
        self.assertEqual(self.client.tasks[task_id]["title"], title_before)
        self.assertEqual(self.client.update_calls, 1)

    def test_recurring_complete_persists_occurrence_witness(self) -> None:
        """F2: durable recurring complete receipt retains occurrence/preimage/postimage."""
        client = FakeClient()
        journal_path = Path(self._tmpdir.name) / "f2-occurrence.sqlite3"
        journal = IntegrationJournal(journal_path)
        journal.open()
        self.addCleanup(journal.close)
        issuer = journal.ensure_issuer(new_issuer_id())
        # Seed a recurring task with full C1 occurrence fields.
        task_id = 4
        preimage = {
            "id": task_id,
            "project_id": 2,
            "title": "EA010-SYNTHETIC-recurring-complete",
            "description": "recurring body",
            "due_date": "2026-12-15T20:59:59Z",
            "done": False,
            "done_at": "0001-01-01T00:00:00Z",
            "start_date": "0001-01-01T00:00:00Z",
            "end_date": "0001-01-01T00:00:00Z",
            "reminders": None,
            "repeat_after": 86400,
            "repeat_mode": 0,
        }
        client.tasks[task_id] = dict(preimage)
        client.next_id = task_id + 1

        postimage = {
            **preimage,
            "due_date": "2026-12-16T20:59:59Z",
            "done_at": "2026-11-10T10:00:00Z",
        }
        occurrence = dict(postimage)

        def recurring_complete(tid: int) -> TaskResult:
            client.complete_calls += 1
            client.tasks[tid] = dict(occurrence)
            return TaskResult(
                outcome=Outcome.CONFIRMED,
                task=dict(occurrence),
                mutation_sent=True,
                http_status=200,
                details={
                    "patch_http": 200,
                    "task_id": tid,
                    "recurring": True,
                    "recurring_transition": True,
                    "preimage": {
                        "id": preimage["id"],
                        "done": preimage["done"],
                        "due_date": preimage["due_date"],
                        "done_at": preimage["done_at"],
                        "start_date": preimage["start_date"],
                        "end_date": preimage["end_date"],
                        "repeat_after": preimage["repeat_after"],
                        "repeat_mode": preimage["repeat_mode"],
                        "reminders": preimage["reminders"],
                        "description": preimage["description"],
                    },
                    "postimage": {
                        "done": postimage["done"],
                        "due_date": postimage["due_date"],
                        "done_at": postimage["done_at"],
                        "start_date": postimage["start_date"],
                        "end_date": postimage["end_date"],
                        "repeat_after": postimage["repeat_after"],
                        "repeat_mode": postimage["repeat_mode"],
                        "reminders": postimage["reminders"],
                        "description": postimage["description"],
                    },
                    "occurrence": {
                        "done": occurrence["done"],
                        "due_date": occurrence["due_date"],
                        "done_at": occurrence["done_at"],
                        "start_date": occurrence["start_date"],
                        "end_date": occurrence["end_date"],
                        "repeat_after": occurrence["repeat_after"],
                        "repeat_mode": occurrence["repeat_mode"],
                        "reminders": occurrence["reminders"],
                        "description": occurrence["description"],
                    },
                },
            )

        client.complete = recurring_complete  # type: ignore[method-assign]
        disp = IntegrationDispatcher(journal, client)
        op = new_operation_id()
        result = disp.execute_complete(issuer_id=issuer, operation_id=op, task_id=task_id)
        self.assertTrue(result.ok, result.details)
        row = journal.get_operation(issuer, op)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertIs(row.state, OperationState.CONFIRMED)
        snap = (row.payload or {}).get("preimage") or {}
        for key in ("done_at", "start_date", "end_date", "reminders"):
            self.assertIn(key, snap, f"durable preimage missing {key}")
        effect = (row.receipt or {}).get("effect") or {}
        for key in ("preimage", "postimage", "occurrence"):
            self.assertIn(key, effect, f"receipt effect missing {key}")
            self.assertIn("done_at", effect[key])
            self.assertIn("reminders", effect[key])
        # Process-loss reopen must retain the same witness.
        journal.close()
        reopened = IntegrationJournal(journal_path)
        reopened.open()
        self.addCleanup(reopened.close)
        again = reopened.get_operation(issuer, op)
        self.assertIsNotNone(again)
        assert again is not None
        again_effect = (again.receipt or {}).get("effect") or {}
        self.assertEqual(again_effect.get("occurrence"), effect.get("occurrence"))
        self.assertEqual(again_effect.get("postimage"), effect.get("postimage"))
        self.assertIn("done_at", (again.payload or {}).get("preimage") or {})

    def test_distinct_ops_stale_preimage_conflicts_before_patch(self) -> None:
        """F1: distinct operations on one target must conflict when the agreed snapshot drifted."""
        disp = IntegrationDispatcher(self.journal, self.client)
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            title="EA010-SYNTHETIC-A",
            user_portion="f1-distinct",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        outer_op = new_operation_id()
        pre = self.client.read(task_id)
        self.assertTrue(pre.ok and pre.task is not None)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=outer_op,
            op_type="update",
            project_id=2,
            target_id=task_id,
            payload={
                "patch": {"title": "EA010-SYNTHETIC-C"},
                "preimage": {
                    "id": pre.task.get("id"),
                    "title": pre.task.get("title"),
                    "due_date": pre.task.get("due_date"),
                    "done": pre.task.get("done"),
                    "description": pre.task.get("description"),
                    "repeat_after": pre.task.get("repeat_after"),
                    "repeat_mode": pre.task.get("repeat_mode"),
                },
            },
        )
        peer = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-B"},
        )
        self.assertTrue(peer.ok, peer.details)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-B")
        updates_before = self.client.update_calls
        outer = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=outer_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-C"},
        )
        self.assertIs(outer.outcome, Outcome.CONFLICT, outer.details)
        self.assertEqual(outer.details.get("reason"), "preimage_drift_before_patch")
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-B")
        row = self.journal.get_operation(self.issuer, outer_op)
        self.assertIs(row.state, OperationState.CONFLICT)

    def test_new_key_refused_while_target_unknown(self) -> None:
        """F2: response-loss unknown blocks a different key for the same target."""
        disp = IntegrationDispatcher(self.journal, self.client)
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            title="EA010-SYNTHETIC-f2-target",
            user_portion="f2",
            extra_fields={"repeat_after": 86400, "repeat_mode": 0},
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        lost_op = new_operation_id()
        pre = self.client.read(task_id)
        self.assertTrue(pre.ok and pre.task is not None)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=lost_op,
            op_type="complete",
            project_id=2,
            target_id=task_id,
            payload={
                "patch": {"done": True},
                "preimage": {
                    "id": pre.task.get("id"),
                    "title": pre.task.get("title"),
                    "due_date": pre.task.get("due_date"),
                    "done": pre.task.get("done"),
                    "description": pre.task.get("description"),
                    "repeat_after": pre.task.get("repeat_after"),
                    "repeat_mode": pre.task.get("repeat_mode"),
                },
            },
        )
        # Simulate successful PATCH with lost response: advance due, leave durable unknown.
        due_after_patch = "2026-12-16T20:59:59Z"
        self.client.tasks[task_id]["due_date"] = due_after_patch
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, lost_op)
            section.mark_unknown(self.issuer, lost_op, reason="simulated_dropped_response")
        completes_before = self.client.complete_calls
        updates_before = self.client.update_calls
        second = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            task_id=task_id,
        )
        self.assertIs(second.outcome, Outcome.CONFLICT, second.details)
        self.assertEqual(second.details.get("reason"), "unresolved_target_mutation")
        self.assertEqual(second.details.get("blocking_operation_id"), lost_op)
        self.assertEqual(self.client.complete_calls, completes_before)
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertEqual(self.client.tasks[task_id]["due_date"], due_after_patch)
        lost_row = self.journal.get_operation(self.issuer, lost_op)
        self.assertIs(lost_row.state, OperationState.UNKNOWN)

    def test_response_loss_incompatible_replay_keeps_new_key_fence(self) -> None:
        """P1: unknown→conflict via same-key source mismatch must still fence new keys.

        Response-loss leaves durable unknown after possible PATCH. An incompatible
        same-key replay durably conflicts that row; a fresh operation_id must not
        send complete again or advance recurring due (C1 §6 / AC10/18).
        """
        disp = IntegrationDispatcher(self.journal, self.client)
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            title="EA010-SYNTHETIC-f2-conflict-fence",
            user_portion="f2c",
            extra_fields={"repeat_after": 86400, "repeat_mode": 0},
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        lost_op = new_operation_id()
        pre = self.client.read(task_id)
        self.assertTrue(pre.ok and pre.task is not None)
        self.journal.prepare_intent(
            issuer_id=self.issuer,
            operation_id=lost_op,
            op_type="complete",
            project_id=2,
            target_id=task_id,
            payload={
                "patch": {"done": True},
                "preimage": {
                    "id": pre.task.get("id"),
                    "title": pre.task.get("title"),
                    "due_date": pre.task.get("due_date"),
                    "done": pre.task.get("done"),
                    "description": pre.task.get("description"),
                    "repeat_after": pre.task.get("repeat_after"),
                    "repeat_mode": pre.task.get("repeat_mode"),
                },
            },
            source_kind="meeting",
            source_key="EA010-SYNTHETIC-f2-source",
        )
        due_after_patch = "2026-12-16T20:59:59Z"
        self.client.tasks[task_id]["due_date"] = due_after_patch
        with self.journal.dispatch_critical_section() as section:
            section.begin_dispatch(self.issuer, lost_op)
            section.mark_unknown(
                self.issuer,
                lost_op,
                reason="simulated_dropped_response",
                details={"mutation_sent": True, "resend_forbidden": True},
            )
        incompatible = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=lost_op,
            task_id=task_id,
            source_kind="meeting",
            source_key="EA010-SYNTHETIC-f2-source-OTHER",
        )
        self.assertIs(incompatible.outcome, Outcome.CONFLICT, incompatible.details)
        lost_row = self.journal.get_operation(self.issuer, lost_op)
        self.assertIsNotNone(lost_row)
        self.assertIs(lost_row.state, OperationState.CONFLICT)
        completes_before = self.client.complete_calls
        updates_before = self.client.update_calls
        second = disp.execute_complete(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            task_id=task_id,
        )
        self.assertIs(second.outcome, Outcome.CONFLICT, second.details)
        self.assertEqual(second.details.get("reason"), "unresolved_target_mutation")
        self.assertEqual(second.details.get("blocking_operation_id"), lost_op)
        self.assertEqual(second.details.get("blocking_state"), OperationState.CONFLICT.value)
        self.assertEqual(self.client.complete_calls, completes_before)
        self.assertEqual(self.client.update_calls, updates_before)
        self.assertEqual(self.client.tasks[task_id]["due_date"], due_after_patch)

    def test_dispatch_api_error_after_claim_is_typed_unknown(self) -> None:
        """F3: ApiError from update/complete after begin_dispatch must not escape."""
        disp = IntegrationDispatcher(self.journal, self.client)
        created = disp.execute_create(
            issuer_id=self.issuer,
            operation_id=new_operation_id(),
            title="EA010-SYNTHETIC-f3-transport",
            user_portion="f3",
        )
        self.assertTrue(created.ok)
        task_id = int(created.vikunja_id)
        self.client.raise_update_with = ApiError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "synthetic second-GET transport failure",
            details={"phase": "preimage", "synthetic": True},
        )
        update_op = new_operation_id()
        result = disp.execute_update(
            issuer_id=self.issuer,
            operation_id=update_op,
            task_id=task_id,
            patch={"title": "EA010-SYNTHETIC-f3-should-not-apply"},
        )
        self.assertIs(result.outcome, Outcome.DEPENDENCY_UNAVAILABLE, result.details)
        self.assertTrue(result.details.get("resend_forbidden"))
        self.assertTrue(result.details.get("api_error"))
        self.assertIs(result.details.get("mutation_sent"), False)
        self.assertEqual(self.client.update_calls, 0)
        self.assertEqual(self.client.tasks[task_id]["title"], "EA010-SYNTHETIC-f3-transport")
        row = self.journal.get_operation(self.issuer, update_op)
        self.assertIs(row.state, OperationState.UNKNOWN)
        events = [e.get("event_type") for e in self.journal.history(self.issuer, update_op)]
        self.assertIn("dispatch_started", events)
        self.assertIn("unknown", events)


class RecoverySuiteRegistrationTests(unittest.TestCase):
    def test_recovery_suite_registered(self) -> None:
        from tests.support.suites import get_suite, list_suites

        self.assertIn("recovery", list_suites())
        self.assertTrue(callable(get_suite("recovery")))


if __name__ == "__main__":
    unittest.main()
