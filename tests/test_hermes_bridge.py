"""Unit tests for durable Hermes bridge (s06 / C2). No FastAPI / live Vikunja."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
import unittest.mock
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local.api import (
    Outcome,
    TaskResult,
    build_create_description,
    build_marker,
    extract_marker,
)
from vikunja_local.hermes_bridge import (
    SOURCE_KIND,
    TRACKER_TARGET,
    BridgeConfig,
    BridgeError,
    HermesBridge,
    HermesOutboxStore,
    acceptance_payload_digest,
    confirmed_due_from_accept,
    durable_clear_path,
    ensure_bridge_issuer,
    frozen_due_payload,
    meeting_provenance_ready,
    meeting_user_portion,
    new_event_id,
    proposal_source_url,
    verified_meeting_provenance,
)
from vikunja_local.journal import IntegrationJournal, JournalError, new_issuer_id


def _ready_portion(proposal_id: str = "prop", version: str = "d" * 64) -> str:
    return meeting_user_portion(
        proposal_id=proposal_id,
        meeting_id="m1",
        meeting_title="Standup",
        meeting_when="2026-11-10T10:00:00+03:00 (Europe/Moscow)",
        quote="do the thing",
        version=version,
        source_url=proposal_source_url(
            hermes_origin="http://127.0.0.1:19347", proposal_id=proposal_id
        ),
    )


class FakeClient:
    def __init__(self, project_id: int = 2):
        self.project_id = project_id
        self.tasks: dict[int, dict[str, Any]] = {}
        self.next_id = 1
        self.create_calls = 0
        fail_with: Outcome | None
        self.fail_create_with: Outcome | None = None

    def create(self, **kwargs: Any) -> TaskResult:
        self.create_calls += 1
        if self.fail_create_with is not None:
            return TaskResult(
                outcome=self.fail_create_with,
                mutation_sent=False,
                http_status=401 if self.fail_create_with is Outcome.AUTH_DENIED else 403,
                details={"phase": "create", "synthetic": True},
            )
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
            "title": kwargs["title"],
            "description": description,
            "project_id": self.project_id,
            "done": False,
            "due_date": due,
        }
        self.tasks[task_id] = task
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=dict(task),
            mutation_sent=True,
            http_status=201,
            details={"create_http": 201},
        )

    def read(self, task_id: int) -> TaskResult:
        task = self.tasks.get(task_id)
        if task is None:
            return TaskResult(outcome=Outcome.VALIDATION_ERROR, http_status=404, details={})
        return TaskResult(outcome=Outcome.CONFIRMED, task=dict(task), http_status=200, details={})

    def list_project_tasks(self) -> TaskResult:
        items = list(self.tasks.values())
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            details={"items": items, "page_count": 1},
            http_status=200,
        )

    def find_by_marker(self, marker: str) -> TaskResult:
        matches = []
        for item in self.tasks.values():
            extracted = extract_marker(item.get("description"))
            if extracted and extracted["marker"] == marker:
                matches.append(item)
        if not matches:
            return TaskResult(outcome=Outcome.EMPTY, details={"marker": marker})
        if len(matches) > 1:
            return TaskResult(
                outcome=Outcome.CONFLICT,
                details={"marker": marker, "ids": [m.get("id") for m in matches]},
            )
        return self.read(int(matches[0]["id"]))


class HermesBridgeUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="ea010-hermes-unit-")
        root = Path(self.tmp.name)
        self.journal_path = root / "journal.sqlite3"
        self.outbox_path = root / "outbox.json"
        self.token_path = root / "token"
        self.token_path.write_text("synthetic-token", encoding="utf-8")
        os.chmod(self.token_path, 0o600)
        self.cfg = BridgeConfig(
            journal_path=self.journal_path,
            api_origin="http://127.0.0.1:19345",
            project_id=2,
            token_ref=__import__("vikunja_local.api", fromlist=["TokenRef"]).TokenRef(
                kind="file", path=self.token_path
            ),
            task_url_template="http://127.0.0.1:19345/tasks/{id}",
            hermes_origin="http://127.0.0.1:19347",
        )
        self.store = HermesOutboxStore(self.outbox_path)
        self.bridge = HermesBridge(self.cfg, self.store)
        self.client = FakeClient()
        # Patch client factory.
        self.bridge._client = lambda: self.client  # type: ignore[method-assign]

        journal = IntegrationJournal(self.journal_path)
        journal.open()
        try:
            self.issuer = journal.ensure_issuer(new_issuer_id())
        finally:
            journal.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_source_kind_constant(self) -> None:
        self.assertEqual(SOURCE_KIND, "hermes_meeting_accept_v1")
        self.assertEqual(TRACKER_TARGET, "vikunja")

    def test_acceptance_digest_stable(self) -> None:
        a = acceptance_payload_digest(title="T", due=None, source_version="a" * 64)
        b = acceptance_payload_digest(title="T", due=None, source_version="a" * 64)
        c = acceptance_payload_digest(title="T2", due=None, source_version="a" * 64)
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_register_and_identical_replay(self) -> None:
        version = "b" * 64
        first = self.bridge.register_acceptance(
            proposal_id="prop1",
            title="Meet",
            due=None,
            source_version=version,
            user_portion="ctx",
        )
        second = self.bridge.register_acceptance(
            proposal_id="prop1",
            title="Meet",
            due=None,
            source_version=version,
            user_portion="ctx",
        )
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(first.state, "pending")

    def test_register_conflict_on_payload_change(self) -> None:
        version = "c" * 64
        self.bridge.register_acceptance(
            proposal_id="prop2",
            title="Meet",
            due=None,
            source_version=version,
            user_portion="ctx",
        )
        with self.assertRaises(BridgeError) as ctx:
            self.bridge.register_acceptance(
                proposal_id="prop2",
                title="Meet changed",
                due=None,
                source_version=version,
                user_portion="ctx",
            )
        self.assertEqual(ctx.exception.code, "conflict")

    def test_deliver_links_once(self) -> None:
        version = "d" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop3",
            title="Deliver once",
            due="2026-11-20",
            source_version=version,
            user_portion=meeting_user_portion(
                proposal_id="prop3",
                meeting_id="m1",
                meeting_title="Standup",
                meeting_when="2026-11-10T10:00:00+03:00",
                quote="do the thing",
                version=version,
                source_url=proposal_source_url(
                    hermes_origin=self.cfg.hermes_origin, proposal_id="prop3"
                ),
            ),
        )
        self.assertIsNotNone(row.frozen_due)
        result = self.bridge.deliver(row.event_id)
        self.assertTrue(result.ok)
        assert result.record is not None
        self.assertEqual(result.record.state, "linked")
        self.assertEqual(self.client.create_calls, 1)
        task_id = result.record.task_id
        desc = self.client.tasks[task_id]["description"]
        self.assertIn("#pt-proposal-prop3", desc)
        self.assertIn("](http://127.0.0.1:19347/#pt-proposal-prop3)", desc)
        # Replay deliver reuses frozen envelope (no caller due).
        again = self.bridge.deliver(row.event_id)
        self.assertTrue(again.ok)
        self.assertEqual(again.record.task_id if again.record else None, task_id)
        self.assertEqual(self.client.create_calls, 1)

    def test_recover_restores_lost_backlink_without_create(self) -> None:
        version = "e" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop4",
            title="Backlink",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop4", version),
        )
        delivered = self.bridge.deliver(row.event_id)
        self.assertTrue(delivered.ok)
        assert delivered.record is not None
        lost = delivered.record
        lost.state = "unknown"
        lost.url = None
        self.store.upsert(lost)
        creates_before = self.client.create_calls
        recovered = self.bridge.recover(row.event_id)
        self.assertTrue(recovered.ok)
        assert recovered.record is not None
        self.assertEqual(recovered.record.state, "linked")
        self.assertIsNotNone(recovered.record.url)
        self.assertEqual(self.client.create_calls, creates_before)

    def test_crash_after_accept_before_dispatch_then_recover(self) -> None:
        version = "f" * 64
        points: list[str] = []

        def crash(point: str) -> None:
            points.append(point)
            if point == "after_acceptance_before_dispatch":
                raise RuntimeError("synthetic_crash")

        self.bridge._crash = crash
        with self.assertRaises(RuntimeError):
            self.bridge.register_acceptance(
                proposal_id="prop5",
                title="Crash",
                due=None,
                source_version=version,
                user_portion=_ready_portion("prop5", version),
            )
        # Acceptance crash before outbox commit should leave no row when raised after upsert?
        # Our hook fires after upsert for after_acceptance_before_dispatch.
        row = self.store.get_by_proposal("prop5")
        self.assertIsNotNone(row)
        self.bridge._crash = None
        delivered = self.bridge.deliver(row.event_id)  # type: ignore[union-attr]
        self.assertTrue(delivered.ok)
        self.assertEqual(self.client.create_calls, 1)

    def test_recover_all_skips_without_outbox(self) -> None:
        results = self.bridge.recover_all_outbox()
        self.assertEqual(results, [])

    def test_auth_denied_does_not_link(self) -> None:
        version = "a" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop6",
            title="Denied",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop6", version),
        )
        self.client.fail_create_with = Outcome.AUTH_DENIED
        result = self.bridge.deliver(row.event_id)
        self.assertEqual(result.outcome, Outcome.AUTH_DENIED)
        assert result.record is not None
        self.assertNotEqual(result.record.state, "linked")
        self.assertIsNone(result.record.task_id)

    def test_new_event_id_uuid4(self) -> None:
        eid = new_event_id()
        uuid.UUID(eid, version=4)

    def test_outbox_lock_serializes_concurrent_upserts(self) -> None:
        import threading
        import time

        version = "1" * 64
        a = self.bridge.register_acceptance(
            proposal_id="lock-a",
            title="A",
            due=None,
            source_version=version,
            user_portion="a",
        )
        paused = threading.Event()
        resume = threading.Event()
        errors: list[BaseException] = []
        b_holder: dict[str, Any] = {}

        def writer_a() -> None:
            try:
                with self.store._lock():
                    data = self.store._load_unlocked()
                    paused.set()
                    resume.wait(timeout=5)
                    current = dict(data["by_event"][a.event_id])
                    current["state"] = "linked"
                    current["task_id"] = 1
                    current["url"] = "http://127.0.0.1:19345/tasks/1"
                    data["by_proposal"][a.proposal_id] = current
                    data["by_event"][a.event_id] = current
                    self.store._save_unlocked(data)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def writer_b() -> None:
            try:
                b_holder["row"] = self.bridge.register_acceptance(
                    proposal_id="lock-b",
                    title="B",
                    due=None,
                    source_version=version,
                    user_portion="b",
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t_a = threading.Thread(target=writer_a, name="backlink-writer")
        t_b = threading.Thread(target=writer_b, name="accept-b")
        t_a.start()
        self.assertTrue(paused.wait(timeout=5))
        t_b.start()
        # Without a shared lock, A could save a stale snapshot and erase B.
        time.sleep(0.05)
        resume.set()
        t_a.join(timeout=5)
        t_b.join(timeout=5)
        self.assertFalse(t_a.is_alive())
        self.assertFalse(t_b.is_alive())
        self.assertEqual(errors, [])
        b = b_holder["row"]
        self.assertIsNotNone(self.store.get_by_proposal("lock-a"))
        self.assertIsNotNone(self.store.get_by_proposal("lock-b"))
        self.assertEqual(self.store.get_by_event(b.event_id).title, "B")  # type: ignore[union-attr]

    def test_delayed_deliver_reuses_frozen_due_envelope(self) -> None:
        version = "2" * 64
        fixed = confirmed_due_from_accept(
            None, confirmed_at=datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
        )
        row = self.bridge.register_acceptance(
            proposal_id="prop-due",
            title="Frozen due",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-due", version),
            frozen_due=frozen_due_payload(fixed),
        )
        self.assertEqual(row.frozen_due, frozen_due_payload(fixed))

        # Prepare intent then crash before HTTP; later deliver must reuse envelope.
        points: list[str] = []

        def crash(point: str) -> None:
            points.append(point)
            if point == "before_http":
                raise RuntimeError("stop_before_http")

        self.bridge._crash = crash
        with self.assertRaises(RuntimeError):
            self.bridge.deliver(row.event_id)
        self.bridge._crash = None
        # Wall-clock advances; still must not conflict on confirmed_at.
        later = confirmed_due_from_accept(
            None, confirmed_at=datetime(2026, 11, 11, 12, 0, 2, tzinfo=timezone.utc)
        )
        # Explicit wrong due must not be required; deliver uses persisted frozen_due.
        result = self.bridge.deliver(row.event_id)
        self.assertTrue(result.ok, result.details)
        self.assertEqual(self.client.create_calls, 1)
        self.assertNotEqual(frozen_due_payload(later), row.frozen_due)

    def test_recover_prepared_without_dispatch_resumes_first_http(self) -> None:
        version = "3" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-prep",
            title="Prepared",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-prep", version),
        )
        points: list[str] = []

        def crash(point: str) -> None:
            points.append(point)
            if point == "before_http":
                raise RuntimeError("prepared_only")

        self.bridge._crash = crash
        with self.assertRaises(RuntimeError):
            self.bridge.deliver(row.event_id)
        self.bridge._crash = None
        self.assertEqual(self.client.create_calls, 0)
        first = self.bridge.recover(row.event_id)
        self.assertTrue(first.ok, first.details)
        self.assertEqual(self.client.create_calls, 1)
        second = self.bridge.recover(row.event_id)
        self.assertTrue(second.ok)
        self.assertEqual(self.client.create_calls, 1)
        self.assertEqual(first.record.task_id if first.record else None, second.record.task_id if second.record else None)

    def test_meeting_user_portion_includes_source_link(self) -> None:
        url = proposal_source_url(hermes_origin="http://127.0.0.1:19347", proposal_id="p-1")
        text = meeting_user_portion(
            proposal_id="p-1",
            meeting_id="m",
            meeting_title="T",
            meeting_when=None,
            quote=None,
            version="a" * 64,
            source_url=url,
        )
        self.assertIn(url, text)
        self.assertIn(f"[Open proposal]({url})", text)

    def test_verified_meeting_provenance_requires_verified_calendar(self) -> None:
        title, starts, tz = verified_meeting_provenance(
            {
                "title": "EA010-SYNTHETIC Calendar title",
                "starts_at": "2026-11-05T09:00:00+03:00",
                "timezone": "Europe/Moscow",
                "verification_status": "verified",
            }
        )
        self.assertEqual(title, "EA010-SYNTHETIC Calendar title")
        self.assertEqual(starts, "2026-11-05T09:00:00+03:00")
        self.assertEqual(tz, "Europe/Moscow")
        missing = verified_meeting_provenance(
            {
                "title": "partial",
                "starts_at": "2026-11-05T09:00:00+03:00",
                "timezone": "Europe/Moscow",
                "verification_status": "partial",
            }
        )
        self.assertEqual(missing, (None, None, None))
        unavailable = meeting_user_portion(
            proposal_id="p",
            meeting_id="m",
            meeting_title=None,
            meeting_when=None,
            quote=None,
            version="b" * 64,
            calendar_unavailable=True,
        )
        self.assertIn("Meeting calendar: unavailable", unavailable)
        self.assertNotIn("Meeting time:", unavailable)

    def test_same_event_concurrent_deliver_preserves_linked_operation(self) -> None:
        import threading
        import time

        version = "4" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-concurrent",
            title="EA010-SYNTHETIC concurrent",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-concurrent", version),
        )
        paused = threading.Event()
        resume = threading.Event()
        results: dict[str, Any] = {}
        errors: list[BaseException] = []

        def crash(point: str) -> None:
            if point == "before_dispatch" and threading.current_thread().name == "deliver-b":
                paused.set()
                resume.wait(timeout=5)

        self.bridge._crash = crash

        def worker_a() -> None:
            try:
                self.assertTrue(paused.wait(timeout=5))
                results["a"] = self.bridge.deliver(row.event_id)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def worker_b() -> None:
            try:
                results["b"] = self.bridge.deliver(row.event_id)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t_b = threading.Thread(target=worker_b, name="deliver-b")
        t_a = threading.Thread(target=worker_a, name="deliver-a")
        t_b.start()
        self.assertTrue(paused.wait(timeout=5))
        t_a.start()
        time.sleep(0.05)
        resume.set()
        t_a.join(timeout=5)
        t_b.join(timeout=5)
        self.bridge._crash = None
        self.assertEqual(errors, [])
        self.assertTrue(results["a"].ok, results["a"].details)
        after = self.store.get_by_event(row.event_id)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(after.state, "linked")
        self.assertEqual(after.task_id, results["a"].record.task_id if results["a"].record else None)
        self.assertEqual(after.operation_id, results["a"].record.operation_id if results["a"].record else None)
        self.assertEqual(self.client.create_calls, 1)
        # Loser must not regress linked receipt/key even if it observed a bound mapping.
        self.assertNotEqual(getattr(results["b"].record, "state", None), "unknown")
        self.assertEqual(results["b"].record.operation_id if results["b"].record else None, after.operation_id)
        self.assertIn(results["b"].outcome, {Outcome.CONFIRMED, Outcome.UNKNOWN})

    def test_recover_persists_multiple_exact_match_conflict(self) -> None:
        version = "5" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-conflict",
            title="EA010-SYNTHETIC conflict",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-conflict", version),
        )
        points: list[str] = []

        def crash(point: str) -> None:
            points.append(point)
            if point == "after_commit_before_receipt":
                raise RuntimeError("stop_before_receipt")

        self.bridge._crash = crash
        with self.assertRaises(RuntimeError):
            self.bridge.deliver(row.event_id)
        self.bridge._crash = None
        self.assertEqual(self.client.create_calls, 1)
        created = next(iter(self.client.tasks.values()))
        marker = extract_marker(created["description"])
        self.assertIsNotNone(marker)
        assert marker is not None
        # Second exact marker match (synthetic ambiguity) without a new create path.
        dup_id = self.client.next_id
        self.client.next_id += 1
        self.client.tasks[dup_id] = {
            "id": dup_id,
            "title": created["title"],
            "description": created["description"],
            "project_id": self.client.project_id,
            "done": False,
            "due_date": None,
        }
        before_creates = self.client.create_calls
        recovered = self.bridge.recover(row.event_id)
        self.assertEqual(recovered.outcome, Outcome.CONFLICT)
        after = self.store.get_by_event(row.event_id)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(after.state, "conflict")
        self.assertIsNone(after.task_id)
        self.assertIsNone(after.url)
        self.assertEqual(self.client.create_calls, before_creates)
        projected = after.tracker().as_dict()
        self.assertEqual(projected["state"], "conflict")
        self.assertIn("multiple_exact", str(after.details.get("reason") or after.details))

    def test_outbox_save_fsyncs_parent_directory(self) -> None:
        """R1: after replace, parent directory must be fsynced before save returns."""
        import stat as stat_mod

        targets: list[str] = []
        real_open = os.open
        real_fsync = os.fsync
        real_fstat = os.fstat

        def tracking_open(path, flags, *args, **kwargs):  # noqa: ANN001
            fd = real_open(path, flags, *args, **kwargs)
            try:
                if stat_mod.S_ISDIR(real_fstat(fd).st_mode):
                    targets.append(f"dir:{fd}")
            except OSError:
                pass
            return fd

        def tracking_fsync(fd: int) -> None:
            if f"dir:{fd}" in targets:
                targets[targets.index(f"dir:{fd}")] = "dir_synced"
            else:
                targets.append("file")
            return real_fsync(fd)

        version = "6" * 64
        with unittest.mock.patch("vikunja_local.hermes_bridge.os.open", tracking_open), unittest.mock.patch(
            "vikunja_local.hermes_bridge.os.fsync", tracking_fsync
        ):
            self.bridge.register_acceptance(
                proposal_id="prop-fsync",
                title="Fsync",
                due=None,
                source_version=version,
                user_portion=_ready_portion("prop-fsync", version),
            )
        self.assertIn("dir_synced", targets)
        self.assertIn("file", targets)

    def test_outbox_save_propagates_directory_fsync_failure(self) -> None:
        """R1: directory barrier failure must not report save success."""
        import stat as stat_mod

        real_open = os.open
        real_fsync = os.fsync
        real_fstat = os.fstat
        dir_fds: set[int] = set()

        def tracking_open(path, flags, *args, **kwargs):  # noqa: ANN001
            fd = real_open(path, flags, *args, **kwargs)
            try:
                if stat_mod.S_ISDIR(real_fstat(fd).st_mode):
                    dir_fds.add(fd)
            except OSError:
                pass
            return fd

        def failing_fsync(fd: int) -> None:
            if fd in dir_fds:
                raise OSError(5, "synthetic directory fsync failure")
            return real_fsync(fd)

        version = "7" * 64
        with unittest.mock.patch("vikunja_local.hermes_bridge.os.open", tracking_open), unittest.mock.patch(
            "vikunja_local.hermes_bridge.os.fsync", failing_fsync
        ):
            with self.assertRaises(OSError):
                self.bridge.register_acceptance(
                    proposal_id="prop-fsync-fail",
                    title="Fsync fail",
                    due=None,
                    source_version=version,
                    user_portion=_ready_portion("prop-fsync-fail", version),
                )

    def test_durable_clear_path_fsyncs_parent_after_unlink(self) -> None:
        import stat as stat_mod

        journal = Path(self.tmp.name) / ".vikunja_accept_commit.json"
        journal.write_text('{"schema":"ea010-vikunja-accept-commit/v1"}\n', encoding="utf-8")
        synced: list[str] = []
        real_open = os.open
        real_fsync = os.fsync
        real_fstat = os.fstat

        def tracking_open(path, flags, *args, **kwargs):  # noqa: ANN001
            fd = real_open(path, flags, *args, **kwargs)
            try:
                if stat_mod.S_ISDIR(real_fstat(fd).st_mode):
                    synced.append(f"pending:{fd}")
            except OSError:
                pass
            return fd

        def tracking_fsync(fd: int) -> None:
            for index, label in enumerate(list(synced)):
                if label == f"pending:{fd}":
                    synced[index] = "cleared"
            return real_fsync(fd)

        with unittest.mock.patch("vikunja_local.hermes_bridge.os.open", tracking_open), unittest.mock.patch(
            "vikunja_local.hermes_bridge.os.fsync", tracking_fsync
        ):
            durable_clear_path(journal)
        self.assertFalse(journal.exists())
        self.assertIn("cleared", synced)

    def test_deliver_blocks_missing_meeting_provenance(self) -> None:
        """R2: frozen incomplete portion must not create a Vikunja task."""
        version = "8" * 64
        incomplete = meeting_user_portion(
            proposal_id="prop-missing",
            meeting_id="m",
            meeting_title=None,
            meeting_when=None,
            quote=None,
            version=version,
            calendar_unavailable=True,
        )
        self.assertFalse(meeting_provenance_ready(incomplete))
        row = self.bridge.register_acceptance(
            proposal_id="prop-missing",
            title="Missing provenance",
            due=None,
            source_version=version,
            user_portion=incomplete,
        )
        result = self.bridge.deliver(row.event_id)
        self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual((result.details or {}).get("reason"), "meeting_provenance_unavailable")
        self.assertEqual(self.client.create_calls, 0)
        after = self.store.get_by_event(row.event_id)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(after.state, "pending")
        self.assertIsNone(after.task_id)

    def test_deliver_blocks_partial_meeting_provenance(self) -> None:
        version = "9" * 64
        partial = meeting_user_portion(
            proposal_id="prop-partial",
            meeting_id="m",
            meeting_title="Only title",
            meeting_when=None,
            quote=None,
            version=version,
        )
        self.assertFalse(meeting_provenance_ready(partial))
        row = self.bridge.register_acceptance(
            proposal_id="prop-partial",
            title="Partial provenance",
            due=None,
            source_version=version,
            user_portion=partial,
        )
        result = self.bridge.deliver(row.event_id)
        self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual(self.client.create_calls, 0)

    def test_linked_then_deleted_recover_deliver_sweep_not_confirmed(self) -> None:
        """R1: linked outbox after task DELETE must not report cached CONFIRMED."""
        version = "b" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-linked-deleted",
            title="Linked then deleted",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-linked-deleted", version),
        )
        delivered = self.bridge.deliver(row.event_id)
        self.assertTrue(delivered.ok, delivered.details)
        assert delivered.record is not None
        task_id = delivered.record.task_id
        self.assertIsNotNone(task_id)
        assert task_id is not None
        event_id = row.event_id
        historical_url = delivered.record.url
        creates_before = self.client.create_calls

        del self.client.tasks[task_id]
        gone = self.client.read(task_id)
        self.assertEqual(gone.http_status, 404)

        recovered = self.bridge.recover(event_id)
        self.assertFalse(recovered.ok)
        self.assertNotEqual(recovered.outcome, Outcome.CONFIRMED)
        self.assertEqual((recovered.details or {}).get("reason"), "target_deleted_or_gone")
        assert recovered.record is not None
        self.assertEqual(recovered.record.state, "linked")
        self.assertEqual(recovered.record.task_id, task_id)
        self.assertEqual(recovered.record.url, historical_url)
        self.assertEqual(recovered.record.event_id, event_id)

        replay = self.bridge.deliver(event_id)
        self.assertFalse(replay.ok)
        self.assertNotEqual(replay.outcome, Outcome.CONFIRMED)
        self.assertEqual((replay.details or {}).get("reason"), "target_deleted_or_gone")
        assert replay.record is not None
        self.assertEqual(replay.record.state, "linked")
        self.assertEqual(replay.record.task_id, task_id)
        public = replay.public_tracker()
        self.assertIsNotNone(public)
        assert public is not None
        self.assertEqual(public["state"], "linked")
        self.assertEqual(public["task_id"], task_id)
        self.assertEqual(public["url"], historical_url)
        avail = public.get("availability") or {}
        self.assertEqual(avail.get("status"), "unavailable")
        self.assertEqual(avail.get("reason"), "target_deleted_or_gone")
        self.assertNotEqual(avail.get("last_check_outcome"), "confirmed")
        # Persisted outbox details alone must also project unavailable for snapshot paths.
        stored = self.store.get_by_event(event_id)
        self.assertIsNotNone(stored)
        assert stored is not None
        projected = stored.tracker().as_dict()
        self.assertEqual((projected.get("availability") or {}).get("status"), "unavailable")

        sweep = self.bridge.recover_all_outbox()
        matched = [item for item in sweep if item.record and item.record.event_id == event_id]
        self.assertEqual(len(matched), 1)
        self.assertFalse(matched[0].ok)
        self.assertNotEqual(matched[0].outcome, Outcome.CONFIRMED)
        self.assertEqual((matched[0].details or {}).get("reason"), "target_deleted_or_gone")
        self.assertEqual(self.client.create_calls, creates_before)

    def test_incompatible_existing_backlink_deliver_recover_sweep_conflict(self) -> None:
        """R1: receipt CONFIRMED must not hide incompatible saved backlink (C2)."""
        version = "c" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-incompatible-link",
            title="EA010-SYNTHETIC incompatible link",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-incompatible-link", version),
        )
        first = self.bridge.deliver(row.event_id)
        self.assertTrue(first.ok, first.details)
        assert first.record is not None
        receipt_task_id = first.record.task_id
        self.assertEqual(receipt_task_id, 1)
        event_id = row.event_id
        creates_before = self.client.create_calls

        foreign_id = 999
        foreign_url = "http://127.0.0.1:19345/tasks/999"
        saved = self.store.get_by_event(event_id)
        self.assertIsNotNone(saved)
        assert saved is not None
        saved.task_id = foreign_id
        saved.url = foreign_url
        self.store.upsert(saved)

        for method in ("deliver", "recover"):
            with self.subTest(method=method):
                # Reset durable conflict so both paths re-enter _verify_linked_outbox.
                reset = self.store.get_by_event(event_id)
                self.assertIsNotNone(reset)
                assert reset is not None
                reset.state = "linked"
                reset.task_id = foreign_id
                reset.url = foreign_url
                reset.details = {
                    k: v
                    for k, v in (reset.details or {}).items()
                    if k
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
                self.store.upsert(reset)

                result = getattr(self.bridge, method)(event_id)
                self.assertEqual(result.outcome, Outcome.CONFLICT)
                self.assertFalse(result.ok)
                assert result.record is not None
                self.assertEqual(result.record.state, "conflict")
                self.assertEqual(result.record.task_id, foreign_id)
                self.assertEqual(result.record.url, foreign_url)
                self.assertEqual(
                    (result.details or {}).get("reason"), "incompatible_existing_link"
                )
                self.assertEqual(result.record.details.get("last_check_outcome"), "conflict")
                self.assertIsNotNone(result.dispatch)
                assert result.dispatch is not None
                link_result = dict(result.dispatch.link_result or {})
                self.assertTrue(link_result.get("conflict"))
                self.assertEqual(link_result.get("error"), "incompatible_existing_link")
                public = result.public_tracker()
                self.assertIsNotNone(public)
                assert public is not None
                self.assertEqual(public["state"], "conflict")
                self.assertEqual(public["task_id"], foreign_id)
                self.assertEqual(public["url"], foreign_url)
                avail = public.get("availability") or {}
                self.assertEqual(avail.get("status"), "conflict")
                self.assertEqual(avail.get("last_check_outcome"), "conflict")
                self.assertEqual(avail.get("reason"), "incompatible_existing_link")
                stored = self.store.get_by_event(event_id)
                self.assertIsNotNone(stored)
                assert stored is not None
                projected = stored.tracker().as_dict()
                self.assertEqual(projected["state"], "conflict")
                self.assertEqual(projected["task_id"], foreign_id)
                self.assertEqual(
                    (projected.get("availability") or {}).get("status"), "conflict"
                )

        sweep = self.bridge.recover_all_outbox()
        matched = [item for item in sweep if item.record and item.record.event_id == event_id]
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0].outcome, Outcome.CONFLICT)
        self.assertFalse(matched[0].ok)
        assert matched[0].record is not None
        self.assertEqual(matched[0].record.task_id, foreign_id)
        self.assertEqual(matched[0].record.url, foreign_url)
        self.assertEqual(self.client.create_calls, creates_before)

    def test_linked_url_only_mismatch_deliver_recover_sweep_conflict(self) -> None:
        """R1: matching task_id with foreign URL must conflict (C2 URL identity)."""
        version = "f" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-url-only-mismatch",
            title="EA010-SYNTHETIC url-only mismatch",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-url-only-mismatch", version),
        )
        first = self.bridge.deliver(row.event_id)
        self.assertTrue(first.ok, first.details)
        assert first.record is not None
        receipt_task_id = first.record.task_id
        self.assertEqual(receipt_task_id, 1)
        expected_url = self.cfg.task_url_template.format(id=receipt_task_id)
        self.assertEqual(first.record.url, expected_url)
        event_id = row.event_id
        creates_before = self.client.create_calls

        foreign_url = "http://127.0.0.1:19345/tasks/999999"
        self.assertNotEqual(foreign_url, expected_url)
        saved = self.store.get_by_event(event_id)
        self.assertIsNotNone(saved)
        assert saved is not None
        saved.task_id = receipt_task_id
        saved.url = foreign_url
        self.store.upsert(saved)

        for method in ("deliver", "recover"):
            with self.subTest(method=method):
                reset = self.store.get_by_event(event_id)
                self.assertIsNotNone(reset)
                assert reset is not None
                reset.state = "linked"
                reset.task_id = receipt_task_id
                reset.url = foreign_url
                reset.details = {
                    k: v
                    for k, v in (reset.details or {}).items()
                    if k
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
                self.store.upsert(reset)

                result = getattr(self.bridge, method)(event_id)
                self.assertEqual(result.outcome, Outcome.CONFLICT)
                self.assertFalse(result.ok)
                assert result.record is not None
                self.assertEqual(result.record.state, "conflict")
                self.assertEqual(result.record.task_id, receipt_task_id)
                self.assertEqual(result.record.url, foreign_url)
                self.assertEqual(
                    (result.details or {}).get("reason"), "incompatible_existing_link"
                )
                self.assertEqual(result.record.details.get("last_check_outcome"), "conflict")
                self.assertIsNotNone(result.dispatch)
                assert result.dispatch is not None
                link_result = dict(result.dispatch.link_result or {})
                self.assertTrue(link_result.get("conflict"))
                self.assertEqual(link_result.get("error"), "incompatible_existing_link")
                public = result.public_tracker()
                self.assertIsNotNone(public)
                assert public is not None
                self.assertEqual(public["state"], "conflict")
                self.assertEqual(public["task_id"], receipt_task_id)
                self.assertEqual(public["url"], foreign_url)
                avail = public.get("availability") or {}
                self.assertEqual(avail.get("status"), "conflict")
                self.assertEqual(avail.get("last_check_outcome"), "conflict")
                self.assertEqual(avail.get("reason"), "incompatible_existing_link")
                stored = self.store.get_by_event(event_id)
                self.assertIsNotNone(stored)
                assert stored is not None
                projected = stored.tracker().as_dict()
                self.assertEqual(projected["state"], "conflict")
                self.assertEqual(projected["task_id"], receipt_task_id)
                self.assertEqual(projected["url"], foreign_url)
                self.assertEqual(
                    (projected.get("availability") or {}).get("status"), "conflict"
                )

        sweep = self.bridge.recover_all_outbox()
        matched = [item for item in sweep if item.record and item.record.event_id == event_id]
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0].outcome, Outcome.CONFLICT)
        self.assertFalse(matched[0].ok)
        assert matched[0].record is not None
        self.assertEqual(matched[0].record.task_id, receipt_task_id)
        self.assertEqual(matched[0].record.url, foreign_url)
        self.assertEqual(self.client.create_calls, creates_before)

    def test_partial_incompatible_backlink_missing_url_propagates_conflict(self) -> None:
        """Receipt success must not map missing-URL incompatible backlink to UNKNOWN."""
        version = "e" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-partial-incompat",
            title="EA010-SYNTHETIC partial incompat",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-partial-incompat", version),
        )
        first = self.bridge.deliver(row.event_id)
        self.assertTrue(first.ok, first.details)
        assert first.record is not None
        receipt_task_id = first.record.task_id
        self.assertIsNotNone(receipt_task_id)
        event_id = row.event_id
        creates_before = self.client.create_calls
        foreign_id = 999

        for method, start_state in (
            ("recover", "linked"),
            ("deliver", "unknown"),
            ("recover", "pending"),
        ):
            with self.subTest(method=method, start_state=start_state):
                reset = self.store.get_by_event(event_id)
                self.assertIsNotNone(reset)
                assert reset is not None
                reset.state = start_state
                reset.task_id = foreign_id
                reset.url = None
                reset.details = {
                    key: value
                    for key, value in (reset.details or {}).items()
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
                self.store.upsert(reset)

                result = getattr(self.bridge, method)(event_id)
                self.assertEqual(result.outcome, Outcome.CONFLICT, result.details)
                self.assertFalse(result.ok)
                assert result.record is not None
                self.assertEqual(result.record.state, "conflict")
                self.assertEqual(result.record.task_id, foreign_id)
                self.assertIsNone(result.record.url)
                self.assertEqual(
                    (result.details or {}).get("reason"), "incompatible_existing_link"
                )
                self.assertEqual(result.record.details.get("reason"), "incompatible_existing_link")
                public = result.public_tracker()
                self.assertIsNotNone(public)
                assert public is not None
                self.assertEqual(public["state"], "conflict")
                self.assertEqual(public["task_id"], foreign_id)
                self.assertIsNone(public["url"])
                avail = public.get("availability") or {}
                self.assertEqual(avail.get("status"), "conflict")
                self.assertEqual(avail.get("outcome"), "conflict")
                self.assertEqual(avail.get("reason"), "incompatible_existing_link")
                self.assertEqual(self.client.create_calls, creates_before)
                self.assertEqual(receipt_task_id, 1)

    def test_ensure_bridge_issuer_reuses_durable_default(self) -> None:
        first = ensure_bridge_issuer(self.journal_path)
        second = ensure_bridge_issuer(self.journal_path)
        self.assertEqual(first, second)
        self.assertEqual(first, self.issuer)
        explicit = ensure_bridge_issuer(self.journal_path, first)
        self.assertEqual(explicit, first)
        with self.assertRaises(JournalError):
            ensure_bridge_issuer(self.journal_path, new_issuer_id())

    def test_missing_by_event_index_rejected_not_silent_empty(self) -> None:
        """R1: readable v1 ledger without by_event must not invent an empty map."""
        version = "c" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-missing-index",
            title="Missing index",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-missing-index", version),
        )
        delivered = self.bridge.deliver(row.event_id)
        self.assertTrue(delivered.ok, delivered.details)
        damaged = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        self.assertIn("by_event", damaged)
        damaged.pop("by_event")
        self.outbox_path.write_text(json.dumps(damaged), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        with self.assertRaises(BridgeError) as load_exc:
            self.store.load()
        self.assertEqual(load_exc.exception.code, "unavailable")
        self.assertIn("schema", str(load_exc.exception).lower())
        with self.assertRaises(BridgeError):
            self.store.get_by_proposal("prop-missing-index")
        miss = self.bridge.deliver(row.event_id)
        self.assertEqual(miss.outcome, Outcome.VALIDATION_ERROR)
        self.assertIsNone(miss.record)
        self.assertEqual((miss.details or {}).get("reason"), "outbox_schema_mismatch")
        self.assertEqual(self.client.create_calls, 1)

    def test_missing_indexed_event_row_rejected_cross_index(self) -> None:
        """R1: by_proposal row without matching by_event entry is inconsistent."""
        version = "d" * 64
        row = self.bridge.register_acceptance(
            proposal_id="prop-missing-row",
            title="Missing indexed row",
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-missing-row", version),
        )
        delivered = self.bridge.deliver(row.event_id)
        self.assertTrue(delivered.ok, delivered.details)
        damaged = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        damaged["by_event"] = {}
        self.outbox_path.write_text(json.dumps(damaged), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        with self.assertRaises(BridgeError) as load_exc:
            self.store.load()
        self.assertEqual(load_exc.exception.code, "unavailable")
        self.assertIn("index", str(load_exc.exception).lower())
        miss = self.bridge.deliver(row.event_id)
        self.assertEqual(miss.outcome, Outcome.VALIDATION_ERROR)
        self.assertIsNone(miss.record)
        self.assertEqual((miss.details or {}).get("reason"), "outbox_index_inconsistent")
        self.assertIsNone(miss.public_tracker())
        self.assertEqual(self.client.create_calls, 1)

    def test_by_event_title_drift_refuses_before_create(self) -> None:
        """R2/C2: altering only by_event.title must not dispatch an unconfirmed title."""
        version = "e" * 64
        original_title = "EA010-SYNTHETIC confirmed title"
        altered_title = "EA010-SYNTHETIC unconfirmed altered title"
        row = self.bridge.register_acceptance(
            proposal_id="prop-payload-drift",
            title=original_title,
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-payload-drift", version),
        )
        self.assertEqual(row.state, "pending")
        creates_before = self.client.create_calls
        ledger = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        event_row = ledger["by_event"][row.event_id]
        prop_row = ledger["by_proposal"][row.proposal_id]
        self.assertEqual(event_row["title"], original_title)
        self.assertEqual(prop_row["title"], original_title)
        self.assertEqual(event_row["payload_digest"], prop_row["payload_digest"])
        event_row["title"] = altered_title
        self.outbox_path.write_text(json.dumps(ledger), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        with self.assertRaises(BridgeError) as load_exc:
            self.store.load()
        self.assertEqual(load_exc.exception.code, "unavailable")
        self.assertIn("index", str(load_exc.exception).lower())
        refused = self.bridge.deliver(row.event_id)
        self.assertEqual(refused.outcome, Outcome.VALIDATION_ERROR)
        self.assertIsNone(refused.record)
        self.assertEqual((refused.details or {}).get("reason"), "outbox_index_inconsistent")
        self.assertEqual(self.client.create_calls, creates_before)
        self.assertEqual(self.client.tasks, {})

    def test_matching_indexes_stale_digest_refuses_before_create(self) -> None:
        """Both indexes can agree on a drifted title; digest recompute still fail-closes."""
        version = "f" * 64
        original_title = "EA010-SYNTHETIC confirmed title"
        altered_title = "EA010-SYNTHETIC unconfirmed altered title"
        row = self.bridge.register_acceptance(
            proposal_id="prop-digest-drift",
            title=original_title,
            due=None,
            source_version=version,
            user_portion=_ready_portion("prop-digest-drift", version),
        )
        creates_before = self.client.create_calls
        ledger = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        for index in ("by_event", "by_proposal"):
            key = row.event_id if index == "by_event" else row.proposal_id
            ledger[index][key]["title"] = altered_title
            # Leave payload_digest as the original acceptance digest.
        self.outbox_path.write_text(json.dumps(ledger), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        with self.assertRaises(BridgeError) as load_exc:
            self.store.load()
        self.assertEqual(load_exc.exception.code, "unavailable")
        self.assertIn("accepted payload", str(load_exc.exception).lower())
        refused = self.bridge.deliver(row.event_id)
        self.assertEqual(refused.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual((refused.details or {}).get("reason"), "outbox_accepted_payload_invalid")
        self.assertEqual(self.client.create_calls, creates_before)
        self.assertEqual(self.client.tasks, {})

    def test_matching_indexes_frozen_wire_drift_refuses_deliver(self) -> None:
        """R1/C2/NFR-11: agreeing indexes with drifted frozen_due.wire must not create."""
        version = "a1" * 32
        accepted_due = "2026-11-12"
        drifted_wire = "2026-12-31T23:59:59+03:00"
        row = self.bridge.register_acceptance(
            proposal_id="prop-frozen-wire-drift",
            title="EA010-SYNTHETIC dated accept",
            due=accepted_due,
            source_version=version,
            user_portion=_ready_portion("prop-frozen-wire-drift", version),
        )
        self.assertEqual(row.due, accepted_due)
        self.assertIsNotNone(row.frozen_due)
        assert row.frozen_due is not None
        original_wire = row.frozen_due.get("wire")
        self.assertEqual(original_wire, "2026-11-12T23:59:59+03:00")
        creates_before = self.client.create_calls
        ledger = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        for index in ("by_event", "by_proposal"):
            key = row.event_id if index == "by_event" else row.proposal_id
            frozen = ledger[index][key]["frozen_due"]
            self.assertEqual(frozen["expression"], accepted_due)
            self.assertEqual(frozen["wire"], original_wire)
            # Leave title/due/source_version/payload_digest and kind/expression intact.
            frozen["wire"] = drifted_wire
        self.outbox_path.write_text(json.dumps(ledger), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        with self.assertRaises(BridgeError) as load_exc:
            self.store.load()
        self.assertEqual(load_exc.exception.code, "unavailable")
        self.assertIn("accepted payload", str(load_exc.exception).lower())
        refused = self.bridge.deliver(row.event_id)
        self.assertEqual(refused.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual((refused.details or {}).get("reason"), "outbox_accepted_payload_invalid")
        self.assertEqual(self.client.create_calls, creates_before)
        self.assertEqual(self.client.tasks, {})

    def test_matching_indexes_frozen_wire_drift_refuses_recover(self) -> None:
        """R1: recover must also refuse drifted frozen absolute before any create."""
        version = "b2" * 32
        accepted_due = "2026-11-12"
        drifted_wire = "2026-12-31T23:59:59+03:00"
        row = self.bridge.register_acceptance(
            proposal_id="prop-frozen-wire-recover",
            title="EA010-SYNTHETIC dated recover",
            due=accepted_due,
            source_version=version,
            user_portion=_ready_portion("prop-frozen-wire-recover", version),
        )
        creates_before = self.client.create_calls
        ledger = json.loads(self.outbox_path.read_text(encoding="utf-8"))
        for index in ("by_event", "by_proposal"):
            key = row.event_id if index == "by_event" else row.proposal_id
            ledger[index][key]["frozen_due"]["wire"] = drifted_wire
        self.outbox_path.write_text(json.dumps(ledger), encoding="utf-8")
        os.chmod(self.outbox_path, 0o600)
        refused = self.bridge.recover(row.event_id)
        self.assertEqual(refused.outcome, Outcome.VALIDATION_ERROR)
        self.assertEqual((refused.details or {}).get("reason"), "outbox_accepted_payload_invalid")
        self.assertEqual(self.client.create_calls, creates_before)
        self.assertEqual(self.client.tasks, {})


class PatchedCoreOutboxProjectionTests(unittest.TestCase):
    """R1: patched Core._project_tracker must not treat a broken outbox as legacy."""

    def setUp(self) -> None:
        os.umask(0o077)
        self.worktree = Path(__file__).resolve().parents[1]
        self.tmp = tempfile.TemporaryDirectory(prefix="ea010-core-outbox-")
        self.root = Path(self.tmp.name).resolve()
        from tests.support.hermes_bridge_suite import _load_module, _prepare_patched_tree

        info = _prepare_patched_tree(self.worktree, self.root / "baseline")
        self.core_mod = _load_module(
            f"ea010_core_outbox_{uuid.uuid4().hex}",
            Path(info["patched_tree"]) / "scripts" / "ea_core.py",
        )
        self.runtime = self.root / "runtime"
        self.tasks = self.root / "tasks"
        self.runtime.mkdir(mode=0o700)
        self.tasks.mkdir(mode=0o700)
        self.policy = self.root / "policy.json"
        self.policy.write_text(
            json.dumps({"text_analysis": {"preferred_agent": "codex", "preferred_model": "fixture"}}),
            encoding="utf-8",
        )
        os.chmod(self.policy, 0o600)
        self.core = self.core_mod.Core(self.runtime, self.tasks, self.policy)
        text = "# EA010-SYNTHETIC unit\nOwner: outbox projection\n"
        self.mid = "c" * 32
        version = __import__("hashlib").sha256(text.encode()).hexdigest()
        transcript = self.runtime / f"transcript-{self.mid}.md"
        transcript.write_text(text, encoding="utf-8")
        os.chmod(transcript, 0o600)
        (self.runtime / ".collector-state.json").write_text(
            json.dumps(
                {
                    "schema": 1,
                    "scope": "owned_and_invited",
                    "first_day": "2026-10-30",
                    "last_complete_day": "2026-11-05",
                    "meetings": {
                        self.mid: {
                            "first_seen": "2026-11-05T12:00:00+03:00",
                            "status": "saved",
                            "transcript": {
                                "sha256": version,
                                "file": f"transcript-{self.mid}.md",
                            },
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        os.chmod(self.runtime / ".collector-state.json", 0o600)
        analysis = self.runtime / ".analysis"
        analysis.mkdir(mode=0o700, exist_ok=True)
        calendar = {
            "schema": "ea009-calendar/v1",
            "checked_at": "2026-11-05T12:00:00+03:00",
            "meetings": {
                self.mid: {
                    "title": "EA010-SYNTHETIC Calendar title",
                    "starts_at": "2026-11-05T09:00:00+03:00",
                    "ends_at": "2026-11-05T10:00:00+03:00",
                    "timezone": "Europe/Moscow",
                    "verification_status": "verified",
                    "checked_at": "2026-11-05T12:00:00+03:00",
                    "time_basis": "calendar",
                    "matched_by": "meeting_and_activity_ids",
                }
            },
        }
        (analysis / "calendar-metadata.json").write_text(
            json.dumps(calendar), encoding="utf-8"
        )
        os.chmod(analysis / "calendar-metadata.json", 0o600)
        self.core.ingest(
            self.mid,
            version,
            {
                "summary": "Synthetic unit",
                "decisions": [],
                "unresolved": [],
                "actions": [
                    {
                        "text": "EA010-SYNTHETIC unit accept",
                        "kind": "self_commitment",
                        "owner": "Owner",
                        "speaker": "Owner",
                        "due_hint": None,
                        "line_start": 2,
                        "line_end": 2,
                        "quote": "Owner: outbox projection",
                    }
                ],
            },
        )
        prop = next(p for p in self.core.snapshot()["proposals"] if p["meeting_id"] == self.mid)
        accepted = self.core.decision(
            prop["id"],
            prop["revision"],
            "accept",
            title="EA010-SYNTHETIC unit accept",
            due=None,
            tracker_target="vikunja",
            source_version=prop["version"],
            due_confirmed=True,
        )
        self.proposal_id = prop["id"]
        self.event_id = (accepted.get("tracker") or {}).get("event_id")
        self.assertEqual((accepted.get("tracker") or {}).get("state"), "pending")
        self.assertIsNotNone(self.event_id)
        self.outbox = self.core._vikunja_outbox_path()
        self.outbox_backup = self.outbox.read_bytes()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _identity(self) -> dict[str, Any]:
        with self.core._lock():
            state = self.core._inventory(self.core._state())
            prop = state["proposals"][self.proposal_id]
            decision = next(
                d
                for d in (prop.get("decisions") or [])
                if isinstance(d, dict) and d.get("event_id") == self.event_id
            )
            return {
                "status": prop.get("status"),
                "event_id": decision.get("event_id"),
                "tracker_target": decision.get("tracker_target"),
            }

    def _restore(self) -> None:
        if self.outbox.exists():
            if self.outbox.is_dir():
                self.outbox.rmdir()
            else:
                self.outbox.unlink()
        self.outbox.write_bytes(self.outbox_backup)
        os.chmod(self.outbox, 0o600)

    def test_corrupt_wrong_schema_read_fail_snapshot_not_legacy(self) -> None:
        CoreError = self.core_mod.CoreError

        self.outbox.write_text('{"schema":', encoding="utf-8")
        os.chmod(self.outbox, 0o600)
        with self.assertRaises(CoreError) as corrupt:
            self.core.snapshot()
        self.assertEqual(corrupt.exception.code, "local_state_unreadable")
        self.assertEqual(self._identity()["status"], "accepted")
        self.assertEqual(self._identity()["event_id"], self.event_id)
        self._restore()

        self.outbox.write_text(
            json.dumps({"schema": "not-ea010-outbox/v0", "by_proposal": {}, "by_event": {}}),
            encoding="utf-8",
        )
        os.chmod(self.outbox, 0o600)
        with self.assertRaises(CoreError) as schema:
            self.core.snapshot()
        self.assertEqual(schema.exception.code, "local_state_schema_changed")
        self.assertEqual(self._identity()["event_id"], self.event_id)
        self._restore()

        self.outbox.unlink()
        self.outbox.mkdir(mode=0o700)
        with self.assertRaises(CoreError) as read_fail:
            self.core.snapshot()
        self.assertEqual(read_fail.exception.code, "local_state_unreadable")
        self.assertEqual(self._identity()["tracker_target"], "vikunja")
        self._restore()

        healed = next(p for p in self.core.snapshot()["proposals"] if p["id"] == self.proposal_id)
        self.assertEqual(healed["status"], "accepted")
        self.assertEqual((healed.get("tracker") or {}).get("event_id"), self.event_id)
        self.assertEqual((healed.get("tracker") or {}).get("state"), "pending")

    def test_missing_outbox_file_snapshot_not_legacy(self) -> None:
        """R1: absent outbox ledger must not project a Vikunja accept as tracker-less legacy."""
        CoreError = self.core_mod.CoreError
        self.outbox.unlink()
        self.assertFalse(self.outbox.exists())
        with self.assertRaises(CoreError) as missing:
            self.core.snapshot()
        self.assertEqual(missing.exception.code, "local_state_unreadable")
        identity = self._identity()
        self.assertEqual(identity["status"], "accepted")
        self.assertEqual(identity["event_id"], self.event_id)
        self.assertEqual(identity["tracker_target"], "vikunja")
        self._restore()
        healed = next(p for p in self.core.snapshot()["proposals"] if p["id"] == self.proposal_id)
        self.assertEqual((healed.get("tracker") or {}).get("event_id"), self.event_id)

    def test_bridge_import_failure_snapshot_not_legacy(self) -> None:
        """R1: missing bridge module must surface unavailable for Vikunja-identified accepts."""
        CoreError = self.core_mod.CoreError
        import builtins

        real_import = builtins.__import__

        def boom(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
            if name == "vikunja_local.hermes_bridge" or name.startswith("vikunja_local.hermes_bridge"):
                raise ModuleNotFoundError(name)
            return real_import(name, globals, locals, fromlist, level)

        builtins.__import__ = boom  # type: ignore[assignment]
        try:
            with self.assertRaises(CoreError) as missing_dep:
                self.core.snapshot()
            self.assertEqual(missing_dep.exception.code, "local_state_unreadable")
            identity = self._identity()
            self.assertEqual(identity["status"], "accepted")
            self.assertEqual(identity["event_id"], self.event_id)
            self.assertEqual(identity["tracker_target"], "vikunja")
        finally:
            builtins.__import__ = real_import  # type: ignore[assignment]

        healed = next(p for p in self.core.snapshot()["proposals"] if p["id"] == self.proposal_id)
        self.assertEqual((healed.get("tracker") or {}).get("event_id"), self.event_id)

    def test_legacy_accept_stays_trackerless_when_outbox_absent(self) -> None:
        """Compatibility: true legacy accepts remain tracker-less when the outbox file is absent."""
        CoreError = self.core_mod.CoreError
        legacy = {
            "id": "legacy-synthetic-1",
            "status": "accepted",
            "decisions": [{"action": "accept", "task_id": "md-legacy-1", "note": ""}],
        }
        self.outbox.unlink()
        self.assertFalse(self.outbox.exists())
        # Vikunja-identified proposal in the same Core still makes full snapshot unavailable.
        with self.assertRaises(CoreError) as blocked:
            self.core.snapshot()
        self.assertEqual(blocked.exception.code, "local_state_unreadable")
        # Direct projection of a legacy-shaped proposal must stay tracker-less (no raise).
        projected = self.core._project_tracker(legacy)
        self.assertEqual(projected["status"], "accepted")
        self.assertIsNone(projected.get("tracker"))
        self.assertIsNone(self.core._vikunja_decision_identity(legacy))
        self._restore()

    def test_missing_by_event_index_snapshot_and_deliver_not_healthy(self) -> None:
        """R1: dropped by_event must not surface healthy linked without availability."""
        CoreError = self.core_mod.CoreError
        token = self.root / "token"
        token.write_text("synthetic-only", encoding="utf-8")
        os.chmod(token, 0o600)
        config = {
            "journal_path": str(self.root / "journal.sqlite3"),
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_ref": {"kind": "file", "path": str(token)},
            "task_url_template": "http://127.0.0.1:19345/tasks/{id}",
            "hermes_origin": "http://127.0.0.1:19347",
        }
        config_path = self.root / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        os.chmod(config_path, 0o600)
        client = FakeClient()
        bridge = HermesBridge(BridgeConfig.from_mapping(config), HermesOutboxStore(self.outbox))
        bridge._client = lambda: client  # type: ignore[method-assign]
        first = bridge.deliver(self.event_id)
        self.assertTrue(first.ok, first.details)
        assert first.record is not None
        historical = first.record.tracker().as_dict()
        self.assertEqual(historical.get("state"), "linked")
        self.assertNotIn("availability", historical)

        damaged = json.loads(self.outbox.read_text(encoding="utf-8"))
        damaged.pop("by_event")
        self.outbox.write_text(json.dumps(damaged), encoding="utf-8")
        os.chmod(self.outbox, 0o600)

        with self.assertRaises(CoreError) as snap:
            self.core.snapshot()
        self.assertEqual(snap.exception.code, "local_state_schema_changed")
        identity = self._identity()
        self.assertEqual(identity["status"], "accepted")
        self.assertEqual(identity["event_id"], self.event_id)

        miss = bridge.deliver(self.event_id)
        self.assertEqual(miss.outcome, Outcome.VALIDATION_ERROR)
        self.assertIsNone(miss.record)
        self.assertEqual((miss.details or {}).get("reason"), "outbox_schema_mismatch")

        proposal = {
            "id": self.proposal_id,
            "status": "accepted",
            "tracker": dict(historical),
        }
        with unittest.mock.patch.dict(os.environ, {"EA010_HERMES_BRIDGE_CONFIG": str(config_path)}):
            response = self.core.deliver_vikunja_outbox(proposal)
        tracker = response.get("tracker") or {}
        self.assertEqual(tracker.get("state"), "linked")
        self.assertEqual(tracker.get("task_id"), historical.get("task_id"))
        self.assertEqual(tracker.get("url"), historical.get("url"))
        self.assertEqual(tracker.get("event_id"), self.event_id)
        avail = tracker.get("availability") or {}
        self.assertEqual(avail.get("status"), "unavailable")
        self.assertEqual(avail.get("reason"), "outbox_schema_mismatch")
        self.assertNotEqual(avail.get("last_check_outcome"), "confirmed")
        self.assertEqual(client.create_calls, 1)
        self._restore()

    def test_missing_indexed_row_deliver_projects_availability(self) -> None:
        """R1: empty by_event with retained by_proposal → unavailable, not healthy link."""
        CoreError = self.core_mod.CoreError
        token = self.root / "token-row"
        token.write_text("synthetic-only", encoding="utf-8")
        os.chmod(token, 0o600)
        config = {
            "journal_path": str(self.root / "journal-row.sqlite3"),
            "api_origin": "http://127.0.0.1:19345",
            "project_id": 2,
            "token_ref": {"kind": "file", "path": str(token)},
            "task_url_template": "http://127.0.0.1:19345/tasks/{id}",
            "hermes_origin": "http://127.0.0.1:19347",
        }
        config_path = self.root / "config-row.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        client = FakeClient()
        bridge = HermesBridge(BridgeConfig.from_mapping(config), HermesOutboxStore(self.outbox))
        bridge._client = lambda: client  # type: ignore[method-assign]
        first = bridge.deliver(self.event_id)
        self.assertTrue(first.ok, first.details)
        assert first.record is not None
        historical = first.record.tracker().as_dict()

        damaged = json.loads(self.outbox.read_text(encoding="utf-8"))
        damaged["by_event"] = {}
        self.outbox.write_text(json.dumps(damaged), encoding="utf-8")
        os.chmod(self.outbox, 0o600)

        with self.assertRaises(CoreError) as snap:
            self.core.snapshot()
        self.assertEqual(snap.exception.code, "local_state_unreadable")

        proposal = {"id": self.proposal_id, "status": "accepted", "tracker": dict(historical)}
        with unittest.mock.patch.dict(os.environ, {"EA010_HERMES_BRIDGE_CONFIG": str(config_path)}):
            response = self.core.deliver_vikunja_outbox(proposal)
        avail = (response.get("tracker") or {}).get("availability") or {}
        self.assertEqual(avail.get("status"), "unavailable")
        self.assertEqual(avail.get("reason"), "outbox_index_inconsistent")
        self.assertEqual(client.create_calls, 1)
        self._restore()


if __name__ == "__main__":
    unittest.main()
