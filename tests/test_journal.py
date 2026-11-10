"""Unit and subprocess checks for durable integration journal (s03)."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vikunja_local.api import Outcome, build_marker  # noqa: E402
from vikunja_local.journal import (  # noqa: E402
    DeliveryState,
    IntegrationJournal,
    JournalError,
    OperationState,
    new_issuer_id,
    new_operation_id,
)
from vikunja_local.storage import (  # noqa: E402
    JOURNAL_SQLITE_USER_VERSION,
    JournalLock,
    SqliteJournalStore,
    StorageError,
    authority_path_for,
    install_path_for,
    lock_path_for,
)

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")


class JournalUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="ea010-journal-unit-")
        self.dir = Path(self._tmpdir.name)
        self.path = self.dir / "journal.sqlite3"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_prepare_dispatch_receipt_roundtrip(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "unit-a"},
            )
            self.assertEqual(prepared.state, OperationState.PREPARED)
            self.assertTrue(prepared.allows_first_http_dispatch())
            self.assertEqual(prepared.marker, build_marker(issuer, op))
            started = journal.begin_dispatch(issuer, op)
            self.assertEqual(started.state, OperationState.DISPATCH_STARTED)
            self.assertFalse(started.allows_first_http_dispatch())
            confirmed = journal.record_receipt(
                issuer,
                op,
                vikunja_id=11,
                project_id=3,
                marker=started.marker,
                effect={"title": "unit-a"},
            )
            self.assertEqual(confirmed.state, OperationState.CONFIRMED)
            self.assertEqual(confirmed.delivery_state, DeliveryState.RECEIPT)
            self.assertIsNotNone(confirmed.receipt)

        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.receipt["vikunja_id"], 11)
            self.assertEqual(journal.get_issuer(), issuer)

    def test_identical_key_replay_does_not_rearm_dispatch(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            payload = {"title": "same"}
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload=payload,
            )
            journal.begin_dispatch(issuer, op)
            again = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload=payload,
            )
            self.assertEqual(again.state, OperationState.DISPATCH_STARTED)
            self.assertFalse(journal.can_dispatch(issuer, op))

    def test_identical_key_replay_across_seconds_reuses_confirmed_at(self) -> None:
        """Same key/type/project/target/payload after >1s must reuse persisted confirmation."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            payload = {"title": "cross-second-replay"}
            first = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload=payload,
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=77,
                project_id=3,
                marker=first.marker,
                effect={"id": 77},
            )
            confirmed = first.confirmed_at

        time.sleep(1.1)

        with IntegrationJournal(self.path) as journal:
            again = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload=payload,
            )
            self.assertEqual(again.confirmed_at, confirmed)
            self.assertEqual(again.state, OperationState.CONFIRMED)
            self.assertIsNotNone(again.receipt)
            self.assertEqual(again.receipt["vikunja_id"], 77)
            self.assertFalse(journal.can_dispatch(issuer, op))
            loaded = journal.get_operation(issuer, op)
            self.assertEqual(loaded.state, OperationState.CONFIRMED)

    def test_incompatible_payload_conflicts(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "a"},
            )
            with self.assertRaises(JournalError) as ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=op,
                    op_type="create",
                    project_id=3,
                    payload={"title": "b"},
                )
            self.assertEqual(ctx.exception.outcome, Outcome.CONFLICT)
            loaded = journal.get_operation(issuer, op)
            self.assertEqual(loaded.state, OperationState.CONFLICT)
            self.assertFalse(journal.can_dispatch(issuer, op))

    def test_source_mapping_unique(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op1 = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op1,
                op_type="create",
                project_id=3,
                payload={"title": "one"},
                source_kind="synthetic",
                source_key="acceptance-1",
            )
            with self.assertRaises(JournalError) as ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "two"},
                    source_kind="synthetic",
                    source_key="acceptance-1",
                )
            self.assertEqual(ctx.exception.outcome, Outcome.CONFLICT)

    def test_intent_alone_does_not_prove_http_unsent(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "x"},
            )
            self.assertTrue(prepared.allows_first_http_dispatch())
            journal.begin_dispatch(issuer, op)
            after = journal.get_operation(issuer, op)
            self.assertFalse(after.allows_first_http_dispatch())
            with self.assertRaises(JournalError):
                journal.begin_dispatch(issuer, op)

    def test_restore_drift_and_unavailable_block_dispatch(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "drift"},
            )
            journal.set_restore_drift(True)
            self.assertFalse(journal.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)

    def test_incompatible_schema_preserves_receipts(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "keep"},
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=5,
                project_id=3,
                marker=build_marker(issuer, op),
                effect={"keep": True},
            )
        conn = sqlite3.connect(str(self.path))
        conn.execute("PRAGMA user_version=42")
        conn.commit()
        before = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
        conn.close()
        self.assertEqual(before, 1)
        with self.assertRaises(JournalError):
            IntegrationJournal(self.path).open()
        conn = sqlite3.connect(str(self.path))
        after = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        conn.close()
        self.assertEqual(after, 1)
        self.assertEqual(version, 42)
        self.assertNotEqual(JOURNAL_SQLITE_USER_VERSION, 42)

    def test_corrupt_journal_fail_closed_no_delete(self) -> None:
        with IntegrationJournal(self.path) as journal:
            journal.ensure_issuer()
        self.path.write_bytes(b"corrupt-not-sqlite")
        with self.assertRaises(JournalError):
            IntegrationJournal(self.path).open()
        self.assertTrue(self.path.is_file())

    def test_relative_path_rejected(self) -> None:
        with self.assertRaises(JournalError):
            IntegrationJournal(Path("relative.sqlite3"))

    def test_unknown_retained_and_no_ttl_api(self) -> None:
        self.assertFalse(hasattr(IntegrationJournal, "purge_expired"))
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "u"},
            )
            journal.begin_dispatch(issuer, op)
            journal.mark_unknown(issuer, op, reason="timeout")
        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertEqual(loaded.state, OperationState.UNKNOWN)

    def test_issuer_is_uuid_v4_lowercase(self) -> None:
        issuer = new_issuer_id()
        self.assertRegex(
            issuer,
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        )

    def test_guard_enter_failure_releases_lock_for_peer(self) -> None:
        """R1: restore_drift reject must not leave OS-lock held after close."""
        with IntegrationJournal(self.path) as journal:
            canonical = journal.path
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "lock-release"},
            )
            journal.set_restore_drift(True)
            with self.assertRaises(JournalError):
                journal.begin_dispatch(issuer, op)
            journal.set_restore_drift(False)
        # Journal closed; a peer nonblocking flock on the sidecar must succeed.
        import fcntl

        lock_path = lock_path_for(canonical)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def test_symlink_journal_alias_rejected(self) -> None:
        """R2: final-component symlink must not open a substituted journal."""
        with IntegrationJournal(self.path) as journal:
            journal.ensure_issuer()
        alias = self.dir / "alias.sqlite3"
        os.symlink(self.path.name, alias)
        with self.assertRaises(JournalError) as ctx:
            IntegrationJournal(alias)
        self.assertIn("symlink", str(ctx.exception).lower())

    def test_issuer_loss_blocks_dispatch_and_preserves_keys(self) -> None:
        """R3: deleting meta.issuer_id must fail-close dispatch; keep operations."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "issuer-loss"},
            )
        conn = sqlite3.connect(str(self.path))
        conn.execute("DELETE FROM meta WHERE key = ?", ("issuer_id",))
        conn.commit()
        remaining = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        conn.close()
        self.assertEqual(remaining, 1)
        with IntegrationJournal(self.path) as journal:
            self.assertIsNone(journal.get_issuer())
            self.assertFalse(journal.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertIn("issuer", str(ctx.exception).lower())
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.state, OperationState.PREPARED)

    def test_issuer_loss_ensure_issuer_does_not_remint(self) -> None:
        """R3: ensure_issuer must not mint a new identity over prior durable rows."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "no-remint"},
            )
        conn = sqlite3.connect(str(self.path))
        conn.execute("DELETE FROM meta WHERE key = ?", ("issuer_id",))
        conn.commit()
        conn.close()
        with IntegrationJournal(self.path) as journal:
            with self.assertRaises(JournalError) as ctx:
                journal.ensure_issuer()
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertIsNone(journal.get_issuer())
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)


class JournalSubprocessTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="ea010-journal-sub-")
        self.dir = Path(self._tmpdir.name)
        self.path = self.dir / "journal.sqlite3"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_sigkill_preserves_dispatch_started(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
        op = new_operation_id()
        ready = self.dir / "ready.json"
        script = r"""
import json, os, signal, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
ready = Path(sys.argv[5])
j = IntegrationJournal(path)
j.open()
j.prepare_intent(issuer_id=issuer, operation_id=op, op_type="create", project_id=3, payload={"title":"kill"})
started = j.begin_dispatch(issuer, op)
ready.write_text(json.dumps({"state": started.state.value}), encoding="utf-8")
time.sleep(0.2)
os.kill(os.getpid(), signal.SIGKILL)
"""
        proc = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), issuer, op, str(ready)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        try:
            for _ in range(200):
                if ready.is_file() and ready.stat().st_size:
                    break
                if proc.poll() is not None:
                    self.fail(f"child exited early rc={proc.returncode}")
                import time as _time

                _time.sleep(0.02)
            else:
                self.fail("ready file not written")
            proc.wait(timeout=10)
            self.assertEqual(proc.returncode, -signal.SIGKILL)
        finally:
            if proc.poll() is None:
                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)

        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.state, OperationState.DISPATCH_STARTED)
            self.assertFalse(journal.can_dispatch(issuer, op))

    def test_two_processes_serialize_dispatch(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "race"},
            )
        gate = self.dir / "gate.txt"
        gate.write_text("wait", encoding="utf-8")
        script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
gate = Path(sys.argv[5])
while gate.read_text(encoding="utf-8").strip() != "go":
    time.sleep(0.01)
j = IntegrationJournal(path)
j.open()
try:
    started = j.begin_dispatch(issuer, op)
    print(json.dumps({"ok": True, "state": started.state.value}))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc)}))
"""
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        c1 = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), issuer, op, str(gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        c2 = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), issuer, op, str(gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            import time as _time

            _time.sleep(0.1)
            gate.write_text("go", encoding="utf-8")
            out1, err1 = c1.communicate(timeout=20)
            out2, err2 = c2.communicate(timeout=20)
            self.assertEqual(c1.returncode, 0, err1)
            self.assertEqual(c2.returncode, 0, err2)
            r1 = json.loads(out1.strip().splitlines()[-1])
            r2 = json.loads(out2.strip().splitlines()[-1])
            oks = [r.get("ok") for r in (r1, r2)]
            self.assertEqual(sorted(oks), [False, True], f"{r1} {r2}")
        finally:
            for proc in (c1, c2):
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)

        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertEqual(loaded.state, OperationState.DISPATCH_STARTED)

    def test_durable_save_failure_child(self) -> None:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
        script = r"""
import json, os, resource, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
resource.setrlimit(resource.RLIMIT_FSIZE, (4096, 4096))
path = Path(sys.argv[2])
issuer = sys.argv[3]
j = IntegrationJournal(path)
try:
    j.open()
    op = new_operation_id()
    j.prepare_intent(
        issuer_id=issuer,
        operation_id=op,
        op_type="create",
        project_id=3,
        payload={"title": "z" * 50000},
    )
    print(json.dumps({"failed": False, "can_dispatch": j.can_dispatch(issuer, op)}))
except JournalError as exc:
    print(json.dumps({"failed": True, "can_dispatch": False, "outcome": exc.outcome.value}))
except Exception as exc:
    print(json.dumps({"failed": True, "can_dispatch": False, "error": str(exc)}))
"""
        proc = subprocess.run(
            [sys.executable, "-c", script, str(ROOT), str(self.path), issuer],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            timeout=20,
        )
        self.assertTrue(proc.stdout.strip(), proc.stderr)
        payload = json.loads(proc.stdout.strip().splitlines()[-1])
        self.assertTrue(payload["failed"])
        self.assertFalse(payload["can_dispatch"])

    def test_peer_lock_free_after_drift_reject_close(self) -> None:
        """R1 peer-process: after drift reject + close, peer flock must not block."""
        with IntegrationJournal(self.path) as journal:
            canonical = str(journal.path)
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "peer-lock"},
            )
            journal.set_restore_drift(True)
            with self.assertRaises(JournalError):
                journal.begin_dispatch(issuer, op)
            journal.set_restore_drift(False)
        script = r"""
import fcntl, json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.storage import lock_path_for
lock = lock_path_for(Path(sys.argv[2]))
fd = os.open(str(lock), os.O_RDWR | os.O_CREAT, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(fd, fcntl.LOCK_UN)
    print(json.dumps({"acquired": True}))
except BlockingIOError as exc:
    print(json.dumps({"acquired": False, "error": str(exc)}))
    raise SystemExit(2)
finally:
    os.close(fd)
"""
        proc = subprocess.run(
            [sys.executable, "-c", script, str(ROOT), canonical],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            timeout=10,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(json.loads(proc.stdout.strip())["acquired"])

    def test_symlink_alias_subprocess_cannot_bypass_lock(self) -> None:
        """R2: child opening alias.sqlite3→real must not commit while lock held."""
        with IntegrationJournal(self.path) as journal:
            real_path = journal.path
            issuer = journal.ensure_issuer()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=3,
                payload={"title": "parent-held"},
            )
        alias = self.dir / "alias.sqlite3"
        os.symlink(real_path.name, alias)
        ready = self.dir / "hold-ready.json"
        hold_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.storage import JournalLock, lock_path_for
lock = JournalLock(lock_path_for(Path(sys.argv[2])))
lock.acquire()
Path(sys.argv[3]).write_text(json.dumps({"held": True}), encoding="utf-8")
time.sleep(8)
lock.release()
"""
        holder = subprocess.Popen(
            [sys.executable, "-c", hold_script, str(ROOT), str(real_path), str(ready)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        try:
            import time as _time

            for _ in range(200):
                if ready.is_file():
                    break
                if holder.poll() is not None:
                    self.fail(f"holder exited early rc={holder.returncode}")
                _time.sleep(0.02)
            else:
                self.fail("holder ready missing")
            child_script = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
alias = Path(sys.argv[2])
issuer = sys.argv[3]
try:
    j = IntegrationJournal(alias)
    j.open()
    j.prepare_intent(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        op_type="create",
        project_id=3,
        payload={"title": "alias-bypass"},
    )
    print(json.dumps({"rc_ok": True, "bypassed": True}))
except JournalError as exc:
    print(json.dumps({"rc_ok": False, "bypassed": False, "error": str(exc)}))
except Exception as exc:
    print(json.dumps({"rc_ok": False, "bypassed": False, "error": str(exc)}))
"""
            child = subprocess.run(
                [sys.executable, "-c", child_script, str(ROOT), str(alias), issuer],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                timeout=10,
            )
            self.assertTrue(child.stdout.strip(), child.stderr)
            payload = json.loads(child.stdout.strip().splitlines()[-1])
            self.assertFalse(payload.get("bypassed"), payload)
            self.assertFalse(payload.get("rc_ok"), payload)
        finally:
            if holder.poll() is None:
                os.kill(holder.pid, signal.SIGKILL)
                holder.wait(timeout=5)

    def test_hardlink_alias_subprocess_cannot_bypass_lock(self) -> None:
        """R2: hard-linked alias must not commit while the real journal OS-lock is held."""
        with IntegrationJournal(self.path) as journal:
            real_path = journal.path
            issuer = journal.ensure_issuer()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=3,
                payload={"title": "hardlink-parent"},
            )
        alias = self.dir / "hardlink-alias.sqlite3"
        os.link(real_path, alias)
        self.assertGreater(os.stat(real_path).st_nlink, 1)
        ready = self.dir / "hardlink-hold-ready.json"
        hold_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.storage import JournalLock, lock_path_for
lock = JournalLock(lock_path_for(Path(sys.argv[2])))
lock.acquire()
Path(sys.argv[3]).write_text(json.dumps({"held": True}), encoding="utf-8")
time.sleep(8)
lock.release()
"""
        holder = subprocess.Popen(
            [sys.executable, "-c", hold_script, str(ROOT), str(real_path), str(ready)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        try:
            for _ in range(200):
                if ready.is_file():
                    break
                if holder.poll() is not None:
                    self.fail(f"holder exited early rc={holder.returncode}")
                time.sleep(0.02)
            else:
                self.fail("hardlink holder ready missing")
            child_script = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
alias = Path(sys.argv[2])
issuer = sys.argv[3]
try:
    j = IntegrationJournal(alias)
    j.open()
    j.prepare_intent(
        issuer_id=issuer,
        operation_id=new_operation_id(),
        op_type="create",
        project_id=3,
        payload={"title": "hardlink-bypass"},
    )
    print(json.dumps({"rc_ok": True, "bypassed": True}))
except Exception as exc:
    print(json.dumps({
        "rc_ok": False,
        "bypassed": False,
        "error": str(exc),
        "type": type(exc).__name__,
    }))
    raise SystemExit(2)
"""
            child = subprocess.run(
                [sys.executable, "-c", child_script, str(ROOT), str(alias), issuer],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                timeout=10,
            )
            self.assertNotEqual(child.returncode, 0, child.stdout + child.stderr)
            self.assertTrue(child.stdout.strip(), child.stderr)
            payload = json.loads(child.stdout.strip().splitlines()[-1])
            self.assertFalse(payload.get("bypassed"), payload)
            self.assertFalse(payload.get("rc_ok"), payload)
            err = str(payload.get("error", "")).lower()
            self.assertTrue(
                "hard-link" in err or "multiply-linked" in err or "nlink" in err
                or "unavailable" in err,
                payload,
            )
        finally:
            if holder.poll() is None:
                os.kill(holder.pid, signal.SIGKILL)
                holder.wait(timeout=5)

    def test_post_construct_symlink_swap_refused(self) -> None:
        """F2: alias created after IntegrationJournal construction must fail closed."""
        with IntegrationJournal(self.path) as journal:
            real_path = journal.path
            issuer = journal.ensure_issuer()
        alias = self.dir / "late-alias.sqlite3"
        late = IntegrationJournal(alias)
        os.symlink(real_path.name, alias)
        lock = JournalLock(lock_path_for(real_path))
        lock.acquire()
        try:
            with self.assertRaises(JournalError) as ctx:
                late.open()
                late.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "late-alias"},
                )
            self.assertIn("symlink", str(ctx.exception).lower())
        finally:
            lock.release()
            try:
                late.close()
            except Exception:  # noqa: BLE001
                pass

    def test_lock_second_thread_cannot_release_owner(self) -> None:
        """F3: non-owner thread must not reenter/release; peer flock stays blocked."""
        lock_path = self.dir / "owner.lock"
        lock = JournalLock(lock_path)
        lock.acquire()
        secondary: dict[str, object] = {}

        def other() -> None:
            secondary["started"] = True
            try:
                lock.acquire()
                secondary["acquired"] = True
                lock.release()
            except Exception as exc:  # noqa: BLE001
                secondary["error"] = str(exc)

        thread = threading.Thread(target=other)
        thread.start()
        time.sleep(0.2)
        self.assertTrue(secondary.get("started"))
        self.assertNotIn("acquired", secondary)
        self.assertTrue(lock.held)
        fd = os.open(str(lock_path), os.O_RDWR)
        try:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)
        lock.release()
        thread.join(timeout=5)
        self.assertTrue(secondary.get("acquired"))


class JournalMixedCopyAndReceiptTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="ea010-journal-mixed-")
        self.dir = Path(self._tmpdir.name)
        self.path = self.dir / "journal.sqlite3"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_mixed_copy_operations_row_cannot_rearm_dispatch(self) -> None:
        """F1: restoring only the prepared operations row must fail closed."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "mixed-copy"},
            )
            conn = sqlite3.connect(str(self.path))
            conn.row_factory = sqlite3.Row
            saved = dict(
                conn.execute(
                    "SELECT * FROM operations WHERE operation_id = ?",
                    (op,),
                ).fetchone()
            )
            conn.close()
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=42,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 42},
            )

        conn = sqlite3.connect(str(self.path))
        cols = ",".join(saved.keys())
        placeholders = ",".join("?" for _ in saved)
        conn.execute("DELETE FROM operations WHERE operation_id = ?", (op,))
        conn.execute(
            f"INSERT INTO operations({cols}) VALUES ({placeholders})",
            tuple(saved.values()),
        )
        conn.commit()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        receipt_n = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
        events = [
            row[0]
            for row in conn.execute(
                "SELECT event_type FROM history WHERE operation_id = ? ORDER BY id",
                (op,),
            )
        ]
        conn.close()
        self.assertEqual(integrity, "ok")
        self.assertEqual(receipt_n, 1)
        self.assertIn("dispatch_started", events)
        self.assertIn("receipt", events)

        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.state, OperationState.PREPARED)
            self.assertIsNotNone(loaded.receipt)
            self.assertFalse(journal.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)

    def test_conflict_history_partial_restore_cannot_rearm_dispatch(self) -> None:
        """R1: conflict history must refuse re-arm after partial prepared restore."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "payload-a"},
            )
            with self.assertRaises(JournalError) as conflict_ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=op,
                    op_type="create",
                    project_id=3,
                    payload={"title": "payload-b-incompatible"},
                )
            self.assertEqual(conflict_ctx.exception.outcome, Outcome.CONFLICT)
            conflicted = journal.get_operation(issuer, op)
            self.assertIsNotNone(conflicted)
            assert conflicted is not None
            self.assertEqual(conflicted.state, OperationState.CONFLICT)
            events_before = [
                row["event_type"] for row in journal.history(issuer, op)
            ]
            self.assertEqual(events_before, ["intent_prepared", "conflict"])

        conn = sqlite3.connect(str(self.path))
        conn.execute(
            """
            UPDATE operations
            SET state = ?, delivery_state = ?
            WHERE operation_id = ?
            """,
            (
                OperationState.PREPARED.value,
                DeliveryState.NONE.value,
                op,
            ),
        )
        conn.commit()
        events = [
            row[0]
            for row in conn.execute(
                "SELECT event_type FROM history WHERE operation_id = ? ORDER BY id",
                (op,),
            )
        ]
        receipt_n = conn.execute(
            "SELECT COUNT(*) FROM receipts WHERE operation_id = ?",
            (op,),
        ).fetchone()[0]
        conn.close()
        self.assertEqual(events, ["intent_prepared", "conflict"])
        self.assertEqual(receipt_n, 0)

        with IntegrationJournal(self.path) as journal:
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.state, OperationState.PREPARED)
            self.assertEqual(loaded.delivery_state, DeliveryState.NONE)
            self.assertIsNone(loaded.receipt)
            # begin_dispatch before can_dispatch so mixed_copy details are observable
            # before quarantine flips writes off for subsequent mutations.
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertTrue(ctx.exception.details.get("mixed_copy"))
            self.assertIn("conflict", ctx.exception.details.get("proof_events", []))
            self.assertFalse(journal.can_dispatch(issuer, op))
            # Mixed-copy refuse must quarantine so further durable writes stay off.
            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "after-quarantine"},
                )

    def test_missing_source_mapping_cannot_mint_second_operation(self) -> None:
        """F1: deleting only source_mappings must not allow a second dispatchable op."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "accept"},
                source_kind="synthetic",
                source_key="EA010-SYNTHETIC-acceptance",
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=777,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 777},
            )

        conn = sqlite3.connect(str(self.path))
        deleted = conn.execute("DELETE FROM source_mappings").rowcount
        conn.commit()
        receipt_n = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
        ops_n = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        conn.close()
        self.assertEqual(deleted, 1)
        self.assertEqual(receipt_n, 1)
        self.assertEqual(ops_n, 1)

        with IntegrationJournal(self.path) as journal:
            with self.assertRaises(JournalError) as ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "accept-again"},
                    source_kind="synthetic",
                    source_key="EA010-SYNTHETIC-acceptance",
                )
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertTrue(ctx.exception.details.get("partial_copy"))
            self.assertFalse(journal.can_dispatch(issuer, op))
            kept = journal.get_operation(issuer, op)
            self.assertIsNotNone(kept)
            assert kept is not None
            self.assertIsNotNone(kept.receipt)
            self.assertEqual(kept.receipt["vikunja_id"], 777)

        conn = sqlite3.connect(str(self.path))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 1)
        conn.close()

    def test_altered_payload_json_cannot_dispatch(self) -> None:
        """F2: payload_json drift with intact digest/history must refuse first dispatch."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-confirmed"},
            )

        conn = sqlite3.connect(str(self.path))
        conn.execute(
            "UPDATE operations SET payload_json = ? WHERE operation_id = ?",
            ('{"title":"ALTERED-UNCONFIRMED"}', op),
        )
        conn.commit()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        row = conn.execute(
            "SELECT payload_json, payload_digest FROM operations WHERE operation_id = ?",
            (op,),
        ).fetchone()
        conn.close()
        self.assertEqual(integrity, "ok")
        self.assertEqual(row[0], '{"title":"ALTERED-UNCONFIRMED"}')

        with IntegrationJournal(self.path) as journal:
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertIs(ctx.exception.details.get("sha256_matches_payload"), False)
            self.assertTrue(ctx.exception.details.get("mixed_copy"))
            self.assertFalse(journal.can_dispatch(issuer, op))
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.payload.get("title"), "ALTERED-UNCONFIRMED")
            self.assertEqual(
                [row["event_type"] for row in journal.history(issuer, op)],
                ["intent_prepared"],
            )

    def test_missing_safety_meta_fails_closed(self) -> None:
        """F3: dropping restore_drift/writes_enabled must not re-enable dispatch."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "drift-guard"},
            )
            journal.set_restore_drift(True)

        for key in ("restore_drift", "writes_enabled", "quarantine"):
            path = self.dir / f"f3-missing-{key}.sqlite3"
            path.write_bytes(self.path.read_bytes())
            conn = sqlite3.connect(str(path))
            conn.execute("DELETE FROM meta WHERE key = ?", (key,))
            conn.commit()
            conn.close()
            with self.assertRaises(JournalError) as ctx:
                with IntegrationJournal(path) as journal:
                    journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertIn("safety", str(ctx.exception).lower())

        conn = sqlite3.connect(str(self.path))
        conn.execute(
            "DELETE FROM meta WHERE key IN ('restore_drift', 'writes_enabled')"
        )
        conn.commit()
        # Preserve issuer/operations/history evidence rows.
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0],
            1,
        )
        self.assertGreaterEqual(
            conn.execute("SELECT COUNT(*) FROM history").fetchone()[0],
            1,
        )
        conn.close()
        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                self.assertTrue(journal.can_dispatch(issuer, op))
                journal.begin_dispatch(issuer, op)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)

    def test_receipt_project_id_mismatch_rejected(self) -> None:
        """F4: receipt project_id must match immutable intent project_id."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="update",
                project_id=7,
                target_id=11,
                payload={"title": "p-mismatch"},
            )
            journal.begin_dispatch(issuer, op)
            with self.assertRaises(JournalError) as ctx:
                journal.record_receipt(
                    issuer,
                    op,
                    vikunja_id=11,
                    project_id=8,
                    marker=prepared.marker,
                    effect={"id": 11},
                )
            self.assertEqual(ctx.exception.outcome, Outcome.CONFLICT)
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.state, OperationState.CONFLICT)
            self.assertIsNone(loaded.receipt)

    def test_receipt_vikunja_id_mismatch_rejected(self) -> None:
        """F4: update/complete receipt vikunja_id must match immutable target_id."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="update",
                project_id=7,
                target_id=11,
                payload={"title": "id-mismatch"},
            )
            journal.begin_dispatch(issuer, op)
            with self.assertRaises(JournalError) as ctx:
                journal.record_receipt(
                    issuer,
                    op,
                    vikunja_id=12,
                    project_id=7,
                    marker=prepared.marker,
                    effect={"id": 12},
                )
            self.assertEqual(ctx.exception.outcome, Outcome.CONFLICT)
            loaded = journal.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.state, OperationState.CONFLICT)
            self.assertIsNone(loaded.receipt)

    def test_altered_envelope_metadata_cannot_dispatch(self) -> None:
        """F1: project_id/target_id/marker drift must refuse first dispatch."""
        cases = (
            ("project_id", "UPDATE operations SET project_id = 4 WHERE operation_id = ?", None),
            ("target_id", "UPDATE operations SET target_id = 42 WHERE operation_id = ?", "update"),
            (
                "marker",
                "UPDATE operations SET marker = ? WHERE operation_id = ?",
                "create",
            ),
        )
        for label, sql, op_type in cases:
            with self.subTest(field=label):
                path = self.dir / f"envelope-{label}.sqlite3"
                with IntegrationJournal(path) as journal:
                    issuer = journal.ensure_issuer()
                    op = new_operation_id()
                    if op_type == "update":
                        journal.prepare_intent(
                            issuer_id=issuer,
                            operation_id=op,
                            op_type="update",
                            project_id=3,
                            target_id=41,
                            payload={"title": f"meta-{label}"},
                        )
                    else:
                        journal.prepare_intent(
                            issuer_id=issuer,
                            operation_id=op,
                            op_type="create",
                            project_id=3,
                            payload={"title": f"meta-{label}"},
                        )

                conn = sqlite3.connect(str(path))
                if label == "marker":
                    conn.execute(sql, ("EA010-SYNTHETIC-altered-marker", op))
                else:
                    conn.execute(sql, (op,))
                conn.commit()
                conn.close()

                with IntegrationJournal(path) as journal:
                    with self.assertRaises(JournalError) as ctx:
                        journal.begin_dispatch(issuer, op)
                    self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
                    self.assertTrue(ctx.exception.details.get("mixed_copy"))
                    self.assertIs(ctx.exception.details.get("sha256_matches_payload"), False)
                    self.assertFalse(journal.can_dispatch(issuer, op))

    def test_version0_nonempty_refuses_bootstrap(self) -> None:
        """F2: user_version=0 with existing tables must not restore permissive flags."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "version0-guard"},
            )
            journal.set_restore_drift(True)

        conn = sqlite3.connect(str(self.path))
        conn.execute("PRAGMA user_version=0")
        conn.execute(
            "DELETE FROM meta WHERE key IN ('writes_enabled', 'restore_drift', 'quarantine')"
        )
        conn.commit()
        ops = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        hist = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        conn.close()
        self.assertEqual(ops, 1)
        self.assertGreaterEqual(hist, 1)

        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                self.assertTrue(journal.can_dispatch(issuer, op))
                journal.begin_dispatch(issuer, op)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertIn("user_version", str(ctx.exception).lower())

        conn = sqlite3.connect(str(self.path))
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 1)
        self.assertIsNone(
            conn.execute(
                "SELECT value FROM meta WHERE key = ?",
                ("writes_enabled",),
            ).fetchone()
        )
        conn.close()

    def test_missing_journal_with_sidecar_refuses_remint(self) -> None:
        """F3: missing DB with retained lock/install evidence must not remint issuer."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "sidecar-guard"},
                source_kind="synthetic",
                source_key="EA010-SYNTHETIC-sidecar-acceptance",
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=99,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 99},
            )

        aside = self.dir / "journal.sqlite3.aside"
        self.path.rename(aside)
        lock = lock_path_for(self.path)
        install = install_path_for(self.path)
        self.assertTrue(lock.exists() or install.exists())
        self.assertFalse(self.path.exists())

        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                journal.ensure_issuer()
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertIn("established installation", str(ctx.exception).lower())
        self.assertFalse(self.path.exists())

        # Preserved aside DB still has the original receipt/issuer.
        conn = sqlite3.connect(str(aside))
        self.assertEqual(
            conn.execute("SELECT value FROM meta WHERE key='issuer_id'").fetchone()[0],
            issuer,
        )
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 1)
        conn.close()

    def _seed_confirmed_install(self) -> tuple[str, str, str]:
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "established-seed"},
                source_kind="synthetic",
                source_key="EA010-SYNTHETIC-established-seed",
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=101,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 101},
            )
            install = install_path_for(self.path)
            install_text = install.read_text(encoding="utf-8")
        return issuer, op, install_text

    def test_truncated_journal_with_sidecar_refuses_remint(self) -> None:
        """Empty/truncated DB with .install/.lock must stay writes-off (no remint)."""
        issuer, _op, install_text = self._seed_confirmed_install()
        lock = lock_path_for(self.path)
        install = install_path_for(self.path)
        self.assertTrue(lock.exists())
        self.assertTrue(install.exists())

        # Truncate only the disposable DB; keep established-install evidence.
        with open(self.path, "wb"):
            pass
        self.assertEqual(self.path.stat().st_size, 0)

        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                journal.ensure_issuer()
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertIn("established installation", str(ctx.exception).lower())

        self.assertTrue(lock.exists())
        self.assertTrue(install.exists())
        self.assertEqual(install.read_text(encoding="utf-8"), install_text)
        # Must not have bootstrapped a new schema/issuer over the truncated file.
        conn = sqlite3.connect(str(self.path))
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)
            tables = [
                str(r[0])
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            self.assertEqual(tables, [])
        finally:
            conn.close()
        # Original issuer identity is only recoverable from preserved evidence/aside;
        # open must not have minted a replacement under the truncated path.
        self.assertTrue(issuer)

    def test_empty_sqlite_replacement_with_sidecar_refuses_remint(self) -> None:
        """Replacing DB with a fresh empty SQLite file must not remint under sidecars."""
        issuer, _op, install_text = self._seed_confirmed_install()
        lock = lock_path_for(self.path)
        install = install_path_for(self.path)

        # Replace journal with a brand-new empty SQLite DB (user_version=0, no tables).
        self.path.unlink()
        empty = sqlite3.connect(str(self.path))
        empty.close()
        self.assertTrue(lock.exists())
        self.assertTrue(install.exists())

        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                journal.ensure_issuer()
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertIn("established installation", str(ctx.exception).lower())
        self.assertEqual(install.read_text(encoding="utf-8"), install_text)

        conn = sqlite3.connect(str(self.path))
        try:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)
            tables = [
                str(r[0])
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            self.assertEqual(tables, [])
        finally:
            conn.close()
        self.assertTrue(issuer)

    def test_concurrent_open_serializes_install_sidecar(self) -> None:
        """Two processes opening the same established journal must not crash on .install."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()

        gate = self.dir / "open-gate.txt"
        gate.write_text("wait", encoding="utf-8")
        script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
gate = Path(sys.argv[3])
while gate.read_text(encoding="utf-8").strip() != "go":
    time.sleep(0.01)
try:
    with IntegrationJournal(path) as journal:
        got = journal.ensure_issuer()
    print(json.dumps({"ok": True, "issuer": got}))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}))
"""
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        c1 = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), str(gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        c2 = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), str(gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            time.sleep(0.1)
            gate.write_text("go", encoding="utf-8")
            out1, err1 = c1.communicate(timeout=20)
            out2, err2 = c2.communicate(timeout=20)
            self.assertEqual(c1.returncode, 0, err1)
            self.assertEqual(c2.returncode, 0, err2)
            r1 = json.loads(out1.strip().splitlines()[-1])
            r2 = json.loads(out2.strip().splitlines()[-1])
            self.assertTrue(r1.get("ok"), r1)
            self.assertTrue(r2.get("ok"), r2)
            self.assertEqual(r1["issuer"], issuer)
            self.assertEqual(r2["issuer"], issuer)
            install = install_path_for(self.path)
            self.assertTrue(install.exists())
            self.assertTrue(install.read_text(encoding="utf-8").strip())
        finally:
            if c1.poll() is None:
                c1.kill()
            if c2.poll() is None:
                c2.kill()

    def test_foreign_db_replacement_refuses_sidecar_overwrite(self) -> None:
        """Valid nonempty foreign DB copy must not overwrite .install or allow dispatch."""
        other = self.dir / "other.sqlite3"
        with IntegrationJournal(self.path) as journal:
            issuer_a = journal.ensure_issuer()
            op_a = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer_a,
                operation_id=op_a,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-original-install"},
            )
        with IntegrationJournal(other) as journal:
            issuer_b = journal.ensure_issuer()
            op_b = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer_b,
                operation_id=op_b,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-foreign-install"},
            )
        self.assertNotEqual(issuer_a, issuer_b)
        original_sidecar = install_path_for(self.path).read_text(encoding="utf-8")
        foreign_sidecar = install_path_for(other).read_text(encoding="utf-8")
        self.assertNotEqual(original_sidecar.strip(), foreign_sidecar.strip())
        shutil.copy2(other, self.path)
        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                journal.can_dispatch(issuer_b, op_b)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(ctx.exception.details.get("installation_mismatch"))
        preserved = install_path_for(self.path).read_text(encoding="utf-8")
        self.assertEqual(preserved, original_sidecar)
        self.assertNotEqual(preserved.strip(), foreign_sidecar.strip())

    def test_uncommitted_receipt_not_visible_to_concurrent_readers(self) -> None:
        """Public get/list/source reads must not publish a receipt before commit succeeds."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            source_key = "EA010-SYNTHETIC-uncommitted-receipt"
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-uncommitted-receipt"},
                source_kind="meeting_proposal",
                source_key=source_key,
            )
            journal.begin_dispatch(issuer, op)
            entered = threading.Event()
            release = threading.Event()
            errors: list[str] = []
            real_commit = journal._store.commit
            first = [True]

            def failed_commit() -> None:
                if first[0]:
                    first[0] = False
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("unit helper synchronization timeout")
                    raise StorageError("full", "synthetic receipt commit failure")
                real_commit()

            journal._store.commit = failed_commit  # type: ignore[method-assign]

            def writer() -> None:
                try:
                    journal.record_receipt(
                        issuer,
                        op,
                        vikunja_id=71,
                        project_id=7,
                        marker=prepared.marker,
                        effect={"title": "EA010-SYNTHETIC-uncommitted-receipt"},
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(type(exc).__name__)

            reader_results: dict[str, object] = {}

            def reader() -> None:
                # Blocks on the mutation lock until the failed commit path finishes.
                reader_results["get"] = journal.get_operation(issuer, op)
                reader_results["list"] = journal.list_operations()
                reader_results["source"] = journal.lookup_source(
                    "meeting_proposal", source_key
                )

            writer_thread = threading.Thread(target=writer)
            reader_thread = threading.Thread(target=reader)
            writer_thread.start()
            try:
                self.assertTrue(entered.wait(5), "writer did not reach delayed commit")
                reader_thread.start()
                # Reader must remain blocked while uncommitted receipt rows exist.
                time.sleep(0.05)
                self.assertTrue(reader_thread.is_alive(), "reader must wait for mutator")
            finally:
                release.set()
                writer_thread.join(5)
                reader_thread.join(5)
            self.assertFalse(writer_thread.is_alive())
            self.assertFalse(reader_thread.is_alive())
            self.assertIn("JournalError", errors)
            seen = reader_results.get("get")
            listed = reader_results.get("list")
            sourced = reader_results.get("source")
            self.assertIsNotNone(seen)
            assert seen is not None
            self.assertEqual(seen.state, OperationState.UNKNOWN)  # type: ignore[union-attr]
            self.assertIsNone(seen.receipt)  # type: ignore[union-attr]
            self.assertIsInstance(listed, list)
            assert isinstance(listed, list)
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0].state, OperationState.UNKNOWN)
            self.assertIsNone(listed[0].receipt)
            self.assertIsNotNone(sourced)
            assert sourced is not None
            self.assertEqual(sourced.state, OperationState.UNKNOWN)  # type: ignore[union-attr]
            self.assertIsNone(sourced.receipt)  # type: ignore[union-attr]
            final = journal.get_operation(issuer, op)
            assert final is not None
            self.assertEqual(final.state, OperationState.UNKNOWN)
            self.assertIsNone(final.receipt)


    def test_same_install_stale_db_copy_refuses_redispatch(self) -> None:
        """R1: old SQLite snapshot + live .install must not re-authorize first dispatch."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-stale-copy"},
            )
            self.assertTrue(prepared.allows_first_http_dispatch())
        prepared_snapshot = self.dir / "prepared-snapshot.sqlite3"
        shutil.copy2(self.path, prepared_snapshot)
        sidecar_before = install_path_for(self.path).read_text(encoding="utf-8")

        with IntegrationJournal(self.path) as journal:
            self.assertTrue(journal.can_dispatch(issuer, op))
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=901,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 901},
            )
            history_events = [
                row["event_type"] for row in journal.history(issuer, op)
            ]
            self.assertIn("dispatch_started", history_events)
            self.assertIn("receipt", history_events)
        sidecar_after = install_path_for(self.path).read_text(encoding="utf-8")
        self.assertNotEqual(sidecar_before, sidecar_after)

        # Restore older same-install DB while retaining the advanced .install fence.
        shutil.copy2(prepared_snapshot, self.path)
        self.assertEqual(
            install_path_for(self.path).read_text(encoding="utf-8"),
            sidecar_after,
        )
        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                self.assertTrue(journal.can_dispatch(issuer, op))
                journal.begin_dispatch(issuer, op)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(ctx.exception.details.get("freshness_mismatch"))
        self.assertTrue(ctx.exception.details.get("stale_copy"))
        preserved = install_path_for(self.path).read_text(encoding="utf-8")
        self.assertEqual(preserved, sidecar_after)

        conn = sqlite3.connect(str(self.path))
        try:
            dispatch_n = conn.execute(
                "SELECT COUNT(*) FROM history WHERE event_type = 'dispatch_started' "
                "AND operation_id = ?",
                (op,),
            ).fetchone()[0]
            # Stale snapshot never contained dispatch_started; reopen must not add one.
            self.assertEqual(dispatch_n, 0)
        finally:
            conn.close()

    def test_live_connection_stale_backup_refuses_redispatch(self) -> None:
        """R1: in-place SQLite backup into a kept-open journal must not re-arm dispatch."""
        journal = IntegrationJournal(self.path)
        journal.open()
        try:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-live-backup"},
            )
            prepared_snapshot = self.dir / "live-prepared-snapshot.sqlite3"
            shutil.copy2(self.path, prepared_snapshot)
            sidecar_before = install_path_for(self.path).read_text(encoding="utf-8")
            self.assertTrue(journal.can_dispatch(issuer, op))
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=902,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 902},
            )
            sidecar_after = install_path_for(self.path).read_text(encoding="utf-8")
            self.assertNotEqual(sidecar_before, sidecar_after)

            # Restore prepared snapshot into the same live SQLite connection/inode
            # while retaining the advanced outside-SQLite .install fence.
            src = sqlite3.connect(str(prepared_snapshot))
            try:
                src.backup(journal._store.conn)
            finally:
                src.close()
            self.assertEqual(
                install_path_for(self.path).read_text(encoding="utf-8"),
                sidecar_after,
            )

            self.assertFalse(journal.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertTrue(ctx.exception.details.get("freshness_mismatch"))
            self.assertTrue(ctx.exception.details.get("stale_copy"))
            self.assertEqual(
                install_path_for(self.path).read_text(encoding="utf-8"),
                sidecar_after,
            )
            # Restored snapshot had no dispatch_started; live begin must not add one.
            dispatch_n = journal._store.conn.execute(
                "SELECT COUNT(*) FROM history WHERE event_type = 'dispatch_started' "
                "AND operation_id = ?",
                (op,),
            ).fetchone()[0]
            self.assertEqual(dispatch_n, 0)
        finally:
            journal.close()
        self.assertEqual(
            install_path_for(self.path).read_text(encoding="utf-8"),
            sidecar_after,
        )

    def test_live_connection_missing_sidecar_refuses_writes(self) -> None:
        """R1: deleting .install after open must not let mutations recreate it."""
        journal = IntegrationJournal(self.path)
        journal.open()
        try:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-missing-sidecar"},
            )
            sidecar = install_path_for(self.path)
            self.assertTrue(sidecar.is_file())
            sidecar.unlink()
            self.assertFalse(sidecar.exists())

            with self.assertRaises(JournalError) as ctx:
                journal.begin_dispatch(issuer, op)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertFalse(sidecar.exists())

            with self.assertRaises(JournalError):
                journal.ensure_issuer()
            self.assertFalse(sidecar.exists())

            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "EA010-SYNTHETIC-missing-sidecar-2"},
                )
            self.assertFalse(sidecar.exists())
            self.assertFalse(journal.can_dispatch(issuer, op))
        finally:
            journal.close()
        self.assertFalse(install_path_for(self.path).exists())

    def test_history_only_residue_quarantines_before_new_source_key(self) -> None:
        """R2: history without parent operations must quarantine; no second dispatch."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            source_key = "EA010-SYNTHETIC-history-only"
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-history-only"},
                source_kind="synthetic",
                source_key=source_key,
            )
            journal.begin_dispatch(issuer, op)

        conn = sqlite3.connect(str(self.path))
        try:
            conn.execute("DELETE FROM source_mappings")
            conn.execute("DELETE FROM operations")
            conn.commit()
            fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
            history_ops = conn.execute(
                "SELECT COUNT(*) FROM history WHERE operation_id != ''"
            ).fetchone()[0]
            self.assertEqual(fk_rows, [])
            self.assertGreaterEqual(history_ops, 1)
        finally:
            conn.close()

        with IntegrationJournal(self.path) as journal:
            with self.assertRaises(JournalError) as ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "EA010-SYNTHETIC-history-only-again"},
                    source_kind="synthetic",
                    source_key=source_key,
                )
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertFalse(journal.can_dispatch(issuer, op))
            quarantine_events = [
                row
                for row in journal.history()
                if row["event_type"] == "quarantine"
                and row["detail"].get("reason") == "history_without_operation"
            ]
            self.assertTrue(quarantine_events)
            self.assertTrue(
                quarantine_events[0]["detail"].get("history_operation_orphans")
            )

        conn = sqlite3.connect(str(self.path))
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 0)
            self.assertEqual(
                conn.execute("SELECT value FROM meta WHERE key = 'quarantine'").fetchone()[0],
                "1",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT value FROM meta WHERE key = 'writes_enabled'"
                ).fetchone()[0],
                "0",
            )
            preserved = conn.execute(
                "SELECT COUNT(*) FROM history WHERE operation_id = ?",
                (op,),
            ).fetchone()[0]
            self.assertGreaterEqual(preserved, 1)
        finally:
            conn.close()

    def test_prepare_intent_rejects_nul_and_surrogate_context(self) -> None:
        """R3: invalid confirmed context must not create a durable intent."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            nul_op = new_operation_id()
            with self.assertRaises(JournalError) as nul_ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=nul_op,
                    op_type="create",
                    project_id=3,
                    payload={
                        "title": "synthetic title",
                        "description_user": "before\u0000after",
                    },
                )
            self.assertEqual(nul_ctx.exception.outcome, Outcome.VALIDATION_ERROR)
            self.assertIn("NUL", str(nul_ctx.exception))
            self.assertIsNone(journal.get_operation(issuer, nul_op))
            self.assertFalse(journal.can_dispatch(issuer, nul_op))

            surrogate_op = new_operation_id()
            with self.assertRaises(JournalError) as sur_ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=surrogate_op,
                    op_type="create",
                    project_id=3,
                    payload={
                        "title": "synthetic title",
                        "description_user": "before\ud800after",
                    },
                )
            self.assertEqual(sur_ctx.exception.outcome, Outcome.VALIDATION_ERROR)
            self.assertIn("surrogate", str(sur_ctx.exception).lower())
            self.assertIsNone(journal.get_operation(issuer, surrogate_op))
            self.assertFalse(journal.can_dispatch(issuer, surrogate_op))

        conn = sqlite3.connect(str(self.path))
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 0)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM history WHERE event_type = 'intent_prepared'"
                ).fetchone()[0],
                0,
            )
        finally:
            conn.close()

    def test_orphan_receipt_partial_copy_quarantines_writes(self) -> None:
        """R2: FK-off delete of parent operation must not allow duplicate source create."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            source_key = "EA010-SYNTHETIC-orphan-receipt"
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-orphan-receipt"},
                source_kind="synthetic",
                source_key=source_key,
            )
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=778,
                project_id=3,
                marker=prepared.marker,
                effect={"id": 778},
            )

        # External connection defaults to FK checks off — simulate partial copy.
        conn = sqlite3.connect(str(self.path))
        try:
            conn.execute("DELETE FROM source_mappings")
            conn.execute("DELETE FROM operations")
            conn.commit()
            fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
            receipt_n = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
            history_n = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
            self.assertTrue(fk_rows)
            self.assertEqual(receipt_n, 1)
            self.assertGreaterEqual(history_n, 1)
            self.assertEqual(
                conn.execute(
                    "SELECT vikunja_id FROM receipts WHERE operation_id = ?",
                    (op,),
                ).fetchone()[0],
                778,
            )
        finally:
            conn.close()

        with IntegrationJournal(self.path) as journal:
            with self.assertRaises(JournalError) as ctx:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=new_operation_id(),
                    op_type="create",
                    project_id=3,
                    payload={"title": "EA010-SYNTHETIC-orphan-receipt-again"},
                    source_kind="synthetic",
                    source_key=source_key,
                )
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            self.assertFalse(journal.can_dispatch(issuer, op))
            self.assertIsNone(journal.get_operation(issuer, op))
            quarantine_events = [
                row
                for row in journal.history()
                if row["event_type"] == "quarantine"
                and row["detail"].get("reason") == "foreign_key_check"
            ]
            self.assertTrue(quarantine_events)

        conn = sqlite3.connect(str(self.path))
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0], 0)
            self.assertEqual(
                conn.execute("SELECT value FROM meta WHERE key = 'quarantine'").fetchone()[0],
                "1",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT value FROM meta WHERE key = 'writes_enabled'"
                ).fetchone()[0],
                "0",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT vikunja_id FROM receipts WHERE operation_id = ?",
                    (op,),
                ).fetchone()[0],
                778,
            )
        finally:
            conn.close()

    def test_dispatch_critical_section_retains_os_lock_and_releases_on_exception(self) -> None:
        """R1: supported dispatch section holds OS-lock through receipt; exception releases."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="update",
                project_id=7,
                target_id=71,
                payload={"title": "EA010-SYNTHETIC-crit-section"},
            )
            self.assertFalse(journal.os_lock_held)
            with journal.dispatch_critical_section() as section:
                self.assertTrue(section.os_lock_held)
                self.assertTrue(journal.os_lock_held)
                started = section.begin_dispatch(issuer, op)
                self.assertEqual(started.state, OperationState.DISPATCH_STARTED)
                self.assertTrue(section.os_lock_held)
                confirmed = section.record_receipt(
                    issuer,
                    op,
                    vikunja_id=71,
                    project_id=7,
                    marker=prepared.marker,
                    effect={"title": "EA010-SYNTHETIC-crit-section"},
                )
                self.assertEqual(confirmed.state, OperationState.CONFIRMED)
                self.assertTrue(section.os_lock_held)
            self.assertFalse(journal.os_lock_held)

            op2 = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op2,
                op_type="update",
                project_id=7,
                target_id=71,
                payload={"title": "EA010-SYNTHETIC-crit-exc"},
            )
            with self.assertRaises(RuntimeError):
                with journal.dispatch_critical_section() as section:
                    section.begin_dispatch(issuer, op2)
                    self.assertTrue(section.os_lock_held)
                    raise RuntimeError("synthetic-section-abort")
            self.assertFalse(journal.os_lock_held)

    def test_two_processes_distinct_keys_serialized_under_dispatch_section(self) -> None:
        """R1: held offline dispatch interval blocks a distinct-key peer begin_dispatch."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op_a = new_operation_id()
            op_b = new_operation_id()
            for op in (op_a, op_b):
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=op,
                    op_type="update",
                    project_id=7,
                    target_id=71,
                    payload={"title": f"EA010-SYNTHETIC-{op}"},
                )

        ready = self.dir / "crit-ready.json"
        release = self.dir / "crit-release.txt"
        release.write_text("hold", encoding="utf-8")
        holder_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
ready = Path(sys.argv[5])
release = Path(sys.argv[6])
j = IntegrationJournal(path)
j.open()
try:
    with j.dispatch_critical_section() as section:
        started = section.begin_dispatch(issuer, op)
        ready.write_text(
            json.dumps({
                "pid": os.getpid(),
                "state": started.state.value,
                "lock_held": section.os_lock_held,
            }),
            encoding="utf-8",
        )
        while release.read_text(encoding="utf-8").strip() != "go":
            time.sleep(0.02)
finally:
    j.close()
print(json.dumps({"ok": True}))
"""
        peer_script = r"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
started_at = time.monotonic()
j = IntegrationJournal(path)
j.open()
try:
    started = j.begin_dispatch(issuer, op)
    print(json.dumps({
        "ok": True,
        "state": started.state.value,
        "waited_s": time.monotonic() - started_at,
    }))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}))
finally:
    j.close()
"""
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                holder_script,
                str(ROOT),
                str(self.path),
                issuer,
                op_a,
                str(ready),
                str(release),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            deadline = time.time() + 10
            while time.time() < deadline:
                if ready.is_file() and ready.stat().st_size:
                    break
                if holder.poll() is not None:
                    out, err = holder.communicate(timeout=1)
                    self.fail(f"holder exited early rc={holder.returncode} out={out!r} err={err!r}")
                time.sleep(0.02)
            else:
                self.fail("holder ready not written")
            holder_ready = json.loads(ready.read_text(encoding="utf-8"))
            self.assertTrue(holder_ready["lock_held"])
            self.assertEqual(holder_ready["state"], OperationState.DISPATCH_STARTED.value)

            peer = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    peer_script,
                    str(ROOT),
                    str(self.path),
                    issuer,
                    op_b,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            try:
                time.sleep(0.4)
                self.assertIsNone(peer.poll(), "peer must block while holder keeps dispatch section")
                release.write_text("go", encoding="utf-8")
                peer_out, peer_err = peer.communicate(timeout=20)
                holder_out, holder_err = holder.communicate(timeout=20)
                self.assertEqual(holder.returncode, 0, holder_err)
                self.assertEqual(peer.returncode, 0, peer_err)
                peer_result = json.loads(peer_out.strip().splitlines()[-1])
                self.assertTrue(peer_result["ok"], peer_result)
                self.assertEqual(peer_result["state"], OperationState.DISPATCH_STARTED.value)
                self.assertGreaterEqual(peer_result["waited_s"], 0.3)
            finally:
                if peer.poll() is None:
                    peer.kill()
                    peer.communicate(timeout=5)
        finally:
            release.write_text("go", encoding="utf-8")
            if holder.poll() is None:
                holder.kill()
                holder.communicate(timeout=5)

    def test_dispatch_section_sigkill_releases_os_lock_for_peer(self) -> None:
        """R1: crash while holding the dispatch section releases the OS-lock for peers."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op_a = new_operation_id()
            op_b = new_operation_id()
            for op in (op_a, op_b):
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=op,
                    op_type="update",
                    project_id=7,
                    target_id=71,
                    payload={"title": f"EA010-SYNTHETIC-kill-{op}"},
                )

        ready = self.dir / "kill-section-ready.json"
        script = r"""
import json, os, signal, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
ready = Path(sys.argv[5])
j = IntegrationJournal(path)
j.open()
with j.dispatch_critical_section() as section:
    started = section.begin_dispatch(issuer, op)
    ready.write_text(
        json.dumps({"pid": os.getpid(), "state": started.state.value, "lock_held": section.os_lock_held}),
        encoding="utf-8",
    )
    time.sleep(0.2)
    os.kill(os.getpid(), signal.SIGKILL)
"""
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        proc = subprocess.Popen(
            [sys.executable, "-c", script, str(ROOT), str(self.path), issuer, op_a, str(ready)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            deadline = time.time() + 10
            while time.time() < deadline:
                if ready.is_file() and ready.stat().st_size:
                    break
                if proc.poll() is not None:
                    self.fail(f"child exited before ready rc={proc.returncode}")
                time.sleep(0.02)
            else:
                self.fail("kill-section ready not written")
            proc.wait(timeout=10)
            self.assertEqual(proc.returncode, -signal.SIGKILL)
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()
        finally:
            if proc.poll() is None:
                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()

        with IntegrationJournal(self.path) as journal:
            started_b = journal.begin_dispatch(issuer, op_b)
            self.assertEqual(started_b.state, OperationState.DISPATCH_STARTED)
            loaded_a = journal.get_operation(issuer, op_a)
            self.assertIsNotNone(loaded_a)
            assert loaded_a is not None
            self.assertEqual(loaded_a.state, OperationState.DISPATCH_STARTED)
            self.assertFalse(journal.can_dispatch(issuer, op_a))

    def test_full_db_and_install_rollback_refuses_redispatch_without_admit(self) -> None:
        """R2: restoring DB+.install together must not re-arm an already delivered create."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-full-restore"},
            )
        snapshot_db = self.path.read_bytes()
        snapshot_install = install_path_for(self.path).read_bytes()
        authority_before = authority_path_for(self.path).read_text(encoding="utf-8")

        with IntegrationJournal(self.path) as journal:
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=777,
                project_id=7,
                marker=prepared.marker,
                effect={"id": 777},
            )
        authority_after = authority_path_for(self.path).read_text(encoding="utf-8")
        self.assertNotEqual(authority_before, authority_after)

        self.path.write_bytes(snapshot_db)
        install_path_for(self.path).write_bytes(snapshot_install)
        # Live authority fence is intentionally not restored with the backup pair.
        self.assertEqual(
            authority_path_for(self.path).read_text(encoding="utf-8"),
            authority_after,
        )

        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                self.assertTrue(journal.can_dispatch(issuer, op))
                journal.begin_dispatch(issuer, op)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(
            ctx.exception.details.get("authority_mismatch")
            or ctx.exception.details.get("full_backup_rollback")
        )

        # Explicit admission rotates a new authority incarnation under writes-off;
        # dispatch stays refused.
        live_gen_before_admit = int(
            authority_after.strip().splitlines()[1]
        )
        restored_gen = int(snapshot_install.decode("utf-8").strip().splitlines()[1])
        admitted = IntegrationJournal(self.path)
        admitted.admit_restored_backup()
        try:
            self.assertFalse(admitted.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as admit_ctx:
                admitted.begin_dispatch(issuer, op)
            self.assertEqual(admit_ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            drift = admitted._get_meta("restore_drift")
            writes = admitted._get_meta("writes_enabled")
            epoch = admitted._get_meta("journal_epoch")
            self.assertEqual(drift, "1")
            self.assertEqual(writes, "0")
            admit_gen = int(
                authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]
            )
            self.assertEqual(int(epoch), admit_gen)
            self.assertGreater(admit_gen, live_gen_before_admit)
            self.assertGreater(admit_gen, restored_gen)
            self.assertEqual(
                int(install_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]),
                admit_gen,
            )
        finally:
            admitted.close()

        with IntegrationJournal(self.path) as journal:
            self.assertFalse(journal.can_dispatch(issuer, op))
            with self.assertRaises(JournalError):
                journal.begin_dispatch(issuer, op)

    def test_multi_restore_never_rewinds_authority_to_reuse_prepared_snapshot(self) -> None:
        """P1: admit must keep authority monotonic so backup B cannot re-arm create.

        Sequence: backup A after issuer, backup B after prepared create, dispatch
        that create, admit A + clear drift, then restore B. Ordinary open of B
        must still require admission; a formerly used generation must not re-admit
        the old prepared snapshot for a second begin_dispatch.
        """
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
        backup_a_db = self.path.read_bytes()
        backup_a_install = install_path_for(self.path).read_bytes()
        gen_a = int(backup_a_install.decode("utf-8").strip().splitlines()[1])

        with IntegrationJournal(self.path) as journal:
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-multi-restore"},
            )
        backup_b_db = self.path.read_bytes()
        backup_b_install = install_path_for(self.path).read_bytes()
        gen_b = int(backup_b_install.decode("utf-8").strip().splitlines()[1])
        self.assertGreater(gen_b, gen_a)

        with IntegrationJournal(self.path) as journal:
            journal.begin_dispatch(issuer, op)
            journal.record_receipt(
                issuer,
                op,
                vikunja_id=888,
                project_id=7,
                marker=prepared.marker,
                effect={"id": 888},
            )
        gen_after_dispatch = int(
            authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        self.assertGreater(gen_after_dispatch, gen_b)

        # Restore A (issuer-only) and explicitly admit.
        self.path.write_bytes(backup_a_db)
        install_path_for(self.path).write_bytes(backup_a_install)
        admitted_a = IntegrationJournal(self.path)
        admitted_a.admit_restored_backup()
        try:
            admit_a_gen = int(
                authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]
            )
            self.assertGreater(admit_a_gen, gen_after_dispatch)
            self.assertGreater(admit_a_gen, gen_a)
            self.assertEqual(admitted_a._get_meta("restore_drift"), "1")
            self.assertEqual(admitted_a._get_meta("writes_enabled"), "0")
            # Issuer survives; the later create key is absent in A (no re-arm).
            self.assertEqual(admitted_a.get_issuer(), issuer)
            self.assertIsNone(admitted_a.get_operation(issuer, op))
        finally:
            admitted_a.close()

        # Clear drift to resume after reconciliation; mutations advance from the
        # new incarnation, not from the restored epoch.
        with IntegrationJournal(self.path) as journal:
            journal.set_restore_drift(False)
            resume_op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=resume_op,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-multi-restore-resume"},
            )
        gen_after_resume = int(
            authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        self.assertGreater(gen_after_resume, admit_a_gen)

        # Restoring B must not pass ordinary open: authority stayed ahead of gen_b.
        self.path.write_bytes(backup_b_db)
        install_path_for(self.path).write_bytes(backup_b_install)
        self.assertEqual(
            authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1],
            str(gen_after_resume),
        )
        with self.assertRaises(JournalError) as ctx:
            with IntegrationJournal(self.path) as journal:
                self.assertTrue(journal.can_dispatch(issuer, op))
                journal.begin_dispatch(issuer, op)
        self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(
            ctx.exception.details.get("authority_mismatch")
            or ctx.exception.details.get("full_backup_rollback")
        )

        # Explicit admit of B rotates again; writes-off still blocks redispatch of
        # the prepared create that already reached Vikunja in the live lineage.
        admitted_b = IntegrationJournal(self.path)
        admitted_b.admit_restored_backup()
        try:
            admit_b_gen = int(
                authority_path_for(self.path).read_text(encoding="utf-8").strip().splitlines()[1]
            )
            self.assertGreater(admit_b_gen, gen_after_resume)
            self.assertGreater(admit_b_gen, gen_b)
            self.assertNotEqual(admit_b_gen, gen_a)
            self.assertNotEqual(admit_b_gen, gen_b)
            self.assertNotEqual(admit_b_gen, gen_after_dispatch)
            self.assertEqual(admitted_b._get_meta("restore_drift"), "1")
            self.assertEqual(admitted_b._get_meta("writes_enabled"), "0")
            self.assertEqual(admitted_b.get_issuer(), issuer)
            loaded = admitted_b.get_operation(issuer, op)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.state, OperationState.PREPARED)
            self.assertFalse(admitted_b.can_dispatch(issuer, op))
            with self.assertRaises(JournalError) as admit_ctx:
                admitted_b.begin_dispatch(issuer, op)
            self.assertEqual(admit_ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)
            # History of admission is retained on the restored set.
            events = [row["event_type"] for row in admitted_b.history()]
            self.assertIn("restored_backup_admitted", events)
        finally:
            admitted_b.close()

        # Former generations still cannot open without another admission.
        self.path.write_bytes(backup_b_db)
        install_path_for(self.path).write_bytes(backup_b_install)
        with self.assertRaises(JournalError) as again:
            with IntegrationJournal(self.path):
                pass
        self.assertEqual(again.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)


    def test_create_update_complete_retains_verified_marker(self) -> None:
        """F1: update/complete keep create M; distinct op keys do not mint a new header."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            create_op = new_operation_id()
            update_op = new_operation_id()
            complete_op = new_operation_id()
            created = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=create_op,
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-marker-chain"},
            )
            create_marker = created.marker
            self.assertEqual(create_marker, build_marker(issuer, create_op))
            journal.begin_dispatch(issuer, create_op)
            journal.record_receipt(
                issuer,
                create_op,
                vikunja_id=4242,
                project_id=7,
                marker=create_marker,
                effect={"id": 4242, "description": create_marker + "\n"},
            )

            updated = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=update_op,
                op_type="update",
                project_id=7,
                target_id=4242,
                payload={"title": "EA010-SYNTHETIC-marker-updated"},
            )
            self.assertEqual(updated.marker, create_marker)
            self.assertNotEqual(updated.marker, build_marker(issuer, update_op))
            with journal.dispatch_critical_section() as section:
                section.begin_dispatch(issuer, update_op)
                update_receipt = section.record_receipt(
                    issuer,
                    update_op,
                    vikunja_id=4242,
                    project_id=7,
                    marker=create_marker,
                    effect={
                        "id": 4242,
                        "title": "EA010-SYNTHETIC-marker-updated",
                        "description": create_marker + "\n",
                    },
                )
            self.assertEqual(update_receipt.receipt["marker"], create_marker)

            completed = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=complete_op,
                op_type="complete",
                project_id=7,
                target_id=4242,
                payload={"done": True},
            )
            self.assertEqual(completed.marker, create_marker)
            with journal.dispatch_critical_section() as section:
                section.begin_dispatch(issuer, complete_op)
                complete_receipt = section.record_receipt(
                    issuer,
                    complete_op,
                    vikunja_id=4242,
                    project_id=7,
                    marker=create_marker,
                    effect={"id": 4242, "done": True, "description": create_marker + "\n"},
                )
            self.assertEqual(complete_receipt.receipt["marker"], create_marker)

    def test_unmarked_manual_complete_accepts_empty_observation_marker(self) -> None:
        """F1: manual tasks without M prepare empty marker and accept None receipt M."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op,
                op_type="complete",
                project_id=7,
                target_id=4243,
                payload={"done": True},
            )
            self.assertEqual(prepared.marker, "")
            self.assertNotEqual(prepared.marker, build_marker(issuer, op))
            with journal.dispatch_critical_section() as section:
                section.begin_dispatch(issuer, op)
                receipt = section.record_receipt(
                    issuer,
                    op,
                    vikunja_id=4243,
                    project_id=7,
                    marker=None,
                    effect={"id": 4243, "done": True, "description": "manual synthetic task"},
                )
            self.assertEqual(receipt.receipt["marker"], "")

    def test_close_waits_for_non_owner_critical_section(self) -> None:
        """F2: close must not release OS-lock while another thread is in the section."""
        with IntegrationJournal(self.path) as journal:
            issuer = journal.ensure_issuer()
            op_a = new_operation_id()
            op_b = new_operation_id()
            for op in (op_a, op_b):
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=op,
                    op_type="update",
                    project_id=7,
                    target_id=71,
                    payload={"title": f"EA010-SYNTHETIC-close-{op}"},
                )

        j = IntegrationJournal(self.path)
        j.open()
        holder_ready = self.dir / "close-section-ready.json"
        release_flag = self.dir / "close-section-release.txt"
        release_flag.write_text("hold", encoding="utf-8")
        section_done = threading.Event()
        close_started = threading.Event()
        close_finished = threading.Event()
        errors: list[BaseException] = []
        close_result: dict[str, Any] = {}

        def section_thread() -> None:
            try:
                with j.dispatch_critical_section() as section:
                    section.begin_dispatch(issuer, op_a)
                    holder_ready.write_text(
                        json.dumps({"lock_held": section.os_lock_held, "pid": os.getpid()}),
                        encoding="utf-8",
                    )
                    while release_flag.read_text(encoding="utf-8").strip() != "go":
                        time.sleep(0.02)
                section_done.set()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=section_thread, name="ea010-section-holder")
        t.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if holder_ready.is_file() and holder_ready.stat().st_size:
                break
            time.sleep(0.02)
        else:
            release_flag.write_text("go", encoding="utf-8")
            t.join(timeout=5)
            self.fail("section holder did not become ready")

        peer_script = r"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[2])
issuer = sys.argv[3]
op = sys.argv[4]
started_at = time.monotonic()
j = IntegrationJournal(path)
j.open()
try:
    started = j.begin_dispatch(issuer, op)
    print(json.dumps({
        "ok": True,
        "state": started.state.value,
        "waited_s": time.monotonic() - started_at,
    }))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}))
finally:
    j.close()
"""
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        peer = subprocess.Popen(
            [
                sys.executable,
                "-c",
                peer_script,
                str(ROOT),
                str(self.path),
                issuer,
                op_b,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        try:
            time.sleep(0.3)
            self.assertIsNone(peer.poll(), "peer must block while section holds OS-lock")

            def close_thread() -> None:
                try:
                    close_started.set()
                    j.close()
                    close_result["ok"] = True
                except BaseException as exc:  # noqa: BLE001
                    close_result["ok"] = False
                    close_result["error"] = str(exc)
                    errors.append(exc)
                finally:
                    close_finished.set()

            ct = threading.Thread(target=close_thread, name="ea010-close-waiter")
            ct.start()
            self.assertTrue(close_started.wait(timeout=5))
            time.sleep(0.3)
            self.assertFalse(
                close_finished.is_set(),
                "close must wait for the active non-owner critical section",
            )
            self.assertIsNone(peer.poll(), "peer must still block through close wait")

            release_flag.write_text("go", encoding="utf-8")
            t.join(timeout=10)
            ct.join(timeout=10)
            self.assertTrue(section_done.wait(timeout=1))
            self.assertTrue(close_finished.wait(timeout=1))
            self.assertTrue(close_result.get("ok"), close_result)

            peer_out, peer_err = peer.communicate(timeout=20)
            self.assertEqual(peer.returncode, 0, peer_err)
            peer_result = json.loads(peer_out.strip().splitlines()[-1])
            self.assertTrue(peer_result["ok"], peer_result)
            self.assertGreaterEqual(peer_result["waited_s"], 0.3)
        finally:
            release_flag.write_text("go", encoding="utf-8")
            if peer.poll() is None:
                peer.kill()
                peer.communicate(timeout=5)
            if t.is_alive():
                t.join(timeout=5)
        self.assertEqual(errors, [])

    def test_fork_child_must_not_reenter_inherited_lock(self) -> None:
        """F3: after os.fork(), child must obtain a real OS-lock, not reenter via tid."""
        lock_path = self.dir / "fork.lock"
        parent_lock = JournalLock(lock_path)
        parent_lock.acquire()
        self.assertTrue(parent_lock.held)
        self.assertEqual(parent_lock.owner_pid, os.getpid())

        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            # Child: inherited state must not grant immediate ownership.
            os.close(read_fd)
            try:
                child_lock = parent_lock
                before = {
                    "held": child_lock.held,
                    "owner_tid": child_lock.owner_tid,
                    "owner_pid": child_lock.owner_pid,
                    "child_pid": os.getpid(),
                }
                # Parent still holds; child must not own. Use LOCK_NB on a raw fd
                # to prove serialization without hanging the child forever.
                fd = os.open(str(lock_path), os.O_RDWR)
                try:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        blocked = False
                    except BlockingIOError:
                        blocked = True
                finally:
                    os.close(fd)
                # Also ensure JournalLock.acquire does not think it already owns.
                owned = child_lock._owns_current()
                payload = json.dumps(
                    {
                        "before": before,
                        "owned_after_fork": owned,
                        "peer_nb_blocked": blocked,
                        "ok": (not owned) and blocked,
                    }
                ).encode("utf-8")
                os.write(write_fd, payload)
                os.close(write_fd)
                os._exit(0 if (not owned) and blocked else 1)
            except BaseException:  # noqa: BLE001
                try:
                    os.write(write_fd, b'{"ok": false}')
                    os.close(write_fd)
                except OSError:
                    pass
                os._exit(2)

        os.close(write_fd)
        try:
            deadline = time.time() + 10
            chunks: list[bytes] = []
            while time.time() < deadline:
                try:
                    piece = os.read(read_fd, 4096)
                except InterruptedError:
                    continue
                if not piece:
                    break
                chunks.append(piece)
                break
            _, status = os.waitpid(pid, 0)
            raw = b"".join(chunks).decode("utf-8")
            self.assertTrue(raw, "child produced no payload")
            result = json.loads(raw)
            self.assertTrue(result.get("ok"), result)
            self.assertTrue(result.get("peer_nb_blocked"))
            self.assertFalse(result.get("owned_after_fork"))
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        finally:
            os.close(read_fd)
            parent_lock.release()


class JournalSuiteRegistrationTests(unittest.TestCase):
    def test_suite_registered(self) -> None:
        from tests.support.suites import list_suites

        self.assertIn("journal", list_suites())


if __name__ == "__main__":
    unittest.main()
