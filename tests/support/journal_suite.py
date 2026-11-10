"""Isolated journal suite for s03: durable intent, OS-lock serialization, fail-closed.

No network. Uses a unique directory under the approved stand run_root.
"""

from __future__ import annotations

import hashlib
import json
import os
import resource
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from vikunja_local.api import Outcome, build_marker
from vikunja_local.journal import (
    DeliveryState,
    IntegrationJournal,
    JournalError,
    OperationState,
    new_operation_id,
)
from vikunja_local.storage import (
    JOURNAL_SQLITE_USER_VERSION,
    StorageError,
    authority_path_for,
    install_path_for,
    lock_path_for,
)

from .stand import StandError, load_profile, sha256_file

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

ROOT = Path(__file__).resolve().parents[2]


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _work_dir(profile) -> Path:
    run_root = Path(profile.run_root)
    run_root.mkdir(mode=0o700, exist_ok=True)
    os.chmod(run_root, 0o700)
    path = Path(
        tempfile.mkdtemp(
            prefix=f"ea010-journal-{uuid.uuid4().hex[:12]}-",
            dir=str(run_root),
        )
    )
    os.chmod(path, 0o700)
    # Containment: must stay under run_root and outside the git worktree.
    resolved = path.resolve()
    _assert(run_root.resolve() in resolved.parents or resolved == run_root.resolve(), "journal suite dir escaped run_root")
    _assert(ROOT.resolve() not in resolved.parents and resolved != ROOT.resolve(), "journal suite dir must be outside git worktree")
    return resolved


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "vikunja_local" / "journal.py",
        worktree / "vikunja_local" / "storage.py",
        worktree / "tests" / "test_journal.py",
        worktree / "tests" / "support" / "journal_suite.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(sha256_file(path).encode())
        digest.update(b"\0")
    return {
        "sha256": digest.hexdigest(),
        "head": subprocess.check_output(["git", "-C", str(worktree), "rev-parse", "HEAD"], text=True).strip(),
    }


def _child_script() -> str:
    return r"""
import os, sys, json, signal, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
from vikunja_local.journal import IntegrationJournal

mode = sys.argv[1]
journal_path = Path(sys.argv[2])
ready = Path(sys.argv[3])
payload = json.loads(sys.argv[4])

j = IntegrationJournal(journal_path)
j.open()
issuer = j.ensure_issuer(payload.get("issuer_id"))
if mode == "prepare_and_wait":
    op = j.prepare_intent(
        issuer_id=issuer,
        operation_id=payload["operation_id"],
        op_type=payload["op_type"],
        project_id=payload["project_id"],
        payload=payload["payload"],
        source_kind=payload.get("source_kind"),
        source_key=payload.get("source_key"),
    )
    ready.write_text(json.dumps({"pid": os.getpid(), "state": op.state.value}), encoding="utf-8")
    while True:
        time.sleep(0.05)
elif mode == "dispatch_then_die":
    op = j.prepare_intent(
        issuer_id=issuer,
        operation_id=payload["operation_id"],
        op_type=payload["op_type"],
        project_id=payload["project_id"],
        payload=payload["payload"],
    )
    started = j.begin_dispatch(issuer, payload["operation_id"])
    ready.write_text(
        json.dumps({"pid": os.getpid(), "state": started.state.value, "marker": started.marker}),
        encoding="utf-8",
    )
    # Hold briefly so parent can observe ready, then die before any HTTP.
    time.sleep(0.2)
    os.kill(os.getpid(), signal.SIGKILL)
elif mode == "race_dispatch":
    # Wait until parent flips ready gate from "wait" to "go".
    while ready.read_text(encoding="utf-8").strip() != "go":
        time.sleep(0.01)
    try:
        started = j.begin_dispatch(issuer, payload["operation_id"])
        print(json.dumps({"ok": True, "state": started.state.value}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}))
elif mode == "hold_lock":
    from vikunja_local.storage import JournalLock, lock_path_for
    lock = JournalLock(lock_path_for(journal_path))
    lock.acquire()
    ready.write_text(json.dumps({"pid": os.getpid(), "held": True}), encoding="utf-8")
    while True:
        time.sleep(0.05)
else:
    raise SystemExit(f"unknown mode {mode}")
"""


def _spawn_child(mode: str, journal_path: Path, ready: Path, payload: dict[str, Any]) -> subprocess.Popen:
    env = os.environ.copy()
    env["EA010_WORKTREE"] = str(ROOT)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.Popen(
        [sys.executable, "-c", _child_script(), mode, str(journal_path), str(ready), json.dumps(payload)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def _wait_ready(path: Path, proc: subprocess.Popen, timeout: float = 15.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.is_file() and path.stat().st_size > 0:
            return json.loads(path.read_text(encoding="utf-8"))
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=1)
            raise StandError(f"child exited early rc={proc.returncode} stdout={out!r} stderr={err!r}")
        time.sleep(0.02)
    raise StandError(f"timed out waiting for ready file {path}")


def _cleanup_proc(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    if proc.poll() is None:
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            pass


def run_journal_scenarios(work_dir: Path) -> dict[str, Any]:
    scenarios: dict[str, Any] = {}
    synthetic_ids: dict[str, Any] = {}
    journal_path = work_dir / "integration-journal.sqlite3"

    # --- restart durability ---
    with IntegrationJournal(journal_path) as journal:
        issuer = journal.ensure_issuer()
        op_id = new_operation_id()
        payload = {"title": "EA010-SYNTHETIC-journal-restart", "description_user": "ctx"}
        prepared = journal.prepare_intent(
            issuer_id=issuer,
            operation_id=op_id,
            op_type="create",
            project_id=7,
            payload=payload,
            source_kind="synthetic",
            source_key=f"src-{op_id}",
        )
        _assert(prepared.state is OperationState.PREPARED, "expected prepared")
        _assert(prepared.allows_first_http_dispatch(), "prepared should allow first dispatch")
        started = journal.begin_dispatch(issuer, op_id)
        _assert(started.state is OperationState.DISPATCH_STARTED, "expected dispatch_started")
        _assert(not started.allows_first_http_dispatch(), "dispatch_started must not allow first HTTP")
        marker = started.marker
        receipt = journal.record_receipt(
            issuer,
            op_id,
            vikunja_id=4242,
            project_id=7,
            marker=marker,
            effect={"title": payload["title"], "done": False},
        )
        _assert(receipt.state is OperationState.CONFIRMED, "expected confirmed receipt")
        synthetic_ids["issuer_id"] = issuer
        synthetic_ids["operation_id"] = op_id
        synthetic_ids["vikunja_id"] = 4242
        synthetic_ids["marker"] = marker
        synthetic_ids["source_key"] = f"src-{op_id}"

    with IntegrationJournal(journal_path) as journal:
        after = journal.get_operation(synthetic_ids["issuer_id"], synthetic_ids["operation_id"])
        _assert(after is not None, "operation missing after reopen")
        _assert(after.receipt is not None, "receipt missing after reopen")
        _assert(after.receipt["vikunja_id"] == 4242, "vikunja id mismatch after reopen")
        _assert(journal.get_issuer() == synthetic_ids["issuer_id"], "issuer lost after reopen")
        mapped = journal.lookup_source("synthetic", synthetic_ids["source_key"])
        _assert(mapped is not None and mapped.operation_id == op_id, "source mapping lost")
        scenarios["restart_preserves_keys_intents_receipts"] = {
            "issuer_id": synthetic_ids["issuer_id"],
            "operation_id": op_id,
            "vikunja_id": 4242,
            "history_events": [h["event_type"] for h in journal.history(issuer, op_id)],
        }

    # --- key / payload conflict ---
    with IntegrationJournal(journal_path) as journal:
        issuer = journal.get_issuer()
        assert issuer is not None
        conflict_id = new_operation_id()
        journal.prepare_intent(
            issuer_id=issuer,
            operation_id=conflict_id,
            op_type="create",
            project_id=7,
            payload={"title": "same-key-a"},
        )
        replay = journal.prepare_intent(
            issuer_id=issuer,
            operation_id=conflict_id,
            op_type="create",
            project_id=7,
            payload={"title": "same-key-a"},
        )
        _assert(replay.state is OperationState.PREPARED, "identical replay should return prepared")
        _assert(journal.can_dispatch(issuer, conflict_id), "identical replay must not consume dispatch")
        try:
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=conflict_id,
                op_type="create",
                project_id=7,
                payload={"title": "same-key-B-incompatible"},
            )
            raise StandError("expected incompatible payload conflict")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.CONFLICT, f"expected conflict, got {exc.outcome}")
            _assert(journal.can_dispatch(issuer, conflict_id) is False, "conflict must block dispatch")
        # Source mapping uniqueness: new key for same source forbidden.
        second = new_operation_id()
        try:
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=second,
                op_type="create",
                project_id=7,
                payload={"title": "other"},
                source_kind="synthetic",
                source_key=synthetic_ids["source_key"],
            )
            raise StandError("expected source mapping conflict")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.CONFLICT, "source remint must conflict")
        scenarios["key_payload_and_source_conflict"] = {
            "conflict_operation_id": conflict_id,
            "blocked_second_operation_id": second,
        }
        synthetic_ids["conflict_operation_id"] = conflict_id

    # --- SIGKILL after dispatch_started ---
    kill_ready = work_dir / "sigkill-ready.json"
    if kill_ready.exists():
        kill_ready.unlink()
    kill_op = new_operation_id()
    kill_payload = {
        "issuer_id": synthetic_ids["issuer_id"],
        "operation_id": kill_op,
        "op_type": "create",
        "project_id": 7,
        "payload": {"title": "EA010-SYNTHETIC-sigkill"},
    }
    proc = _spawn_child("dispatch_then_die", journal_path, kill_ready, kill_payload)
    try:
        ready = _wait_ready(kill_ready, proc)
        _assert(ready.get("state") == OperationState.DISPATCH_STARTED.value, "child did not mark dispatch_started")
        proc.wait(timeout=10)
        _assert(proc.returncode == -signal.SIGKILL, f"expected SIGKILL, got rc={proc.returncode}")
    finally:
        _cleanup_proc(proc)
    with IntegrationJournal(journal_path) as journal:
        killed = journal.get_operation(synthetic_ids["issuer_id"], kill_op)
        _assert(killed is not None, "operation lost after SIGKILL")
        _assert(killed.state is OperationState.DISPATCH_STARTED, "dispatch_started must survive SIGKILL")
        _assert(not killed.allows_first_http_dispatch(), "intent alone must not prove HTTP unsent")
        _assert(journal.can_dispatch(synthetic_ids["issuer_id"], kill_op) is False, "no first dispatch after crash")
        scenarios["sigkill_after_dispatch_started"] = {
            "operation_id": kill_op,
            "state": killed.state.value,
            "child_rc": -signal.SIGKILL,
            "marker": killed.marker,
        }
        synthetic_ids["sigkill_operation_id"] = kill_op

    # --- two concurrent dispatchers serialized ---
    race_op = new_operation_id()
    with IntegrationJournal(journal_path) as journal:
        journal.prepare_intent(
            issuer_id=synthetic_ids["issuer_id"],
            operation_id=race_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-race"},
        )
    gate = work_dir / "race-gate.txt"
    gate.write_text("wait", encoding="utf-8")
    race_payload = {
        "issuer_id": synthetic_ids["issuer_id"],
        "operation_id": race_op,
        "op_type": "create",
        "project_id": 7,
        "payload": {"title": "EA010-SYNTHETIC-race"},
    }
    c1 = _spawn_child("race_dispatch", journal_path, gate, race_payload)
    c2 = _spawn_child("race_dispatch", journal_path, gate, race_payload)
    try:
        time.sleep(0.15)
        gate.write_text("go", encoding="utf-8")
        out1, err1 = c1.communicate(timeout=20)
        out2, err2 = c2.communicate(timeout=20)
        _assert(c1.returncode == 0 and c2.returncode == 0, f"race children failed: {err1!r} {err2!r}")
        r1 = json.loads(out1.strip().splitlines()[-1])
        r2 = json.loads(out2.strip().splitlines()[-1])
        wins = [r for r in (r1, r2) if r.get("ok") is True]
        losses = [r for r in (r1, r2) if r.get("ok") is False]
        _assert(len(wins) == 1, f"exactly one dispatcher must win, got {r1} {r2}")
        _assert(len(losses) == 1, f"exactly one dispatcher must lose, got {r1} {r2}")
        with IntegrationJournal(journal_path) as journal:
            raced = journal.get_operation(synthetic_ids["issuer_id"], race_op)
            _assert(raced is not None and raced.state is OperationState.DISPATCH_STARTED, "race must leave dispatch_started once")
        scenarios["concurrent_dispatch_serialized"] = {
            "operation_id": race_op,
            "winner": wins[0],
            "loser": losses[0],
        }
        synthetic_ids["race_operation_id"] = race_op
    finally:
        _cleanup_proc(c1)
        _cleanup_proc(c2)

    # --- concurrent open must not race shared .install.tmp ---
    open_gate = work_dir / "open-gate.txt"
    open_gate.write_text("wait", encoding="utf-8")
    open_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[1])
gate = Path(sys.argv[2])
while gate.read_text(encoding="utf-8").strip() != "go":
    time.sleep(0.01)
try:
    with IntegrationJournal(path) as journal:
        got = journal.ensure_issuer()
    print(json.dumps({"ok": True, "issuer": got}))
except Exception as exc:
    print(json.dumps({"ok": False, "error": str(exc), "type": type(exc).__name__}))
"""
    env_open = os.environ.copy()
    env_open["EA010_WORKTREE"] = str(ROOT)
    env_open["PYTHONDONTWRITEBYTECODE"] = "1"
    o1 = subprocess.Popen(
        [sys.executable, "-c", open_script, str(journal_path), str(open_gate)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env_open,
    )
    o2 = subprocess.Popen(
        [sys.executable, "-c", open_script, str(journal_path), str(open_gate)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env_open,
    )
    try:
        time.sleep(0.15)
        open_gate.write_text("go", encoding="utf-8")
        out_o1, err_o1 = o1.communicate(timeout=20)
        out_o2, err_o2 = o2.communicate(timeout=20)
        _assert(o1.returncode == 0 and o2.returncode == 0, f"concurrent open children failed: {err_o1!r} {err_o2!r}")
        ro1 = json.loads(out_o1.strip().splitlines()[-1])
        ro2 = json.loads(out_o2.strip().splitlines()[-1])
        _assert(ro1.get("ok") is True and ro2.get("ok") is True, f"concurrent open must succeed: {ro1} {ro2}")
        _assert(ro1["issuer"] == synthetic_ids["issuer_id"], "concurrent open issuer child1")
        _assert(ro2["issuer"] == synthetic_ids["issuer_id"], "concurrent open issuer child2")
        scenarios["concurrent_open_serialized"] = {
            "issuer_id": synthetic_ids["issuer_id"],
            "child1": ro1,
            "child2": ro2,
        }
    finally:
        _cleanup_proc(o1)
        _cleanup_proc(o2)

    # --- cross-second / reopen identical replay reuses confirmed_at ---
    cross_op = new_operation_id()
    with IntegrationJournal(journal_path) as journal:
        cross_prepared = journal.prepare_intent(
            issuer_id=synthetic_ids["issuer_id"],
            operation_id=cross_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-cross-second"},
        )
        journal.begin_dispatch(synthetic_ids["issuer_id"], cross_op)
        journal.record_receipt(
            synthetic_ids["issuer_id"],
            cross_op,
            vikunja_id=7777,
            project_id=7,
            marker=cross_prepared.marker,
            effect={"id": 7777},
        )
        cross_confirmed = cross_prepared.confirmed_at
    time.sleep(1.1)
    with IntegrationJournal(journal_path) as journal:
        cross_replay = journal.prepare_intent(
            issuer_id=synthetic_ids["issuer_id"],
            operation_id=cross_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-cross-second"},
        )
        _assert(cross_replay.confirmed_at == cross_confirmed, "cross-second replay must reuse confirmed_at")
        _assert(cross_replay.state is OperationState.CONFIRMED, "cross-second replay must return receipt state")
        _assert(cross_replay.receipt is not None and cross_replay.receipt["vikunja_id"] == 7777, "cross-second receipt")
        _assert(journal.can_dispatch(synthetic_ids["issuer_id"], cross_op) is False, "cross-second must not rearm")
    scenarios["cross_second_replay_reuses_confirmed_at"] = {
        "operation_id": cross_op,
        "confirmed_at": cross_confirmed,
        "replay_confirmed_at": cross_replay.confirmed_at,
        "state": cross_replay.state.value,
    }
    synthetic_ids["cross_second_operation_id"] = cross_op

    # --- OS lock held by peer blocks mutation ---
    lock_ready = work_dir / "lock-ready.json"
    if lock_ready.exists():
        lock_ready.unlink()
    locker = _spawn_child("hold_lock", journal_path, lock_ready, {"issuer_id": synthetic_ids["issuer_id"]})
    try:
        _wait_ready(lock_ready, locker)
        blocked_op = new_operation_id()
        blocked = False
        started_at = time.time()
        # Non-blocking attempt via short child timeout: parent tries prepare while lock held.
        # Use a subprocess with alarm so we don't hang the suite forever.
        blocker_script = r"""
import os, sys, json, signal
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal, JournalError
signal.alarm(2)
try:
    j = IntegrationJournal(Path(sys.argv[1]))
    j.open()
    j.prepare_intent(
        issuer_id=sys.argv[2],
        operation_id=sys.argv[3],
        op_type="create",
        project_id=7,
        payload={"title": "should-block"},
    )
    print(json.dumps({"blocked": False}))
except Exception as exc:
    print(json.dumps({"blocked": True, "error": str(exc), "type": type(exc).__name__}))
"""
        env = os.environ.copy()
        env["EA010_WORKTREE"] = str(ROOT)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                blocker_script,
                str(journal_path),
                synthetic_ids["issuer_id"],
                blocked_op,
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
        # Alarm may exit 0 with blocked True, or non-zero on SIGALRM.
        if probe.stdout.strip():
            probe_payload = json.loads(probe.stdout.strip().splitlines()[-1])
            blocked = bool(probe_payload.get("blocked"))
        else:
            blocked = probe.returncode != 0
        _assert(blocked, f"OS lock did not serialize peer mutation: rc={probe.returncode} out={probe.stdout!r} err={probe.stderr!r}")
        scenarios["os_lock_blocks_peer"] = {
            "blocked": blocked,
            "elapsed_s": round(time.time() - started_at, 3),
            "probe_rc": probe.returncode,
        }
    finally:
        _cleanup_proc(locker)

    # --- durable-save failure forbids dispatch ---
    full_dir = work_dir / "full-store"
    full_dir.mkdir(mode=0o700, exist_ok=True)
    full_journal = full_dir / "full.sqlite3"
    # Pre-create a tiny journal then impose RLIMIT_FSIZE in a child that must fail durable write.
    with IntegrationJournal(full_journal) as journal:
        full_issuer = journal.ensure_issuer()
    fail_script = r"""
import os, sys, json, resource
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
from vikunja_local.api import Outcome

# Keep room for interpreter pages but starve SQLite growth/commits.
resource.setrlimit(resource.RLIMIT_FSIZE, (8 * 1024, 8 * 1024))
path = Path(sys.argv[1])
issuer = sys.argv[2]
j = IntegrationJournal(path)
try:
    j.open()
    op = new_operation_id()
    # Large-ish payload to force page allocation / journal growth under tiny FSIZE.
    payload = {"title": "x" * 20000, "blob": "y" * 20000}
    j.prepare_intent(
        issuer_id=issuer,
        operation_id=op,
        op_type="create",
        project_id=7,
        payload=payload,
    )
    # If prepare somehow succeeded, begin_dispatch must still be attempted and reported.
    can = j.can_dispatch(issuer, op)
    print(json.dumps({"failed": False, "can_dispatch": can, "operation_id": op}))
except JournalError as exc:
    print(json.dumps({
        "failed": True,
        "outcome": exc.outcome.value,
        "storage_kind": exc.storage_kind,
        "message": str(exc),
        "can_dispatch": False,
    }))
except Exception as exc:
    print(json.dumps({"failed": True, "outcome": "dependency_unavailable", "message": str(exc), "can_dispatch": False}))
"""
    env = os.environ.copy()
    env["EA010_WORKTREE"] = str(ROOT)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    fail_proc = subprocess.run(
        [sys.executable, "-c", fail_script, str(full_journal), full_issuer],
        capture_output=True,
        text=True,
        env=env,
        timeout=20,
    )
    _assert(fail_proc.stdout.strip(), f"durable-save child produced no stdout: stderr={fail_proc.stderr!r}")
    fail_payload = json.loads(fail_proc.stdout.strip().splitlines()[-1])
    _assert(fail_payload.get("failed") is True, f"durable-save should fail closed: {fail_payload}")
    _assert(fail_payload.get("can_dispatch") is False, "failed durable-save must not allow dispatch")
    scenarios["durable_save_failure_blocks_dispatch"] = fail_payload

    # --- unavailable path ---
    missing_parent = work_dir / "missing-parent-dir" / "nope.sqlite3"
    # Make parent a file so mkdir/open fails closed.
    blocker = work_dir / "missing-parent-dir"
    blocker.write_text("not-a-directory", encoding="utf-8")
    try:
        with IntegrationJournal(missing_parent) as journal:
            journal.ensure_issuer()
        raise StandError("unavailable parent should fail")
    except (JournalError, StorageError) as exc:
        scenarios["unavailable_storage_fail_closed"] = {
            "error": str(exc),
            "type": type(exc).__name__,
        }

    # --- corrupt refuse wipe ---
    corrupt_path = work_dir / "corrupt.sqlite3"
    with IntegrationJournal(corrupt_path) as journal:
        c_issuer = journal.ensure_issuer()
        c_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=c_issuer,
            operation_id=c_op,
            op_type="create",
            project_id=7,
            payload={"title": "before-corrupt"},
        )
        journal.begin_dispatch(c_issuer, c_op)
        journal.record_receipt(
            c_issuer,
            c_op,
            vikunja_id=99,
            project_id=7,
            marker=build_marker(c_issuer, c_op),
            effect={"ok": True},
        )
    # Overwrite with garbage while preserving file presence.
    corrupt_path.write_bytes(b"NOT A SQLITE DATABASE" + os.urandom(64))
    try:
        with IntegrationJournal(corrupt_path) as journal:
            journal.list_operations()
        raise StandError("corrupt journal should fail closed")
    except (JournalError, StorageError) as exc:
        scenarios["corrupt_fail_closed_no_wipe"] = {
            "error": str(exc),
            "file_still_present": corrupt_path.is_file(),
            "size": corrupt_path.stat().st_size,
        }
        _assert(corrupt_path.is_file(), "corrupt file must not be deleted")

    # --- incompatible schema: no wipe ---
    incompat_path = work_dir / "incompat.sqlite3"
    with IntegrationJournal(incompat_path) as journal:
        i_issuer = journal.ensure_issuer()
        i_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=i_issuer,
            operation_id=i_op,
            op_type="create",
            project_id=7,
            payload={"title": "keep-me"},
        )
        journal.begin_dispatch(i_issuer, i_op)
        journal.record_receipt(
            i_issuer,
            i_op,
            vikunja_id=1001,
            project_id=7,
            marker=build_marker(i_issuer, i_op),
            effect={"kept": True},
        )
    conn = sqlite3.connect(str(incompat_path))
    conn.execute("PRAGMA user_version=99")
    conn.commit()
    # Prove receipt row still physically present before refused open.
    receipt_count = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
    conn.close()
    _assert(receipt_count == 1, "precondition: receipt must exist before incompat open")
    try:
        with IntegrationJournal(incompat_path) as journal:
            journal.get_issuer()
        raise StandError("incompatible schema should fail closed")
    except (JournalError, StorageError) as exc:
        # Re-open with raw sqlite to prove no wipe occurred.
        conn = sqlite3.connect(str(incompat_path))
        still = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        conn.close()
        _assert(still == 1, "incompatible open must not wipe receipts")
        _assert(version == 99, "user_version must remain incompatible")
        scenarios["incompatible_schema_no_wipe"] = {
            "error": str(exc),
            "receipts_preserved": still,
            "user_version": version,
            "expected_code_version": JOURNAL_SQLITE_USER_VERSION,
        }

    # --- unknown retention: no TTL deletion API / survives reopen ---
    with IntegrationJournal(journal_path) as journal:
        issuer = synthetic_ids["issuer_id"]
        unk = new_operation_id()
        journal.prepare_intent(
            issuer_id=issuer,
            operation_id=unk,
            op_type="create",
            project_id=7,
            payload={"title": "unknown-keep"},
        )
        journal.begin_dispatch(issuer, unk)
        journal.mark_unknown(issuer, unk, reason="synthetic_timeout")
        # There is no TTL purge API; listing must still include unknown.
        states = {op.operation_id: op.state for op in journal.list_operations()}
        _assert(states.get(unk) is OperationState.UNKNOWN, "unknown must remain listed")
        synthetic_ids["unknown_operation_id"] = unk
    with IntegrationJournal(journal_path) as journal:
        unk_rec = journal.get_operation(synthetic_ids["issuer_id"], synthetic_ids["unknown_operation_id"])
        _assert(unk_rec is not None and unk_rec.state is OperationState.UNKNOWN, "unknown lost after reopen")
        scenarios["unknown_retained_no_ttl"] = {
            "operation_id": synthetic_ids["unknown_operation_id"],
            "state": unk_rec.state.value,
            "ttl_purge_api_present": hasattr(IntegrationJournal, "purge_expired"),
        }
        _assert(not hasattr(IntegrationJournal, "purge_expired"), "TTL purge API must not exist")

    # --- restore/drift disables dispatch ---
    with IntegrationJournal(journal_path) as journal:
        drift_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=synthetic_ids["issuer_id"],
            operation_id=drift_op,
            op_type="create",
            project_id=7,
            payload={"title": "drift"},
        )
        journal.set_restore_drift(True)
        _assert(journal.can_dispatch(synthetic_ids["issuer_id"], drift_op) is False, "drift must block dispatch")
        try:
            journal.begin_dispatch(synthetic_ids["issuer_id"], drift_op)
            raise StandError("begin_dispatch must fail under restore_drift")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "drift begin_dispatch outcome")
        scenarios["restore_drift_blocks_dispatch"] = {"operation_id": drift_op}
        # Clear drift for disposable suite cleanup paths.
        journal.set_restore_drift(False)

    # --- R1: drift reject must release OS-lock so a peer can flock after close ---
    r1_path = work_dir / "r1-lock-release.sqlite3"
    with IntegrationJournal(r1_path) as journal:
        r1_canonical = journal.path
        r1_issuer = journal.ensure_issuer()
        r1_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=r1_issuer,
            operation_id=r1_op,
            op_type="create",
            project_id=7,
            payload={"title": "r1-lock"},
        )
        journal.set_restore_drift(True)
        try:
            journal.begin_dispatch(r1_issuer, r1_op)
            raise StandError("r1 begin_dispatch must fail under drift")
        except JournalError:
            pass
        journal.set_restore_drift(False)
    r1_script = r"""
import fcntl, json, os, sys
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.storage import lock_path_for
lock = lock_path_for(Path(sys.argv[1]))
fd = os.open(str(lock), os.O_RDWR | os.O_CREAT, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(fd, fcntl.LOCK_UN)
    print(json.dumps({"acquired": True}))
except BlockingIOError:
    print(json.dumps({"acquired": False}))
    raise SystemExit(2)
finally:
    os.close(fd)
"""
    r1_probe = subprocess.run(
        [sys.executable, "-c", r1_script, str(r1_canonical)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "EA010_WORKTREE": str(ROOT)},
        timeout=10,
    )
    _assert(r1_probe.returncode == 0, f"R1 peer flock blocked: rc={r1_probe.returncode} err={r1_probe.stderr!r}")
    r1_payload = json.loads(r1_probe.stdout.strip())
    _assert(r1_payload.get("acquired") is True, "R1 peer must acquire lock after close")
    scenarios["guard_enter_failure_releases_peer_lock"] = {
        "acquired": True,
        "journal_path": str(r1_canonical),
    }

    # --- R2: symlink alias must not bypass the canonical OS-lock ---
    r2_real = work_dir / "r2-real.sqlite3"
    with IntegrationJournal(r2_real) as journal:
        r2_canonical = journal.path
        r2_issuer = journal.ensure_issuer()
    r2_alias = work_dir / "r2-alias.sqlite3"
    os.symlink(r2_canonical.name, r2_alias)
    r2_ready = work_dir / "r2-hold-ready.json"
    if r2_ready.exists():
        r2_ready.unlink()
    r2_hold = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.storage import JournalLock, lock_path_for
lock = JournalLock(lock_path_for(Path(sys.argv[1])))
lock.acquire()
Path(sys.argv[2]).write_text(json.dumps({"held": True}), encoding="utf-8")
time.sleep(8)
lock.release()
"""
    r2_holder = subprocess.Popen(
        [sys.executable, "-c", r2_hold, str(r2_canonical), str(r2_ready)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "EA010_WORKTREE": str(ROOT)},
    )
    try:
        _wait_ready(r2_ready, r2_holder)
        r2_child = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
try:
    j = IntegrationJournal(Path(sys.argv[1]))
    j.open()
    j.prepare_intent(
        issuer_id=sys.argv[2],
        operation_id=new_operation_id(),
        op_type="create",
        project_id=7,
        payload={"title": "alias-bypass"},
    )
    print(json.dumps({"bypassed": True}))
except Exception as exc:
    print(json.dumps({"bypassed": False, "error": str(exc), "type": type(exc).__name__}))
"""
        r2_probe = subprocess.run(
            [sys.executable, "-c", r2_child, str(r2_alias), r2_issuer],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "EA010_WORKTREE": str(ROOT)},
            timeout=10,
        )
        _assert(r2_probe.stdout.strip(), f"R2 alias child silent: err={r2_probe.stderr!r}")
        r2_payload = json.loads(r2_probe.stdout.strip().splitlines()[-1])
        _assert(r2_payload.get("bypassed") is False, f"R2 alias bypassed lock: {r2_payload}")
        scenarios["symlink_alias_cannot_bypass_lock"] = r2_payload
    finally:
        _cleanup_proc(r2_holder)

    # --- R2 hard-link: multiply-linked alias must not bypass OS-lock serialization ---
    r2h_real = work_dir / "r2-hardlink-real.sqlite3"
    with IntegrationJournal(r2h_real) as journal:
        r2h_canonical = journal.path
        r2h_issuer = journal.ensure_issuer()
        journal.prepare_intent(
            issuer_id=r2h_issuer,
            operation_id=new_operation_id(),
            op_type="create",
            project_id=7,
            payload={"title": "hardlink-parent"},
        )
    r2h_alias = work_dir / "r2-hardlink-alias.sqlite3"
    os.link(r2h_canonical, r2h_alias)
    _assert(os.stat(r2h_canonical).st_nlink > 1, "R2 hardlink must raise nlink")
    r2h_ready = work_dir / "r2-hardlink-hold-ready.json"
    if r2h_ready.exists():
        r2h_ready.unlink()
    r2h_hold = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.storage import JournalLock, lock_path_for
lock = JournalLock(lock_path_for(Path(sys.argv[1])))
lock.acquire()
Path(sys.argv[2]).write_text(json.dumps({"held": True}), encoding="utf-8")
time.sleep(8)
lock.release()
"""
    r2h_holder = subprocess.Popen(
        [sys.executable, "-c", r2h_hold, str(r2h_canonical), str(r2h_ready)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "EA010_WORKTREE": str(ROOT)},
    )
    try:
        _wait_ready(r2h_ready, r2h_holder)
        r2h_child = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal, JournalError, new_operation_id
try:
    j = IntegrationJournal(Path(sys.argv[1]))
    j.open()
    j.prepare_intent(
        issuer_id=sys.argv[2],
        operation_id=new_operation_id(),
        op_type="create",
        project_id=7,
        payload={"title": "hardlink-bypass"},
    )
    print(json.dumps({"bypassed": True, "rc_ok": True}))
except Exception as exc:
    print(json.dumps({
        "bypassed": False,
        "rc_ok": False,
        "error": str(exc),
        "type": type(exc).__name__,
    }))
    raise SystemExit(2)
"""
        r2h_probe = subprocess.run(
            [sys.executable, "-c", r2h_child, str(r2h_alias), r2h_issuer],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "EA010_WORKTREE": str(ROOT)},
            timeout=10,
        )
        _assert(r2h_probe.stdout.strip(), f"R2 hardlink child silent: err={r2h_probe.stderr!r}")
        r2h_payload = json.loads(r2h_probe.stdout.strip().splitlines()[-1])
        _assert(r2h_probe.returncode != 0, f"R2 hardlink child must fail: {r2h_payload}")
        _assert(r2h_payload.get("bypassed") is False, f"R2 hardlink bypassed lock: {r2h_payload}")
        scenarios["hardlink_alias_cannot_bypass_lock"] = r2h_payload
    finally:
        _cleanup_proc(r2h_holder)

    # --- R3: issuer meta loss fail-closes dispatch and refuses remint ---
    r3_path = work_dir / "r3-issuer-loss.sqlite3"
    with IntegrationJournal(r3_path) as journal:
        r3_issuer = journal.ensure_issuer()
        r3_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=r3_issuer,
            operation_id=r3_op,
            op_type="create",
            project_id=7,
            payload={"title": "issuer-loss"},
        )
    conn = sqlite3.connect(str(r3_path.resolve()))
    conn.execute("DELETE FROM meta WHERE key = ?", ("issuer_id",))
    conn.commit()
    ops_left = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
    conn.close()
    _assert(ops_left == 1, "R3 fixture must keep operations after issuer delete")
    with IntegrationJournal(r3_path) as journal:
        _assert(journal.get_issuer() is None, "R3 issuer must be absent")
        _assert(journal.can_dispatch(r3_issuer, r3_op) is False, "R3 can_dispatch must be false")
        try:
            journal.begin_dispatch(r3_issuer, r3_op)
            raise StandError("R3 begin_dispatch must fail after issuer loss")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R3 dispatch outcome")
        try:
            journal.ensure_issuer()
            raise StandError("R3 ensure_issuer must not remint over prior identity")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R3 remint outcome")
        kept = journal.get_operation(r3_issuer, r3_op)
        _assert(kept is not None and kept.state is OperationState.PREPARED, "R3 must preserve operation")
        scenarios["issuer_loss_fail_closed"] = {
            "operation_id": r3_op,
            "issuer_id": r3_issuer,
            "remint_refused": True,
            "dispatch_refused": True,
            "operation_preserved": True,
        }

    # --- F1: mixed-copy prepared operations row must not re-arm dispatch ---
    f1_path = work_dir / "f1-mixed-copy.sqlite3"
    with IntegrationJournal(f1_path) as journal:
        f1_issuer = journal.ensure_issuer()
        f1_op = new_operation_id()
        f1_prepared = journal.prepare_intent(
            issuer_id=f1_issuer,
            operation_id=f1_op,
            op_type="create",
            project_id=7,
            payload={"title": "mixed-copy"},
        )
        conn = sqlite3.connect(str(f1_path.resolve()))
        conn.row_factory = sqlite3.Row
        f1_saved = dict(
            conn.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (f1_op,),
            ).fetchone()
        )
        conn.close()
        journal.begin_dispatch(f1_issuer, f1_op)
        journal.record_receipt(
            f1_issuer,
            f1_op,
            vikunja_id=42,
            project_id=7,
            marker=f1_prepared.marker,
            effect={"id": 42},
        )
    conn = sqlite3.connect(str(f1_path.resolve()))
    cols = ",".join(f1_saved.keys())
    placeholders = ",".join("?" for _ in f1_saved)
    conn.execute("DELETE FROM operations WHERE operation_id = ?", (f1_op,))
    conn.execute(
        f"INSERT INTO operations({cols}) VALUES ({placeholders})",
        tuple(f1_saved.values()),
    )
    conn.commit()
    f1_integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    f1_receipts = int(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])
    f1_events = [
        str(row[0])
        for row in conn.execute(
            "SELECT event_type FROM history WHERE operation_id = ? ORDER BY id",
            (f1_op,),
        )
    ]
    conn.close()
    _assert(f1_integrity == "ok", "F1 integrity_check must remain ok")
    _assert(f1_receipts == 1, "F1 must retain durable receipt")
    _assert("dispatch_started" in f1_events and "receipt" in f1_events, "F1 history proof")
    with IntegrationJournal(f1_path) as journal:
        f1_loaded = journal.get_operation(f1_issuer, f1_op)
        _assert(f1_loaded is not None and f1_loaded.receipt is not None, "F1 exposes receipt")
        _assert(journal.can_dispatch(f1_issuer, f1_op) is False, "F1 can_dispatch must be false")
        try:
            journal.begin_dispatch(f1_issuer, f1_op)
            raise StandError("F1 begin_dispatch must fail for mixed copy")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "F1 mixed-copy begin_dispatch outcome",
            )
        scenarios["mixed_copy_cannot_rearm_dispatch"] = {
            "operation_id": f1_op,
            "can_dispatch": False,
            "receipt_retained": True,
            "integrity_ok": True,
        }

    # --- R1 conflict history: partial prepared restore must not re-arm dispatch ---
    r1c_path = work_dir / "r1-conflict-partial-restore.sqlite3"
    with IntegrationJournal(r1c_path) as journal:
        r1c_issuer = journal.ensure_issuer()
        r1c_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=r1c_issuer,
            operation_id=r1c_op,
            op_type="create",
            project_id=7,
            payload={"title": "payload-a"},
        )
        try:
            journal.prepare_intent(
                issuer_id=r1c_issuer,
                operation_id=r1c_op,
                op_type="create",
                project_id=7,
                payload={"title": "payload-b-incompatible"},
            )
            raise StandError("R1 conflict fixture must raise CONFLICT")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.CONFLICT, "R1 incompatible payload outcome")
        r1c_events = [
            str(row["event_type"]) for row in journal.history(r1c_issuer, r1c_op)
        ]
        _assert(
            r1c_events == ["intent_prepared", "conflict"],
            f"R1 expected conflict history, got {r1c_events}",
        )
    conn = sqlite3.connect(str(r1c_path.resolve()))
    conn.execute(
        """
        UPDATE operations
        SET state = ?, delivery_state = ?
        WHERE operation_id = ?
        """,
        (OperationState.PREPARED.value, DeliveryState.NONE.value, r1c_op),
    )
    conn.commit()
    r1c_events_after = [
        str(row[0])
        for row in conn.execute(
            "SELECT event_type FROM history WHERE operation_id = ? ORDER BY id",
            (r1c_op,),
        )
    ]
    conn.close()
    _assert(
        r1c_events_after == ["intent_prepared", "conflict"],
        "R1 partial restore must retain conflict history",
    )
    with IntegrationJournal(r1c_path) as journal:
        r1c_loaded = journal.get_operation(r1c_issuer, r1c_op)
        _assert(
            r1c_loaded is not None
            and r1c_loaded.state is OperationState.PREPARED
            and r1c_loaded.delivery_state is DeliveryState.NONE
            and r1c_loaded.receipt is None,
            "R1 restored row claims prepared/none without receipt",
        )
        try:
            journal.begin_dispatch(r1c_issuer, r1c_op)
            raise StandError("R1 begin_dispatch must refuse conflict-history mixed copy")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "R1 conflict mixed-copy begin_dispatch outcome",
            )
            _assert(exc.details.get("mixed_copy") is True, "R1 mixed_copy detail")
            _assert(
                "conflict" in list(exc.details.get("proof_events") or []),
                "R1 proof_events must include conflict",
            )
        _assert(
            journal.can_dispatch(r1c_issuer, r1c_op) is False,
            "R1 conflict-history can_dispatch must be false",
        )
        scenarios["conflict_history_cannot_rearm_dispatch"] = {
            "operation_id": r1c_op,
            "can_dispatch": False,
            "history": r1c_events_after,
            "mixed_copy": True,
        }

    # --- F2: post-construct symlink alias must not bypass canonical lock ---
    f2_real = work_dir / "f2-real.sqlite3"
    with IntegrationJournal(f2_real) as journal:
        f2_canonical = journal.path
        f2_issuer = journal.ensure_issuer()
    f2_alias = work_dir / "f2-late-alias.sqlite3"
    f2_construct = IntegrationJournal(f2_alias)
    os.symlink(f2_canonical.name, f2_alias)
    f2_lock = None
    try:
        from vikunja_local.storage import JournalLock

        f2_lock = JournalLock(lock_path_for(f2_canonical))
        f2_lock.acquire()
        try:
            f2_construct.open()
            f2_construct.prepare_intent(
                issuer_id=f2_issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=7,
                payload={"title": "late-alias-bypass"},
            )
            raise StandError("F2 late alias must not commit while canonical lock held")
        except JournalError as exc:
            scenarios["post_construct_symlink_refused"] = {
                "bypassed": False,
                "error": str(exc),
            }
    finally:
        if f2_lock is not None and f2_lock.held:
            f2_lock.release()
        try:
            f2_construct.close()
        except Exception:  # noqa: BLE001
            pass

    # --- F4: incompatible receipt project_id / vikunja_id rejected before insert ---
    f4_path = work_dir / "f4-receipt-identity.sqlite3"
    with IntegrationJournal(f4_path) as journal:
        f4_issuer = journal.ensure_issuer()
        f4_op_p = new_operation_id()
        f4_prep_p = journal.prepare_intent(
            issuer_id=f4_issuer,
            operation_id=f4_op_p,
            op_type="update",
            project_id=7,
            target_id=11,
            payload={"title": "receipt-p"},
        )
        journal.begin_dispatch(f4_issuer, f4_op_p)
        try:
            journal.record_receipt(
                f4_issuer,
                f4_op_p,
                vikunja_id=11,
                project_id=8,
                marker=f4_prep_p.marker,
                effect={"id": 11},
            )
            raise StandError("F4 project_id mismatch must be rejected")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.CONFLICT, "F4 project mismatch outcome")
        f4_loaded_p = journal.get_operation(f4_issuer, f4_op_p)
        _assert(
            f4_loaded_p is not None
            and f4_loaded_p.state is OperationState.CONFLICT
            and f4_loaded_p.receipt is None,
            "F4 project mismatch must not insert receipt",
        )
    # Fresh journal for vikunja_id mismatch (prior case quarantines writes).
    f4b_path = work_dir / "f4b-receipt-id.sqlite3"
    with IntegrationJournal(f4b_path) as journal:
        f4b_issuer = journal.ensure_issuer()
        f4_op_id = new_operation_id()
        f4_prep_id = journal.prepare_intent(
            issuer_id=f4b_issuer,
            operation_id=f4_op_id,
            op_type="complete",
            project_id=7,
            target_id=11,
            payload={"done": True},
        )
        journal.begin_dispatch(f4b_issuer, f4_op_id)
        try:
            journal.record_receipt(
                f4b_issuer,
                f4_op_id,
                vikunja_id=12,
                project_id=7,
                marker=f4_prep_id.marker,
                effect={"id": 12},
            )
            raise StandError("F4 vikunja_id mismatch must be rejected")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.CONFLICT, "F4 id mismatch outcome")
        f4_loaded_id = journal.get_operation(f4b_issuer, f4_op_id)
        _assert(
            f4_loaded_id is not None
            and f4_loaded_id.state is OperationState.CONFLICT
            and f4_loaded_id.receipt is None,
            "F4 id mismatch must not insert receipt",
        )
        scenarios["receipt_identity_mismatches_rejected"] = {
            "project_mismatch_rejected": True,
            "vikunja_id_mismatch_rejected": True,
            "project_op": f4_op_p,
            "id_op": f4_op_id,
        }

    # --- F1b: missing source_mappings must not mint a second dispatchable source op ---
    f1b_path = work_dir / "f1b-missing-source-mapping.sqlite3"
    with IntegrationJournal(f1b_path) as journal:
        f1b_issuer = journal.ensure_issuer()
        f1b_op = new_operation_id()
        f1b_prepared = journal.prepare_intent(
            issuer_id=f1b_issuer,
            operation_id=f1b_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-acceptance"},
            source_kind="synthetic",
            source_key="EA010-SYNTHETIC-acceptance",
        )
        journal.begin_dispatch(f1b_issuer, f1b_op)
        journal.record_receipt(
            f1b_issuer,
            f1b_op,
            vikunja_id=777,
            project_id=7,
            marker=f1b_prepared.marker,
            effect={"id": 777},
        )
    conn = sqlite3.connect(str(f1b_path.resolve()))
    f1b_deleted = int(conn.execute("DELETE FROM source_mappings").rowcount)
    conn.commit()
    f1b_receipts = int(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])
    f1b_ops_before = int(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0])
    conn.close()
    _assert(f1b_deleted == 1, "F1b must delete exactly one source_mappings row")
    _assert(f1b_receipts == 1 and f1b_ops_before == 1, "F1b must retain receipt/operation")
    with IntegrationJournal(f1b_path) as journal:
        try:
            journal.prepare_intent(
                issuer_id=f1b_issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-acceptance-retry"},
                source_kind="synthetic",
                source_key="EA010-SYNTHETIC-acceptance",
            )
            raise StandError("F1b prepare_intent must refuse new key for retained source")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "F1b missing-mapping prepare outcome",
            )
            _assert(exc.details.get("partial_copy") is True, "F1b partial_copy detail")
        _assert(journal.can_dispatch(f1b_issuer, f1b_op) is False, "F1b can_dispatch false")
        f1b_kept = journal.get_operation(f1b_issuer, f1b_op)
        _assert(
            f1b_kept is not None and f1b_kept.receipt is not None and f1b_kept.receipt["vikunja_id"] == 777,
            "F1b must preserve confirmed receipt",
        )
    conn = sqlite3.connect(str(f1b_path.resolve()))
    f1b_ops_after = int(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0])
    f1b_receipts_after = int(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])
    conn.close()
    _assert(f1b_ops_after == 1 and f1b_receipts_after == 1, "F1b must not create a second operation")
    scenarios["missing_source_mapping_cannot_second_dispatch"] = {
        "operation_id": f1b_op,
        "operations_count": f1b_ops_after,
        "receipt_retained": True,
        "partial_copy": True,
    }

    # --- F2b: altered payload_json with intact digest must refuse first dispatch ---
    f2b_path = work_dir / "f2b-altered-payload-digest.sqlite3"
    with IntegrationJournal(f2b_path) as journal:
        f2b_issuer = journal.ensure_issuer()
        f2b_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=f2b_issuer,
            operation_id=f2b_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-confirmed"},
        )
    conn = sqlite3.connect(str(f2b_path.resolve()))
    conn.execute(
        "UPDATE operations SET payload_json = ? WHERE operation_id = ?",
        ('{"title":"ALTERED-UNCONFIRMED"}', f2b_op),
    )
    conn.commit()
    f2b_integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    f2b_row = conn.execute(
        "SELECT payload_json, payload_digest FROM operations WHERE operation_id = ?",
        (f2b_op,),
    ).fetchone()
    conn.close()
    _assert(f2b_integrity == "ok", "F2b integrity_check must remain ok")
    _assert(f2b_row is not None and f2b_row[0] == '{"title":"ALTERED-UNCONFIRMED"}', "F2b payload altered")
    with IntegrationJournal(f2b_path) as journal:
        try:
            journal.begin_dispatch(f2b_issuer, f2b_op)
            raise StandError("F2b begin_dispatch must refuse digest mismatch")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "F2b altered-payload begin_dispatch outcome",
            )
            _assert(exc.details.get("sha256_matches_payload") is False, "F2b sha256_matches_payload")
            _assert(exc.details.get("mixed_copy") is True, "F2b mixed_copy detail")
        _assert(journal.can_dispatch(f2b_issuer, f2b_op) is False, "F2b can_dispatch must be false")
        f2b_loaded = journal.get_operation(f2b_issuer, f2b_op)
        _assert(
            f2b_loaded is not None and f2b_loaded.payload.get("title") == "ALTERED-UNCONFIRMED",
            "F2b must preserve altered row evidence",
        )
        f2b_events = [str(row["event_type"]) for row in journal.history(f2b_issuer, f2b_op)]
        _assert(f2b_events == ["intent_prepared"], "F2b must retain intent history without dispatch")
    scenarios["altered_payload_digest_blocks_dispatch"] = {
        "operation_id": f2b_op,
        "can_dispatch": False,
        "sha256_matches_payload": False,
        "integrity_ok": True,
    }

    # --- F3: missing safety meta must fail closed (each guard + combined) ---
    f3_base = work_dir / "f3-safety-meta.sqlite3"
    with IntegrationJournal(f3_base) as journal:
        f3_issuer = journal.ensure_issuer()
        f3_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=f3_issuer,
            operation_id=f3_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-safety-meta"},
        )
        journal.set_restore_drift(True)
    f3_guard_results: dict[str, str] = {}
    for key in ("restore_drift", "writes_enabled", "quarantine"):
        guard_path = work_dir / f"f3-missing-{key}.sqlite3"
        shutil.copy2(f3_base, guard_path)
        conn = sqlite3.connect(str(guard_path.resolve()))
        conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        conn.commit()
        conn.close()
        try:
            with IntegrationJournal(guard_path) as journal:
                journal.begin_dispatch(f3_issuer, f3_op)
            raise StandError(f"F3 missing {key} must refuse open/dispatch")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                f"F3 missing {key} outcome",
            )
            f3_guard_results[key] = "refused"
    conn = sqlite3.connect(str(f3_base.resolve()))
    conn.execute("DELETE FROM meta WHERE key IN ('restore_drift', 'writes_enabled')")
    conn.commit()
    f3_ops = int(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0])
    f3_hist = int(conn.execute("SELECT COUNT(*) FROM history").fetchone()[0])
    conn.close()
    _assert(f3_ops == 1 and f3_hist >= 1, "F3 combined case must retain ops/history")
    try:
        with IntegrationJournal(f3_base) as journal:
            if journal.can_dispatch(f3_issuer, f3_op):
                raise StandError("F3 combined missing meta must not report can_dispatch")
            journal.begin_dispatch(f3_issuer, f3_op)
        raise StandError("F3 combined missing meta must refuse open/dispatch")
    except JournalError as exc:
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F3 combined missing meta outcome",
        )
        f3_guard_results["restore_drift+writes_enabled"] = "refused"
    scenarios["missing_safety_meta_fails_closed"] = {
        "operation_id": f3_op,
        "guards": f3_guard_results,
        "operations_preserved": True,
        "history_preserved": True,
    }

    # --- F1c: immutable envelope metadata drift must refuse first dispatch ---
    f1c_results: dict[str, dict[str, Any]] = {}
    for label, mutate_sql, mutate_params, prepare_kwargs in (
        (
            "project_id",
            "UPDATE operations SET project_id = 4 WHERE operation_id = ?",
            None,
            {"op_type": "create", "project_id": 3, "payload": {"title": "env-project"}},
        ),
        (
            "target_id",
            "UPDATE operations SET target_id = 42 WHERE operation_id = ?",
            None,
            {
                "op_type": "update",
                "project_id": 3,
                "target_id": 41,
                "payload": {"title": "env-target"},
            },
        ),
        (
            "marker",
            "UPDATE operations SET marker = ? WHERE operation_id = ?",
            ("EA010-SYNTHETIC-altered-marker",),
            {"op_type": "create", "project_id": 3, "payload": {"title": "env-marker"}},
        ),
    ):
        f1c_path = work_dir / f"f1c-envelope-{label}.sqlite3"
        with IntegrationJournal(f1c_path) as journal:
            f1c_issuer = journal.ensure_issuer()
            f1c_op = new_operation_id()
            journal.prepare_intent(
                issuer_id=f1c_issuer,
                operation_id=f1c_op,
                **prepare_kwargs,
            )
        conn = sqlite3.connect(str(f1c_path.resolve()))
        params = (*(mutate_params or ()), f1c_op)
        conn.execute(mutate_sql, params)
        conn.commit()
        conn.close()
        with IntegrationJournal(f1c_path) as journal:
            try:
                journal.begin_dispatch(f1c_issuer, f1c_op)
                raise StandError(f"F1c {label} begin_dispatch must refuse")
            except JournalError as exc:
                _assert(
                    exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                    f"F1c {label} begin_dispatch outcome",
                )
                _assert(exc.details.get("mixed_copy") is True, f"F1c {label} mixed_copy")
                _assert(
                    exc.details.get("sha256_matches_payload") is False,
                    f"F1c {label} sha256_matches_payload",
                )
            _assert(
                journal.can_dispatch(f1c_issuer, f1c_op) is False,
                f"F1c {label} can_dispatch must be false",
            )
        f1c_results[label] = {
            "operation_id": f1c_op,
            "can_dispatch": False,
            "refused": True,
        }
    scenarios["altered_envelope_metadata_blocks_dispatch"] = f1c_results

    # --- F2c: version-0 nonempty must not bootstrap permissive safety flags ---
    f2c_path = work_dir / "f2c-version0-nonempty.sqlite3"
    with IntegrationJournal(f2c_path) as journal:
        f2c_issuer = journal.ensure_issuer()
        f2c_op = new_operation_id()
        journal.prepare_intent(
            issuer_id=f2c_issuer,
            operation_id=f2c_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-version0"},
        )
        journal.set_restore_drift(True)
    conn = sqlite3.connect(str(f2c_path.resolve()))
    conn.execute("PRAGMA user_version=0")
    conn.execute(
        "DELETE FROM meta WHERE key IN ('writes_enabled', 'restore_drift', 'quarantine')"
    )
    conn.commit()
    f2c_ops = int(conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0])
    conn.close()
    _assert(f2c_ops == 1, "F2c must retain operations before reopen")
    try:
        with IntegrationJournal(f2c_path) as journal:
            if journal.can_dispatch(f2c_issuer, f2c_op):
                raise StandError("F2c version0 nonempty must not report can_dispatch")
            journal.begin_dispatch(f2c_issuer, f2c_op)
        raise StandError("F2c version0 nonempty must refuse open/dispatch")
    except JournalError as exc:
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F2c version0 nonempty outcome",
        )
    conn = sqlite3.connect(str(f2c_path.resolve()))
    f2c_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    f2c_writes = conn.execute(
        "SELECT value FROM meta WHERE key = ?",
        ("writes_enabled",),
    ).fetchone()
    conn.close()
    _assert(f2c_version == 0, "F2c must leave user_version=0")
    _assert(f2c_writes is None, "F2c must not recreate writes_enabled")
    scenarios["version0_nonempty_refuses_bootstrap"] = {
        "operation_id": f2c_op,
        "user_version": f2c_version,
        "writes_enabled_recreated": False,
        "operations_preserved": True,
    }

    # --- F3c: missing journal with lock/install evidence refuses remint ---
    f3c_path = work_dir / "f3c-missing-with-sidecar.sqlite3"
    with IntegrationJournal(f3c_path) as journal:
        f3c_issuer = journal.ensure_issuer()
        f3c_op = new_operation_id()
        f3c_prepared = journal.prepare_intent(
            issuer_id=f3c_issuer,
            operation_id=f3c_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-sidecar"},
            source_kind="synthetic",
            source_key="EA010-SYNTHETIC-sidecar-acceptance",
        )
        journal.begin_dispatch(f3c_issuer, f3c_op)
        journal.record_receipt(
            f3c_issuer,
            f3c_op,
            vikunja_id=4242,
            project_id=7,
            marker=f3c_prepared.marker,
            effect={"id": 4242},
        )
    f3c_aside = work_dir / "f3c-missing-with-sidecar.sqlite3.aside"
    f3c_path.rename(f3c_aside)
    f3c_lock = lock_path_for(f3c_path)
    f3c_install = install_path_for(f3c_path)
    _assert(
        f3c_lock.exists() or f3c_install.exists(),
        "F3c must retain lock or install sidecar evidence",
    )
    _assert(not f3c_path.exists(), "F3c journal file must be absent")
    try:
        with IntegrationJournal(f3c_path) as journal:
            journal.ensure_issuer()
            journal.prepare_intent(
                issuer_id=journal.ensure_issuer(),
                operation_id=new_operation_id(),
                op_type="create",
                project_id=7,
                payload={"title": "EA010-SYNTHETIC-duplicate"},
                source_kind="synthetic",
                source_key="EA010-SYNTHETIC-sidecar-acceptance",
            )
        raise StandError("F3c missing journal with sidecar must refuse remint/prepare")
    except JournalError as exc:
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F3c missing-with-sidecar outcome",
        )
        _assert("established installation" in str(exc).lower(), "F3c established installation message")
    _assert(not f3c_path.exists(), "F3c must not recreate journal file")
    conn = sqlite3.connect(str(f3c_aside.resolve()))
    f3c_aside_issuer = conn.execute(
        "SELECT value FROM meta WHERE key = ?",
        ("issuer_id",),
    ).fetchone()[0]
    f3c_aside_receipts = int(conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])
    conn.close()
    _assert(f3c_aside_issuer == f3c_issuer, "F3c aside must keep original issuer")
    _assert(f3c_aside_receipts == 1, "F3c aside must keep receipt")
    scenarios["missing_journal_with_sidecar_refuses_remint"] = {
        "operation_id": f3c_op,
        "issuer_id": f3c_issuer,
        "journal_recreated": False,
        "aside_receipts": f3c_aside_receipts,
        "remint_refused": True,
    }

    # --- F3d: truncated DB with sidecar evidence refuses remint ---
    f3d_path = work_dir / "f3d-truncated-with-sidecar.sqlite3"
    with IntegrationJournal(f3d_path) as journal:
        f3d_issuer = journal.ensure_issuer()
        f3d_op = new_operation_id()
        f3d_prepared = journal.prepare_intent(
            issuer_id=f3d_issuer,
            operation_id=f3d_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-truncated"},
            source_kind="synthetic",
            source_key="EA010-SYNTHETIC-truncated-acceptance",
        )
        journal.begin_dispatch(f3d_issuer, f3d_op)
        journal.record_receipt(
            f3d_issuer,
            f3d_op,
            vikunja_id=4343,
            project_id=7,
            marker=f3d_prepared.marker,
            effect={"id": 4343},
        )
    f3d_lock = lock_path_for(f3d_path)
    f3d_install = install_path_for(f3d_path)
    f3d_install_text = f3d_install.read_text(encoding="utf-8")
    with open(f3d_path, "wb"):
        pass
    _assert(f3d_path.stat().st_size == 0, "F3d journal must be truncated to zero bytes")
    _assert(f3d_lock.exists() and f3d_install.exists(), "F3d must retain lock and install evidence")
    try:
        with IntegrationJournal(f3d_path) as journal:
            journal.ensure_issuer()
        raise StandError("F3d truncated journal with sidecar must refuse remint")
    except JournalError as exc:
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F3d truncated-with-sidecar outcome",
        )
        _assert("established installation" in str(exc).lower(), "F3d established installation message")
    _assert(f3d_install.read_text(encoding="utf-8") == f3d_install_text, "F3d must preserve .install")
    conn = sqlite3.connect(str(f3d_path.resolve()))
    f3d_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    f3d_tables = [
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    conn.close()
    _assert(f3d_version == 0 and f3d_tables == [], "F3d must not bootstrap over truncated DB")
    scenarios["truncated_journal_with_sidecar_refuses_remint"] = {
        "operation_id": f3d_op,
        "issuer_id": f3d_issuer,
        "install_preserved": True,
        "bootstrapped": False,
        "remint_refused": True,
    }

    # --- F3e: empty-SQLite replacement with sidecar evidence refuses remint ---
    f3e_path = work_dir / "f3e-empty-sqlite-with-sidecar.sqlite3"
    with IntegrationJournal(f3e_path) as journal:
        f3e_issuer = journal.ensure_issuer()
        f3e_op = new_operation_id()
        f3e_prepared = journal.prepare_intent(
            issuer_id=f3e_issuer,
            operation_id=f3e_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-empty-sqlite"},
            source_kind="synthetic",
            source_key="EA010-SYNTHETIC-empty-sqlite-acceptance",
        )
        journal.begin_dispatch(f3e_issuer, f3e_op)
        journal.record_receipt(
            f3e_issuer,
            f3e_op,
            vikunja_id=4444,
            project_id=7,
            marker=f3e_prepared.marker,
            effect={"id": 4444},
        )
    f3e_lock = lock_path_for(f3e_path)
    f3e_install = install_path_for(f3e_path)
    f3e_install_text = f3e_install.read_text(encoding="utf-8")
    f3e_path.unlink()
    empty_conn = sqlite3.connect(str(f3e_path))
    empty_conn.close()
    _assert(f3e_lock.exists() and f3e_install.exists(), "F3e must retain lock and install evidence")
    try:
        with IntegrationJournal(f3e_path) as journal:
            journal.ensure_issuer()
        raise StandError("F3e empty-SQLite replacement with sidecar must refuse remint")
    except JournalError as exc:
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F3e empty-SQLite-with-sidecar outcome",
        )
        _assert("established installation" in str(exc).lower(), "F3e established installation message")
    _assert(f3e_install.read_text(encoding="utf-8") == f3e_install_text, "F3e must preserve .install")
    conn = sqlite3.connect(str(f3e_path.resolve()))
    f3e_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    f3e_tables = [
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    conn.close()
    _assert(f3e_version == 0 and f3e_tables == [], "F3e must not bootstrap over empty SQLite")
    scenarios["empty_sqlite_with_sidecar_refuses_remint"] = {
        "operation_id": f3e_op,
        "issuer_id": f3e_issuer,
        "install_preserved": True,
        "bootstrapped": False,
        "remint_refused": True,
    }

    # --- F1: valid nonempty foreign DB replacement must not overwrite sidecar ---
    f1_path = work_dir / "f1-original-install.sqlite3"
    f1_other = work_dir / "f1-foreign-install.sqlite3"
    with IntegrationJournal(f1_path) as journal:
        f1_issuer_a = journal.ensure_issuer()
        f1_op_a = new_operation_id()
        journal.prepare_intent(
            issuer_id=f1_issuer_a,
            operation_id=f1_op_a,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-original-install"},
        )
    with IntegrationJournal(f1_other) as journal:
        f1_issuer_b = journal.ensure_issuer()
        f1_op_b = new_operation_id()
        journal.prepare_intent(
            issuer_id=f1_issuer_b,
            operation_id=f1_op_b,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-foreign-install"},
        )
    _assert(f1_issuer_a != f1_issuer_b, "F1 foreign replacement needs distinct issuers")
    f1_original_sidecar = install_path_for(f1_path).read_text(encoding="utf-8")
    f1_foreign_sidecar = install_path_for(f1_other).read_text(encoding="utf-8")
    _assert(
        f1_original_sidecar.strip() != f1_foreign_sidecar.strip(),
        "F1 foreign replacement needs distinct installation sidecars",
    )
    shutil.copy2(f1_other, f1_path)
    f1_reopen_refused = False
    try:
        with IntegrationJournal(f1_path) as journal:
            _assert(
                not journal.can_dispatch(f1_issuer_b, f1_op_b),
                "F1 must not allow dispatch after foreign DB copy",
            )
    except JournalError as exc:
        f1_reopen_refused = True
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "F1 foreign replacement outcome",
        )
        _assert(
            exc.details.get("installation_mismatch") is True,
            "F1 installation_mismatch detail",
        )
    _assert(f1_reopen_refused, "F1 foreign nonempty replacement must refuse reopen")
    f1_preserved = install_path_for(f1_path).read_text(encoding="utf-8")
    _assert(f1_preserved == f1_original_sidecar, "F1 must preserve original .install sidecar")
    _assert(
        f1_preserved.strip() != f1_foreign_sidecar.strip(),
        "F1 must not silently adopt foreign installation identity",
    )
    scenarios["foreign_db_replacement_refuses_sidecar_overwrite"] = {
        "issuer_a": f1_issuer_a,
        "issuer_b": f1_issuer_b,
        "operation_id": f1_op_b,
        "sidecar_preserved": True,
        "sidecar_silently_overwritten": False,
        "reopen_refused": True,
        "dispatch_allowed": False,
    }

    # --- R1: same-install stale SQLite restore must not re-authorize first dispatch ---
    r1_path = work_dir / "r1-same-install-stale.sqlite3"
    with IntegrationJournal(r1_path) as journal:
        r1_issuer = journal.ensure_issuer()
        r1_op = new_operation_id()
        r1_prepared = journal.prepare_intent(
            issuer_id=r1_issuer,
            operation_id=r1_op,
            op_type="create",
            project_id=3,
            payload={"title": "EA010-SYNTHETIC-r1-stale-copy"},
        )
    r1_snapshot = work_dir / "r1-prepared-snapshot.sqlite3"
    shutil.copy2(r1_path, r1_snapshot)
    r1_sidecar_before = install_path_for(r1_path).read_text(encoding="utf-8")
    with IntegrationJournal(r1_path) as journal:
        _assert(journal.can_dispatch(r1_issuer, r1_op) is True, "R1 live journal can_dispatch")
        journal.begin_dispatch(r1_issuer, r1_op)
        journal.record_receipt(
            r1_issuer,
            r1_op,
            vikunja_id=901,
            project_id=3,
            marker=r1_prepared.marker,
            effect={"id": 901},
        )
    r1_sidecar_after = install_path_for(r1_path).read_text(encoding="utf-8")
    _assert(r1_sidecar_before != r1_sidecar_after, "R1 sidecar epoch must advance after dispatch")
    shutil.copy2(r1_snapshot, r1_path)
    _assert(
        install_path_for(r1_path).read_text(encoding="utf-8") == r1_sidecar_after,
        "R1 must retain advanced .install while restoring old DB",
    )
    r1_reopen_refused = False
    try:
        with IntegrationJournal(r1_path) as journal:
            _assert(
                journal.can_dispatch(r1_issuer, r1_op) is False,
                "R1 stale copy must not can_dispatch",
            )
            journal.begin_dispatch(r1_issuer, r1_op)
            raise StandError("R1 begin_dispatch must refuse stale same-install copy")
    except JournalError as exc:
        r1_reopen_refused = True
        _assert(
            exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
            "R1 stale-copy outcome",
        )
        _assert(exc.details.get("freshness_mismatch") is True, "R1 freshness_mismatch")
        _assert(exc.details.get("stale_copy") is True, "R1 stale_copy")
    _assert(r1_reopen_refused, "R1 same-install stale restore must refuse reopen/dispatch")
    conn = sqlite3.connect(str(r1_path))
    r1_dispatch_n = conn.execute(
        "SELECT COUNT(*) FROM history WHERE event_type = 'dispatch_started' AND operation_id = ?",
        (r1_op,),
    ).fetchone()[0]
    conn.close()
    _assert(r1_dispatch_n == 0, "R1 must not commit a second dispatch_started on stale restore")
    scenarios["same_install_stale_db_copy_refuses_redispatch"] = {
        "issuer_id": r1_issuer,
        "operation_id": r1_op,
        "freshness_mismatch": True,
        "stale_copy": True,
        "dispatch_started_count": r1_dispatch_n,
        "sidecar_retained": True,
    }

    # --- R2: orphan receipt after FK-off parent delete must quarantine writes ---
    r2_path = work_dir / "r2-orphan-receipt.sqlite3"
    with IntegrationJournal(r2_path) as journal:
        r2_issuer = journal.ensure_issuer()
        r2_op = new_operation_id()
        r2_source_key = "EA010-SYNTHETIC-r2-orphan-receipt"
        r2_prepared = journal.prepare_intent(
            issuer_id=r2_issuer,
            operation_id=r2_op,
            op_type="create",
            project_id=3,
            payload={"title": "EA010-SYNTHETIC-r2-orphan-receipt"},
            source_kind="synthetic",
            source_key=r2_source_key,
        )
        journal.begin_dispatch(r2_issuer, r2_op)
        journal.record_receipt(
            r2_issuer,
            r2_op,
            vikunja_id=778,
            project_id=3,
            marker=r2_prepared.marker,
            effect={"id": 778},
        )
    conn = sqlite3.connect(str(r2_path))
    conn.execute("DELETE FROM source_mappings")
    conn.execute("DELETE FROM operations")
    conn.commit()
    r2_fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    r2_receipt_before = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
    r2_vikunja_id = conn.execute(
        "SELECT vikunja_id FROM receipts WHERE operation_id = ?",
        (r2_op,),
    ).fetchone()[0]
    conn.close()
    _assert(bool(r2_fk_rows), "R2 foreign_key_check must report orphan receipt")
    _assert(r2_receipt_before == 1 and r2_vikunja_id == 778, "R2 must retain receipt 778")
    with IntegrationJournal(r2_path) as journal:
        try:
            journal.prepare_intent(
                issuer_id=r2_issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-r2-orphan-receipt-again"},
                source_kind="synthetic",
                source_key=r2_source_key,
            )
            raise StandError("R2 prepare_intent must refuse after orphan-receipt partial copy")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "R2 orphan-receipt prepare outcome",
            )
        _assert(journal.can_dispatch(r2_issuer, r2_op) is False, "R2 can_dispatch false")
        _assert(journal.get_operation(r2_issuer, r2_op) is None, "R2 operation row remains deleted")
        r2_quarantine = [
            row
            for row in journal.history()
            if row["event_type"] == "quarantine"
            and row["detail"].get("reason") == "foreign_key_check"
        ]
        _assert(bool(r2_quarantine), "R2 must record foreign_key_check quarantine")
    conn = sqlite3.connect(str(r2_path))
    r2_receipt_after = conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]
    r2_ops_after = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
    r2_quarantine_flag = conn.execute(
        "SELECT value FROM meta WHERE key = 'quarantine'"
    ).fetchone()[0]
    r2_writes = conn.execute(
        "SELECT value FROM meta WHERE key = 'writes_enabled'"
    ).fetchone()[0]
    conn.close()
    _assert(r2_receipt_after == 1 and r2_ops_after == 0, "R2 must quarantine without recreating")
    _assert(r2_quarantine_flag == "1" and r2_writes == "0", "R2 writes-off flags")
    scenarios["orphan_receipt_partial_copy_quarantines_writes"] = {
        "issuer_id": r2_issuer,
        "operation_id": r2_op,
        "vikunja_id": 778,
        "foreign_key_violations": True,
        "receipts_preserved": True,
        "operations_recreated": False,
        "writes_enabled": False,
    }

    # --- R1 live: SQLite backup into kept-open connection must not re-arm dispatch ---
    r1_live_path = work_dir / "r1-live-backup-stale.sqlite3"
    r1_live_journal = IntegrationJournal(r1_live_path)
    r1_live_journal.open()
    try:
        r1_live_issuer = r1_live_journal.ensure_issuer()
        r1_live_op = new_operation_id()
        r1_live_prepared = r1_live_journal.prepare_intent(
            issuer_id=r1_live_issuer,
            operation_id=r1_live_op,
            op_type="create",
            project_id=3,
            payload={"title": "EA010-SYNTHETIC-r1-live-backup"},
        )
        r1_live_snapshot = work_dir / "r1-live-prepared-snapshot.sqlite3"
        shutil.copy2(r1_live_path, r1_live_snapshot)
        r1_live_sidecar_before = install_path_for(r1_live_path).read_text(encoding="utf-8")
        _assert(
            r1_live_journal.can_dispatch(r1_live_issuer, r1_live_op) is True,
            "R1 live can_dispatch before backup",
        )
        r1_live_journal.begin_dispatch(r1_live_issuer, r1_live_op)
        r1_live_journal.record_receipt(
            r1_live_issuer,
            r1_live_op,
            vikunja_id=902,
            project_id=3,
            marker=r1_live_prepared.marker,
            effect={"id": 902},
        )
        r1_live_sidecar_after = install_path_for(r1_live_path).read_text(encoding="utf-8")
        _assert(
            r1_live_sidecar_before != r1_live_sidecar_after,
            "R1 live sidecar epoch must advance",
        )
        src = sqlite3.connect(str(r1_live_snapshot))
        try:
            src.backup(r1_live_journal._store.conn)
        finally:
            src.close()
        _assert(
            install_path_for(r1_live_path).read_text(encoding="utf-8") == r1_live_sidecar_after,
            "R1 live must retain advanced .install after in-place backup",
        )
        _assert(
            r1_live_journal.can_dispatch(r1_live_issuer, r1_live_op) is False,
            "R1 live can_dispatch must be false after stale backup",
        )
        r1_live_refused = False
        try:
            r1_live_journal.begin_dispatch(r1_live_issuer, r1_live_op)
            raise StandError("R1 live begin_dispatch must refuse stale in-place backup")
        except JournalError as exc:
            r1_live_refused = True
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R1 live outcome")
            _assert(exc.details.get("freshness_mismatch") is True, "R1 live freshness_mismatch")
            _assert(exc.details.get("stale_copy") is True, "R1 live stale_copy")
        _assert(r1_live_refused, "R1 live begin_dispatch refused")
        _assert(
            install_path_for(r1_live_path).read_text(encoding="utf-8") == r1_live_sidecar_after,
            "R1 live must not overwrite advanced sidecar after refused dispatch",
        )
        r1_live_dispatch_n = r1_live_journal._store.conn.execute(
            "SELECT COUNT(*) FROM history WHERE event_type = 'dispatch_started' "
            "AND operation_id = ?",
            (r1_live_op,),
        ).fetchone()[0]
        _assert(r1_live_dispatch_n == 0, "R1 live must not commit second dispatch_started")
    finally:
        r1_live_journal.close()
    scenarios["live_connection_stale_backup_refuses_redispatch"] = {
        "issuer_id": r1_live_issuer,
        "operation_id": r1_live_op,
        "freshness_mismatch": True,
        "stale_copy": True,
        "dispatch_started_count": r1_live_dispatch_n,
        "sidecar_retained": True,
    }

    # --- R1 live: missing .install after open must not recreate via mutations ---
    r1_miss_path = work_dir / "r1-live-missing-sidecar.sqlite3"
    r1_miss_journal = IntegrationJournal(r1_miss_path)
    r1_miss_journal.open()
    try:
        r1_miss_issuer = r1_miss_journal.ensure_issuer()
        r1_miss_op = new_operation_id()
        r1_miss_journal.prepare_intent(
            issuer_id=r1_miss_issuer,
            operation_id=r1_miss_op,
            op_type="create",
            project_id=3,
            payload={"title": "EA010-SYNTHETIC-r1-missing-sidecar"},
        )
        r1_miss_sidecar = install_path_for(r1_miss_path)
        _assert(r1_miss_sidecar.is_file(), "R1 missing-sidecar precondition")
        r1_miss_sidecar.unlink()
        _assert(not r1_miss_sidecar.exists(), "R1 missing-sidecar deleted")
        try:
            r1_miss_journal.begin_dispatch(r1_miss_issuer, r1_miss_op)
            raise StandError("R1 missing-sidecar begin_dispatch must refuse")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R1 missing-sidecar outcome")
        _assert(not r1_miss_sidecar.exists(), "R1 begin must not recreate .install")
        try:
            r1_miss_journal.ensure_issuer()
            raise StandError("R1 missing-sidecar ensure_issuer must refuse")
        except JournalError:
            pass
        _assert(not r1_miss_sidecar.exists(), "R1 ensure_issuer must not recreate .install")
        try:
            r1_miss_journal.prepare_intent(
                issuer_id=r1_miss_issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-r1-missing-sidecar-2"},
            )
            raise StandError("R1 missing-sidecar prepare_intent must refuse")
        except JournalError:
            pass
        _assert(not r1_miss_sidecar.exists(), "R1 prepare must not recreate .install")
        _assert(
            r1_miss_journal.can_dispatch(r1_miss_issuer, r1_miss_op) is False,
            "R1 missing-sidecar can_dispatch false",
        )
    finally:
        r1_miss_journal.close()
    _assert(
        not install_path_for(r1_miss_path).exists(),
        "R1 missing-sidecar must stay absent after close",
    )
    scenarios["live_connection_missing_sidecar_refuses_writes"] = {
        "issuer_id": r1_miss_issuer,
        "operation_id": r1_miss_op,
        "sidecar_recreated": False,
    }

    # --- R2: history-only residue (no FK) must quarantine before admitting a new key ---
    r2h_path = work_dir / "r2-history-only-residue.sqlite3"
    with IntegrationJournal(r2h_path) as journal:
        r2h_issuer = journal.ensure_issuer()
        r2h_op = new_operation_id()
        r2h_source_key = "EA010-SYNTHETIC-r2-history-only"
        journal.prepare_intent(
            issuer_id=r2h_issuer,
            operation_id=r2h_op,
            op_type="create",
            project_id=3,
            payload={"title": "EA010-SYNTHETIC-r2-history-only"},
            source_kind="synthetic",
            source_key=r2h_source_key,
        )
        journal.begin_dispatch(r2h_issuer, r2h_op)
    conn = sqlite3.connect(str(r2h_path))
    conn.execute("DELETE FROM source_mappings")
    conn.execute("DELETE FROM operations")
    conn.commit()
    r2h_fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    r2h_hist = conn.execute(
        "SELECT COUNT(*) FROM history WHERE operation_id != ''"
    ).fetchone()[0]
    conn.close()
    _assert(r2h_fk == [], "R2 history-only must have no FK violations")
    _assert(r2h_hist >= 1, "R2 history-only must retain operation history")
    with IntegrationJournal(r2h_path) as journal:
        try:
            journal.prepare_intent(
                issuer_id=r2h_issuer,
                operation_id=new_operation_id(),
                op_type="create",
                project_id=3,
                payload={"title": "EA010-SYNTHETIC-r2-history-only-again"},
                source_kind="synthetic",
                source_key=r2h_source_key,
            )
            raise StandError("R2 history-only prepare_intent must refuse")
        except JournalError as exc:
            _assert(
                exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE,
                "R2 history-only prepare outcome",
            )
        _assert(journal.can_dispatch(r2h_issuer, r2h_op) is False, "R2 history-only can_dispatch")
        r2h_quarantine = [
            row
            for row in journal.history()
            if row["event_type"] == "quarantine"
            and row["detail"].get("reason") == "history_without_operation"
        ]
        _assert(bool(r2h_quarantine), "R2 history-only quarantine event")
        _assert(
            bool(r2h_quarantine[0]["detail"].get("history_operation_orphans")),
            "R2 history-only orphans detail",
        )
    conn = sqlite3.connect(str(r2h_path))
    r2h_ops = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
    r2h_q = conn.execute("SELECT value FROM meta WHERE key = 'quarantine'").fetchone()[0]
    r2h_w = conn.execute("SELECT value FROM meta WHERE key = 'writes_enabled'").fetchone()[0]
    r2h_preserved = conn.execute(
        "SELECT COUNT(*) FROM history WHERE operation_id = ?",
        (r2h_op,),
    ).fetchone()[0]
    conn.close()
    _assert(r2h_ops == 0, "R2 history-only must not recreate operations")
    _assert(r2h_q == "1" and r2h_w == "0", "R2 history-only writes-off")
    _assert(r2h_preserved >= 1, "R2 history-only must preserve history residue")
    scenarios["history_only_residue_quarantines_before_new_source_key"] = {
        "issuer_id": r2h_issuer,
        "operation_id": r2h_op,
        "foreign_key_violations": False,
        "history_preserved": True,
        "operations_recreated": False,
        "writes_enabled": False,
        "quarantine_reason": "history_without_operation",
    }

    # --- R3: NUL / unpaired surrogate confirmed context rejected before durable intent ---
    r3_path = work_dir / "r3-invalid-confirmed-context.sqlite3"
    with IntegrationJournal(r3_path) as journal:
        r3_issuer = journal.ensure_issuer()
        r3_nul_op = new_operation_id()
        try:
            journal.prepare_intent(
                issuer_id=r3_issuer,
                operation_id=r3_nul_op,
                op_type="create",
                project_id=3,
                payload={
                    "title": "synthetic title",
                    "description_user": "before\u0000after",
                },
            )
            raise StandError("R3 NUL prepare_intent must refuse")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.VALIDATION_ERROR, "R3 NUL outcome")
            _assert("NUL" in str(exc), "R3 NUL message")
        _assert(journal.get_operation(r3_issuer, r3_nul_op) is None, "R3 NUL no durable intent")
        r3_sur_op = new_operation_id()
        try:
            journal.prepare_intent(
                issuer_id=r3_issuer,
                operation_id=r3_sur_op,
                op_type="create",
                project_id=3,
                payload={
                    "title": "synthetic title",
                    "description_user": "before\ud800after",
                },
            )
            raise StandError("R3 surrogate prepare_intent must refuse")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.VALIDATION_ERROR, "R3 surrogate outcome")
            _assert("surrogate" in str(exc).lower(), "R3 surrogate message")
        _assert(
            journal.get_operation(r3_issuer, r3_sur_op) is None,
            "R3 surrogate no durable intent",
        )
    conn = sqlite3.connect(str(r3_path))
    r3_ops = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
    r3_intent_hist = conn.execute(
        "SELECT COUNT(*) FROM history WHERE event_type = 'intent_prepared'"
    ).fetchone()[0]
    conn.close()
    _assert(r3_ops == 0 and r3_intent_hist == 0, "R3 must leave no durable intent rows")
    scenarios["prepare_intent_rejects_nul_and_surrogate_context"] = {
        "issuer_id": r3_issuer,
        "nul_operation_id": r3_nul_op,
        "surrogate_operation_id": r3_sur_op,
        "operations_created": 0,
        "intent_prepared_history": 0,
    }

    # --- F2: concurrent public reads must not publish uncommitted receipt ---
    f2_path = work_dir / "f2-uncommitted-receipt.sqlite3"
    with IntegrationJournal(f2_path) as journal:
        f2_issuer = journal.ensure_issuer()
        f2_op = new_operation_id()
        f2_source_key = "EA010-SYNTHETIC-uncommitted-receipt"
        f2_prepared = journal.prepare_intent(
            issuer_id=f2_issuer,
            operation_id=f2_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-uncommitted-receipt"},
            source_kind="meeting_proposal",
            source_key=f2_source_key,
        )
        journal.begin_dispatch(f2_issuer, f2_op)
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
                    raise RuntimeError("suite helper synchronization timeout")
                raise StorageError("full", "synthetic receipt commit failure")
            real_commit()

        journal._store.commit = failed_commit  # type: ignore[method-assign]

        def writer() -> None:
            try:
                journal.record_receipt(
                    f2_issuer,
                    f2_op,
                    vikunja_id=71,
                    project_id=7,
                    marker=f2_prepared.marker,
                    effect={"title": "EA010-SYNTHETIC-uncommitted-receipt"},
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(type(exc).__name__)

        reader_results: dict[str, Any] = {}

        def reader() -> None:
            reader_results["get"] = journal.get_operation(f2_issuer, f2_op)
            reader_results["list"] = journal.list_operations()
            reader_results["source"] = journal.lookup_source(
                "meeting_proposal", f2_source_key
            )

        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)
        writer_thread.start()
        try:
            _assert(entered.wait(5), "F2 writer did not reach delayed commit")
            reader_thread.start()
            time.sleep(0.05)
            _assert(reader_thread.is_alive(), "F2 reader must wait for mutator lock")
        finally:
            release.set()
            writer_thread.join(5)
            reader_thread.join(5)
        _assert(not writer_thread.is_alive(), "F2 writer thread did not stop")
        _assert(not reader_thread.is_alive(), "F2 reader thread did not stop")
        _assert("JournalError" in errors, "F2 writer must surface JournalError")
        seen = reader_results.get("get")
        listed = reader_results.get("list")
        sourced = reader_results.get("source")
        _assert(seen is not None, "F2 get_operation must return a record")
        assert seen is not None
        _assert(seen.state is OperationState.UNKNOWN, "F2 concurrent get must stay unknown")
        _assert(seen.receipt is None, "F2 concurrent get must not publish receipt")
        _assert(isinstance(listed, list) and len(listed) == 1, "F2 list_operations count")
        assert isinstance(listed, list)
        _assert(listed[0].state is OperationState.UNKNOWN, "F2 list must stay unknown")
        _assert(listed[0].receipt is None, "F2 list must not publish receipt")
        _assert(sourced is not None, "F2 lookup_source must return a record")
        assert sourced is not None
        _assert(sourced.state is OperationState.UNKNOWN, "F2 source lookup must stay unknown")
        _assert(sourced.receipt is None, "F2 source lookup must not publish receipt")
        final = journal.get_operation(f2_issuer, f2_op)
        assert final is not None
        _assert(final.state is OperationState.UNKNOWN, "F2 final state must be unknown")
        _assert(final.receipt is None, "F2 final receipt must be absent")
    scenarios["uncommitted_receipt_not_visible_to_readers"] = {
        "issuer_id": f2_issuer,
        "operation_id": f2_op,
        "read_state": OperationState.UNKNOWN.value,
        "read_receipt": False,
        "writer_errors": errors,
        "final_state": OperationState.UNKNOWN.value,
        "final_receipt": False,
    }

    # --- R1: distinct-key overlap blocked while dispatch critical section is held ---
    dko_path = work_dir / "r1-distinct-key-overlap.sqlite3"
    with IntegrationJournal(dko_path) as journal:
        dko_issuer = journal.ensure_issuer()
        dko_a = new_operation_id()
        dko_b = new_operation_id()
        for op in (dko_a, dko_b):
            journal.prepare_intent(
                issuer_id=dko_issuer,
                operation_id=op,
                op_type="update",
                project_id=7,
                target_id=71,
                payload={"title": f"EA010-SYNTHETIC-dko-{op}"},
            )
    dko_ready = work_dir / "dko-ready.json"
    dko_release = work_dir / "dko-release.txt"
    dko_release.write_text("hold", encoding="utf-8")
    dko_holder_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[1])
issuer = sys.argv[2]
op = sys.argv[3]
ready = Path(sys.argv[4])
release = Path(sys.argv[5])
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
    dko_peer_script = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[1])
issuer = sys.argv[2]
op = sys.argv[3]
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
    dko_env = {**os.environ, "EA010_WORKTREE": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    dko_holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            dko_holder_script,
            str(dko_path),
            dko_issuer,
            dko_a,
            str(dko_ready),
            str(dko_release),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=dko_env,
    )
    try:
        dko_holder_ready = _wait_ready(dko_ready, dko_holder)
        _assert(dko_holder_ready.get("lock_held") is True, "R1 holder must retain OS-lock")
        _assert(
            dko_holder_ready.get("state") == OperationState.DISPATCH_STARTED.value,
            "R1 holder begin_dispatch",
        )
        dko_peer = subprocess.Popen(
            [sys.executable, "-c", dko_peer_script, str(dko_path), dko_issuer, dko_b],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=dko_env,
        )
        try:
            time.sleep(0.4)
            _assert(dko_peer.poll() is None, "R1 peer must block on distinct-key overlap")
            dko_release.write_text("go", encoding="utf-8")
            dko_peer_out, dko_peer_err = dko_peer.communicate(timeout=20)
            dko_holder_out, dko_holder_err = dko_holder.communicate(timeout=20)
            _assert(dko_holder.returncode == 0, f"R1 holder failed: {dko_holder_err!r}")
            _assert(dko_peer.returncode == 0, f"R1 peer failed: {dko_peer_err!r}")
            dko_peer_result = json.loads(dko_peer_out.strip().splitlines()[-1])
            _assert(dko_peer_result.get("ok") is True, f"R1 peer result {dko_peer_result}")
            _assert(float(dko_peer_result.get("waited_s", 0)) >= 0.3, "R1 peer must have waited")
        finally:
            _cleanup_proc(dko_peer)
    finally:
        dko_release.write_text("go", encoding="utf-8")
        _cleanup_proc(dko_holder)
    scenarios["distinct_key_dispatch_section_serialized"] = {
        "issuer_id": dko_issuer,
        "operation_a": dko_a,
        "operation_b": dko_b,
        "holder_lock_held": True,
        "peer_blocked_until_release": True,
    }
    synthetic_ids["distinct_key_operation_a"] = dko_a
    synthetic_ids["distinct_key_operation_b"] = dko_b

    # --- R1: SIGKILL while holding dispatch section releases lock for peer ---
    dks_path = work_dir / "r1-dispatch-section-sigkill.sqlite3"
    with IntegrationJournal(dks_path) as journal:
        dks_issuer = journal.ensure_issuer()
        dks_a = new_operation_id()
        dks_b = new_operation_id()
        for op in (dks_a, dks_b):
            journal.prepare_intent(
                issuer_id=dks_issuer,
                operation_id=op,
                op_type="update",
                project_id=7,
                target_id=71,
                payload={"title": f"EA010-SYNTHETIC-dks-{op}"},
            )
    dks_ready = work_dir / "dks-ready.json"
    if dks_ready.exists():
        dks_ready.unlink()
    dks_script = r"""
import json, os, signal, sys, time
from pathlib import Path
sys.path.insert(0, os.environ["EA010_WORKTREE"])
from vikunja_local.journal import IntegrationJournal
path = Path(sys.argv[1])
issuer = sys.argv[2]
op = sys.argv[3]
ready = Path(sys.argv[4])
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
    dks_proc = subprocess.Popen(
        [sys.executable, "-c", dks_script, str(dks_path), dks_issuer, dks_a, str(dks_ready)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "EA010_WORKTREE": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"},
    )
    try:
        dks_ready_payload = _wait_ready(dks_ready, dks_proc)
        _assert(dks_ready_payload.get("lock_held") is True, "R1 kill holder lock_held")
        dks_proc.wait(timeout=10)
        _assert(dks_proc.returncode == -signal.SIGKILL, f"R1 expected SIGKILL rc={dks_proc.returncode}")
    finally:
        _cleanup_proc(dks_proc)
    with IntegrationJournal(dks_path) as journal:
        dks_started_b = journal.begin_dispatch(dks_issuer, dks_b)
        _assert(dks_started_b.state is OperationState.DISPATCH_STARTED, "R1 peer after crash")
        dks_loaded_a = journal.get_operation(dks_issuer, dks_a)
        _assert(dks_loaded_a is not None, "R1 killed op retained")
        assert dks_loaded_a is not None
        _assert(dks_loaded_a.state is OperationState.DISPATCH_STARTED, "R1 killed op state")
        _assert(journal.can_dispatch(dks_issuer, dks_a) is False, "R1 no redisatch after crash")
    scenarios["dispatch_section_sigkill_releases_lock"] = {
        "issuer_id": dks_issuer,
        "killed_operation_id": dks_a,
        "peer_operation_id": dks_b,
        "child_rc": -signal.SIGKILL,
        "peer_dispatched_after_crash": True,
    }

    # --- R2: full DB+.install rollback cannot re-arm create; admit stays writes-off ---
    fr_path = work_dir / "r2-full-restore.sqlite3"
    with IntegrationJournal(fr_path) as journal:
        fr_issuer = journal.ensure_issuer()
        fr_op = new_operation_id()
        fr_prepared = journal.prepare_intent(
            issuer_id=fr_issuer,
            operation_id=fr_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-full-restore"},
        )
    fr_db_bytes = fr_path.read_bytes()
    fr_install_bytes = install_path_for(fr_path).read_bytes()
    fr_authority_before = authority_path_for(fr_path).read_text(encoding="utf-8")
    with IntegrationJournal(fr_path) as journal:
        journal.begin_dispatch(fr_issuer, fr_op)
        journal.record_receipt(
            fr_issuer,
            fr_op,
            vikunja_id=777,
            project_id=7,
            marker=fr_prepared.marker,
            effect={"id": 777},
        )
    fr_authority_after = authority_path_for(fr_path).read_text(encoding="utf-8")
    _assert(fr_authority_before != fr_authority_after, "R2 authority must advance with receipt")
    fr_path.write_bytes(fr_db_bytes)
    install_path_for(fr_path).write_bytes(fr_install_bytes)
    _assert(
        authority_path_for(fr_path).read_text(encoding="utf-8") == fr_authority_after,
        "R2 live authority must survive DB+.install restore",
    )
    fr_refused = False
    try:
        with IntegrationJournal(fr_path) as journal:
            _assert(journal.can_dispatch(fr_issuer, fr_op) is False, "R2 must not can_dispatch")
            journal.begin_dispatch(fr_issuer, fr_op)
            raise StandError("R2 begin_dispatch must refuse full DB+.install rollback")
    except JournalError as exc:
        fr_refused = True
        _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R2 full-restore outcome")
        _assert(
            bool(exc.details.get("authority_mismatch") or exc.details.get("full_backup_rollback")),
            "R2 authority fence details",
        )
    _assert(fr_refused, "R2 full DB+.install rollback must fail closed")
    fr_live_gen = int(fr_authority_after.strip().splitlines()[1])
    fr_restored_gen = int(fr_install_bytes.decode("utf-8").strip().splitlines()[1])
    fr_admitted = IntegrationJournal(fr_path)
    fr_admitted.admit_restored_backup()
    try:
        _assert(fr_admitted.can_dispatch(fr_issuer, fr_op) is False, "R2 admit keeps can_dispatch false")
        try:
            fr_admitted.begin_dispatch(fr_issuer, fr_op)
            raise StandError("R2 admit must not authorize begin_dispatch")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R2 admit dispatch outcome")
        fr_conn = sqlite3.connect(str(fr_path))
        fr_drift = fr_conn.execute(
            "SELECT value FROM meta WHERE key = 'restore_drift'"
        ).fetchone()[0]
        fr_writes = fr_conn.execute(
            "SELECT value FROM meta WHERE key = 'writes_enabled'"
        ).fetchone()[0]
        fr_epoch = int(
            fr_conn.execute(
                "SELECT value FROM meta WHERE key = 'journal_epoch'"
            ).fetchone()[0]
        )
        fr_conn.close()
        fr_admit_gen = int(
            authority_path_for(fr_path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        fr_install_gen = int(
            install_path_for(fr_path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        _assert(fr_drift == "1" and fr_writes == "0", "R2 admit must force writes-off")
        _assert(fr_admit_gen == fr_epoch == fr_install_gen, "R2 admit binds DB/.install/authority")
        _assert(fr_admit_gen > fr_live_gen, "R2 admit must advance past live authority")
        _assert(fr_admit_gen > fr_restored_gen, "R2 admit must not rewind to restored epoch")
    finally:
        fr_admitted.close()
    with IntegrationJournal(fr_path) as journal:
        _assert(journal.can_dispatch(fr_issuer, fr_op) is False, "R2 reopen after admit still off")
    scenarios["full_db_install_rollback_requires_admit"] = {
        "issuer_id": fr_issuer,
        "operation_id": fr_op,
        "prior_receipt_id": 777,
        "reopen_refused_without_admit": True,
        "can_dispatch_after_admit": False,
        "writes_enabled_after_admit": False,
        "authority_retained_across_restore": True,
        "authority_advanced_on_admit": True,
    }
    synthetic_ids["full_restore_operation_id"] = fr_op

    # --- R3: multi-restore must not rewind authority to re-arm a prepared create ---
    mr_path = work_dir / "r3-multi-restore.sqlite3"
    with IntegrationJournal(mr_path) as journal:
        mr_issuer = journal.ensure_issuer()
    mr_a_db = mr_path.read_bytes()
    mr_a_install = install_path_for(mr_path).read_bytes()
    mr_gen_a = int(mr_a_install.decode("utf-8").strip().splitlines()[1])
    with IntegrationJournal(mr_path) as journal:
        mr_op = new_operation_id()
        mr_prepared = journal.prepare_intent(
            issuer_id=mr_issuer,
            operation_id=mr_op,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-multi-restore"},
        )
    mr_b_db = mr_path.read_bytes()
    mr_b_install = install_path_for(mr_path).read_bytes()
    mr_gen_b = int(mr_b_install.decode("utf-8").strip().splitlines()[1])
    _assert(mr_gen_b > mr_gen_a, "R3 backup B generation must advance past A")
    with IntegrationJournal(mr_path) as journal:
        journal.begin_dispatch(mr_issuer, mr_op)
        journal.record_receipt(
            mr_issuer,
            mr_op,
            vikunja_id=888,
            project_id=7,
            marker=mr_prepared.marker,
            effect={"id": 888},
        )
    mr_gen_dispatch = int(
        authority_path_for(mr_path).read_text(encoding="utf-8").strip().splitlines()[1]
    )
    _assert(mr_gen_dispatch > mr_gen_b, "R3 dispatch must advance authority past B")

    mr_path.write_bytes(mr_a_db)
    install_path_for(mr_path).write_bytes(mr_a_install)
    mr_admitted_a = IntegrationJournal(mr_path)
    mr_admitted_a.admit_restored_backup()
    try:
        mr_admit_a_gen = int(
            authority_path_for(mr_path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        _assert(mr_admit_a_gen > mr_gen_dispatch, "R3 admit A must advance past live authority")
        _assert(mr_admitted_a.get_issuer() == mr_issuer, "R3 admit A preserves issuer")
        _assert(mr_admitted_a.get_operation(mr_issuer, mr_op) is None, "R3 A lacks create key")
    finally:
        mr_admitted_a.close()

    with IntegrationJournal(mr_path) as journal:
        journal.set_restore_drift(False)
        mr_resume = new_operation_id()
        journal.prepare_intent(
            issuer_id=mr_issuer,
            operation_id=mr_resume,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-multi-restore-resume"},
        )
    mr_gen_resume = int(
        authority_path_for(mr_path).read_text(encoding="utf-8").strip().splitlines()[1]
    )
    _assert(mr_gen_resume > mr_admit_a_gen, "R3 resume must advance from admit incarnation")

    mr_path.write_bytes(mr_b_db)
    install_path_for(mr_path).write_bytes(mr_b_install)
    _assert(
        int(authority_path_for(mr_path).read_text(encoding="utf-8").strip().splitlines()[1])
        == mr_gen_resume,
        "R3 live authority must survive restore of B",
    )
    mr_b_open_refused = False
    try:
        with IntegrationJournal(mr_path) as journal:
            _assert(journal.can_dispatch(mr_issuer, mr_op) is False, "R3 B must not open for dispatch")
            journal.begin_dispatch(mr_issuer, mr_op)
            raise StandError("R3 restoring B must not pass ordinary open/dispatch")
    except JournalError as exc:
        mr_b_open_refused = True
        _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R3 B ordinary open outcome")
        _assert(
            bool(exc.details.get("authority_mismatch") or exc.details.get("full_backup_rollback")),
            "R3 B authority fence details",
        )
    _assert(mr_b_open_refused, "R3 restoring B must refuse ordinary open")

    mr_admitted_b = IntegrationJournal(mr_path)
    mr_admitted_b.admit_restored_backup()
    try:
        mr_admit_b_gen = int(
            authority_path_for(mr_path).read_text(encoding="utf-8").strip().splitlines()[1]
        )
        _assert(mr_admit_b_gen > mr_gen_resume, "R3 admit B must rotate past resume generation")
        _assert(mr_admit_b_gen not in {mr_gen_a, mr_gen_b, mr_gen_dispatch}, "R3 never reuses old gens")
        _assert(mr_admitted_b.get_issuer() == mr_issuer, "R3 admit B preserves issuer")
        mr_loaded = mr_admitted_b.get_operation(mr_issuer, mr_op)
        _assert(mr_loaded is not None, "R3 admit B retains prepared key")
        assert mr_loaded is not None
        _assert(mr_loaded.state is OperationState.PREPARED, "R3 B snapshot stays prepared")
        _assert(mr_admitted_b.can_dispatch(mr_issuer, mr_op) is False, "R3 admit B keeps dispatch off")
        try:
            mr_admitted_b.begin_dispatch(mr_issuer, mr_op)
            raise StandError("R3 admit B must not authorize second begin_dispatch")
        except JournalError as exc:
            _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R3 admit B dispatch outcome")
        mr_events = [row["event_type"] for row in mr_admitted_b.history()]
        _assert("restored_backup_admitted" in mr_events, "R3 admit history retained")
    finally:
        mr_admitted_b.close()

    mr_path.write_bytes(mr_b_db)
    install_path_for(mr_path).write_bytes(mr_b_install)
    mr_reuse_refused = False
    try:
        with IntegrationJournal(mr_path):
            raise StandError("R3 former generation must not re-open without admit")
    except JournalError as exc:
        mr_reuse_refused = True
        _assert(exc.outcome is Outcome.DEPENDENCY_UNAVAILABLE, "R3 former generation open outcome")
    _assert(mr_reuse_refused, "R3 formerly used generation must not re-admit B")

    scenarios["multi_restore_authority_monotonic"] = {
        "issuer_id": mr_issuer,
        "operation_id": mr_op,
        "prior_receipt_id": 888,
        "backup_a_generation": mr_gen_a,
        "backup_b_generation": mr_gen_b,
        "dispatch_generation": mr_gen_dispatch,
        "admit_a_generation": mr_admit_a_gen,
        "resume_generation": mr_gen_resume,
        "admit_b_generation": mr_admit_b_gen,
        "b_ordinary_open_refused": True,
        "can_dispatch_after_admit_b": False,
        "former_generation_reopen_refused": True,
    }
    synthetic_ids["multi_restore_operation_id"] = mr_op

    # --- F1: create→update→complete retains verified task M; unmarked manual OK ---
    marker_path = work_dir / "f1-marker-chain.sqlite3"
    with IntegrationJournal(marker_path) as journal:
        mc_issuer = journal.ensure_issuer()
        mc_create = new_operation_id()
        mc_update = new_operation_id()
        mc_complete = new_operation_id()
        mc_created = journal.prepare_intent(
            issuer_id=mc_issuer,
            operation_id=mc_create,
            op_type="create",
            project_id=7,
            payload={"title": "EA010-SYNTHETIC-suite-marker-chain"},
        )
        mc_marker = mc_created.marker
        _assert(mc_marker == build_marker(mc_issuer, mc_create), "create mints M from create op")
        journal.begin_dispatch(mc_issuer, mc_create)
        journal.record_receipt(
            mc_issuer,
            mc_create,
            vikunja_id=4242,
            project_id=7,
            marker=mc_marker,
            effect={"id": 4242, "description": mc_marker + "\n"},
        )
        mc_updated = journal.prepare_intent(
            issuer_id=mc_issuer,
            operation_id=mc_update,
            op_type="update",
            project_id=7,
            target_id=4242,
            payload={"title": "EA010-SYNTHETIC-suite-marker-updated"},
        )
        _assert(mc_updated.marker == mc_marker, "update retains create M")
        _assert(
            mc_updated.marker != build_marker(mc_issuer, mc_update),
            "update must not mint M from new operation_id",
        )
        with journal.dispatch_critical_section() as section:
            section.begin_dispatch(mc_issuer, mc_update)
            mc_upd_receipt = section.record_receipt(
                mc_issuer,
                mc_update,
                vikunja_id=4242,
                project_id=7,
                marker=mc_marker,
                effect={
                    "id": 4242,
                    "title": "EA010-SYNTHETIC-suite-marker-updated",
                    "description": mc_marker + "\n",
                },
            )
        _assert(mc_upd_receipt.receipt is not None, "update receipt persisted")
        assert mc_upd_receipt.receipt is not None
        _assert(mc_upd_receipt.receipt["marker"] == mc_marker, "update receipt keeps create M")
        mc_completed = journal.prepare_intent(
            issuer_id=mc_issuer,
            operation_id=mc_complete,
            op_type="complete",
            project_id=7,
            target_id=4242,
            payload={"done": True},
        )
        _assert(mc_completed.marker == mc_marker, "complete retains create M")
        with journal.dispatch_critical_section() as section:
            section.begin_dispatch(mc_issuer, mc_complete)
            mc_cmp_receipt = section.record_receipt(
                mc_issuer,
                mc_complete,
                vikunja_id=4242,
                project_id=7,
                marker=mc_marker,
                effect={"id": 4242, "done": True, "description": mc_marker + "\n"},
            )
        _assert(mc_cmp_receipt.receipt is not None, "complete receipt persisted")
        assert mc_cmp_receipt.receipt is not None
        _assert(mc_cmp_receipt.receipt["marker"] == mc_marker, "complete receipt keeps create M")
    scenarios["create_update_complete_retains_marker"] = {
        "issuer_id": mc_issuer,
        "create_operation_id": mc_create,
        "update_operation_id": mc_update,
        "complete_operation_id": mc_complete,
        "vikunja_id": 4242,
        "marker": mc_marker,
        "update_marker": mc_updated.marker,
        "complete_marker": mc_completed.marker,
    }
    synthetic_ids["marker_chain_create_operation_id"] = mc_create
    synthetic_ids["marker_chain_update_operation_id"] = mc_update
    synthetic_ids["marker_chain_complete_operation_id"] = mc_complete

    unmarked_path = work_dir / "f1-unmarked-manual.sqlite3"
    with IntegrationJournal(unmarked_path) as journal:
        um_issuer = journal.ensure_issuer()
        um_op = new_operation_id()
        um_prepared = journal.prepare_intent(
            issuer_id=um_issuer,
            operation_id=um_op,
            op_type="complete",
            project_id=7,
            target_id=4243,
            payload={"done": True},
        )
        _assert(um_prepared.marker == "", "unmarked manual prepares empty marker")
        with journal.dispatch_critical_section() as section:
            section.begin_dispatch(um_issuer, um_op)
            um_receipt = section.record_receipt(
                um_issuer,
                um_op,
                vikunja_id=4243,
                project_id=7,
                marker=None,
                effect={"id": 4243, "done": True, "description": "manual synthetic task"},
            )
        _assert(um_receipt.receipt is not None, "unmarked receipt persisted")
        assert um_receipt.receipt is not None
        _assert(um_receipt.receipt["marker"] == "", "unmarked receipt marker empty")
    scenarios["unmarked_manual_complete"] = {
        "issuer_id": um_issuer,
        "operation_id": um_op,
        "vikunja_id": 4243,
        "prepared_marker": um_prepared.marker,
        "receipt_marker": um_receipt.receipt["marker"],
    }
    synthetic_ids["unmarked_manual_operation_id"] = um_op

    # Network must not have been required: no sockets opened by suite logic beyond local FS.
    scenarios["no_network"] = {"http_invoked": False, "suite": "journal"}

    _assert(len(scenarios) > 0, "journal suite executed_count must be positive")
    return {
        "scenarios": scenarios,
        "scenario_names": sorted(scenarios),
        "executed_count": len(scenarios),
        "synthetic_ids": synthetic_ids,
        "journal_path": str(journal_path),
        "lock_path": str(lock_path_for(journal_path)),
        "sqlite_user_version": JOURNAL_SQLITE_USER_VERSION,
    }


def run_journal_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite journal supports only isolated mode, got {mode!r}")
    profile = load_profile()
    work_dir = _work_dir(profile)
    cleanup_ok = False
    cleanup_error: str | None = None
    try:
        payload = run_journal_scenarios(work_dir)
        result = {
            "run_id": work_dir.name,
            "synthetic_ids": payload["synthetic_ids"],
            "scenarios": payload["scenarios"],
            "scenario_names": payload["scenario_names"],
            "executed_count": payload["executed_count"],
            "source": _source_digest(worktree),
            "argv": list(argv),
            "suite": "journal",
            "mode": mode,
            "journal_path": payload["journal_path"],
            "sqlite_user_version": payload["sqlite_user_version"],
            "work_dir": str(work_dir),
        }
        if result["executed_count"] <= 0:
            raise StandError("journal suite executed_count must be positive")
        return result
    finally:
        try:
            shutil.rmtree(work_dir, ignore_errors=False)
            cleanup_ok = not work_dir.exists()
        except Exception as exc:  # noqa: BLE001
            cleanup_error = str(exc)
            cleanup_ok = False
        # Attach cleanup onto result if it still exists in locals.
        # Caller contract: cleanup confirmed required.
        if "result" in locals():
            result["cleanup"] = {"confirmed": cleanup_ok, "error": cleanup_error}
            if not cleanup_ok:
                raise StandError(f"journal suite cleanup was not confirmed: {cleanup_error}")
        elif not cleanup_ok and cleanup_error:
            # Best-effort note; original exception dominates.
            pass
