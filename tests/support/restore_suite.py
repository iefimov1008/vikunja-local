"""Isolated restore suite (s09): real Vikunja manual copy + empty-target restore."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from vikunja_local.api import Outcome, TokenRef, VikunjaClient, build_create_description, extract_marker
from vikunja_local.dispatcher import IntegrationDispatcher
from vikunja_local.hermes_bridge import SOURCE_KIND, meeting_user_portion
from vikunja_local.journal import IntegrationJournal, new_operation_id
from vikunja_local.restore import (
    ContourPaths,
    RestoreError,
    attachment_checksums,
    create_manual_copy,
    reconcile_restored_contour,
    restore_into_empty_target,
    verify_consistent_copy,
)
from vikunja_local.storage import install_path_for

from .stand import (
    IsolatedStand,
    StandError,
    http_json,
    listeners,
    load_profile,
    owned_root_path,
    sha256_file,
)

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

WORKTREE = Path(__file__).resolve().parents[2]
_OPENER = build_opener(ProxyHandler({}))


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "vikunja_local" / "restore.py",
        worktree / "scripts" / "ea010_backup.py",
        worktree / "tests" / "test_restore.py",
        worktree / "tests" / "support" / "restore_suite.py",
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
    diff = subprocess.run(
        ["git", "diff", "HEAD", "--", *[str(p.relative_to(worktree)) for p in paths]],
        cwd=worktree,
        capture_output=True,
        check=False,
    )
    return {
        "sha256": digest.hexdigest(),
        "head": head.stdout.strip() if head.returncode == 0 else "unknown",
        "diff_sha256": hashlib.sha256(diff.stdout or b"").hexdigest(),
    }


def _upload_attachment(
    *,
    host: str,
    port: int,
    task_id: int,
    token: str,
    filename: str,
    payload: bytes,
) -> dict[str, Any]:
    boundary = f"ea010boundary{uuid.uuid4().hex}"
    body = b"".join(
        [
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="files"; filename="{filename}"\r\n'.encode(),
            b"Content-Type: application/octet-stream\r\n\r\n",
            payload,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    request = Request(
        f"http://{host}:{port}/api/v1/tasks/{task_id}/attachments",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "application/json",
        },
        method="PUT",
    )
    try:
        with _OPENER.open(request, timeout=10) as response:
            raw = response.read(1024 * 1024)
            status = response.status
            data = json.loads(raw.decode()) if raw else None
    except HTTPError as exc:
        raw = exc.read(1024 * 1024) if hasattr(exc, "read") else b""
        raise StandError(f"attachment upload HTTP {exc.code}: {raw[:200]!r}") from exc
    except URLError as exc:
        raise StandError(f"attachment upload failed: {exc}") from exc
    _assert(status == 200 and isinstance(data, dict), f"attachment upload bad response HTTP {status}")
    success = data.get("success") or []
    _assert(success, f"attachment upload produced no success entries: {data}")
    return success[0]


def _download_attachment(
    *,
    host: str,
    port: int,
    task_id: int,
    attachment_id: int,
    token: str,
) -> bytes:
    request = Request(
        f"http://{host}:{port}/api/v1/tasks/{task_id}/attachments/{attachment_id}",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with _OPENER.open(request, timeout=10) as response:
            return response.read(1024 * 1024)
    except HTTPError as exc:
        raise StandError(f"attachment download HTTP {exc.code}") from exc


def _write_token_file(path: Path, token: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, token.encode("utf-8"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return path


def _count_exact_marker(tasks: list[dict[str, Any]], marker: str) -> int:
    count = 0
    for item in tasks:
        extracted = extract_marker(item.get("description"))
        if extracted is not None and extracted["marker"] == marker:
            count += 1
    return count


def _list_project_tasks(
    *,
    host: str,
    port: int,
    project_id: int,
    token: str,
) -> list[dict[str, Any]]:
    status, payload = http_json(
        host,
        port,
        f"projects/{project_id}/tasks",
        token=token,
    )
    _assert(status == 200, f"list tasks HTTP {status}")
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        items = payload.get("tasks") or payload.get("items") or []
        return [item for item in items if isinstance(item, dict)]
    return []


def run_restore_scenarios(stand: IsolatedStand, worktree: Path) -> dict[str, Any]:
    profile = stand.profile
    scenarios: dict[str, Any] = {}
    executed = 0
    older_role: str | None = None
    restored_role: str | None = None
    session = stand.synthetic_ids
    token = stand._session_token  # noqa: SLF001 — suite-owned session
    _assert(token is not None, "session token required")
    project_id = int(session["project_id"])
    fixture_marker = str(session["marker"])
    username = stand._username  # noqa: SLF001
    password = stand._password  # noqa: SLF001

    journal_path = stand.run_dir / "journal" / "integration.sqlite3"
    journal_path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(journal_path.parent, 0o700)

    token_path = _write_token_file(stand.run_dir / "secrets" / "api.token", token)
    backlinks: list[dict[str, Any]] = []

    def _link_callback(payload: dict[str, Any]) -> dict[str, Any]:
        backlinks.append(dict(payload))
        return {"ok": True, "synthetic": True, "vikunja_id": payload.get("vikunja_id")}

    try:
        client = VikunjaClient(
            api_origin=f"http://{profile.listen_host}:{profile.vikunja_port}",
            project_id=project_id,
            token_ref=TokenRef(kind="file", path=token_path),
            token=token,
        )
        proposal_id = f"ea010-synthetic-proposal-{stand.run_id}"
        source_key = f"ea010-synthetic-hermes-{stand.run_id}"
        source_url = f"http://127.0.0.1:9/synthetic/proposals/{proposal_id}"
        user_portion = meeting_user_portion(
            proposal_id=proposal_id,
            meeting_id=f"ea010-synthetic-meeting-{stand.run_id}",
            meeting_title=stand.fixture_name("restore-meeting"),
            meeting_when="2026-11-13T10:00:00Z (UTC)",
            quote="EA010 synthetic restore brief",
            version="1",
            source_url=source_url,
        )

        journal = IntegrationJournal(journal_path)
        journal.open()
        try:
            issuer = journal.ensure_issuer()
            op = new_operation_id()
            dispatcher = IntegrationDispatcher(
                journal,
                client,
                link_callback=_link_callback,
            )
            created = dispatcher.execute_create(
                issuer_id=issuer,
                operation_id=op,
                title=stand.fixture_name("restore-title"),
                project_id=project_id,
                user_portion=user_portion,
                source_kind=SOURCE_KIND,
                source_key=source_key,
            )
            _assert(created.ok, f"dispatcher create failed: {created.outcome} {created.details}")
            _assert(created.vikunja_id is not None, "dispatcher create missing vikunja_id")
            task_id = int(created.vikunja_id)
            prepared_marker = created.operation.marker if created.operation else ""
            _assert(created.linked is True, "hermes backlink callback was not invoked")
            _assert(backlinks and backlinks[0].get("vikunja_id") == task_id, "backlink payload mismatch")
        finally:
            journal.close()

        status, task = stand.api(f"tasks/{task_id}", token=token)
        _assert(status == 200 and isinstance(task, dict), f"seeded task read HTTP {status}")
        description = str(task.get("description") or "")
        extracted = extract_marker(description)
        _assert(extracted is not None and extracted["marker"] == prepared_marker, "exact marker header missing")
        _assert(source_url in description, "source URL backlink missing from description")
        _assert(f"[Open proposal]({source_url})" in description, "markdown source link missing")
        scenarios["hermes_source_backlink_seeded"] = {
            "task_id": task_id,
            "source_kind": SOURCE_KIND,
            "source_key": source_key,
            "source_url": source_url,
            "backlink_ok": True,
            "operation_id": op,
            "marker": prepared_marker,
        }
        executed += 1

        attachment_bytes = b"EA010-SYNTHETIC-RESTORE-ATTACHMENT\n"
        attachment_sha = hashlib.sha256(attachment_bytes).hexdigest()
        uploaded = _upload_attachment(
            host=profile.listen_host,
            port=profile.vikunja_port,
            task_id=task_id,
            token=token,
            filename="ea010-synthetic-restore.txt",
            payload=attachment_bytes,
        )
        attachment_id = int(uploaded["id"])
        downloaded = _download_attachment(
            host=profile.listen_host,
            port=profile.vikunja_port,
            task_id=task_id,
            attachment_id=attachment_id,
            token=token,
        )
        _assert(hashlib.sha256(downloaded).hexdigest() == attachment_sha, "upload checksum mismatch")
        scenarios["synthetic_attachment_seeded"] = {
            "task_id": task_id,
            "attachment_id": attachment_id,
            "sha256": attachment_sha,
        }
        executed += 1

        scenarios["journal_mapping_seeded"] = {
            "issuer_id": issuer,
            "operation_id": op,
            "vikunja_id": task_id,
            "source_kind": SOURCE_KIND,
            "source_key": source_key,
            "prepared_marker": prepared_marker,
        }
        executed += 1

        # Distinct older consistent copy (A confirmed only) before pending B is added.
        # Source Vikunja must be stopped so create_manual_copy can establish DB quiescence.
        older_copy = stand.claim_owned_root(prefix="EA010-SYNTHETIC-older-")
        source = ContourPaths.from_stand_root(stand.run_dir)
        if stand._watchdog is not None:  # noqa: SLF001 — pause owned watchdog across quiescent copy
            stand._watchdog.stop()
            stand._watchdog = None  # noqa: SLF001
        stand.stop_process()
        stand._child = None  # noqa: SLF001
        # Intentional stop before the planned revoke: drop the session so a
        # mid-copy failure cannot make stand.cleanup() HTTP-revoke a dead process.
        stand._session_token = None  # noqa: SLF001
        time.sleep(0.2)
        older_manifest = create_manual_copy(
            source,
            older_copy,
            secrets_extra={
                "username": username,
                "password": password,
                "run_id": stand.run_id,
                "generation": "older",
            },
            note="Older consistent copy before pending prepared intent.",
        )
        verify_consistent_copy(older_copy)
        scenarios["older_journal_snapshot"] = {
            "copy_id": older_manifest["copy_id"],
            "receipt_count": older_manifest["components"]["links"]["receipt_count"],
            "mapping_count": older_manifest["components"]["links"]["mapping_count"],
            "source_quiescence": older_manifest.get("source_quiescence"),
        }
        executed += 1

        # Restart the same synthetic contour to create pending B against live Vikunja.
        stand.start()
        stand.wait_ready()
        status, login = http_json(
            profile.listen_host,
            profile.vikunja_port,
            "login",
            method="POST",
            body={"username": username, "password": password},
        )
        _assert(status == 200 and isinstance(login, dict) and "token" in login, f"relogin HTTP {status}")
        token = login["token"]
        stand._session_token = token  # noqa: SLF001 — suite-owned session refresh

        # Pending prepared B + observed Vikunja task with the same exact marker
        # (simulates old backup / lost receipt while the live task already exists).
        pending_title = stand.fixture_name("restore-pending")
        pending_user_portion = f"{fixture_marker}\nEA010 synthetic pending observed task"
        with IntegrationJournal(journal_path) as journal:
            pending_op = new_operation_id()
            pending = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=project_id,
                payload={"title": pending_title, "user_portion": pending_user_portion},
                source_kind=SOURCE_KIND,
                source_key=f"ea010-synthetic-pending-{stand.run_id}",
            )
        pending_description = build_create_description(
            issuer,
            pending_op,
            pending_user_portion,
        )
        status, pending_task = stand.api(
            f"projects/{project_id}/tasks",
            "PUT",
            {
                "title": stand.fixture_name("restore-pending-title"),
                "description": pending_description,
            },
            token=token,
        )
        _assert(status == 201 and isinstance(pending_task, dict), f"pending task create HTTP {status}")
        pending_task_id = int(pending_task["id"])
        scenarios["pending_prepared_with_observed_task"] = {
            "operation_id": pending_op,
            "marker": pending.marker,
            "observed_task_id": pending_task_id,
            "receipt": False,
        }
        executed += 1

        # Revoke while source still runs, then clear session so later cleanup
        # after an intentional stop does not treat revoke failure as product fail.
        revoke_evidence = stand.revoke_token()
        stand._session_token = None  # noqa: SLF001 — revoke already proven
        scenarios["pre_backup_revoke"] = {
            "revoked": True,
            "revoked_token_http": revoke_evidence.get("revoked_token_http"),
        }
        executed += 1

        # Stop source Vikunja before the current consistent DB/files copy.
        stand.stop_process()
        stand._child = None  # noqa: SLF001
        time.sleep(0.2)

        copy_dir = stand.claim_owned_root(prefix="EA010-SYNTHETIC-copy-")
        manifest = create_manual_copy(
            source,
            copy_dir,
            secrets_extra={
                "username": username,
                "password": password,
                "run_id": stand.run_id,
                "generation": "current",
            },
        )
        verified = verify_consistent_copy(copy_dir)
        scenarios["manual_consistent_copy"] = {
            "copy_id": manifest["copy_id"],
            "scheduler": False,
            "rpo": None,
            "rto": None,
            "retention": None,
            "authority_included": False,
            "mini_local_copy_not_device_loss_protection": True,
            "verified": verified["ok"],
            "components": sorted(manifest["components"]),
            "receipt_count": manifest["components"]["links"]["receipt_count"],
            "mapping_count": manifest["components"]["links"]["mapping_count"],
        }
        executed += 1

        # Partial copy must refuse. Populate the claimed inode in place — never
        # rmtree/recreate the registered directory (NFR-18 durable identity).
        partial_dir = stand.claim_owned_root(prefix="EA010-SYNTHETIC-partial-")
        for child in copy_dir.iterdir():
            dest = partial_dir / child.name
            if child.is_dir():
                shutil.copytree(child, dest)
            else:
                shutil.copy2(child, dest)
        os.chmod(partial_dir, 0o700)
        (partial_dir / "journal" / "integration.sqlite3").unlink()
        partial_refused = False
        partial_code = None
        try:
            verify_consistent_copy(partial_dir)
        except RestoreError as exc:
            partial_refused = True
            partial_code = exc.code
        _assert(partial_refused and partial_code == "partial_copy", f"partial not refused: {partial_code}")
        scenarios["partial_copy_refuses"] = {"refused": True, "code": partial_code}
        executed += 1

        # Corrupt attachment refuses (same in-place populate rule).
        corrupt_dir = stand.claim_owned_root(prefix="EA010-SYNTHETIC-corrupt-")
        for child in copy_dir.iterdir():
            dest = corrupt_dir / child.name
            if child.is_dir():
                shutil.copytree(child, dest)
            else:
                shutil.copy2(child, dest)
        os.chmod(corrupt_dir, 0o700)
        files = [p for p in (corrupt_dir / "attachments").rglob("*") if p.is_file()]
        _assert(files, "no attachment files in copy")
        files[0].write_bytes(b"tampered-attachment\n")
        corrupt_refused = False
        corrupt_code = None
        try:
            verify_consistent_copy(corrupt_dir)
        except RestoreError as exc:
            corrupt_refused = True
            corrupt_code = exc.code
        _assert(corrupt_refused and corrupt_code == "copy_drift", f"corrupt not refused: {corrupt_code}")
        scenarios["corrupt_copy_refuses"] = {"refused": True, "code": corrupt_code}
        executed += 1

        # Restore distinct older snapshot first: writes off until reconcile; replay same ID.
        older_target = stand.claim_owned_root(prefix="EA010-SYNTHETIC-older-target-")
        older_restored = restore_into_empty_target(
            older_copy,
            older_target,
            listen_host=profile.listen_host,
            listen_port=profile.vikunja_port,
        )
        _assert(older_restored["writes_enabled"] is False, "older restore writes must stay off")
        _assert(older_restored["restore_drift"] is True, "older restore_drift must be set")
        older_role = "restore-older"
        older_child = stand.start_owned_vikunja(target=older_target, role=older_role)
        status, login = http_json(
            profile.listen_host,
            profile.vikunja_port,
            "login",
            method="POST",
            body={"username": username, "password": password},
        )
        _assert(status == 200 and isinstance(login, dict) and "token" in login, f"older login HTTP {status}")
        older_token = login["token"]
        status, older_task = http_json(
            profile.listen_host,
            profile.vikunja_port,
            f"tasks/{task_id}",
            token=older_token,
        )
        _assert(status == 200 and isinstance(older_task, dict), f"older task read HTTP {status}")
        older_attachments = attachment_checksums(older_target / "files")

        # P1 regression: newer Vikunja identity entirely absent from older journal.
        # Entire operation/mapping missing → writes stay fenced; replay must not create.
        lost_op = new_operation_id()
        lost_user_portion = "EA010 synthetic lost-op observed after older journal"
        lost_description = build_create_description(issuer, lost_op, lost_user_portion)
        status, lost_task = http_json(
            profile.listen_host,
            profile.vikunja_port,
            f"projects/{project_id}/tasks",
            method="PUT",
            body={
                "title": stand.fixture_name("restore-lost-op"),
                "description": lost_description,
            },
            token=older_token,
        )
        _assert(status == 201 and isinstance(lost_task, dict), f"lost-op task create HTTP {status}")
        lost_task_id = int(lost_task["id"])
        lost_extracted = extract_marker(lost_task.get("description"))
        _assert(lost_extracted is not None, "lost-op exact marker missing")
        lost_marker = lost_extracted["marker"]
        with IntegrationJournal(Path(older_restored["journal_path"])) as journal:
            _assert(journal.get_operation(issuer, lost_op) is None, "lost op must be absent from older journal")
        older_unaccounted_blocked = False
        older_unaccounted_code = None
        older_unaccounted_reasons: list[str] = []
        try:
            reconcile_restored_contour(
                journal_path=older_restored["journal_path"],
                links=older_restored["links"],
                observed_tasks={
                    task_id: {
                        "id": task_id,
                        "project_id": project_id,
                        "description": older_task.get("description"),
                        "title": older_task.get("title"),
                    },
                    lost_task_id: {
                        "id": lost_task_id,
                        "project_id": project_id,
                        "description": lost_task.get("description"),
                        "title": lost_task.get("title"),
                    },
                },
                observed_attachment_checksums=older_attachments,
                expected_attachment_checksums=older_attachments,
            )
        except RestoreError as exc:
            older_unaccounted_blocked = True
            older_unaccounted_code = exc.code
            _assert(exc.details.get("creates_issued") is False, "unaccounted must not create")
            _assert(exc.details.get("writes_enabled") is False, "unaccounted must report writes off")
            _assert(exc.details.get("restore_drift") is True, "unaccounted must keep restore_drift")
            older_unaccounted_reasons = [
                str(item.get("reason"))
                for item in (exc.details.get("mismatches") or [])
                if isinstance(item, dict)
            ]
            _assert(
                "observed_marker_unaccounted" in older_unaccounted_reasons,
                f"expected observed_marker_unaccounted, got {older_unaccounted_reasons}",
            )
        _assert(
            older_unaccounted_blocked and older_unaccounted_code == "reconcile_failed",
            f"unaccounted marker not blocked: {older_unaccounted_code}",
        )
        older_token_path = _write_token_file(older_target / "secrets" / "api.token", older_token)
        older_client = VikunjaClient(
            api_origin=f"http://{profile.listen_host}:{profile.vikunja_port}",
            project_id=project_id,
            token_ref=TokenRef(kind="file", path=older_token_path),
            token=older_token,
        )
        with IntegrationJournal(Path(older_restored["journal_path"])) as journal:
            older_lost_dispatcher = IntegrationDispatcher(journal, older_client)
            lost_replay = older_lost_dispatcher.execute_create(
                issuer_id=issuer,
                operation_id=lost_op,
                title=stand.fixture_name("restore-lost-op"),
                project_id=project_id,
                user_portion=lost_user_portion,
                source_kind=SOURCE_KIND,
                source_key=f"ea010-synthetic-lost-{stand.run_id}",
            )
            lost_create_http = older_lost_dispatcher.create_http_attempts(issuer, lost_op)
            _assert(journal.get_operation(issuer, lost_op) is None, "lost replay must not seed journal op")
            _assert(journal.can_dispatch(issuer, lost_op) is False, "lost replay must stay dispatch-closed")
        _assert(not lost_replay.ok, f"lost replay must fail closed, got {lost_replay.outcome} {lost_replay.details}")
        _assert(lost_create_http == 0, "lost replay issued create HTTP")
        older_listed_before_delete = _list_project_tasks(
            host=profile.listen_host,
            port=profile.vikunja_port,
            project_id=project_id,
            token=older_token,
        )
        lost_marker_count = _count_exact_marker(older_listed_before_delete, lost_marker)
        _assert(lost_marker_count == 1, f"lost replay duplicated marker count={lost_marker_count}")
        scenarios["older_journal_unaccounted_marker_fenced"] = {
            "copy_id": older_manifest["copy_id"],
            "accounted_task_id": task_id,
            "lost_operation_id": lost_op,
            "lost_task_id": lost_task_id,
            "lost_marker": lost_marker,
            "reconcile_code": older_unaccounted_code,
            "writes_enabled": False,
            "restore_drift": True,
            "creates_issued": False,
            "replay_ok": lost_replay.ok,
            "replay_outcome": getattr(lost_replay.outcome, "value", str(lost_replay.outcome)),
            "create_http_attempts": lost_create_http,
            "exact_marker_count": lost_marker_count,
            "mismatch_reasons": older_unaccounted_reasons,
        }
        executed += 1

        # P1: malformed extra inventory row alongside a valid confirmed task must fence.
        malformed_extra_blocked = False
        malformed_extra_code = None
        malformed_extra_reasons: list[str] = []
        try:
            reconcile_restored_contour(
                journal_path=older_restored["journal_path"],
                links=older_restored["links"],
                observed_tasks={
                    task_id: {
                        "id": task_id,
                        "project_id": project_id,
                        "description": older_task.get("description"),
                        "title": older_task.get("title"),
                    },
                    lost_task_id + 1000: {
                        "id": 999999,
                        "project_id": project_id,
                        "description": older_task.get("description"),
                        "title": "EA010-SYNTHETIC-malformed-extra",
                    },
                },
                observed_attachment_checksums=older_attachments,
                expected_attachment_checksums=older_attachments,
            )
        except RestoreError as exc:
            malformed_extra_blocked = True
            malformed_extra_code = exc.code
            _assert(exc.details.get("writes_enabled") is False, "malformed extra must keep writes off")
            _assert(exc.details.get("restore_drift") is True, "malformed extra must keep restore_drift")
            malformed_extra_reasons = [
                str(item.get("reason"))
                for item in (exc.details.get("mismatches") or [])
                if isinstance(item, dict)
            ]
            _assert(
                "observed_identity_malformed" in malformed_extra_reasons,
                f"expected observed_identity_malformed, got {malformed_extra_reasons}",
            )
        _assert(
            malformed_extra_blocked and malformed_extra_code == "reconcile_failed",
            f"malformed extra not blocked: {malformed_extra_code}",
        )
        with IntegrationJournal(Path(older_restored["journal_path"])) as journal:
            fresh_malformed = new_operation_id()
            try:
                journal.prepare_intent(
                    issuer_id=issuer,
                    operation_id=fresh_malformed,
                    op_type="create",
                    project_id=project_id,
                    payload={"title": stand.fixture_name("malformed-extra-create")},
                    source_kind=SOURCE_KIND,
                    source_key=f"ea010-synthetic-malformed-extra-{stand.run_id}",
                )
                raise StandError("malformed-extra fence allowed prepare_intent")
            except Exception as exc:  # noqa: BLE001 — JournalError expected under fence
                _assert("JournalError" in type(exc).__name__, f"expected JournalError, got {exc!r}")
            _assert(journal.can_dispatch(issuer, fresh_malformed) is False, "malformed extra rearmed create")
        scenarios["malformed_extra_identity_fenced"] = {
            "reconcile_code": malformed_extra_code,
            "writes_enabled": False,
            "restore_drift": True,
            "mismatch_reasons": malformed_extra_reasons,
            "create_http_attempts": 0,
        }
        executed += 1

        # Remove the synthetic newer identity so the consistent older reconcile path can proceed.
        status, _ = http_json(
            profile.listen_host,
            profile.vikunja_port,
            f"tasks/{lost_task_id}",
            method="DELETE",
            token=older_token,
        )
        _assert(status == 200, f"cleanup lost-op DELETE HTTP {status}")

        older_reconciled = reconcile_restored_contour(
            journal_path=older_restored["journal_path"],
            links=older_restored["links"],
            observed_tasks={
                task_id: {
                    "id": task_id,
                    "project_id": project_id,
                    "description": older_task.get("description"),
                    "title": older_task.get("title"),
                }
            },
            observed_attachment_checksums=older_attachments,
            expected_attachment_checksums=older_attachments,
        )
        _assert(older_reconciled["ok"] is True, "older reconcile should succeed")
        _assert(older_reconciled["writes_enabled"] is True, "older writes should reopen")
        with IntegrationJournal(Path(older_restored["journal_path"])) as journal:
            older_dispatcher = IntegrationDispatcher(journal, older_client)
            older_replay = older_dispatcher.execute_create(
                issuer_id=issuer,
                operation_id=op,
                title=stand.fixture_name("restore-title"),
                project_id=project_id,
                user_portion=user_portion,
                source_kind=SOURCE_KIND,
                source_key=source_key,
            )
        _assert(older_replay.ok, f"older replay failed: {older_replay.outcome} {older_replay.details}")
        _assert(older_replay.vikunja_id == task_id, "older replay changed task id")
        _assert(older_dispatcher.create_http_attempts(issuer, op) == 0, "older replay issued create HTTP")
        older_listed = _list_project_tasks(
            host=profile.listen_host,
            port=profile.vikunja_port,
            project_id=project_id,
            token=older_token,
        )
        older_marker_count = _count_exact_marker(older_listed, prepared_marker)
        _assert(older_marker_count == 1, f"older replay duplicate marker count={older_marker_count}")
        scenarios["older_snapshot_restore_replay"] = {
            "copy_id": older_manifest["copy_id"],
            "task_id": task_id,
            "replay_vikunja_id": older_replay.vikunja_id,
            "create_http_attempts": older_dispatcher.create_http_attempts(issuer, op),
            "exact_marker_count": older_marker_count,
            "writes_enabled": older_reconciled["writes_enabled"],
        }
        executed += 1
        stand.stop_owned_vikunja(older_role)
        older_role = None
        older_child = None
        time.sleep(0.2)

        target = stand.claim_owned_root(prefix="EA010-SYNTHETIC-target-")
        restored = restore_into_empty_target(
            copy_dir,
            target,
            listen_host=profile.listen_host,
            listen_port=profile.vikunja_port,
        )
        _assert(restored["writes_enabled"] is False, "writes must stay off after restore")
        _assert(restored["restore_drift"] is True, "restore_drift must be set")
        _assert(restored["admitted"] is True, "journal admit required")
        expected_attachments = attachment_checksums(target / "files")
        _assert(attachment_sha in expected_attachments.values(), "restored attachment checksum missing")
        scenarios["isolated_restore_writes_off"] = {
            "target": str(target),
            "writes_enabled": False,
            "restore_drift": True,
            "admitted": True,
            "attachment_files": len(expected_attachments),
            "working_contour_restore": False,
        }
        executed += 1

        restored_role = "restore-current"
        restored_child = stand.start_owned_vikunja(target=target, role=restored_role)
        status, login = http_json(
            profile.listen_host,
            profile.vikunja_port,
            "login",
            method="POST",
            body={"username": username, "password": password},
        )
        _assert(status == 200 and isinstance(login, dict) and "token" in login, f"restored login HTTP {status}")
        restored_token = login["token"]
        status, task = http_json(
            profile.listen_host,
            profile.vikunja_port,
            f"tasks/{task_id}",
            token=restored_token,
        )
        _assert(status == 200 and isinstance(task, dict), f"restored task read HTTP {status}")
        _assert(task.get("id") == task_id, "restored task id changed")
        restored_extracted = extract_marker(task.get("description"))
        _assert(restored_extracted is not None, "exact marker lost after restore")
        _assert(restored_extracted["marker"] == prepared_marker, "exact marker drift after restore")
        _assert(source_url in str(task.get("description") or ""), "restored source URL lost")
        status, pending_observed = http_json(
            profile.listen_host,
            profile.vikunja_port,
            f"tasks/{pending_task_id}",
            token=restored_token,
        )
        _assert(status == 200 and isinstance(pending_observed, dict), f"pending task read HTTP {status}")
        downloaded_after = _download_attachment(
            host=profile.listen_host,
            port=profile.vikunja_port,
            task_id=task_id,
            attachment_id=attachment_id,
            token=restored_token,
        )
        _assert(
            hashlib.sha256(downloaded_after).hexdigest() == attachment_sha,
            "restored attachment checksum mismatch",
        )
        scenarios["isolated_restore_preserves_ids"] = {
            "task_id": task_id,
            "project_id": project_id,
            "attachment_id": attachment_id,
            "attachment_sha256": attachment_sha,
            "marker_present": True,
            "source_url_present": True,
            "pending_task_id": pending_task_id,
        }
        executed += 1

        with IntegrationJournal(Path(restored["journal_path"])) as journal:
            mapped = journal.lookup_source(SOURCE_KIND, source_key)
            _assert(mapped is not None, "mapping missing after restore")
            _assert(mapped.operation_id == op, "mapping operation drift")
            _assert(journal.can_dispatch(issuer, op) is False, "dispatch open before reconcile")
            pending_mapped = journal.lookup_source(
                SOURCE_KIND,
                f"ea010-synthetic-pending-{stand.run_id}",
            )
            _assert(pending_mapped is not None, "pending mapping missing after restore")
            _assert(pending_mapped.state.value == "prepared", "pending should still be prepared pre-reconcile")
            unknown = new_operation_id()
            _assert(journal.can_dispatch(issuer, unknown) is False, "unknown create allowed")
        scenarios["mapping_and_unknown_create_closed"] = {
            "mapping_operation_id": op,
            "can_dispatch": False,
            "unknown_create_allowed": False,
            "pending_prepared": True,
        }
        executed += 1

        # Empty observed inventory must keep writes off (fence + fail).
        stale_blocked = False
        stale_code = None
        try:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks={},
                observed_attachment_checksums=expected_attachments,
                expected_attachment_checksums=expected_attachments,
            )
        except RestoreError as exc:
            stale_blocked = True
            stale_code = exc.code
            _assert(exc.details.get("creates_issued") is False, "stale reconcile must not create")
            _assert(exc.details.get("writes_enabled") is False, "stale must report writes off")
            _assert(exc.details.get("restore_drift") is True, "stale must keep restore_drift")
        _assert(stale_blocked and stale_code == "reconcile_failed", f"stale not blocked: {stale_code}")
        scenarios["old_or_inconsistent_keeps_writes_off"] = {
            "refused": True,
            "code": stale_code,
            "creates_issued": False,
            "older_copy_id": older_manifest["copy_id"],
            "current_copy_id": manifest["copy_id"],
            "distinct_older_snapshot": older_manifest["copy_id"] != manifest["copy_id"],
        }
        executed += 1

        observed_tasks = {
            task_id: {
                "id": task_id,
                "project_id": project_id,
                "description": task.get("description"),
                "title": task.get("title"),
            },
            pending_task_id: {
                "id": pending_task_id,
                "project_id": project_id,
                "description": pending_observed.get("description"),
                "title": pending_observed.get("title"),
            },
        }
        reconciled = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks=observed_tasks,
            observed_attachment_checksums=expected_attachments,
            expected_attachment_checksums=expected_attachments,
        )
        _assert(reconciled["ok"] is True, "reconcile should succeed")
        _assert(reconciled["creates_issued"] is False, "reconcile must not create")
        _assert(reconciled["writes_enabled"] is True, "writes should reopen after match")
        with IntegrationJournal(Path(restored["journal_path"])) as journal:
            recovered_pending = journal.get_operation(issuer, pending_op)
            _assert(recovered_pending is not None, "pending op missing after reconcile")
            _assert(recovered_pending.state.value == "confirmed", "pending must be recovered to receipt")
            _assert(int(recovered_pending.receipt["vikunja_id"]) == pending_task_id, "pending receipt id drift")
            _assert(recovered_pending.allows_first_http_dispatch() is False, "pending still armed")
        scenarios["reconcile_enables_writes_after_match"] = {
            "writes_enabled": reconciled["writes_enabled"],
            "matched_receipts": reconciled["matched_receipts"],
            "matched_mappings": reconciled["matched_mappings"],
            "creates_issued": False,
            "pending_recovered_task_id": pending_task_id,
        }
        executed += 1

        # Post-restore admitted writer replay: same IDs, exactly one correlated task each.
        restored_token_path = _write_token_file(target / "secrets" / "api.token", restored_token)
        restored_client = VikunjaClient(
            api_origin=f"http://{profile.listen_host}:{profile.vikunja_port}",
            project_id=project_id,
            token_ref=TokenRef(kind="file", path=restored_token_path),
            token=restored_token,
        )
        with IntegrationJournal(Path(restored["journal_path"])) as journal:
            dispatcher = IntegrationDispatcher(journal, restored_client)
            replay_a = dispatcher.execute_create(
                issuer_id=issuer,
                operation_id=op,
                title=stand.fixture_name("restore-title"),
                project_id=project_id,
                user_portion=user_portion,
                source_kind=SOURCE_KIND,
                source_key=source_key,
            )
            replay_b = dispatcher.execute_create(
                issuer_id=issuer,
                operation_id=pending_op,
                title=pending_title,
                project_id=project_id,
                user_portion=pending_user_portion,
                source_kind=SOURCE_KIND,
                source_key=f"ea010-synthetic-pending-{stand.run_id}",
            )
            create_http_a = dispatcher.create_http_attempts(issuer, op)
            create_http_b = dispatcher.create_http_attempts(issuer, pending_op)
        _assert(replay_a.ok, f"replay A failed: {replay_a.outcome} {replay_a.details}")
        _assert(replay_a.vikunja_id == task_id, "replay A changed id")
        _assert(create_http_a == 0, "replay A issued create HTTP")
        _assert(replay_b.ok, f"replay B failed: {replay_b.outcome} {replay_b.details}")
        _assert(replay_b.vikunja_id == pending_task_id, "replay B changed id")
        _assert(create_http_b == 0, "replay B issued create HTTP")
        listed = _list_project_tasks(
            host=profile.listen_host,
            port=profile.vikunja_port,
            project_id=project_id,
            token=restored_token,
        )
        count_a = _count_exact_marker(listed, prepared_marker)
        count_b = _count_exact_marker(listed, pending.marker)
        _assert(count_a == 1, f"correlated A count={count_a}")
        _assert(count_b == 1, f"correlated B count={count_b}")
        scenarios["post_restore_dispatcher_replay"] = {
            "task_id": task_id,
            "pending_task_id": pending_task_id,
            "replay_a_id": replay_a.vikunja_id,
            "replay_b_id": replay_b.vikunja_id,
            "create_http_a": create_http_a,
            "create_http_b": create_http_b,
            "exact_marker_count_a": count_a,
            "exact_marker_count_b": count_b,
        }
        executed += 1

        scenarios["limits"] = {
            "scheduler": False,
            "regular_backup": False,
            "working_restore": False,
            "rpo": None,
            "rto": None,
            "retention": None,
            "mini_local_copy_not_device_loss_protection": True,
            "note": "Copy on Mini is not protection against physical loss of the device.",
        }
        executed += 1

        return {
            "scenarios": scenarios,
            "executed_count": executed,
            "synthetic_ids": {
                "run_id": stand.run_id,
                "task_id": task_id,
                "pending_task_id": pending_task_id,
                "project_id": project_id,
                "attachment_id": attachment_id,
                "attachment_sha256": attachment_sha,
                "issuer_id": issuer,
                "operation_id": op,
                "pending_operation_id": pending_op,
                "prepared_marker": prepared_marker,
                "pending_marker": pending.marker,
                "source_kind": SOURCE_KIND,
                "source_key": source_key,
                "source_url": source_url,
                "live_marker": fixture_marker,
                "copy_id": manifest["copy_id"],
                "older_copy_id": older_manifest["copy_id"],
            },
            "owned_dirs": [
                str(owned_root_path(item))
                for item in (stand.manifest.get("owned_roots") or [])
            ],
            "owned_children": [
                {
                    "role": item.get("role"),
                    "pid": item.get("pid"),
                    "root": item.get("root"),
                    "status": item.get("status"),
                }
                for item in (stand.manifest.get("owned_children") or [])
            ],
        }
    finally:
        # Stop owned restore children via durable protocol; stand.cleanup removes roots.
        try:
            if older_role is not None:
                stand.stop_owned_vikunja(older_role)
        except StandError:
            pass
        try:
            if restored_role is not None:
                stand.stop_owned_vikunja(restored_role)
        except StandError:
            pass
        time.sleep(0.1)


def run_restore_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite restore supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            session_ids = stand.create_synthetic_session()
            body = run_restore_scenarios(stand, worktree)
            source = _source_digest(worktree)
            synthetic_ids = {**dict(session_ids), **body["synthetic_ids"]}
            owned_snapshot = list(body.get("owned_dirs") or [])
            result = {
                "suite": "restore",
                "mode": mode,
                "argv": list(argv),
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "executed_count": body["executed_count"],
                "scenarios": body["scenarios"],
                "scenario_names": sorted(body["scenarios"]),
                "synthetic_ids": synthetic_ids,
                "source": {
                    **(stand.manifest.get("source") or {}),
                    **source,
                },
                "binary": stand.manifest.get("binary"),
                "owned_dirs": owned_snapshot,
                "owned_children": body.get("owned_children") or [],
            }
        owned = [Path(path) for path in result.get("owned_dirs") or []]
        owned_removed = all(not path.exists() for path in owned)
        result["cleanup"] = {
            "confirmed": stand.cleaned,
            "owned_dirs_removed": owned_removed,
            "owned_dirs_checked": [str(path) for path in owned],
        }
        result.pop("owned_dirs", None)
        if not stand.cleaned:
            raise StandError("restore suite cleanup was not confirmed")
        if not owned_removed:
            remaining = [str(path) for path in owned if path.exists()]
            raise StandError(f"restore suite owned dirs still present: {remaining}")
        if result["executed_count"] <= 0:
            raise StandError("restore suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
