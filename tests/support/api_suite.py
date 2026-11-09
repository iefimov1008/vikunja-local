"""Real isolated API suite for s02 (stock client + MSK dates, no CAS)."""

from __future__ import annotations

import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
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
from vikunja_local.dates import (
    MSK,
    ConfirmedDue,
    DueKind,
    confirm_date_only,
    confirm_datetime,
    is_absent_due,
    same_instant,
)

from .stand import IsolatedStand, StandError, load_profile

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")


WRITE_PERMS = {"tasks": ["create", "read_all", "read_one", "update"]}
READ_PERMS = {"tasks": ["read_all", "read_one"]}


def _token_path(stand: IsolatedStand, name: str) -> Path:
    path = stand.run_dir / "secrets" / name
    path.parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(path.parent, 0o700)
    return path


def _store_token(stand: IsolatedStand, name: str, secret: str) -> Path:
    path = _token_path(stand, name)
    # Single line, no trailing newline per TokenRef.load contract.
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


def _client_for(stand: IsolatedStand, token_path: Path, project_id: int, *, page_size: int = 2) -> VikunjaClient:
    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    return VikunjaClient(
        api_origin=origin,
        project_id=project_id,
        token_ref=TokenRef(kind="file", path=token_path),
        page_size=page_size,
        max_items_per_page=50,
    )


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _token_ids_from_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return [row.get("id") for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("tokens", payload.get("data", []))
        if isinstance(rows, list):
            return [row.get("id") for row in rows if isinstance(row, dict)]
    return []


def _revoke_suite_token(
    stand: IsolatedStand,
    *,
    handle: str,
    token_id: Any,
    secret: str,
) -> dict[str, Any]:
    """Fail-closed revoke with deny read-back; retain handle until proven revoked."""
    if not stand._session_token:
        raise StandError(f"Cannot revoke {handle}: session missing")
    if token_id is None:
        raise StandError(f"Cannot revoke {handle}: token id missing")
    if handle not in stand._token_secrets:
        raise StandError(f"Cannot revoke {handle}: secret handle already dropped")

    status, _ = stand.api("tokens", token=stand._session_token)
    if status != 200:
        raise StandError(f"Active session required before revoke of {handle}; HTTP {status}")

    delete_status, _ = stand.api(f"tokens/{token_id}", "DELETE", token=stand._session_token)
    if delete_status != 200:
        # Retain handle/secret for later cleanup/repair; do not continue as success.
        raise StandError(f"Token revoke failed for {handle}: HTTP {delete_status}")

    denied_status, _ = stand.api("tasks", token=secret)
    if denied_status not in (401, 403):
        raise StandError(
            f"Revoked {handle} still authorized: HTTP {denied_status}"
        )

    list_status, tokens = stand.api("tokens", token=stand._session_token)
    if list_status != 200:
        raise StandError(f"Session token list after revoke of {handle} failed: HTTP {list_status}")
    remaining_ids = _token_ids_from_list(tokens)
    if token_id in remaining_ids:
        raise StandError(f"Revoked {handle} id {token_id} still present in token list")

    evidence = {
        "handle": handle,
        "token_id": token_id,
        "delete_http": delete_status,
        "revoked_token_http": denied_status,
        "session_list_http": list_status,
        "token_absent_after_revoke": True,
    }
    stand.manifest.setdefault("revoke_readback", []).append(evidence)
    stand.manifest["token_handles"] = [
        item for item in stand.manifest.get("token_handles", []) if item != handle
    ]
    stand._token_secrets.pop(handle, None)
    stand._save_manifest()
    stand._save_secrets()
    return evidence


def _revoke_remaining_suite_tokens(stand: IsolatedStand) -> list[dict[str, Any]]:
    """Best-effort fail-closed revoke for any suite handles still retained."""
    evidence: list[dict[str, Any]] = []
    mapping = {
        "api_token_write": stand.synthetic_ids.get("write_token_id")
        or (stand.manifest.get("synthetic_ids") or {}).get("write_token_id"),
        "api_token_read": stand.synthetic_ids.get("read_token_id")
        or (stand.manifest.get("synthetic_ids") or {}).get("read_token_id"),
    }
    # Prefer IDs recorded on the stand during the scenario.
    for handle, token_id in list(mapping.items()):
        if handle not in stand._token_secrets:
            continue
        secret = stand._token_secrets[handle]
        evidence.append(
            _revoke_suite_token(stand, handle=handle, token_id=token_id, secret=secret)
        )
    return evidence


def run_api_scenarios(stand: IsolatedStand) -> dict[str, Any]:
    """Execute real API round-trips; caller owns stand lifecycle/cleanup."""
    session = stand.create_synthetic_session()
    project_id = int(session["project_id"])
    seed_task_id = int(session["task_id"])

    write_token = _create_api_token(
        stand,
        title=stand.fixture_name("write-token"),
        permissions=WRITE_PERMS,
    )
    read_token = _create_api_token(
        stand,
        title=stand.fixture_name("read-token"),
        permissions=READ_PERMS,
    )
    write_path = _store_token(stand, "write.token", write_token["token"])
    read_path = _store_token(stand, "read.token", read_token["token"])
    bad_path = _store_token(stand, "bad.token", "ea010-bad-token-" + secrets.token_hex(8))

    # Keep secrets only in protected files / stand secret store, not in returned evidence.
    stand._token_secrets["api_token_write"] = write_token["token"]
    stand._token_secrets["api_token_read"] = read_token["token"]
    stand.manifest.setdefault("token_handles", [])
    for handle in ("api_token_write", "api_token_read"):
        if handle not in stand.manifest["token_handles"]:
            stand.manifest["token_handles"].append(handle)

    synthetic_ids: dict[str, Any] = {
        "project_id": project_id,
        "seed_task_id": seed_task_id,
        "write_token_id": write_token.get("id"),
        "read_token_id": read_token.get("id"),
    }
    # Retain IDs on the stand so failure-path cleanup can still revoke extras.
    stand.synthetic_ids["write_token_id"] = write_token.get("id")
    stand.synthetic_ids["read_token_id"] = read_token.get("id")
    stand.manifest["synthetic_ids"] = dict(stand.synthetic_ids)
    stand._save_secrets()
    stand._save_manifest()

    client = _client_for(stand, write_path, project_id, page_size=1)
    readonly = _client_for(stand, read_path, project_id, page_size=1)
    bad = _client_for(stand, bad_path, project_id, page_size=1)

    scenarios: dict[str, Any] = {}

    # --- create with approved marker + date-only due ---
    issuer_id, operation_id = new_operation_ids()
    confirmed_at = datetime(2026, 11, 9, 20, 0, tzinfo=MSK)
    due = confirm_date_only("2026-11-30", confirmed_at=confirmed_at, expression="2026-11-30")
    create = client.create(
        title=stand.fixture_name("api-create"),
        issuer_id=issuer_id,
        operation_id=operation_id,
        user_portion="synthetic context & <link>",
        due=due,
    )
    _assert(create.ok and create.task is not None, f"create failed: {create.outcome} {create.details}")
    created_id = int(create.task["id"])
    synthetic_ids["created_task_id"] = created_id
    marker = build_marker(issuer_id, operation_id)
    extracted = extract_marker(create.task.get("description"))
    _assert(extracted is not None and extracted["marker"] == marker, "create marker missing on GET")
    _assert(same_instant(create.task.get("due_date"), due.wire_value()), "date-only MSK due mismatch")
    scenarios["create_marker_date_only"] = {
        "task_id": created_id,
        "marker": marker,
        "due_wire": due.wire_value(),
        "due_read": create.task.get("due_date"),
        "create_http": create.details.get("create_http"),
    }

    # --- pagination / ID selection ---
    filler_ids = []
    for idx in range(2):
        i2, o2 = new_operation_ids()
        result = client.create(
            title=stand.fixture_name(f"page-{idx}"),
            issuer_id=i2,
            operation_id=o2,
            user_portion="",
        )
        _assert(result.ok and result.task is not None, f"pagination filler create failed: {result.outcome}")
        filler_ids.append(int(result.task["id"]))
    listed = client.list_project_tasks()
    _assert(listed.outcome is Outcome.CONFIRMED, f"list failed: {listed.outcome} {listed.details}")
    pages_read = listed.details.get("pages_read")
    _assert(isinstance(pages_read, int) and pages_read >= 2, f"expected multi-page scan, got {pages_read}")
    found = client.find_by_marker(marker)
    _assert(found.ok and found.task is not None and int(found.task["id"]) == created_id, "marker ID select failed")
    empty = client.find_by_marker(build_marker(str(uuid.uuid4()), str(uuid.uuid4())))
    _assert(empty.outcome is Outcome.EMPTY, f"expected EMPTY search, got {empty.outcome}")
    scenarios["pagination_and_id_select"] = {
        "pages_read": pages_read,
        "total": listed.details.get("total"),
        "filler_ids": filler_ids,
        "selected_id": created_id,
        "empty_outcome": empty.outcome.value,
    }
    synthetic_ids["filler_ids"] = filler_ids

    # --- omission preserves due/marker; title changes ---
    before = client.read(created_id)
    _assert(before.ok and before.task is not None, "pre-omission read failed")
    new_title = stand.fixture_name("renamed")
    renamed = client.update(created_id, {"title": new_title})
    _assert(renamed.ok and renamed.task is not None, f"omission update failed: {renamed.outcome}")
    _assert(renamed.task.get("title") == new_title, "title not updated")
    _assert(same_instant(renamed.task.get("due_date"), before.task.get("due_date")), "omission changed due")
    _assert(
        extract_marker(renamed.task.get("description"))["marker"] == marker,
        "omission changed marker",
    )
    scenarios["omission_preserves_marker_due"] = {
        "task_id": created_id,
        "patch_http": renamed.details.get("patch_http"),
        "due": renamed.task.get("due_date"),
    }

    # --- empty U and substituted marker text must preserve verified M (C1 §3/§6) ---
    empty_ctx = client.update(created_id, {"description": ""})
    _assert(empty_ctx.ok and empty_ctx.task is not None, f"empty context update failed: {empty_ctx.outcome}")
    empty_extracted = extract_marker(empty_ctx.task.get("description"))
    _assert(empty_extracted is not None and empty_extracted["marker"] == marker, "empty U lost marker")
    _assert(empty_extracted["user_portion"] == "", "empty U must keep marker with empty suffix")
    fake_marker = build_marker(str(uuid.uuid4()), str(uuid.uuid4()))
    substituted = client.update(created_id, {"description": fake_marker + "\nsubstituted"})
    _assert(
        substituted.ok and substituted.task is not None,
        f"substituted-marker update failed: {substituted.outcome}",
    )
    sub_extracted = extract_marker(substituted.task.get("description"))
    _assert(sub_extracted is not None and sub_extracted["marker"] == marker, "substituted header replaced M")
    _assert(fake_marker not in (sub_extracted["marker"] or ""), "fake marker became recovery header")
    scenarios["description_preserves_marker"] = {
        "task_id": created_id,
        "empty_description": empty_ctx.task.get("description"),
        "empty_patch_http": empty_ctx.details.get("patch_http"),
        "substituted_marker_header": sub_extracted["marker"],
        "substituted_user_portion": sub_extracted["user_portion"],
        "substituted_patch_http": substituted.details.get("patch_http"),
    }

    # --- explicit null clears due only ---
    cleared = client.update(created_id, {"due_date": None})
    _assert(cleared.ok and cleared.task is not None, f"null clear failed: {cleared.outcome}")
    _assert(is_absent_due(cleared.task.get("due_date")), "due not cleared by null")
    _assert(
        extract_marker(cleared.task.get("description"))["marker"] == marker,
        "null clear damaged marker",
    )
    scenarios["null_clears_due"] = {
        "task_id": created_id,
        "due_after": cleared.task.get("due_date"),
        "patch_http": cleared.details.get("patch_http"),
    }

    # --- day-boundary MSK date-only after midnight confirmation freeze ---
    late_confirm = datetime(2026, 11, 10, 0, 30, tzinfo=MSK)
    boundary = confirm_date_only("2026-11-09", confirmed_at=late_confirm, expression="2026-11-09")
    boundary_set = client.update(created_id, {"due_date": boundary.wire_value()})
    _assert(boundary_set.ok and boundary_set.task is not None, f"boundary due failed: {boundary_set.outcome}")
    _assert(same_instant(boundary_set.task.get("due_date"), boundary.wire_value()), "boundary due mismatch")
    scenarios["msk_day_boundary"] = {
        "confirmed_at": late_confirm.isoformat(),
        "wire": boundary.wire_value(),
        "read": boundary_set.task.get("due_date"),
    }

    # --- F1: fractional caller targets rejected before HTTP (create + update) ---
    fractional_wire = "2026-12-01T12:00:00.123456Z"
    frac_confirm_error = None
    try:
        confirm_datetime(
            datetime(2026, 12, 1, 12, 0, 0, 123456, tzinfo=timezone.utc),
            confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
            expression=fractional_wire,
        )
    except ValueError as exc:
        frac_confirm_error = str(exc)
    _assert(frac_confirm_error is not None, "confirm_datetime must reject fractional target")

    before_frac = client.read(created_id)
    _assert(before_frac.ok and before_frac.task is not None, "pre-fractional read failed")
    frac_update = client.update(created_id, {"due_date": fractional_wire})
    _assert(
        frac_update.outcome is Outcome.VALIDATION_ERROR and frac_update.mutation_sent is False,
        f"fractional update must fail closed before HTTP, got {frac_update.outcome}",
    )
    # Sub-microsecond nonzero tails must not truncate into an accepted deadline.
    sub_micro_rejected: list[dict[str, object]] = []
    for sub_wire in (
        "2026-12-01T12:00:00.0000001Z",
        "2026-12-01T12:00:00.000000999Z",
    ):
        sub_update = client.update(created_id, {"due_date": sub_wire})
        _assert(
            sub_update.outcome is Outcome.VALIDATION_ERROR and sub_update.mutation_sent is False,
            f"sub-microsecond update must fail closed before HTTP, got {sub_update.outcome} for {sub_wire}",
        )
        sub_micro_rejected.append(
            {
                "wire": sub_wire,
                "outcome": sub_update.outcome.value,
                "mutation_sent": sub_update.mutation_sent,
            }
        )
    after_frac = client.read(created_id)
    _assert(after_frac.ok and after_frac.task is not None, "post-fractional read failed")
    _assert(
        same_instant(after_frac.task.get("due_date"), before_frac.task.get("due_date")),
        "fractional update must not mutate due",
    )

    hand_built = ConfirmedDue(
        kind=DueKind.DATETIME,
        confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
        timezone_name="Europe/Moscow",
        expression=fractional_wire,
        absolute=datetime(2026, 12, 1, 12, 0, 0, 123456, tzinfo=timezone.utc),
    )
    frac_create_outcome = None
    frac_create_mutation = None
    try:
        frac_create = client.create(
            title=stand.fixture_name("frac-create"),
            issuer_id=str(uuid.uuid4()),
            operation_id=str(uuid.uuid4()),
            user_portion="fractional-rejected",
            due=hand_built,
        )
        frac_create_outcome = frac_create.outcome.value
        frac_create_mutation = frac_create.mutation_sent
    except ApiError as exc:
        frac_create_outcome = exc.outcome.value
        frac_create_mutation = False
    _assert(
        frac_create_outcome == Outcome.VALIDATION_ERROR.value and frac_create_mutation is False,
        f"fractional create must fail closed before HTTP, got {frac_create_outcome}",
    )

    # Whole-second datetime create+update still confirm with exact instant equality.
    second_due = confirm_datetime(
        datetime(2026, 12, 1, 12, 0, 0, tzinfo=timezone.utc),
        confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
        expression="2026-12-01T12:00:00Z",
    )
    si, so = new_operation_ids()
    second_create = client.create(
        title=stand.fixture_name("second-due"),
        issuer_id=si,
        operation_id=so,
        user_portion="whole-second",
        due=second_due,
    )
    _assert(
        second_create.ok and second_create.task is not None,
        f"whole-second create failed: {second_create.outcome} {second_create.details}",
    )
    second_id = int(second_create.task["id"])
    synthetic_ids["second_precision_task_id"] = second_id
    _assert(
        same_instant(second_create.task.get("due_date"), second_due.wire_value()),
        "whole-second create due mismatch",
    )
    second_update = client.update(second_id, {"due_date": "2026-12-15T09:30:00Z"})
    _assert(
        second_update.ok and second_update.task is not None,
        f"whole-second update failed: {second_update.outcome} {second_update.details}",
    )
    _assert(
        same_instant(second_update.task.get("due_date"), "2026-12-15T09:30:00Z"),
        "whole-second update due mismatch",
    )
    scenarios["fractional_due_rejected"] = {
        "confirm_error": frac_confirm_error,
        "update_outcome": frac_update.outcome.value,
        "update_mutation_sent": frac_update.mutation_sent,
        "sub_microsecond_rejected": sub_micro_rejected,
        "create_outcome": frac_create_outcome,
        "create_mutation_sent": frac_create_mutation,
        "due_unchanged": same_instant(after_frac.task.get("due_date"), before_frac.task.get("due_date")),
        "second_precision_task_id": second_id,
        "second_create_due": second_create.task.get("due_date"),
        "second_update_due": second_update.task.get("due_date"),
        "second_update_patch_http": second_update.details.get("patch_http"),
    }

    # --- non-recurring complete + field PATCH read-back; done=true update is no-op ---
    done1 = client.complete(created_id)
    _assert(done1.ok and done1.task is not None and done1.task.get("done") is True, "complete failed")
    # Second complete should no-op without mutation for non-recurring done=true.
    done2 = client.complete(created_id)
    _assert(done2.ok and done2.mutation_sent is False, "second complete should be no-op read")
    # update(done=true) on already-done must not dispatch (centralized no-op).
    done_update_noop = client.update(created_id, {"done": True})
    _assert(
        done_update_noop.ok and done_update_noop.mutation_sent is False,
        "update(done=true) on already-done must be no-op",
    )
    # Genuine field PATCH exercises 200/304 + GET read-back (not the done no-op path).
    same_title = done1.task["title"]
    patch_rb = client.update(created_id, {"title": same_title})
    _assert(patch_rb.ok and patch_rb.task is not None, f"field PATCH/GET path failed: {patch_rb.outcome}")
    _assert(patch_rb.mutation_sent is True, "same-title PATCH must still be dispatched")
    _assert(patch_rb.details.get("patch_http") in (200, 304), "expected 200 or 304 before GET")
    scenarios["complete_and_304_get"] = {
        "task_id": created_id,
        "first_mutation": done1.mutation_sent,
        "second_mutation": done2.mutation_sent,
        "done_update_noop_mutation": done_update_noop.mutation_sent,
        "field_patch_http": patch_rb.details.get("patch_http"),
        "done": patch_rb.task.get("done"),
    }

    # --- absolute reminder (stock relative_to="") must remain readable/completable ---
    ari, aro = new_operation_ids()
    reminder_due = confirm_datetime(
        datetime(2026, 12, 1, 12, 0, 0, tzinfo=timezone.utc),
        confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
        expression="2026-12-01T12:00:00Z",
    )
    reminder_create = client.create(
        title=stand.fixture_name("absolute-reminder"),
        issuer_id=ari,
        operation_id=aro,
        user_portion="absolute reminder",
        due=reminder_due,
        extra_fields={
            "reminders": [{"reminder": "2026-12-01T11:00:00Z"}],
        },
    )
    _assert(
        reminder_create.ok and reminder_create.task is not None,
        f"absolute-reminder create failed: {reminder_create.outcome} {reminder_create.details}",
    )
    reminder_id = int(reminder_create.task["id"])
    synthetic_ids["absolute_reminder_task_id"] = reminder_id
    wire_reminders = reminder_create.task.get("reminders")
    _assert(isinstance(wire_reminders, list) and len(wire_reminders) == 1, "absolute reminder missing on create GET")
    _assert(
        isinstance(wire_reminders[0], dict)
        and wire_reminders[0].get("relative_to") in ("", None)
        and same_instant(wire_reminders[0].get("reminder"), "2026-12-01T11:00:00Z"),
        f"unexpected absolute reminder wire: {wire_reminders}",
    )
    reminder_read = client.read(reminder_id)
    _assert(
        reminder_read.ok and reminder_read.task is not None,
        f"absolute-reminder read failed: {reminder_read.outcome} {reminder_read.details}",
    )
    reminder_complete = client.complete(reminder_id)
    _assert(
        reminder_complete.ok
        and reminder_complete.task is not None
        and reminder_complete.task.get("done") is True,
        f"absolute-reminder complete failed: {reminder_complete.outcome} {reminder_complete.details}",
    )
    _assert(reminder_complete.mutation_sent is True, "absolute-reminder complete must send one PATCH")
    scenarios["absolute_reminder_read_complete"] = {
        "task_id": reminder_id,
        "wire_reminders": wire_reminders,
        "read_outcome": reminder_read.outcome.value,
        "complete_outcome": reminder_complete.outcome.value,
        "mutation_sent": reminder_complete.mutation_sent,
        "done_after": reminder_complete.task.get("done"),
        "patch_http": reminder_complete.details.get("patch_http"),
    }

    # --- recurring mode=0: exactly one complete, accept done=false + new due ---
    ri, ro = new_operation_ids()
    recurring_due = confirm_date_only(
        "2026-12-01",
        confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
    )
    recurring = client.create(
        title=stand.fixture_name("recurring"),
        issuer_id=ri,
        operation_id=ro,
        user_portion="recurring",
        due=recurring_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(recurring.ok and recurring.task is not None, f"recurring create failed: {recurring.outcome}")
    recurring_id = int(recurring.task["id"])
    synthetic_ids["recurring_task_id"] = recurring_id
    due_before = recurring.task.get("due_date")
    one = client.complete(recurring_id)
    _assert(one.ok and one.task is not None, f"recurring complete failed: {one.outcome}")
    _assert(one.mutation_sent is True, "recurring complete must send one PATCH")
    _assert(one.details.get("recurring") is True, "recurring flag missing")
    _assert(int(one.task["id"]) == recurring_id, "recurring id changed")
    # Stock may leave done=false and advance due; either observed transition is accepted once.
    _assert(
        one.task.get("done") is False or one.task.get("done") is True,
        "recurring done field missing",
    )
    if one.task.get("done") is False:
        _assert(not same_instant(one.task.get("due_date"), due_before), "expected due advance")
    scenarios["recurring_one_complete"] = {
        "task_id": recurring_id,
        "mutation_sent": one.mutation_sent,
        "done_after": one.task.get("done"),
        "due_before": due_before,
        "due_after": one.task.get("due_date"),
        "patch_http": one.details.get("patch_http"),
        "repeat_mode": 0,
    }

    # --- recurring mode=2 with due: one PATCH; stock may truncate usecs on GET ---
    m2i, m2o = new_operation_ids()
    mode2_due = confirm_date_only(
        "2026-12-01",
        confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
    )
    mode2 = client.create(
        title=stand.fixture_name("recurring-m2"),
        issuer_id=m2i,
        operation_id=m2o,
        user_portion="recurring-mode-2",
        due=mode2_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 2},
    )
    _assert(mode2.ok and mode2.task is not None, f"mode=2 create failed: {mode2.outcome}")
    mode2_id = int(mode2.task["id"])
    synthetic_ids["recurring_mode2_task_id"] = mode2_id
    mode2_due_before = mode2.task.get("due_date")
    mode2_complete = client.complete(mode2_id)
    _assert(
        mode2_complete.ok and mode2_complete.task is not None,
        f"mode=2 complete failed: {mode2_complete.outcome} {mode2_complete.details}",
    )
    _assert(mode2_complete.mutation_sent is True, "mode=2 complete must send one PATCH")
    _assert(mode2_complete.details.get("patch_http") == 200, "mode=2 expected PATCH 200")
    _assert(int(mode2_complete.task["id"]) == mode2_id, "mode=2 id changed")
    if mode2_complete.task.get("done") is False:
        _assert(
            not same_instant(mode2_complete.task.get("due_date"), mode2_due_before),
            "mode=2 expected due advance",
        )
    scenarios["recurring_mode2_with_due"] = {
        "task_id": mode2_id,
        "mutation_sent": mode2_complete.mutation_sent,
        "done_after": mode2_complete.task.get("done"),
        "due_before": mode2_due_before,
        "due_after": mode2_complete.task.get("due_date"),
        "patch_http": mode2_complete.details.get("patch_http"),
        "repeat_mode": 2,
    }

    # --- recurring mode=2 without due but with start/end: occurrence dates advance ---
    m2bi, m2bo = new_operation_ids()
    mode2_nodue = client.create(
        title=stand.fixture_name("recurring-m2-nodue"),
        issuer_id=m2bi,
        operation_id=m2bo,
        user_portion="recurring-mode-2-nodue",
        extra_fields={
            "repeat_after": 86400,
            "repeat_mode": 2,
            "start_date": "2026-12-01T10:00:00Z",
            "end_date": "2026-12-01T11:00:00Z",
        },
    )
    _assert(
        mode2_nodue.ok and mode2_nodue.task is not None,
        f"mode=2 no-due create failed: {mode2_nodue.outcome}",
    )
    mode2_nodue_id = int(mode2_nodue.task["id"])
    synthetic_ids["recurring_mode2_nodue_task_id"] = mode2_nodue_id
    start_before = mode2_nodue.task.get("start_date")
    end_before = mode2_nodue.task.get("end_date")
    mode2_nodue_complete = client.complete(mode2_nodue_id)
    _assert(
        mode2_nodue_complete.ok and mode2_nodue_complete.task is not None,
        f"mode=2 no-due complete failed: {mode2_nodue_complete.outcome} {mode2_nodue_complete.details}",
    )
    _assert(mode2_nodue_complete.mutation_sent is True, "mode=2 no-due must send one PATCH")
    _assert(mode2_nodue_complete.details.get("patch_http") == 200, "mode=2 no-due expected PATCH 200")
    if mode2_nodue_complete.task.get("done") is False:
        _assert(
            not same_instant(mode2_nodue_complete.task.get("start_date"), start_before)
            or not same_instant(mode2_nodue_complete.task.get("end_date"), end_before),
            "mode=2 no-due expected start/end advance",
        )
    scenarios["recurring_mode2_without_due"] = {
        "task_id": mode2_nodue_id,
        "mutation_sent": mode2_nodue_complete.mutation_sent,
        "done_after": mode2_nodue_complete.task.get("done"),
        "start_before": start_before,
        "start_after": mode2_nodue_complete.task.get("start_date"),
        "end_before": end_before,
        "end_after": mode2_nodue_complete.task.get("end_date"),
        "patch_http": mode2_nodue_complete.details.get("patch_http"),
        "repeat_mode": 2,
    }

    # --- access: read-only / bad token must not mutate; revoke denies ---
    snapshot = client.read(recurring_id)
    _assert(snapshot.ok and snapshot.task is not None, "snapshot before denied writes failed")
    denied_ro = readonly.update(recurring_id, {"title": "should-not-apply"})
    _assert(denied_ro.outcome is Outcome.AUTH_DENIED, f"read-only expected AUTH_DENIED, got {denied_ro.outcome}")
    denied_bad = bad.update(recurring_id, {"title": "should-not-apply"})
    _assert(denied_bad.outcome is Outcome.AUTH_DENIED, f"bad token expected AUTH_DENIED, got {denied_bad.outcome}")
    after_denied = client.read(recurring_id)
    _assert(after_denied.ok and after_denied.task is not None, "read after denied failed")
    _assert(after_denied.task.get("title") == snapshot.task.get("title"), "401 path mutated title")
    _assert(same_instant(after_denied.task.get("due_date"), snapshot.task.get("due_date")), "401 path mutated due")

    write_revoke = _revoke_suite_token(
        stand,
        handle="api_token_write",
        token_id=write_token["id"],
        secret=write_token["token"],
    )
    revoked = client.update(recurring_id, {"title": "after-revoke"})
    _assert(revoked.outcome is Outcome.AUTH_DENIED, f"revoked write expected AUTH_DENIED, got {revoked.outcome}")
    scenarios["access_denied"] = {
        "read_only_outcome": denied_ro.outcome.value,
        "bad_token_outcome": denied_bad.outcome.value,
        "revoked_outcome": revoked.outcome.value,
        "title_unchanged": after_denied.task.get("title") == snapshot.task.get("title"),
        "write_token_revoked_http": write_revoke["delete_http"],
        "write_token_denied_http": write_revoke["revoked_token_http"],
    }

    read_revoke = _revoke_suite_token(
        stand,
        handle="api_token_read",
        token_id=read_token["id"],
        secret=read_token["token"],
    )
    scenarios["token_cleanup"] = {
        "write_revoke_http": write_revoke["delete_http"],
        "write_denied_http": write_revoke["revoked_token_http"],
        "read_revoke_http": read_revoke["delete_http"],
        "read_denied_http": read_revoke["revoked_token_http"],
        "write_absent": write_revoke["token_absent_after_revoke"],
        "read_absent": read_revoke["token_absent_after_revoke"],
    }

    return {
        "scenarios": scenarios,
        "synthetic_ids": synthetic_ids,
        "executed_count": len(scenarios),
        "scenario_names": sorted(scenarios),
    }


def run_api_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite api supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            try:
                payload = run_api_scenarios(stand)
            except BaseException:
                # Extra suite tokens must still be revoked before stand cleanup.
                try:
                    _revoke_remaining_suite_tokens(stand)
                except Exception as cleanup_exc:  # noqa: BLE001
                    stand.manifest.setdefault("suite_token_cleanup_errors", []).append(str(cleanup_exc))
                    stand._save_manifest()
                raise
            # Primary stand token from create_synthetic_session is still revoked by cleanup.
            result = {
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "synthetic_ids": payload["synthetic_ids"],
                "scenarios": payload["scenarios"],
                "scenario_names": payload["scenario_names"],
                "executed_count": payload["executed_count"],
                "source": stand.manifest.get("source"),
                "binary": stand.manifest.get("binary"),
                "argv": list(argv),
                "suite": "api",
                "mode": mode,
            }
        result["cleanup"] = {"confirmed": stand.cleaned}
        if not stand.cleaned:
            raise StandError("API suite cleanup was not confirmed")
        if result["executed_count"] <= 0:
            raise StandError("API suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
