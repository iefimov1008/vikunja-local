"""Exact marker reconcile for uncertain integration outcomes (C1 §4 / I6).

Substring / decoy headers never count: only extract_marker() exact grammar.
UID/labels are not correlation. Empty or incomplete scans stay fail-closed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .api import (
    ApiError,
    Outcome,
    TaskResult,
    VikunjaClient,
    build_marker,
    extract_marker,
    validate_uuid_v4,
)
from .journal import IntegrationJournal, JournalError, OperationRecord, OperationState


@dataclass
class ReconcileResult:
    outcome: Outcome
    task: dict[str, Any] | None = None
    vikunja_id: int | None = None
    matches: list[int] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    operation: OperationRecord | None = None

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.CONFIRMED and self.vikunja_id is not None


def _positive_id(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _task_result_from_api_error(exc: ApiError) -> TaskResult:
    """Normalize transport/API exceptions to typed TaskResult at the reconcile boundary."""
    return TaskResult(
        outcome=exc.outcome,
        details={"reason": str(exc), "api_error": True, **dict(exc.details)},
    )


def _safe_client_read(client: VikunjaClient, task_id: int) -> TaskResult:
    try:
        return client.read(task_id)
    except ApiError as exc:
        return _task_result_from_api_error(exc)


def _matching_confirmed_create_receipt(
    record: OperationRecord,
    *,
    vikunja_id: int,
    project_id: int,
    marker: str,
) -> bool:
    """True when durable receipt already binds the same create correspondence."""
    if record.op_type != "create":
        return False
    if record.state is not OperationState.CONFIRMED or record.receipt is None:
        return False
    receipt = record.receipt
    try:
        return (
            int(receipt["vikunja_id"]) == vikunja_id
            and int(receipt["project_id"]) == project_id
            and str(receipt["marker"]) == marker
        )
    except (KeyError, TypeError, ValueError):
        return False


def _reuse_confirmed_create_receipt(
    record: OperationRecord,
    *,
    task: dict[str, Any] | None,
    vikunja_id: int,
    marker: str,
) -> ReconcileResult:
    effect = {}
    if isinstance(record.receipt, dict):
        raw_effect = record.receipt.get("effect")
        if isinstance(raw_effect, Mapping):
            effect = dict(raw_effect)
    return ReconcileResult(
        outcome=Outcome.CONFIRMED,
        task=task,
        vikunja_id=vikunja_id,
        matches=[vikunja_id],
        operation=record,
        details={
            "reason": "reused_confirmed_receipt",
            "marker": marker,
            "receipt_preserved": True,
            "effect_source": effect.get("source"),
        },
    )


def exact_marker_scan(client: VikunjaClient, marker: str) -> TaskResult:
    """Full-project exact marker scan; never uses q/s substring search."""
    if not isinstance(marker, str) or not marker:
        return TaskResult(
            outcome=Outcome.VALIDATION_ERROR,
            details={"reason": "marker_required"},
        )
    # find_by_marker already: complete paging + extract_marker exact header match.
    # Final matching-ID read-back can raise ApiError on transport faults — normalize.
    try:
        return client.find_by_marker(marker)
    except ApiError as exc:
        return _task_result_from_api_error(exc)


def reconcile_create_marker(
    client: VikunjaClient,
    *,
    issuer_id: str,
    operation_id: str,
) -> ReconcileResult:
    """Classify create uncertainty by exact marker presence in allowed project."""
    issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
    operation_id = validate_uuid_v4(operation_id, label="operation_id")
    marker = build_marker(issuer_id, operation_id)
    scanned = exact_marker_scan(client, marker)
    if scanned.outcome is Outcome.EMPTY:
        return ReconcileResult(
            outcome=Outcome.EMPTY,
            details={
                "marker": marker,
                "reason": "zero_exact_matches",
                "forbid_create": True,
            },
        )
    if scanned.outcome is Outcome.CONFLICT:
        ids = [_positive_id(x) for x in (scanned.details.get("ids") or [])]
        return ReconcileResult(
            outcome=Outcome.CONFLICT,
            matches=[i for i in ids if i is not None],
            details={"marker": marker, "reason": "multiple_exact_matches", **scanned.details},
        )
    if scanned.outcome is Outcome.INCOMPLETE_SCAN:
        return ReconcileResult(
            outcome=Outcome.INCOMPLETE_SCAN,
            details={"marker": marker, "reason": "incomplete_scan", **scanned.details},
        )
    if scanned.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED}:
        return ReconcileResult(
            outcome=scanned.outcome,
            details={"marker": marker, "reason": "access_denied", **scanned.details},
        )
    if scanned.outcome is Outcome.DEPENDENCY_UNAVAILABLE:
        return ReconcileResult(
            outcome=Outcome.DEPENDENCY_UNAVAILABLE,
            details={"marker": marker, "reason": "dependency", **scanned.details},
        )
    if not scanned.ok or scanned.task is None:
        return ReconcileResult(
            outcome=Outcome.UNKNOWN if scanned.outcome is Outcome.CONFIRMED else scanned.outcome,
            details={"marker": marker, "reason": "unconfirmed_scan", **scanned.details},
        )
    task_id = _positive_id(scanned.task.get("id"))
    if task_id is None:
        return ReconcileResult(
            outcome=Outcome.UNKNOWN,
            task=scanned.task,
            details={"marker": marker, "reason": "invalid_task_id"},
        )
    extracted = extract_marker(scanned.task.get("description"))
    if extracted is None or extracted["marker"] != marker:
        return ReconcileResult(
            outcome=Outcome.UNKNOWN,
            task=scanned.task,
            vikunja_id=task_id,
            details={"marker": marker, "reason": "marker_lost_or_corrupted"},
        )
    return ReconcileResult(
        outcome=Outcome.CONFIRMED,
        task=scanned.task,
        vikunja_id=task_id,
        matches=[task_id],
        details={"marker": marker},
    )


def reconcile_unknown_mutation(
    client: VikunjaClient,
    *,
    task_id: int,
    expected_marker: str | None,
    project_id: int,
    preimage: Mapping[str, Any] | None = None,
) -> ReconcileResult:
    """Read-only reconcile for unknown update/complete — never re-sends PATCH.

    Recurring: observe current due/done without claiming the prior command ran.
    Desired-state match is reported as observed fact, not causal proof.
    """
    if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
        return ReconcileResult(
            outcome=Outcome.VALIDATION_ERROR,
            details={"reason": "task_id"},
        )
    read = _safe_client_read(client, task_id)
    if read.outcome in {
        Outcome.AUTH_DENIED,
        Outcome.SCOPE_DENIED,
        Outcome.DEPENDENCY_UNAVAILABLE,
        Outcome.INCOMPLETE_SCAN,
    }:
        return ReconcileResult(
            outcome=read.outcome,
            details={
                "phase": "unknown_mutation_read",
                "resend_forbidden": True,
                "forbid_create": True,
                **read.details,
            },
        )
    if not read.ok or read.task is None:
        # 404 / missing after possible write: keep unknown; never recreate.
        return ReconcileResult(
            outcome=Outcome.UNKNOWN if read.outcome is Outcome.CONFIRMED else read.outcome,
            details={
                "phase": "unknown_mutation_read",
                "reason": "object_unreadable_or_gone",
                "forbid_create": True,
                "resend_forbidden": True,
                **read.details,
            },
        )
    task = read.task
    observed_project = _positive_id(task.get("project_id"))
    if observed_project != project_id:
        return ReconcileResult(
            outcome=Outcome.SCOPE_DENIED,
            task=task,
            vikunja_id=task_id,
            details={
                "reason": "project_mismatch",
                "expected_project_id": project_id,
                "got_project_id": observed_project,
            },
        )
    extracted = extract_marker(task.get("description"))
    if expected_marker:
        if extracted is None or extracted["marker"] != expected_marker:
            return ReconcileResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                vikunja_id=task_id,
                details={
                    "reason": "marker_corrupted_or_removed",
                    "forbid_create": True,
                    "expected_marker": expected_marker,
                },
            )
    elif extracted is not None:
        # Unmarked manual intent must not suddenly gain an integration header
        # and be treated as causal proof of our mutation.
        return ReconcileResult(
            outcome=Outcome.UNKNOWN,
            task=task,
            vikunja_id=task_id,
            details={"reason": "unexpected_marker_on_unmarked", "forbid_create": True},
        )

    details: dict[str, Any] = {
        "phase": "unknown_mutation_observe",
        "observed_done": task.get("done"),
        "observed_due": task.get("due_date"),
        "causal_proof": False,
    }
    if preimage is not None:
        details["preimage_due"] = preimage.get("due_date")
        details["preimage_done"] = preimage.get("done")
        details["due_changed"] = task.get("due_date") != preimage.get("due_date")
    return ReconcileResult(
        outcome=Outcome.CONFIRMED,
        task=task,
        vikunja_id=task_id,
        matches=[task_id],
        details=details,
    )


def apply_create_reconcile_to_journal(
    journal: IntegrationJournal,
    *,
    issuer_id: str,
    operation_id: str,
    client: VikunjaClient,
) -> ReconcileResult:
    """Persist exact-create reconcile: one match → receipt; many → conflict; zero → unknown."""
    issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
    operation_id = validate_uuid_v4(operation_id, label="operation_id")
    record = journal.get_operation(issuer_id, operation_id)
    if record is None:
        return ReconcileResult(
            outcome=Outcome.VALIDATION_ERROR,
            details={"reason": "operation_not_found"},
        )
    # Kind must win over receipt shortcuts: confirmed mutations are not creates.
    if record.op_type != "create":
        return ReconcileResult(
            outcome=Outcome.VALIDATION_ERROR,
            operation=record,
            details={"reason": "not_a_create", "op_type": record.op_type},
        )

    # Exact reconcile is scoped to the immutable intent project only (FR-EA010-16).
    # Scope must precede receipt shortcuts so unknown→confirmed races cannot
    # return CONFIRMED to a foreign/denied client without live validation.
    if client.project_id != record.project_id:
        return ReconcileResult(
            outcome=Outcome.SCOPE_DENIED,
            operation=record,
            details={
                "reason": "client_project_mismatch",
                "forbid_create": True,
                "client_project_id": client.project_id,
                "record_project_id": record.project_id,
                "marker": record.marker,
            },
        )

    if record.state is OperationState.CONFIRMED and record.receipt is not None:
        return ReconcileResult(
            outcome=Outcome.CONFIRMED,
            vikunja_id=int(record.receipt["vikunja_id"]),
            operation=record,
            details={
                "reason": "already_confirmed",
                "marker": record.marker,
                "receipt_only": True,
                "requires_live_validation": True,
            },
        )

    result = reconcile_create_marker(
        client,
        issuer_id=issuer_id,
        operation_id=operation_id,
    )
    try:
        if result.outcome is Outcome.CONFIRMED and result.vikunja_id is not None and result.task is not None:
            observed_project = _positive_id(result.task.get("project_id"))
            if observed_project != record.project_id or client.project_id != record.project_id:
                return ReconcileResult(
                    outcome=Outcome.SCOPE_DENIED,
                    operation=record,
                    task=result.task,
                    vikunja_id=result.vikunja_id,
                    matches=result.matches,
                    details={
                        "reason": "observed_project_mismatch",
                        "forbid_create": True,
                        "expected_project_id": record.project_id,
                        "got_project_id": observed_project,
                        "client_project_id": client.project_id,
                        "marker": record.marker,
                    },
                )
            # Re-read under the receipt serialization boundary: the original
            # create writer may have committed create_read_back during the scan.
            # Matching correspondence must reuse that effect, not conflict on
            # source=exact_reconcile (FR-EA010-10 concurrent-repeat).
            fresh = journal.get_operation(issuer_id, operation_id)
            if fresh is not None and _matching_confirmed_create_receipt(
                fresh,
                vikunja_id=result.vikunja_id,
                project_id=record.project_id,
                marker=record.marker,
            ):
                return _reuse_confirmed_create_receipt(
                    fresh,
                    task=result.task,
                    vikunja_id=result.vikunja_id,
                    marker=record.marker,
                )
            try:
                updated = journal.record_receipt(
                    issuer_id,
                    operation_id,
                    vikunja_id=result.vikunja_id,
                    project_id=record.project_id,
                    marker=record.marker,
                    effect={
                        "title": result.task.get("title"),
                        "due_date": result.task.get("due_date"),
                        "done": result.task.get("done"),
                        "project_id": observed_project,
                        "source": "exact_reconcile",
                    },
                )
            except JournalError as receipt_exc:
                # Race: writer confirmed between get_operation and record_receipt.
                if receipt_exc.outcome is Outcome.CONFLICT:
                    raced = journal.get_operation(issuer_id, operation_id)
                    if raced is not None and _matching_confirmed_create_receipt(
                        raced,
                        vikunja_id=result.vikunja_id,
                        project_id=record.project_id,
                        marker=record.marker,
                    ):
                        return _reuse_confirmed_create_receipt(
                            raced,
                            task=result.task,
                            vikunja_id=result.vikunja_id,
                            marker=record.marker,
                        )
                raise
            result.operation = updated
            return result
        if result.outcome is Outcome.CONFLICT:
            updated = journal.mark_conflict(
                issuer_id,
                operation_id,
                reason="multiple_exact_marker_matches",
                details={"matches": result.matches, **result.details},
            )
            result.operation = updated
            return result
        if result.outcome is Outcome.EMPTY:
            updated = journal.mark_unknown(
                issuer_id,
                operation_id,
                reason="zero_exact_matches_after_uncertain_dispatch",
                details={**result.details, "forbid_create": True},
            )
            result.operation = updated
            # Keep EMPTY so callers can distinguish "no match" from other unknowns,
            # while durable state remains unknown and create stays forbidden.
            return result
        # Incomplete / access / dependency: durable unknown, no create.
        updated = journal.mark_unknown(
            issuer_id,
            operation_id,
            reason=f"reconcile_{result.outcome.value}",
            details={**result.details, "forbid_create": True, "resend_forbidden": True},
        )
        result.operation = updated
        return result
    except JournalError as exc:
        # Final safety: a confirmed matching receipt must never surface as conflict
        # solely because reconcile effect source differs from create_read_back.
        current = journal.get_operation(issuer_id, operation_id)
        if (
            current is not None
            and current.receipt is not None
            and result.vikunja_id is not None
            and _matching_confirmed_create_receipt(
                current,
                vikunja_id=result.vikunja_id,
                project_id=current.project_id,
                marker=current.marker,
            )
        ):
            return _reuse_confirmed_create_receipt(
                current,
                task=result.task,
                vikunja_id=result.vikunja_id,
                marker=current.marker,
            )
        return ReconcileResult(
            outcome=exc.outcome if exc.outcome is not Outcome.VALIDATION_ERROR else Outcome.UNKNOWN,
            operation=current,
            details={"reason": str(exc), **exc.details, "forbid_create": True},
        )
