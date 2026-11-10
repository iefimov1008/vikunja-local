"""Unified integration writer: durable intent → claim → HTTP → receipt → link callback.

Connects journal (s03) and stock API client (s02). First HTTP only after durable
dispatch_started. Uncertain outcomes use exact reconcile only — never blind create
retry. Hermes backlink is an optional controlled callback (not a production bridge).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .api import (
    ApiError,
    Outcome,
    TaskResult,
    VikunjaClient,
    build_marker,
    extract_marker,
    validate_uuid_v4,
)
from .dates import ConfirmedDue, same_instant, same_stock_persisted_instant
from .journal import (
    DispatchCriticalSection,
    IntegrationJournal,
    JournalError,
    OperationRecord,
    OperationState,
    new_operation_id,
)

# C1 §5.4/§6: agreed mutation snapshot and durable recurring complete witness.
_OCCURRENCE_SNAPSHOT_KEYS = (
    "id",
    "title",
    "project_id",
    "done",
    "due_date",
    "done_at",
    "start_date",
    "end_date",
    "repeat_after",
    "repeat_mode",
    "reminders",
    "description",
)
_OCCURRENCE_SCALAR_KEYS = (
    "id",
    "title",
    "project_id",
    "done",
    "repeat_after",
    "repeat_mode",
    "description",
)
_OCCURRENCE_INSTANT_KEYS = ("due_date", "done_at", "start_date", "end_date")
_RECEIPT_WITNESS_KEYS = (
    "preimage",
    "postimage",
    "occurrence",
    "recurring_transition",
    "noop_304",
    "patch_http",
)
from .reconcile import (
    ReconcileResult,
    apply_create_reconcile_to_journal,
    reconcile_unknown_mutation,
)

LinkCallback = Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
CrashHook = Callable[[str], None]

# Definitive API refusals must stay typed (auth/scope/validation). They are not
# ambiguous unknown outcomes and must not be erased by empty/exact reconcile.
_DEFINITIVE_REFUSAL_OUTCOMES = frozenset(
    {
        Outcome.AUTH_DENIED,
        Outcome.SCOPE_DENIED,
        Outcome.VALIDATION_ERROR,
    }
)


@dataclass
class DispatchResult:
    outcome: Outcome
    operation: OperationRecord | None = None
    task: dict[str, Any] | None = None
    vikunja_id: int | None = None
    create_http_attempts: int = 0
    linked: bool = False
    link_result: Mapping[str, Any] | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.CONFIRMED and self.vikunja_id is not None


def _due_payload(due: ConfirmedDue | None) -> dict[str, Any] | None:
    if due is None:
        return None
    return {
        "kind": due.kind.value,
        "timezone_name": due.timezone_name,
        "expression": due.expression,
        "wire": due.wire_value(),
        "confirmed_at": due.confirmed_at.isoformat(),
    }


class IntegrationDispatcher:
    """Single writer for synthetic create/update/complete with recovery."""

    def __init__(
        self,
        journal: IntegrationJournal,
        client: VikunjaClient,
        *,
        link_callback: LinkCallback | None = None,
        crash_hook: CrashHook | None = None,
    ):
        self.journal = journal
        self.client = client
        self.link_callback = link_callback
        # Test-only: invoked at named barriers. Production callers leave unset.
        self.crash_hook = crash_hook
        self._create_http_attempts: dict[tuple[str, str], int] = {}

    def _fire_crash(self, point: str) -> None:
        if self.crash_hook is not None:
            self.crash_hook(point)

    def _bump_create_attempt(self, issuer_id: str, operation_id: str) -> int:
        key = (issuer_id, operation_id)
        self._create_http_attempts[key] = self._create_http_attempts.get(key, 0) + 1
        return self._create_http_attempts[key]

    def create_http_attempts(self, issuer_id: str, operation_id: str) -> int:
        return self._create_http_attempts.get((issuer_id, operation_id), 0)

    def _call_link(
        self,
        *,
        issuer_id: str,
        operation_id: str,
        vikunja_id: int,
        marker: str,
        record: OperationRecord,
    ) -> tuple[bool, Mapping[str, Any] | None]:
        """Controlled Hermes backlink hook. Not a production bridge claim."""
        if self.link_callback is None:
            return False, {"skipped": True, "reason": "no_callback", "production_bridge": False}
        payload = {
            "issuer_id": issuer_id,
            "operation_id": operation_id,
            "vikunja_id": vikunja_id,
            "marker": marker,
            "project_id": record.project_id,
            "source_kind": record.source_kind,
            "source_key": record.source_key,
            "controlled_callback": True,
            "production_bridge": False,
        }
        self._fire_crash("after_receipt_before_link")
        try:
            result = self.link_callback(payload)
        except Exception as exc:  # noqa: BLE001 - link failure must not undo receipt
            return False, {
                "ok": False,
                "error": str(exc),
                "production_bridge": False,
                "receipt_preserved": True,
            }
        if result is None:
            result = {"ok": True}
        ok = bool(result.get("ok", True))
        out = dict(result)
        out.setdefault("production_bridge", False)
        out.setdefault("controlled_callback", True)
        return ok, out

    def _result_from_confirmed(
        self,
        record: OperationRecord,
        *,
        task: dict[str, Any] | None = None,
        linked: bool = False,
        link_result: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> DispatchResult:
        vikunja_id = None
        if record.receipt is not None:
            vikunja_id = int(record.receipt["vikunja_id"])
        return DispatchResult(
            outcome=Outcome.CONFIRMED,
            operation=record,
            task=task,
            vikunja_id=vikunja_id,
            create_http_attempts=self.create_http_attempts(record.issuer_id, record.operation_id),
            linked=linked,
            link_result=link_result,
            details=dict(details or {}),
        )

    def recover_create(self, issuer_id: str, operation_id: str) -> DispatchResult:
        """Recover after crash/timeout: exact reconcile only, never a second create."""
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        record = self.journal.get_operation(issuer_id, operation_id)
        if record is None:
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "operation_not_found"},
            )
        # Kind must win over receipt shortcuts: a confirmed update/complete must
        # never enter create backlink recovery (FR-EA010-10/13).
        if record.op_type != "create":
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                operation=record,
                details={"reason": "recover_create_requires_create_op"},
            )
        if record.state is OperationState.CONFIRMED and record.receipt is not None:
            return self._recover_confirmed_create(record)
        # First HTTP is forbidden once dispatch may have started or receipt is missing
        # after a possible send. Exact reconcile only.
        if record.allows_first_http_dispatch() and self.journal.can_dispatch(issuer_id, operation_id):
            # Durable claim never started — still may run first create via execute path.
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=record,
                details={
                    "reason": "prepared_without_dispatch",
                    "allows_first_http": True,
                    "path": "recover_needs_execute",
                },
            )
        refusal = self._refusal_from_history(issuer_id, operation_id)
        if refusal is not None:
            outcome, detail = refusal
            return self._result_from_definitive_refusal(
                issuer_id=issuer_id,
                operation_id=operation_id,
                outcome=outcome,
                path="recover_definitive_create_refusal",
                mutation_sent=detail.get("mutation_sent")
                if isinstance(detail.get("mutation_sent"), bool)
                else None,
                extra=detail,
            )
        reconciled = apply_create_reconcile_to_journal(
            self.journal,
            issuer_id=issuer_id,
            operation_id=operation_id,
            client=self.client,
        )
        if reconciled.ok and reconciled.operation is not None and reconciled.vikunja_id is not None:
            # Receipt-only shortcuts (including unknown→confirmed races) must still
            # pass live scope/access/marker read-back before any backlink callback.
            validated = self._recover_confirmed_create(reconciled.operation)
            if validated.ok:
                validated.details = {
                    **reconciled.details,
                    **validated.details,
                    "path": "recover_exact_one",
                    "live_validated": True,
                }
            return validated
        return DispatchResult(
            outcome=reconciled.outcome if reconciled.outcome is not Outcome.EMPTY else Outcome.UNKNOWN,
            operation=reconciled.operation,
            task=reconciled.task,
            vikunja_id=reconciled.vikunja_id,
            create_http_attempts=self.create_http_attempts(issuer_id, operation_id),
            details={
                "path": "recover_unresolved",
                "forbid_create": True,
                "scan_outcome": reconciled.outcome.value,
                **reconciled.details,
            },
        )

    def execute_create(
        self,
        *,
        issuer_id: str,
        operation_id: str | None = None,
        title: str,
        project_id: int | None = None,
        user_portion: str = "",
        due: ConfirmedDue | None = None,
        source_kind: str | None = None,
        source_key: str | None = None,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> DispatchResult:
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        op_id = validate_uuid_v4(operation_id or new_operation_id(), label="operation_id")
        pid = project_id if project_id is not None else self.client.project_id
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "project_id"},
            )
        if pid != self.client.project_id:
            return DispatchResult(
                outcome=Outcome.SCOPE_DENIED,
                details={
                    "reason": "dispatcher_project_mismatch",
                    "client_project_id": self.client.project_id,
                    "requested_project_id": pid,
                },
            )
        if not isinstance(title, str) or not title.strip():
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "title"},
            )

        payload: dict[str, Any] = {
            "title": title,
            "user_portion": user_portion,
        }
        if extra_fields:
            payload["extra_fields"] = dict(extra_fields)
        frozen = _due_payload(due)

        try:
            record = self.journal.prepare_intent(
                issuer_id=issuer_id,
                operation_id=op_id,
                op_type="create",
                project_id=pid,
                payload=payload,
                frozen_due=frozen,
                source_kind=source_kind,
                source_key=source_key,
            )
        except JournalError as exc:
            return DispatchResult(
                outcome=exc.outcome,
                details={"phase": "prepare_intent", "reason": str(exc), **exc.details},
            )

        if record.state is OperationState.CONFIRMED and record.receipt is not None:
            return self.recover_create(issuer_id, op_id)

        if record.state is OperationState.CONFLICT:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=record,
                details={"phase": "prepare_intent", "reason": "durable_conflict"},
            )

        # After possible prior dispatch / unknown: reconcile only.
        if not record.allows_first_http_dispatch() or not self.journal.can_dispatch(issuer_id, op_id):
            return self.recover_create(issuer_id, op_id)

        self._fire_crash("before_http")

        try:
            with self.journal.dispatch_critical_section() as section:
                claimed = section.begin_dispatch(issuer_id, op_id)
                # Crash after durable claim, still before socket send.
                self._fire_crash("after_dispatch_claim_before_http")
                attempts = self._bump_create_attempt(issuer_id, op_id)
                create_result = self.client.create(
                    title=title,
                    issuer_id=issuer_id,
                    operation_id=op_id,
                    user_portion=user_portion,
                    due=due,
                    extra_fields=extra_fields,
                )
                if create_result.ok and create_result.task is not None:
                    task_id = int(create_result.task["id"])
                    marker = build_marker(issuer_id, op_id)
                    # Barrier: HTTP create committed/observed, receipt not yet durable.
                    self._fire_crash("after_commit_before_receipt")
                    confirmed = section.record_receipt(
                        issuer_id,
                        op_id,
                        vikunja_id=task_id,
                        project_id=pid,
                        marker=marker,
                        effect={
                            "title": create_result.task.get("title"),
                            "due_date": create_result.task.get("due_date"),
                            "done": create_result.task.get("done"),
                            "create_http": create_result.details.get("create_http"),
                            "source": "create_read_back",
                        },
                    )
                    linked, link_result = self._call_link(
                        issuer_id=issuer_id,
                        operation_id=op_id,
                        vikunja_id=task_id,
                        marker=marker,
                        record=confirmed,
                    )
                    return DispatchResult(
                        outcome=Outcome.CONFIRMED,
                        operation=confirmed,
                        task=create_result.task,
                        vikunja_id=task_id,
                        create_http_attempts=attempts,
                        linked=linked,
                        link_result=link_result,
                        details={"path": "first_create", "create_http": create_result.details.get("create_http")},
                    )

                if self._is_definitive_refusal(create_result):
                    # Typed auth/scope/validation refusal: durable safety against
                    # resend, but do not erase the class via empty reconcile.
                    refused = section.mark_unknown(
                        issuer_id,
                        op_id,
                        reason="definitive_api_refusal",
                        details={
                            "api_outcome": create_result.outcome.value,
                            "mutation_sent": create_result.mutation_sent,
                            "definitive_refusal": True,
                            **create_result.details,
                        },
                    )
                    return DispatchResult(
                        outcome=create_result.outcome,
                        operation=refused,
                        create_http_attempts=attempts,
                        details={
                            "path": "definitive_create_refusal",
                            "resend_forbidden": True,
                            "forbid_create": True,
                            "api_outcome": create_result.outcome.value,
                            "mutation_sent": create_result.mutation_sent,
                            "definitive_refusal": True,
                            **{
                                k: v
                                for k, v in create_result.details.items()
                                if k
                                not in {
                                    "path",
                                    "resend_forbidden",
                                    "forbid_create",
                                    "definitive_refusal",
                                    "api_outcome",
                                    "mutation_sent",
                                }
                            },
                        },
                    )

                # Ambiguous outcome (timeout / unknown). Mark unknown then exact
                # reconcile; never issue a second create here.
                section.mark_unknown(
                    issuer_id,
                    op_id,
                    reason="create_outcome_uncertain",
                    details={
                        "api_outcome": create_result.outcome.value,
                        "mutation_sent": create_result.mutation_sent,
                        **create_result.details,
                    },
                )
        except JournalError as exc:
            # Claim race: peer may already have confirmed. Recover outside the lock
            # instead of returning obsolete uncertainty for a durable correspondence.
            raced = self._recover_after_claim_race(
                issuer_id,
                op_id,
                exc,
                create_http_attempts=self.create_http_attempts(issuer_id, op_id),
            )
            if raced is not None:
                return raced
            return DispatchResult(
                outcome=exc.outcome,
                operation=self.journal.get_operation(issuer_id, op_id),
                create_http_attempts=self.create_http_attempts(issuer_id, op_id),
                details={"phase": "dispatch", "reason": str(exc), **exc.details},
            )

        # Outside the critical section: read-only exact reconcile under a fresh lock.
        return self.recover_create(issuer_id, op_id)

    def execute_update(
        self,
        *,
        issuer_id: str,
        operation_id: str | None = None,
        task_id: int,
        patch: Mapping[str, Any],
        source_kind: str | None = None,
        source_key: str | None = None,
    ) -> DispatchResult:
        return self._execute_mutation(
            issuer_id=issuer_id,
            operation_id=operation_id,
            op_type="update",
            task_id=task_id,
            patch=patch,
            source_kind=source_kind,
            source_key=source_key,
        )

    def execute_complete(
        self,
        *,
        issuer_id: str,
        operation_id: str | None = None,
        task_id: int,
        source_kind: str | None = None,
        source_key: str | None = None,
    ) -> DispatchResult:
        return self._execute_mutation(
            issuer_id=issuer_id,
            operation_id=operation_id,
            op_type="complete",
            task_id=task_id,
            patch={"done": True},
            source_kind=source_kind,
            source_key=source_key,
        )

    def recover_unknown_mutation(self, issuer_id: str, operation_id: str) -> DispatchResult:
        """Observe unknown update/complete without re-sending; especially recurring."""
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        record = self.journal.get_operation(issuer_id, operation_id)
        if record is None:
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "operation_not_found"},
            )
        if record.op_type not in {"update", "complete"}:
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                operation=record,
                details={"reason": "not_a_mutation"},
            )
        if record.state is OperationState.CONFIRMED and record.receipt is not None:
            return self._recover_confirmed_mutation(
                record,
                path="mutation_already_confirmed",
            )
        if record.target_id is None:
            return DispatchResult(
                outcome=Outcome.VALIDATION_ERROR,
                operation=record,
                details={"reason": "missing_target_id"},
            )
        refusal = self._refusal_from_history(issuer_id, operation_id)
        if refusal is not None:
            outcome, detail = refusal
            return self._result_from_definitive_refusal(
                issuer_id=issuer_id,
                operation_id=operation_id,
                outcome=outcome,
                path="recover_definitive_mutation_refusal",
                mutation_sent=detail.get("mutation_sent")
                if isinstance(detail.get("mutation_sent"), bool)
                else None,
                extra=detail,
            )
        expected_marker = record.marker or None
        if expected_marker == "":
            expected_marker = None
        preimage = None
        if isinstance(record.payload, dict):
            preimage = record.payload.get("preimage")
            if not isinstance(preimage, dict):
                preimage = None
        observed = reconcile_unknown_mutation(
            self.client,
            task_id=record.target_id,
            expected_marker=expected_marker,
            project_id=record.project_id,
            preimage=preimage,
        )
        # Never record a causal receipt for unknown mutation without witness;
        # keep durable unknown. Confirmed observation is informational only.
        if observed.outcome is Outcome.CONFIRMED and observed.task is not None:
            try:
                updated = self.journal.mark_unknown(
                    issuer_id,
                    operation_id,
                    reason="mutation_observed_without_causal_proof",
                    details={
                        "observed_done": observed.task.get("done"),
                        "observed_due": observed.task.get("due_date"),
                        "causal_proof": False,
                        **observed.details,
                    },
                )
            except JournalError as exc:
                updated = record
                return DispatchResult(
                    outcome=exc.outcome,
                    operation=updated,
                    task=observed.task,
                    vikunja_id=observed.vikunja_id,
                    details={"reason": str(exc), **exc.details},
                )
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=updated,
                task=observed.task,
                vikunja_id=observed.vikunja_id,
                details={
                    "path": "mutation_observe_only",
                    "resend_forbidden": True,
                    **observed.details,
                },
            )
        try:
            updated = self.journal.mark_unknown(
                issuer_id,
                operation_id,
                reason=f"mutation_reconcile_{observed.outcome.value}",
                details={**observed.details, "resend_forbidden": True},
            )
        except JournalError:
            updated = record
        return DispatchResult(
            outcome=observed.outcome if observed.outcome is not Outcome.CONFIRMED else Outcome.UNKNOWN,
            operation=updated,
            task=observed.task,
            vikunja_id=observed.vikunja_id,
            details={"path": "mutation_unresolved", "resend_forbidden": True, **observed.details},
        )

    def _observe_confirmed_target(
        self,
        record: OperationRecord,
        *,
        path: str,
        vikunja_id: int,
        expected_marker: str,
        callback_skipped: bool,
    ) -> DispatchResult | tuple[dict[str, Any], int]:
        """Fresh GET + access/project/marker check for a durable receipt target.

        Preserves the historical receipt. Returns a typed DispatchResult on
        denial/deletion/unavailable/marker loss, or (task, vikunja_id) when live
        state still matches the receipt correlation.
        """
        if self.client.project_id != record.project_id:
            return DispatchResult(
                outcome=Outcome.SCOPE_DENIED,
                operation=record,
                vikunja_id=vikunja_id,
                details={
                    "path": path,
                    "reason": "client_project_mismatch",
                    "receipt_preserved": True,
                    **({"callback_skipped": True} if callback_skipped else {}),
                    "client_project_id": self.client.project_id,
                    "record_project_id": record.project_id,
                },
            )
        try:
            read = self.client.read(vikunja_id)
        except ApiError as exc:
            # Transport / token / non-JSON faults raise ApiError from VikunjaClient.read
            # and must not escape as uncaught exceptions (FR-EA010-21 / NFR-16).
            read = TaskResult(
                outcome=exc.outcome,
                details={"reason": str(exc), "api_error": True, **dict(exc.details)},
            )
        if read.outcome in {
            Outcome.AUTH_DENIED,
            Outcome.SCOPE_DENIED,
            Outcome.DEPENDENCY_UNAVAILABLE,
            Outcome.INCOMPLETE_SCAN,
        }:
            return DispatchResult(
                outcome=read.outcome,
                operation=record,
                vikunja_id=vikunja_id,
                details={
                    "path": path,
                    "reason": "read_back_denied_or_unavailable",
                    "receipt_preserved": True,
                    "resend_forbidden": True,
                    "forbid_create": True,
                    **({"callback_skipped": True} if callback_skipped else {}),
                    **read.details,
                },
            )
        if not read.ok or read.task is None:
            # Stock maps missing/deleted tasks to HTTP 404 → VALIDATION_ERROR.
            # Keep durable receipt; surface deletion/unavailable distinctly from success.
            gone_outcome = (
                Outcome.UNKNOWN
                if read.outcome
                in {Outcome.CONFIRMED, Outcome.VALIDATION_ERROR, Outcome.EMPTY, Outcome.UNKNOWN}
                else read.outcome
            )
            deletion_like = read.outcome in {
                Outcome.VALIDATION_ERROR,
                Outcome.EMPTY,
                Outcome.UNKNOWN,
            } or read.http_status in {404, 410}
            return DispatchResult(
                outcome=gone_outcome,
                operation=record,
                vikunja_id=vikunja_id,
                details={
                    "path": path,
                    "reason": (
                        "target_deleted_or_gone" if deletion_like else "linked_task_unreadable_or_gone"
                    ),
                    "receipt_preserved": True,
                    "resend_forbidden": True,
                    "forbid_create": True,
                    **({"callback_skipped": True} if callback_skipped else {}),
                    "read_outcome": read.outcome.value,
                    "http_status": read.http_status,
                    **read.details,
                },
            )
        task = read.task
        observed_project = task.get("project_id")
        if observed_project != record.project_id:
            return DispatchResult(
                outcome=Outcome.SCOPE_DENIED,
                operation=record,
                task=task,
                vikunja_id=vikunja_id,
                details={
                    "path": path,
                    "reason": "observed_project_mismatch",
                    "receipt_preserved": True,
                    "resend_forbidden": True,
                    **({"callback_skipped": True} if callback_skipped else {}),
                    "expected_project_id": record.project_id,
                    "got_project_id": observed_project,
                },
            )
        extracted = extract_marker(task.get("description"))
        observed_marker = extracted["marker"] if extracted else ""
        if (expected_marker or "") != observed_marker:
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=record,
                task=task,
                vikunja_id=vikunja_id,
                details={
                    "path": path,
                    "reason": "marker_corrupted_or_removed",
                    "receipt_preserved": True,
                    "resend_forbidden": True,
                    "forbid_create": True,
                    **({"callback_skipped": True} if callback_skipped else {}),
                    "expected_marker": expected_marker,
                    "observed_marker": observed_marker or None,
                },
            )
        return task, vikunja_id

    def _recover_confirmed_create(self, record: OperationRecord) -> DispatchResult:
        """Validate live read-back before any backlink callback for a durable receipt."""
        receipt = record.receipt
        if receipt is None:
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=record,
                details={"path": "recover_confirmed_missing_receipt", "forbid_create": True},
            )
        vikunja_id = int(receipt["vikunja_id"])
        observed = self._observe_confirmed_target(
            record,
            path="recover_already_confirmed",
            vikunja_id=vikunja_id,
            expected_marker=record.marker or "",
            callback_skipped=True,
        )
        if isinstance(observed, DispatchResult):
            return observed
        task, vikunja_id = observed
        linked, link_result = self._call_link(
            issuer_id=record.issuer_id,
            operation_id=record.operation_id,
            vikunja_id=vikunja_id,
            marker=record.marker,
            record=record,
        )
        return self._result_from_confirmed(
            record,
            task=task,
            linked=linked,
            link_result=link_result,
            details={"path": "recover_already_confirmed"},
        )

    def _recover_confirmed_mutation(
        self,
        record: OperationRecord,
        *,
        path: str,
    ) -> DispatchResult:
        """Fresh GET for confirmed update/complete; never re-send PATCH."""
        receipt = record.receipt
        if receipt is None:
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=record,
                details={"path": path, "reason": "missing_receipt", "resend_forbidden": True},
            )
        vikunja_id = int(receipt["vikunja_id"])
        expected_marker = record.marker or ""
        if record.receipt is not None and isinstance(record.receipt.get("marker"), str):
            expected_marker = record.receipt["marker"]
        observed = self._observe_confirmed_target(
            record,
            path=path,
            vikunja_id=vikunja_id,
            expected_marker=expected_marker,
            callback_skipped=False,
        )
        if isinstance(observed, DispatchResult):
            return observed
        task, vikunja_id = observed
        return self._result_from_confirmed(
            record,
            task=task,
            details={"path": path, "receipt_preserved": True, "live_read_back": True},
        )

    def _recover_after_claim_race(
        self,
        issuer_id: str,
        operation_id: str,
        exc: JournalError,
        *,
        create_http_attempts: int | None = None,
        mutation: bool = False,
    ) -> DispatchResult | None:
        """Re-read durable state after begin_dispatch refusal; recover instead of stale unknown."""
        current = self.journal.get_operation(issuer_id, operation_id)
        if current is None:
            return None
        if current.allows_first_http_dispatch() and self.journal.can_dispatch(issuer_id, operation_id):
            return None
        if mutation:
            recovered = self.recover_unknown_mutation(issuer_id, operation_id)
        else:
            recovered = self.recover_create(issuer_id, operation_id)
        recovered.details = {
            "path": "claim_race_recover",
            "prior_dispatch_error": str(exc),
            "prior_outcome": exc.outcome.value,
            **recovered.details,
        }
        if create_http_attempts is not None:
            recovered.create_http_attempts = create_http_attempts
        return recovered

    def _mutation_bound_envelope(
        self,
        existing: OperationRecord,
        *,
        op_type: str,
        task_id: int,
        patch: Mapping[str, Any],
    ) -> DispatchResult | tuple[dict[str, Any], str, int]:
        """Validate identity against a durable mutation envelope and reuse its preimage.

        Returns (payload, marker, intent_project_id) on success, or a typed refusal.
        Caller-supplied patch/source still go through prepare_intent for mismatch checks.
        """
        if existing.op_type != op_type:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=existing,
                details={
                    "phase": "prepare_intent",
                    "reason": "op_type_mismatch",
                    "existing_op_type": existing.op_type,
                    "requested_op_type": op_type,
                },
            )
        if existing.target_id != task_id:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=existing,
                details={
                    "phase": "prepare_intent",
                    "reason": "target_id_mismatch",
                    "existing_target_id": existing.target_id,
                    "requested_target_id": task_id,
                },
            )
        if self.client.project_id != existing.project_id:
            return DispatchResult(
                outcome=Outcome.SCOPE_DENIED,
                operation=existing,
                details={
                    "phase": "prepare_intent",
                    "reason": "client_project_mismatch",
                    "client_project_id": self.client.project_id,
                    "record_project_id": existing.project_id,
                },
            )
        if existing.state is OperationState.CONFLICT:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=existing,
                details={"phase": "prepare_intent", "reason": "durable_conflict"},
            )
        if not isinstance(existing.payload, dict) or "preimage" not in existing.payload:
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=existing,
                details={"reason": "missing_persisted_preimage", "resend_forbidden": True},
            )
        payload = {
            "patch": dict(patch),
            "preimage": dict(existing.payload["preimage"]),
        }
        return payload, existing.marker or "", existing.project_id

    @staticmethod
    def _occurrence_snapshot(task: Mapping[str, Any]) -> dict[str, Any]:
        """Persist present C1 occurrence fields without collapsing absences via .get()."""
        snap: dict[str, Any] = {}
        for key in _OCCURRENCE_SNAPSHOT_KEYS:
            if key in task:
                snap[key] = task[key]
        return snap

    def _mutation_preimage_payload(
        self,
        preimage: Mapping[str, Any],
        patch: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "patch": dict(patch),
            "preimage": self._occurrence_snapshot(preimage),
        }

    def _mutation_receipt_effect(
        self,
        *,
        patch: Mapping[str, Any],
        api_result: TaskResult,
        observed_marker: str | None,
    ) -> dict[str, Any]:
        """Durable receipt effect including C1 recurring occurrence witness when present."""
        effect: dict[str, Any] = {
            "patch": dict(patch),
            "done": api_result.task.get("done") if api_result.task is not None else None,
            "due_date": api_result.task.get("due_date") if api_result.task is not None else None,
            "mutation_sent": api_result.mutation_sent,
            "noop": api_result.details.get("noop", False),
            "recurring": api_result.details.get("recurring"),
            "source": "mutation_read_back",
            "observed_marker": observed_marker,
        }
        for key in _RECEIPT_WITNESS_KEYS:
            if key in api_result.details:
                effect[key] = api_result.details[key]
        return effect

    def _reminders_match(self, left: Any, right: Any) -> bool:
        if left == right:
            return True
        rem_eq = getattr(self.client, "_reminders_equal", None)
        if callable(rem_eq):
            try:
                return bool(rem_eq(left, right))
            except (TypeError, ValueError, ApiError):
                return False
        return False

    def _conflict_keeps_unresolved_target_fence(self, row: OperationRecord) -> bool:
        """True when CONFLICT still fences the target against a fresh mutation key.

        prepare_intent may durably move an unknown/dispatch_started row to CONFLICT
        on an incompatible same-key replay (source/patch/target mismatch). That does
        not resolve whether the earlier PATCH ran: without a causal receipt, history
        proof of possible dispatch must keep the C1 §6 resend fence (AC10/18).
        """
        if row.receipt is not None:
            return False
        events = {
            str(event.get("event_type") or "")
            for event in self.journal.history(row.issuer_id, row.operation_id)
        }
        if "receipt" in events:
            return False
        return bool(events & {"dispatch_started", "unknown"})

    def _unresolved_mutation_blocker(
        self,
        *,
        task_id: int,
        exclude_operation_id: str,
    ) -> OperationRecord | None:
        """Return a durable unresolved mutation for the same target, if any.

        C1 §6: a new key does not authorize retry while a prior update/complete for
        the same target/occurrence remains unknown, dispatch_started, or an
        unresolved CONFLICT with possible prior dispatch and no causal receipt.
        Confirmed peers (and conflicts that never claimed HTTP) do not block the
        next explicit occurrence command.
        """
        for row in self.journal.list_operations():
            if row.operation_id == exclude_operation_id:
                continue
            if row.op_type not in {"update", "complete"}:
                continue
            if row.target_id != task_id:
                continue
            if row.state in {OperationState.DISPATCH_STARTED, OperationState.UNKNOWN}:
                return row
            if row.state is OperationState.CONFLICT and self._conflict_keeps_unresolved_target_fence(
                row
            ):
                return row
        return None

    def _result_unresolved_target_blocker(
        self,
        blocker: OperationRecord,
        *,
        record: OperationRecord | None = None,
        phase: str,
    ) -> DispatchResult:
        return DispatchResult(
            outcome=Outcome.CONFLICT,
            operation=record,
            details={
                "phase": phase,
                "reason": "unresolved_target_mutation",
                "blocking_operation_id": blocker.operation_id,
                "blocking_issuer_id": blocker.issuer_id,
                "blocking_state": blocker.state.value,
                "blocking_op_type": blocker.op_type,
                "resend_forbidden": True,
            },
        )

    def _mutation_preimage_mismatch(
        self,
        live: Mapping[str, Any],
        preimage: Mapping[str, Any],
    ) -> str | None:
        """Return the first disagreed agreed-snapshot field, or None when it matches.

        Only keys present in the durable preimage are compared so older reduced
        envelopes remain classifiable; new envelopes persist the full C1 set.
        """
        for key in _OCCURRENCE_SCALAR_KEYS:
            if key not in preimage:
                continue
            if key not in live or live[key] != preimage[key]:
                return key
        for key in _OCCURRENCE_INSTANT_KEYS:
            if key not in preimage:
                continue
            if key not in live:
                return key
            try:
                # due_date kept on exact-instant semantics for ordinary targets;
                # occurrence side-fields tolerate stock second truncation.
                if key == "due_date":
                    if not same_instant(live[key], preimage[key]):
                        return key
                elif not same_stock_persisted_instant(live[key], preimage[key]):
                    return key
            except ValueError:
                return key
        if "reminders" in preimage:
            if "reminders" not in live or not self._reminders_match(
                live["reminders"], preimage["reminders"]
            ):
                return "reminders"
        return None

    def _classify_locked_mutation(
        self,
        *,
        issuer_id: str,
        operation_id: str,
        record: OperationRecord,
    ) -> DispatchResult | OperationRecord:
        """Under the dispatch lock, classify durable state before first-send gates.

        Confirmed receipts are reused; already-claimed/unknown rows are observed
        without PATCH; only a still-dispatchable prepared intent proceeds.
        """
        if record.state is OperationState.CONFIRMED and record.receipt is not None:
            return self._recover_confirmed_mutation(
                record,
                path="mutation_locked_replay",
            )
        if record.state is OperationState.CONFLICT:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=record,
                details={"phase": "locked_reload", "reason": "durable_conflict"},
            )
        if not record.allows_first_http_dispatch() or not self.journal.can_dispatch(
            issuer_id, operation_id
        ):
            return self.recover_unknown_mutation(issuer_id, operation_id)
        return record

    def _locked_mutation_preimage_gate(
        self,
        *,
        section: DispatchCriticalSection,
        issuer_id: str,
        operation_id: str,
        task_id: int,
        record: OperationRecord,
    ) -> DispatchResult | None:
        """Under the dispatch lock, re-read and compare the agreed snapshot before PATCH.

        Detected drift → durable conflict without begin_dispatch / HTTP. Same-key
        replay of a confirmed receipt never reaches this gate (caller reloads first).
        """
        if not isinstance(record.payload, dict):
            refused = section.mark_unknown(
                issuer_id,
                operation_id,
                reason="missing_persisted_preimage",
                details={"resend_forbidden": True},
            )
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=refused,
                details={
                    "phase": "locked_preimage",
                    "reason": "missing_persisted_preimage",
                    "resend_forbidden": True,
                },
            )
        preimage = record.payload.get("preimage")
        if not isinstance(preimage, dict):
            refused = section.mark_unknown(
                issuer_id,
                operation_id,
                reason="missing_persisted_preimage",
                details={"resend_forbidden": True},
            )
            return DispatchResult(
                outcome=Outcome.UNKNOWN,
                operation=refused,
                details={
                    "phase": "locked_preimage",
                    "reason": "missing_persisted_preimage",
                    "resend_forbidden": True,
                },
            )
        try:
            live_read = self.client.read(task_id)
        except ApiError as exc:
            live_read = TaskResult(
                outcome=exc.outcome,
                details={"reason": str(exc), "api_error": True, **dict(exc.details)},
            )
        if not live_read.ok or live_read.task is None:
            return DispatchResult(
                outcome=(
                    live_read.outcome
                    if live_read.outcome is not Outcome.CONFIRMED
                    else Outcome.UNKNOWN
                ),
                operation=record,
                details={
                    "phase": "locked_preimage",
                    "reason": "live_preimage_unreadable",
                    "resend_forbidden": True,
                    "forbid_create": True,
                    **live_read.details,
                },
            )
        mismatched = self._mutation_preimage_mismatch(live_read.task, preimage)
        if mismatched is None:
            return None
        detail = {
            "reason": "preimage_drift_before_patch",
            "field": mismatched,
            "resend_forbidden": True,
        }
        refused = section.mark_conflict(
            issuer_id,
            operation_id,
            reason="preimage_drift_before_patch",
            details=detail,
        )
        return DispatchResult(
            outcome=Outcome.CONFLICT,
            operation=refused,
            task=live_read.task,
            vikunja_id=task_id,
            details={
                "path": "locked_preimage_conflict",
                "phase": "locked_preimage",
                **detail,
            },
        )

    @staticmethod
    def _is_definitive_refusal(result: TaskResult) -> bool:
        return result.outcome in _DEFINITIVE_REFUSAL_OUTCOMES

    def _refusal_from_history(
        self, issuer_id: str, operation_id: str
    ) -> tuple[Outcome, dict[str, Any]] | None:
        """Return the latest durable definitive refusal, if any.

        Later ambiguous-reconcile events must not erase an earlier typed refusal
        that already proved authorization/validation failure.
        """
        for event in reversed(self.journal.history(issuer_id, operation_id)):
            event_type = event.get("event_type")
            if event_type in {"receipt", "conflict"}:
                return None
            if event_type != "unknown":
                continue
            detail = dict(event.get("detail") or {})
            api_raw = detail.get("api_outcome")
            if not isinstance(api_raw, str):
                continue
            try:
                api_outcome = Outcome(api_raw)
            except ValueError:
                continue
            if api_outcome not in _DEFINITIVE_REFUSAL_OUTCOMES:
                continue
            if detail.get("definitive_refusal") is False:
                continue
            return api_outcome, detail
        return None

    def _result_from_definitive_refusal(
        self,
        *,
        issuer_id: str,
        operation_id: str,
        outcome: Outcome,
        path: str,
        mutation_sent: bool | None = None,
        extra: Mapping[str, Any] | None = None,
        create_http_attempts: int | None = None,
    ) -> DispatchResult:
        details: dict[str, Any] = {
            "path": path,
            "resend_forbidden": True,
            "forbid_create": True,
            "api_outcome": outcome.value,
            "definitive_refusal": True,
        }
        if mutation_sent is not None:
            details["mutation_sent"] = mutation_sent
        if extra:
            for key, value in extra.items():
                if key in details and key in {"path", "resend_forbidden", "forbid_create", "definitive_refusal"}:
                    continue
                details[key] = value
        result = DispatchResult(
            outcome=outcome,
            operation=self.journal.get_operation(issuer_id, operation_id),
            details=details,
        )
        if create_http_attempts is not None:
            result.create_http_attempts = create_http_attempts
        else:
            result.create_http_attempts = self.create_http_attempts(issuer_id, operation_id)
        return result

    def _execute_mutation(
        self,
        *,
        issuer_id: str,
        operation_id: str | None,
        op_type: str,
        task_id: int,
        patch: Mapping[str, Any],
        source_kind: str | None,
        source_key: str | None,
    ) -> DispatchResult:
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        op_id = validate_uuid_v4(operation_id or new_operation_id(), label="operation_id")
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            return DispatchResult(outcome=Outcome.VALIDATION_ERROR, details={"reason": "task_id"})

        # Immutable command identity first: never replace a durable preimage with a
        # fresh GET (successful update/complete changes the object and would conflict).
        # Every replay/recovery shortcut must validate patch/source/project against
        # the persisted envelope before acknowledging the prior receipt.
        existing = self.journal.get_operation(issuer_id, op_id)
        record: OperationRecord | None = None
        if existing is not None:
            bound = self._mutation_bound_envelope(
                existing,
                op_type=op_type,
                task_id=task_id,
                patch=patch,
            )
            if isinstance(bound, DispatchResult):
                return bound
            payload, marker, intent_project_id = bound
            # Prepared intents with a known marker must re-check live correlation
            # before any first HTTP; otherwise a prior marker-loss refusal could
            # still dispatch on identical-key retry using the cached envelope M.
            if (
                (existing.marker or "")
                and existing.state is not OperationState.CONFIRMED
                and existing.allows_first_http_dispatch()
            ):
                try:
                    live_marker_read = self.client.read(task_id)
                except ApiError as exc:
                    live_marker_read = TaskResult(
                        outcome=exc.outcome,
                        details={"reason": str(exc), "api_error": True, **dict(exc.details)},
                    )
                if not live_marker_read.ok or live_marker_read.task is None:
                    return DispatchResult(
                        outcome=(
                            live_marker_read.outcome
                            if live_marker_read.outcome is not Outcome.CONFIRMED
                            else Outcome.UNKNOWN
                        ),
                        operation=existing,
                        details={
                            "phase": "live_marker_recheck",
                            "resend_forbidden": True,
                            "forbid_create": True,
                            **live_marker_read.details,
                        },
                    )
                live_extracted = extract_marker(live_marker_read.task.get("description"))
                marker = live_extracted["marker"] if live_extracted else ""
        else:
            if not isinstance(self.client.project_id, int) or self.client.project_id <= 0:
                return DispatchResult(
                    outcome=Outcome.VALIDATION_ERROR,
                    details={"reason": "project_id"},
                )
            # C1 §6: a new key must not sneak a retry past unresolved uncertainty
            # for the same target/occurrence.
            blocker = self._unresolved_mutation_blocker(
                task_id=task_id,
                exclude_operation_id=op_id,
            )
            if blocker is not None:
                return self._result_unresolved_target_blocker(
                    blocker,
                    phase="admission",
                )
            try:
                pre = self.client.read(task_id)
            except ApiError as exc:
                pre = TaskResult(
                    outcome=exc.outcome,
                    details={"reason": str(exc), "api_error": True, **dict(exc.details)},
                )
            if not pre.ok or pre.task is None:
                return DispatchResult(
                    outcome=pre.outcome if pre.outcome is not Outcome.CONFIRMED else Outcome.UNKNOWN,
                    details={
                        "phase": "preimage",
                        "resend_forbidden": True,
                        "forbid_create": True,
                        **pre.details,
                    },
                )
            live_preimage = pre.task
            live_extracted = extract_marker(live_preimage.get("description"))
            live_marker = live_extracted["marker"] if live_extracted else ""
            # Overlapping identical first calls: a peer may prepare/confirm while this
            # caller GETs. Re-read under the OS-lock and reuse that immutable envelope
            # before prepare_intent compares digests (FR-EA010-10 / NFR-06). Real
            # patch/source/target mismatches still conflict via prepare_intent.
            try:
                with self.journal.dispatch_critical_section():
                    peer = self.journal.get_operation(issuer_id, op_id)
                    if peer is not None:
                        bound = self._mutation_bound_envelope(
                            peer,
                            op_type=op_type,
                            task_id=task_id,
                            patch=patch,
                        )
                        if isinstance(bound, DispatchResult):
                            return bound
                        payload, marker, intent_project_id = bound
                        # Reuse the GET already performed for first-call preimage as the
                        # live marker observation when the peer envelope is still dispatchable.
                        if (
                            (peer.marker or "")
                            and peer.state is not OperationState.CONFIRMED
                            and peer.allows_first_http_dispatch()
                        ):
                            marker = live_marker
                    else:
                        payload = self._mutation_preimage_payload(live_preimage, patch)
                        marker = live_marker
                        intent_project_id = self.client.project_id
                    record = self.journal.prepare_intent(
                        issuer_id=issuer_id,
                        operation_id=op_id,
                        op_type=op_type,
                        project_id=intent_project_id,
                        target_id=task_id,
                        payload=payload,
                        source_kind=source_kind,
                        source_key=source_key,
                    )
            except JournalError as exc:
                return DispatchResult(
                    outcome=exc.outcome,
                    operation=self.journal.get_operation(issuer_id, op_id),
                    details={"phase": "prepare_intent", "reason": str(exc), **exc.details},
                )

        if record is None:
            try:
                record = self.journal.prepare_intent(
                    issuer_id=issuer_id,
                    operation_id=op_id,
                    op_type=op_type,
                    project_id=intent_project_id,
                    target_id=task_id,
                    payload=payload,
                    source_kind=source_kind,
                    source_key=source_key,
                )
            except JournalError as exc:
                # Identity / source / patch mismatches must surface as conflicts (or the
                # journal's typed outcome). Never swallow prepare_intent errors into a
                # false CONFIRMED replay of a prior receipt.
                return DispatchResult(
                    outcome=exc.outcome,
                    operation=self.journal.get_operation(issuer_id, op_id),
                    details={"phase": "prepare_intent", "reason": str(exc), **exc.details},
                )

        # Known nonempty target marker must match live extract; empty live marker
        # against a verified receipt marker is marker loss, not "unmarked manual".
        # Genuinely unmarked manual targets keep both sides empty and stay allowed.
        record_marker = record.marker or ""
        observed_marker = marker or ""
        if record_marker != observed_marker:
            missing = bool(record_marker) and not observed_marker
            outcome = Outcome.UNKNOWN if missing else Outcome.CONFLICT
            reason = (
                "marker_missing_on_known_target"
                if missing
                else "marker_mismatch_on_intent"
            )
            detail = {
                "reason": reason,
                "expected_marker": record_marker or None,
                "observed_marker": observed_marker or None,
                "resend_forbidden": True,
            }
            # Durable refuse so identical-key retry cannot begin_dispatch later.
            try:
                if outcome is Outcome.UNKNOWN:
                    record = self.journal.mark_unknown(
                        issuer_id,
                        op_id,
                        reason=reason,
                        details=detail,
                    )
                else:
                    record = self.journal.mark_conflict(
                        issuer_id,
                        op_id,
                        reason=reason,
                        details=detail,
                    )
            except JournalError:
                record = self.journal.get_operation(issuer_id, op_id) or record
            return DispatchResult(
                outcome=outcome,
                operation=record,
                details=detail,
            )

        if record.state is OperationState.CONFIRMED and record.receipt is not None:
            return self._recover_confirmed_mutation(record, path="mutation_replay")

        if record.state is OperationState.CONFLICT:
            return DispatchResult(
                outcome=Outcome.CONFLICT,
                operation=record,
                details={"phase": "prepare_intent", "reason": "durable_conflict"},
            )

        if not record.allows_first_http_dispatch() or not self.journal.can_dispatch(issuer_id, op_id):
            return self.recover_unknown_mutation(issuer_id, op_id)

        # Serialize admission against peers that already claimed/uncertain the target.
        blocker = self._unresolved_mutation_blocker(
            task_id=task_id,
            exclude_operation_id=op_id,
        )
        if blocker is not None:
            return self._result_unresolved_target_blocker(
                blocker,
                record=record,
                phase="admission",
            )

        # Test barrier: after durable prepare/admission, before the final lock.
        # Covers the identical-key peer-confirm race (F1 / FR-EA010-10).
        self._fire_crash("before_mutation_dispatch_lock")

        try:
            with self.journal.dispatch_critical_section() as section:
                # Reload under the final lock before any preimage compare / claim.
                reloaded = self.journal.get_operation(issuer_id, op_id)
                if reloaded is None:
                    return DispatchResult(
                        outcome=Outcome.UNKNOWN,
                        details={
                            "phase": "locked_reload",
                            "reason": "operation_missing",
                            "resend_forbidden": True,
                        },
                    )
                classified = self._classify_locked_mutation(
                    issuer_id=issuer_id,
                    operation_id=op_id,
                    record=reloaded,
                )
                if isinstance(classified, DispatchResult):
                    return classified
                record = classified
                blocker = self._unresolved_mutation_blocker(
                    task_id=task_id,
                    exclude_operation_id=op_id,
                )
                if blocker is not None:
                    return self._result_unresolved_target_blocker(
                        blocker,
                        record=record,
                        phase="locked_admission",
                    )
                # C1 §6: fresh locked GET vs agreed snapshot → conflict before PATCH.
                # Accepted lack of CAS covers only the remaining GET→PATCH race.
                gated = self._locked_mutation_preimage_gate(
                    section=section,
                    issuer_id=issuer_id,
                    operation_id=op_id,
                    task_id=task_id,
                    record=record,
                )
                if gated is not None:
                    return gated
                section.begin_dispatch(issuer_id, op_id)
                try:
                    if op_type == "complete":
                        api_result: TaskResult = self.client.complete(task_id)
                    else:
                        api_result = self.client.update(task_id, patch)
                except ApiError as exc:
                    # VikunjaClient.update/complete may raise on the pre-PATCH GET
                    # before their transport catch; normalize after durable claim.
                    refused = section.mark_unknown(
                        issuer_id,
                        op_id,
                        reason="dispatch_api_error_before_or_without_patch",
                        details={
                            "api_outcome": exc.outcome.value,
                            "mutation_sent": False,
                            "api_error": True,
                            "resend_forbidden": True,
                            **dict(exc.details),
                        },
                    )
                    return DispatchResult(
                        outcome=(
                            exc.outcome
                            if exc.outcome is not Outcome.CONFIRMED
                            else Outcome.UNKNOWN
                        ),
                        operation=refused,
                        details={
                            "path": "dispatch_api_error",
                            "phase": "dispatch",
                            "reason": str(exc),
                            "api_outcome": exc.outcome.value,
                            "mutation_sent": False,
                            "resend_forbidden": True,
                            "api_error": True,
                            **dict(exc.details),
                        },
                    )
                if api_result.ok and api_result.task is not None:
                    rb_extracted = extract_marker(api_result.task.get("description"))
                    rb_marker = rb_extracted["marker"] if rb_extracted else ""
                    expected_marker = record.marker or ""
                    if expected_marker != rb_marker:
                        # Never certify a receipt marker that the live object lacks.
                        refused = section.mark_unknown(
                            issuer_id,
                            op_id,
                            reason="marker_mismatch_on_mutation_read_back",
                            details={
                                "expected_marker": expected_marker or None,
                                "observed_marker": rb_marker or None,
                                "mutation_sent": api_result.mutation_sent,
                                "resend_forbidden": True,
                            },
                        )
                        return DispatchResult(
                            outcome=Outcome.UNKNOWN,
                            operation=refused,
                            task=api_result.task,
                            vikunja_id=task_id,
                            details={
                                "path": "mutation_marker_read_back",
                                "reason": "marker_mismatch_on_mutation_read_back",
                                "resend_forbidden": True,
                                "mutation_sent": api_result.mutation_sent,
                                "expected_marker": expected_marker or None,
                                "observed_marker": rb_marker or None,
                            },
                        )
                    # Test barrier: HTTP mutation committed; receipt not yet durable.
                    self._fire_crash("after_mutation_commit_before_receipt")
                    confirmed = section.record_receipt(
                        issuer_id,
                        op_id,
                        vikunja_id=task_id,
                        project_id=intent_project_id,
                        marker=rb_marker or None,
                        effect=self._mutation_receipt_effect(
                            patch=patch,
                            api_result=api_result,
                            observed_marker=rb_marker or None,
                        ),
                    )
                    return DispatchResult(
                        outcome=Outcome.CONFIRMED,
                        operation=confirmed,
                        task=api_result.task,
                        vikunja_id=task_id,
                        details={
                            "path": "first_mutation",
                            "mutation_sent": api_result.mutation_sent,
                            **{k: v for k, v in api_result.details.items() if k in {"patch_http", "noop", "recurring"}},
                        },
                    )
                if self._is_definitive_refusal(api_result):
                    refused = section.mark_unknown(
                        issuer_id,
                        op_id,
                        reason="definitive_api_refusal",
                        details={
                            "api_outcome": api_result.outcome.value,
                            "mutation_sent": api_result.mutation_sent,
                            "definitive_refusal": True,
                            **api_result.details,
                        },
                    )
                    return DispatchResult(
                        outcome=api_result.outcome,
                        operation=refused,
                        details={
                            "path": "definitive_mutation_refusal",
                            "resend_forbidden": True,
                            "api_outcome": api_result.outcome.value,
                            "mutation_sent": api_result.mutation_sent,
                            "definitive_refusal": True,
                            **{
                                k: v
                                for k, v in api_result.details.items()
                                if k
                                not in {
                                    "path",
                                    "resend_forbidden",
                                    "definitive_refusal",
                                    "api_outcome",
                                    "mutation_sent",
                                }
                            },
                        },
                    )
                section.mark_unknown(
                    issuer_id,
                    op_id,
                    reason="mutation_outcome_uncertain",
                    details={
                        "api_outcome": api_result.outcome.value,
                        "mutation_sent": api_result.mutation_sent,
                        **api_result.details,
                    },
                )
        except JournalError as exc:
            raced = self._recover_after_claim_race(issuer_id, op_id, exc, mutation=True)
            if raced is not None:
                return raced
            return DispatchResult(
                outcome=exc.outcome,
                operation=self.journal.get_operation(issuer_id, op_id),
                details={"phase": "dispatch", "reason": str(exc), **exc.details},
            )
        return self.recover_unknown_mutation(issuer_id, op_id)
