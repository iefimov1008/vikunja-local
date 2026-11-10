"""Durable integration journal: intent, dispatch serialization, receipts, mappings.

C1 §3/§5/§8: network is out of scope for s03. Presence of a prepared intent alone
does not prove HTTP was never sent — only prepared without dispatch_started (and
without restore/drift/quarantine) may begin a first send. Retention is indefinite;
no TTL/GC of unknown/keys.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping

from .api import MARKER_RE, Outcome, build_marker, validate_uuid_v4
from .storage import JOURNAL_SQLITE_USER_VERSION, SqliteJournalStore, StorageError

ALLOWED_OP_TYPES = frozenset({"create", "update", "complete", "find"})
OPAQUE_SOURCE_MAX_LEN = 512


class DeliveryState(str, Enum):
    NONE = "none"
    DISPATCH_STARTED = "dispatch_started"
    RECEIPT = "receipt"
    UNKNOWN = "unknown"
    CONFLICT = "conflict"


class OperationState(str, Enum):
    PREPARED = "prepared"
    DISPATCH_STARTED = "dispatch_started"
    UNKNOWN = "unknown"
    CONFLICT = "conflict"
    CONFIRMED = "confirmed"


class JournalError(RuntimeError):
    def __init__(
        self,
        outcome: Outcome,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        storage_kind: str | None = None,
    ):
        super().__init__(message)
        self.outcome = outcome
        self.details = dict(details or {})
        self.storage_kind = storage_kind


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_payload(payload: Mapping[str, Any]) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise JournalError(Outcome.VALIDATION_ERROR, f"payload is not JSON-serializable: {exc}") from exc


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_confirmed_text(value: str, *, label: str) -> str:
    """Reject NUL / invalid UTF-8 / unpaired surrogates before durable intent (C1 §3)."""
    if not isinstance(value, str):
        raise JournalError(Outcome.VALIDATION_ERROR, f"{label} must be a string")
    if "\x00" in value:
        raise JournalError(Outcome.VALIDATION_ERROR, f"{label} must not contain NUL")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise JournalError(
            Outcome.VALIDATION_ERROR,
            f"{label} must be valid UTF-8 without unpaired surrogates",
        ) from exc
    return value


def _validate_confirmed_context_strings(value: Any, *, label: str) -> None:
    """Walk confirmed JSON context and apply C1 §3 text rules to every string leaf."""
    if isinstance(value, str):
        validate_confirmed_text(value, label=label)
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_label = key if isinstance(key, str) else label
            if isinstance(key, str):
                validate_confirmed_text(key, label=f"{label}.key")
            child_label = f"{label}.{key_label}" if isinstance(key, str) else label
            _validate_confirmed_context_strings(child, label=child_label)
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _validate_confirmed_context_strings(child, label=f"{label}[{index}]")
        return


def _canonical_intent_envelope(
    *,
    issuer_id: str,
    operation_id: str,
    op_type: str,
    project_id: int,
    target_id: int | None,
    marker: str,
    payload_json: str,
    frozen_due_json: str | None,
    source_kind: str | None,
    source_key: str | None,
    confirmed_at: str,
) -> str:
    """Canonical form of the immutable confirmed-intent envelope (C1 §3/§5).

    Digested and stored in operations.payload_digest. Covers P/target/M/payload
    plus frozen/source/confirmation fields so metadata drift fails closed.
    """
    envelope = {
        "confirmed_at": confirmed_at,
        "frozen_due_json": frozen_due_json,
        "issuer_id": issuer_id,
        "marker": marker,
        "op_type": op_type,
        "operation_id": operation_id,
        "payload_json": payload_json,
        "project_id": project_id,
        "source_key": source_key,
        "source_kind": source_kind,
        "target_id": target_id,
    }
    return json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _envelope_digest(**kwargs: Any) -> str:
    return _digest(_canonical_intent_envelope(**kwargs))


def _map_storage(exc: StorageError) -> JournalError:
    if exc.kind == "corrupt":
        outcome = Outcome.DEPENDENCY_UNAVAILABLE
    elif exc.kind == "full":
        outcome = Outcome.DEPENDENCY_UNAVAILABLE
    elif exc.kind == "incompatible_schema":
        outcome = Outcome.DEPENDENCY_UNAVAILABLE
    else:
        outcome = Outcome.DEPENDENCY_UNAVAILABLE
    return JournalError(outcome, str(exc), details=exc.details, storage_kind=exc.kind)


def new_issuer_id() -> str:
    return str(uuid.uuid4())


def new_operation_id() -> str:
    return str(uuid.uuid4())


@dataclass(frozen=True)
class OperationRecord:
    issuer_id: str
    operation_id: str
    op_type: str
    project_id: int
    target_id: int | None
    marker: str
    payload: dict[str, Any]
    payload_digest: str
    state: OperationState
    delivery_state: DeliveryState
    frozen_due: dict[str, Any] | None
    source_kind: str | None
    source_key: str | None
    confirmed_at: str
    created_at: str
    updated_at: str
    receipt: dict[str, Any] | None = None

    @property
    def key(self) -> tuple[str, str]:
        return self.issuer_id, self.operation_id

    def allows_first_http_dispatch(self) -> bool:
        """True only when a first network mutation may be attempted later.

        Intent alone is insufficient: dispatch_started means HTTP may already
        have been attempted even if no receipt exists. A durable receipt on the
        same key also proves prior completion even if the operations row was
        partially restored to prepared/none.
        """
        return (
            self.state is OperationState.PREPARED
            and self.delivery_state is DeliveryState.NONE
            and self.receipt is None
        )


@dataclass
class DispatchCriticalSection:
    """Caller-controlled dispatch interval with the canonical OS-lock held.

    SQLite transactions still commit inside begin_dispatch / record_receipt /
    mark_* before those methods return — the DB transaction is never held across
    HTTP. The OS-lock remains held for the whole section so distinct operation
    keys cannot overlap mutations of the same journal (C1 §5.2).
    """

    _journal: "IntegrationJournal"

    @property
    def os_lock_held(self) -> bool:
        return self._journal._store.lock.held

    def begin_dispatch(self, issuer_id: str, operation_id: str) -> OperationRecord:
        return self._journal.begin_dispatch(issuer_id, operation_id)

    def record_receipt(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        vikunja_id: int,
        project_id: int,
        marker: str | None,
        effect: Mapping[str, Any],
        witnessed_at: str | None = None,
    ) -> OperationRecord:
        return self._journal.record_receipt(
            issuer_id,
            operation_id,
            vikunja_id=vikunja_id,
            project_id=project_id,
            marker=marker,
            effect=effect,
            witnessed_at=witnessed_at,
        )

    def mark_unknown(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        return self._journal.mark_unknown(
            issuer_id,
            operation_id,
            reason=reason,
            details=details,
        )

    def mark_conflict(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        return self._journal.mark_conflict(
            issuer_id,
            operation_id,
            reason=reason,
            details=details,
        )


class IntegrationJournal:
    """SQLite journal for one Mini adapter installation."""

    def __init__(self, journal_path: Path):
        if not isinstance(journal_path, Path):
            journal_path = Path(journal_path)
        if not journal_path.is_absolute():
            raise JournalError(Outcome.VALIDATION_ERROR, "journal_path must be absolute")
        try:
            self._store = SqliteJournalStore(journal_path)
        except StorageError as exc:
            raise _map_storage(exc) from exc
        # Always expose the canonical path bound to the OS-lock.
        self.path = self._store.path

    @property
    def sqlite_user_version(self) -> int:
        return JOURNAL_SQLITE_USER_VERSION

    @property
    def os_lock_held(self) -> bool:
        """Whether this process currently holds the canonical journal OS-lock."""
        return self._store.lock.held

    def open(self, *, admit_restored: bool = False) -> None:
        try:
            self._store.open(admit_restored=admit_restored)
        except StorageError as exc:
            raise _map_storage(exc) from exc

    def admit_restored_backup(self) -> None:
        """Explicit admission after restoring a DB+.install backup set.

        Rotates a new outside-backup authority incarnation (never rewinds to the
        restored meta epoch), binds DB+.install to that fence, and forces
        restore_drift / writes_enabled=0. First HTTP dispatch stays refused until
        reconcile clears drift (C1 §5/§8). Journal must be closed.
        """
        self.open(admit_restored=True)

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "IntegrationJournal":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()

    @contextmanager
    def dispatch_critical_section(self) -> Iterator[DispatchCriticalSection]:
        """Retain the OS-lock through caller-controlled dispatch/read-back/receipt.

        C1 §5.2: all integration mutations are serialized under the canonical-path
        OS-lock; the SQLite transaction (not the OS-lock) is released before HTTP.
        Nested begin_dispatch / record_receipt commit SQLite and return while this
        section keeps the OS-lock held until exit — including on exception.
        """
        with self._mutation_guard():
            section = DispatchCriticalSection(self)
            try:
                yield section
            finally:
                # Lock release is owned by _mutation_guard.__exit__; section is
                # invalid after the context ends.
                pass

    def ensure_issuer(self, issuer_id: str | None = None) -> str:
        """Load durable issuer or mint once on a genuinely empty install.

        Issuer loss / partial copy (history or operations without meta.issuer_id)
        fail-closes writes until reconcile; never remint over existing keys.
        """
        # Flag checks without requiring an issuer (mint path); still OS-locked.
        with self._mutation_guard(require_writable=False):
            self._assert_write_flags()
            existing = self._get_meta("issuer_id")
            if existing:
                validate_uuid_v4(existing, label="issuer_id")
                if issuer_id is not None and issuer_id != existing:
                    raise JournalError(
                        Outcome.CONFLICT,
                        "issuer_id mismatch with durable journal issuer",
                        details={"durable": existing},
                    )
                return existing
            if self._has_prior_durable_identity():
                self._mark_issuer_lost()
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "durable issuer lost; writes refused until reconcile",
                    storage_kind="unavailable",
                    details={"prior_durable_identity": True},
                )
            minted = validate_uuid_v4(issuer_id or new_issuer_id(), label="issuer_id")
            lost = False
            try:
                with self._store.transaction():
                    again = self._get_meta("issuer_id")
                    if again:
                        return validate_uuid_v4(again, label="issuer_id")
                    if self._has_prior_durable_identity():
                        # Commit fail-closed flags; do not raise inside the
                        # transaction or the flags would roll back.
                        self._mark_issuer_lost(inside_transaction=True)
                        lost = True
                    else:
                        self._store.execute(
                            "INSERT INTO meta(key, value) VALUES(?, ?)",
                            ("issuer_id", minted),
                        )
                        self._append_history(
                            minted,
                            "",
                            "issuer_minted",
                            {"issuer_id": minted},
                        )
            except StorageError as exc:
                raise _map_storage(exc) from exc
            if lost:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "durable issuer lost; writes refused until reconcile",
                    storage_kind="unavailable",
                    details={"prior_durable_identity": True},
                )
            return minted

    def get_issuer(self) -> str | None:
        # Serialize with mutators so another thread cannot observe uncommitted meta.
        with self._mutation_guard(require_writable=False):
            value = self._get_meta("issuer_id")
            return validate_uuid_v4(value, label="issuer_id") if value else None

    def prepare_intent(
        self,
        *,
        issuer_id: str,
        operation_id: str,
        op_type: str,
        project_id: int,
        payload: Mapping[str, Any],
        target_id: int | None = None,
        frozen_due: Mapping[str, Any] | None = None,
        source_kind: str | None = None,
        source_key: str | None = None,
        confirmed_at: str | None = None,
    ) -> OperationRecord:
        """Durable commit of a confirmed intent before any HTTP may begin."""
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        if op_type not in ALLOWED_OP_TYPES:
            raise JournalError(Outcome.VALIDATION_ERROR, f"unsupported op_type: {op_type}")
        if not isinstance(project_id, int) or isinstance(project_id, bool) or project_id <= 0:
            raise JournalError(Outcome.VALIDATION_ERROR, "project_id must be a positive int")
        if target_id is not None and (
            not isinstance(target_id, int) or isinstance(target_id, bool) or target_id <= 0
        ):
            raise JournalError(Outcome.VALIDATION_ERROR, "target_id must be a positive int or None")
        if op_type == "create" and target_id is not None:
            raise JournalError(Outcome.VALIDATION_ERROR, "create intent must not set target_id")
        if op_type in {"update", "complete"} and target_id is None:
            raise JournalError(Outcome.VALIDATION_ERROR, f"{op_type} intent requires target_id")
        source_kind, source_key = self._normalize_source(source_kind, source_key)
        # C1 §3: reject invalid confirmed user-context before any durable intent row.
        _validate_confirmed_context_strings(payload, label="payload")
        if frozen_due is not None:
            _validate_confirmed_context_strings(frozen_due, label="frozen_due")
        payload_json = _canonical_payload(payload)
        frozen_json = (
            None if frozen_due is None else _canonical_payload(frozen_due)
        )

        with self._mutation_guard():
            durable_issuer = self._require_issuer(issuer_id)
            existing = self._load_operation(durable_issuer, operation_id)
            # confirmed_at is part of the immutable envelope digest. When the
            # caller omits it on a same-key replay (including after reopen /
            # across seconds), reuse the persisted confirmation so a stable
            # payload is not misclassified as conflict.
            if confirmed_at is not None:
                confirmed = confirmed_at
            elif existing is not None:
                confirmed = existing.confirmed_at
            else:
                confirmed = _utc_now()
            # Operation identity is (issuer_id, operation_id). Correlation marker M
            # is minted only for create; update/complete retain the verified task M
            # (or empty for unmarked manual tasks) — C1 §3 / §5.4.
            marker = self._resolve_intent_marker(
                durable_issuer=durable_issuer,
                operation_id=operation_id,
                op_type=op_type,
                target_id=target_id,
            )
            digest = _envelope_digest(
                issuer_id=durable_issuer,
                operation_id=operation_id,
                op_type=op_type,
                project_id=project_id,
                target_id=target_id,
                marker=marker,
                payload_json=payload_json,
                frozen_due_json=frozen_json,
                source_kind=source_kind,
                source_key=source_key,
                confirmed_at=confirmed,
            )
            if existing is not None:
                self._assert_operation_source_consistent(existing)
                return self._replay_or_conflict(
                    existing,
                    op_type=op_type,
                    project_id=project_id,
                    target_id=target_id,
                    payload_digest=digest,
                    payload=dict(json.loads(payload_json)),
                    frozen_due=None if frozen_json is None else dict(json.loads(frozen_json)),
                    source_kind=source_kind,
                    source_key=source_key,
                )

            if source_kind is not None and source_key is not None:
                bound = self._resolve_source_binding(
                    source_kind,
                    source_key,
                    durable_issuer=durable_issuer,
                    operation_id=operation_id,
                )
                if bound is not None:
                    return bound

            # New key requires durable write capacity.
            self._require_writable()
            now = _utc_now()
            try:
                with self._store.transaction():
                    self._store.execute(
                        """
                        INSERT INTO operations(
                            issuer_id, operation_id, op_type, project_id, target_id,
                            marker, payload_json, payload_digest, state, delivery_state,
                            frozen_due_json, source_kind, source_key, confirmed_at,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            durable_issuer,
                            operation_id,
                            op_type,
                            project_id,
                            target_id,
                            marker,
                            payload_json,
                            digest,
                            OperationState.PREPARED.value,
                            DeliveryState.NONE.value,
                            frozen_json,
                            source_kind,
                            source_key,
                            confirmed,
                            now,
                            now,
                        ),
                    )
                    if source_kind is not None and source_key is not None:
                        self._store.execute(
                            """
                            INSERT INTO source_mappings(
                                source_kind, source_key, issuer_id, operation_id, created_at
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (source_kind, source_key, durable_issuer, operation_id, now),
                        )
                    self._append_history(
                        durable_issuer,
                        operation_id,
                        "intent_prepared",
                        {
                            "marker": marker,
                            "op_type": op_type,
                            "project_id": project_id,
                            "target_id": target_id,
                            "frozen_due_json": frozen_json,
                            "source_kind": source_kind,
                            "source_key": source_key,
                            "confirmed_at": confirmed,
                            "payload_digest": digest,
                        },
                    )
            except StorageError as exc:
                raise _map_storage(exc) from exc
            except JournalError:
                raise
            record = self._load_operation(durable_issuer, operation_id)
            if record is None:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "prepared intent missing after durable commit",
                )
            return record

    def begin_dispatch(self, issuer_id: str, operation_id: str) -> OperationRecord:
        """Under OS-lock, durable-mark dispatch_started before any HTTP.

        Returns the updated record. Callers must not send HTTP if this raises.
        Holding a DB transaction across the network is forbidden; this method
        commits dispatch_started and releases the SQLite transaction before return.
        """
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        with self._mutation_guard():
            durable_issuer = self._require_issuer(issuer_id)
            record = self._load_operation(durable_issuer, operation_id)
            if record is None:
                raise JournalError(Outcome.VALIDATION_ERROR, "operation not found")
            self._authorize_first_dispatch(record)
            now = _utc_now()
            try:
                with self._store.transaction():
                    # Re-check under the write transaction against durable proof.
                    self._assert_intact_never_dispatched(durable_issuer, operation_id)
                    self._assert_intent_envelope_intact(record)
                    self._assert_operation_source_consistent(record)
                    cur = self._store.execute(
                        """
                        UPDATE operations
                        SET state = ?, delivery_state = ?, updated_at = ?
                        WHERE issuer_id = ? AND operation_id = ?
                          AND state = ? AND delivery_state = ?
                        """,
                        (
                            OperationState.DISPATCH_STARTED.value,
                            DeliveryState.DISPATCH_STARTED.value,
                            now,
                            durable_issuer,
                            operation_id,
                            OperationState.PREPARED.value,
                            DeliveryState.NONE.value,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise JournalError(
                            Outcome.CONFLICT,
                            "concurrent dispatch transition lost the prepared row",
                        )
                    self._append_history(
                        durable_issuer,
                        operation_id,
                        "dispatch_started",
                        {"at": now},
                    )
            except StorageError as exc:
                raise _map_storage(exc) from exc
            except JournalError as exc:
                if exc.details.get("mixed_copy"):
                    try:
                        self._quarantine_unlocked(
                            reason="mixed_copy_dispatch_refused",
                            details=exc.details,
                        )
                    except JournalError:
                        pass
                raise
            updated = self._load_operation(durable_issuer, operation_id)
            if updated is None:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "operation missing after dispatch_started commit",
                )
            return updated

    def record_receipt(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        vikunja_id: int,
        project_id: int,
        marker: str | None,
        effect: Mapping[str, Any],
        witnessed_at: str | None = None,
    ) -> OperationRecord:
        """Atomically persist a confirmed receipt/mapping after verified observation."""
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        if not isinstance(vikunja_id, int) or isinstance(vikunja_id, bool) or vikunja_id <= 0:
            raise JournalError(Outcome.VALIDATION_ERROR, "vikunja_id must be a positive int")
        if not isinstance(project_id, int) or isinstance(project_id, bool) or project_id <= 0:
            raise JournalError(Outcome.VALIDATION_ERROR, "project_id must be a positive int")
        # C1 §5.4: receipt M comes from verified observation. Unmarked manual
        # tasks have no integration header — None/"" is the durable sentinel.
        if marker is None:
            observed_marker = ""
        elif isinstance(marker, str):
            observed_marker = marker
        else:
            raise JournalError(Outcome.VALIDATION_ERROR, "receipt marker must be a string or None")
        if observed_marker != "" and MARKER_RE.fullmatch(observed_marker) is None:
            raise JournalError(
                Outcome.VALIDATION_ERROR,
                "receipt marker must be empty or a valid EA010 correlation marker",
            )
        effect_json = _canonical_payload(effect)
        witnessed = witnessed_at or _utc_now()
        now = _utc_now()
        with self._mutation_guard():
            record = self._load_operation(issuer_id, operation_id)
            if record is None:
                raise JournalError(Outcome.VALIDATION_ERROR, "operation not found")
            if observed_marker != record.marker:
                raise JournalError(Outcome.CONFLICT, "receipt marker mismatch")
            if project_id != record.project_id:
                self._reject_incompatible_receipt(
                    record,
                    reason="receipt_project_id_mismatch",
                    details={
                        "intent_project_id": record.project_id,
                        "receipt_project_id": project_id,
                    },
                )
            if record.op_type in {"update", "complete"}:
                if record.target_id is None or vikunja_id != record.target_id:
                    self._reject_incompatible_receipt(
                        record,
                        reason="receipt_vikunja_id_mismatch",
                        details={
                            "intent_target_id": record.target_id,
                            "receipt_vikunja_id": vikunja_id,
                            "op_type": record.op_type,
                        },
                    )
            if record.state is OperationState.CONFIRMED and record.receipt is not None:
                if (
                    int(record.receipt["vikunja_id"]) == vikunja_id
                    and int(record.receipt["project_id"]) == project_id
                    and record.receipt["marker"] == observed_marker
                    and _digest(record.receipt["effect_json"]) == _digest(effect_json)
                ):
                    return record
                raise JournalError(Outcome.CONFLICT, "incompatible receipt for confirmed operation")
            if record.state not in {
                OperationState.DISPATCH_STARTED,
                OperationState.UNKNOWN,
            }:
                raise JournalError(
                    Outcome.CONFLICT,
                    "receipt requires dispatch_started or unknown state",
                    details={"state": record.state.value},
                )
            try:
                with self._store.transaction():
                    self._store.execute(
                        """
                        INSERT INTO receipts(
                            issuer_id, operation_id, vikunja_id, project_id,
                            marker, effect_json, witnessed_at, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            issuer_id,
                            operation_id,
                            vikunja_id,
                            project_id,
                            observed_marker,
                            effect_json,
                            witnessed,
                            now,
                        ),
                    )
                    self._store.execute(
                        """
                        UPDATE operations
                        SET state = ?, delivery_state = ?, updated_at = ?
                        WHERE issuer_id = ? AND operation_id = ?
                        """,
                        (
                            OperationState.CONFIRMED.value,
                            DeliveryState.RECEIPT.value,
                            now,
                            issuer_id,
                            operation_id,
                        ),
                    )
                    self._append_history(
                        issuer_id,
                        operation_id,
                        "receipt",
                        {
                            "vikunja_id": vikunja_id,
                            "project_id": project_id,
                            "marker": observed_marker,
                        },
                    )
            except StorageError as exc:
                # Failed receipt commit leaves unknown — never treat in-memory ID as success.
                try:
                    self.mark_unknown(
                        issuer_id,
                        operation_id,
                        reason="receipt_commit_failed",
                        details={"storage_kind": exc.kind},
                    )
                except JournalError:
                    pass
                raise _map_storage(exc) from exc
            updated = self._load_operation(issuer_id, operation_id)
            if updated is None or updated.receipt is None:
                raise JournalError(
                    Outcome.UNKNOWN,
                    "receipt commit did not persist; treat as unknown",
                )
            return updated

    def mark_unknown(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        now = _utc_now()
        with self._mutation_guard():
            record = self._load_operation(issuer_id, operation_id)
            if record is None:
                raise JournalError(Outcome.VALIDATION_ERROR, "operation not found")
            if record.state is OperationState.CONFIRMED:
                return record
            try:
                with self._store.transaction():
                    self._store.execute(
                        """
                        UPDATE operations
                        SET state = ?, delivery_state = ?, updated_at = ?
                        WHERE issuer_id = ? AND operation_id = ?
                        """,
                        (
                            OperationState.UNKNOWN.value,
                            DeliveryState.UNKNOWN.value,
                            now,
                            issuer_id,
                            operation_id,
                        ),
                    )
                    self._append_history(
                        issuer_id,
                        operation_id,
                        "unknown",
                        {"reason": reason, **dict(details or {})},
                    )
            except StorageError as exc:
                raise _map_storage(exc) from exc
            updated = self._load_operation(issuer_id, operation_id)
            assert updated is not None
            return updated

    def mark_conflict(
        self,
        issuer_id: str,
        operation_id: str,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
        operation_id = validate_uuid_v4(operation_id, label="operation_id")
        now = _utc_now()
        with self._mutation_guard():
            record = self._load_operation(issuer_id, operation_id)
            if record is None:
                raise JournalError(Outcome.VALIDATION_ERROR, "operation not found")
            try:
                with self._store.transaction():
                    self._store.execute(
                        """
                        UPDATE operations
                        SET state = ?, delivery_state = ?, updated_at = ?
                        WHERE issuer_id = ? AND operation_id = ?
                        """,
                        (
                            OperationState.CONFLICT.value,
                            DeliveryState.CONFLICT.value,
                            now,
                            issuer_id,
                            operation_id,
                        ),
                    )
                    self._append_history(
                        issuer_id,
                        operation_id,
                        "conflict",
                        {"reason": reason, **dict(details or {})},
                    )
            except StorageError as exc:
                raise _map_storage(exc) from exc
            updated = self._load_operation(issuer_id, operation_id)
            assert updated is not None
            return updated

    def get_operation(self, issuer_id: str, operation_id: str) -> OperationRecord | None:
        # Public reads must not observe another thread's uncommitted mutator work
        # on the shared connection (C1 §5.4: failed receipt commit stays unknown).
        with self._mutation_guard(require_writable=False):
            issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
            operation_id = validate_uuid_v4(operation_id, label="operation_id")
            return self._load_operation(issuer_id, operation_id)

    def list_operations(self) -> list[OperationRecord]:
        with self._mutation_guard(require_writable=False):
            rows = self._store.execute(
                "SELECT issuer_id, operation_id FROM operations "
                "ORDER BY created_at ASC, operation_id ASC"
            ).fetchall()
            out: list[OperationRecord] = []
            for row in rows:
                record = self._load_operation(row["issuer_id"], row["operation_id"])
                if record is not None:
                    out.append(record)
            return out

    def lookup_source(self, source_kind: str, source_key: str) -> OperationRecord | None:
        with self._mutation_guard(require_writable=False):
            source_kind, source_key = self._normalize_source(source_kind, source_key)
            assert source_kind is not None and source_key is not None
            mapped = self._load_source_mapping(source_kind, source_key)
            if mapped is None:
                return None
            return self._load_operation(mapped[0], mapped[1])

    def can_dispatch(self, issuer_id: str, operation_id: str) -> bool:
        """Whether a first HTTP mutation may start. Never true when journal writes are off."""
        try:
            with self._mutation_guard():
                durable = self._require_issuer(validate_uuid_v4(issuer_id, label="issuer_id"))
                operation_id = validate_uuid_v4(operation_id, label="operation_id")
                record = self._load_operation(durable, operation_id)
                if record is None:
                    return False
                self._authorize_first_dispatch(record)
                return True
        except JournalError:
            return False

    def set_restore_drift(self, enabled: bool) -> None:
        with self._mutation_guard(require_writable=False):
            try:
                with self._store.transaction():
                    self._store.execute(
                        "INSERT INTO meta(key, value) VALUES(?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        ("restore_drift", "1" if enabled else "0"),
                    )
                    # Drift forces writes off. Clearing drift alone does not auto-heal
                    # quarantine; tests may re-enable writes only when drift is cleared
                    # and quarantine is unset.
                    if enabled:
                        self._store.execute(
                            "INSERT INTO meta(key, value) VALUES(?, ?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            ("writes_enabled", "0"),
                        )
                    elif self._get_meta("quarantine") != "1":
                        self._store.execute(
                            "INSERT INTO meta(key, value) VALUES(?, ?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            ("writes_enabled", "1"),
                        )
            except StorageError as exc:
                raise _map_storage(exc) from exc

    def quarantine(self) -> None:
        """Mark journal quarantined after corruption suspicion; refuse writes."""
        with self._mutation_guard(require_writable=False):
            self._quarantine_unlocked(reason="explicit_quarantine")

    def history(self, issuer_id: str | None = None, operation_id: str | None = None) -> list[dict[str, Any]]:
        with self._mutation_guard(require_writable=False):
            if issuer_id is None:
                rows = self._store.execute(
                    "SELECT * FROM history ORDER BY id ASC"
                ).fetchall()
            elif operation_id is None:
                rows = self._store.execute(
                    "SELECT * FROM history WHERE issuer_id = ? ORDER BY id ASC",
                    (validate_uuid_v4(issuer_id, label="issuer_id"),),
                ).fetchall()
            else:
                rows = self._store.execute(
                    "SELECT * FROM history WHERE issuer_id = ? AND operation_id = ? "
                    "ORDER BY id ASC",
                    (
                        validate_uuid_v4(issuer_id, label="issuer_id"),
                        validate_uuid_v4(operation_id, label="operation_id"),
                    ),
                ).fetchall()
            out: list[dict[str, Any]] = []
            for row in rows:
                out.append(
                    {
                        "id": row["id"],
                        "issuer_id": row["issuer_id"],
                        "operation_id": row["operation_id"],
                        "event_type": row["event_type"],
                        "detail": json.loads(row["detail_json"]),
                        "created_at": row["created_at"],
                    }
                )
            return out

    # --- internals ---------------------------------------------------------

    # Only a single intent_prepared history row is intact for first dispatch.
    # conflict/dispatch_started/receipt/unknown (and any other residue) refuse re-arm.
    _INTACT_PREPARED_HISTORY = ("intent_prepared",)
    _DISPATCH_PROOF_EVENTS = frozenset(
        {"dispatch_started", "receipt", "unknown", "conflict"}
    )

    def _history_event_types(self, issuer_id: str, operation_id: str) -> list[str]:
        rows = self._store.execute(
            """
            SELECT event_type FROM history
            WHERE issuer_id = ? AND operation_id = ?
            ORDER BY id ASC
            """,
            (issuer_id, operation_id),
        ).fetchall()
        return [str(row["event_type"]) for row in rows]

    def _assert_intact_never_dispatched(self, issuer_id: str, operation_id: str) -> None:
        """Fail closed when receipt/history prove the prepared row is not intact.

        An intact never-dispatched intent has exactly one intent_prepared history
        event and no receipt. Partial restoration of state/delivery_state while
        retaining conflict or other inconsistent/missing intent-history must
        quarantine and refuse first HTTP (C1 §3/§5).
        """
        receipt = self._store.execute(
            """
            SELECT 1 FROM receipts
            WHERE issuer_id = ? AND operation_id = ?
            """,
            (issuer_id, operation_id),
        ).fetchone()
        events = self._history_event_types(issuer_id, operation_id)
        proof = [event for event in events if event in self._DISPATCH_PROOF_EVENTS]
        non_intact = [event for event in events if event != "intent_prepared"]
        missing_intent = "intent_prepared" not in events
        intact_history = tuple(events) == self._INTACT_PREPARED_HISTORY
        if receipt is not None or proof or non_intact or missing_intent or not intact_history:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "operation is not an intact never-dispatched prepared intent",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "has_receipt": receipt is not None,
                    "proof_events": proof,
                    "history_events": events,
                    "missing_intent_prepared": missing_intent,
                    "inconsistent_history": not intact_history,
                },
            )

    def _authorize_first_dispatch(self, record: OperationRecord) -> None:
        """Authorize first HTTP only for intact prepared intents; quarantine mixed copies.

        Quarantine applies only when the operations row claims prepared/none (a
        first-dispatch candidate) while durable receipt/history prove otherwise.
        A normal dispatch_started/confirmed/conflict row simply refuses without
        quarantining the whole journal.
        """
        row_claims_first_dispatch = (
            record.state is OperationState.PREPARED
            and record.delivery_state is DeliveryState.NONE
        )
        if row_claims_first_dispatch:
            try:
                self._assert_intact_never_dispatched(record.issuer_id, record.operation_id)
                self._assert_intent_envelope_intact(record)
                self._assert_operation_source_consistent(record)
            except JournalError as exc:
                if exc.details.get("mixed_copy") or exc.details.get("partial_copy"):
                    try:
                        self._quarantine_unlocked(
                            reason="mixed_copy_before_dispatch",
                            details=exc.details,
                        )
                    except JournalError:
                        pass
                raise
            if record.receipt is not None:
                details = {
                    "mixed_copy": True,
                    "has_receipt": True,
                    "proof_events": ["receipt"],
                }
                try:
                    self._quarantine_unlocked(
                        reason="mixed_copy_receipt_row",
                        details=details,
                    )
                except JournalError:
                    pass
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "operation has durable receipt; refusing first dispatch",
                    storage_kind="corrupt",
                    details=details,
                )
            return
        raise JournalError(
            Outcome.CONFLICT if record.state is OperationState.CONFLICT else Outcome.UNKNOWN,
            "first HTTP dispatch is not allowed for this operation state",
            details={
                "state": record.state.value,
                "delivery_state": record.delivery_state.value,
                "intent_alone_not_proof_http_unsent": True,
            },
        )

    def _quarantine_unlocked(
        self,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist quarantine flags while the OS-lock is already held."""
        try:
            with self._store.transaction():
                self._store.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("quarantine", "1"),
                )
                self._store.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("writes_enabled", "0"),
                )
                self._append_history(
                    "",
                    "",
                    "quarantine",
                    {"reason": reason, **dict(details or {})},
                )
        except StorageError as exc:
            raise _map_storage(exc) from exc

    def _reject_incompatible_receipt(
        self,
        record: OperationRecord,
        *,
        reason: str,
        details: Mapping[str, Any],
    ) -> None:
        """Refuse incompatible receipt before insert; mark conflict and quarantine."""
        now = _utc_now()
        try:
            with self._store.transaction():
                self._store.execute(
                    """
                    UPDATE operations
                    SET state = ?, delivery_state = ?, updated_at = ?
                    WHERE issuer_id = ? AND operation_id = ?
                    """,
                    (
                        OperationState.CONFLICT.value,
                        DeliveryState.CONFLICT.value,
                        now,
                        record.issuer_id,
                        record.operation_id,
                    ),
                )
                self._append_history(
                    record.issuer_id,
                    record.operation_id,
                    "conflict",
                    {"reason": reason, **dict(details)},
                )
                self._store.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("quarantine", "1"),
                )
                self._store.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("writes_enabled", "0"),
                )
                self._append_history(
                    "",
                    "",
                    "quarantine",
                    {"reason": reason, **dict(details)},
                )
        except StorageError as exc:
            raise _map_storage(exc) from exc
        raise JournalError(
            Outcome.CONFLICT,
            "incompatible receipt for immutable intent identity",
            details={"reason": reason, **dict(details)},
            storage_kind="corrupt",
        )

    def _mutation_guard(self, *, require_writable: bool = True):
        class _Guard:
            def __init__(self, journal: "IntegrationJournal"):
                self.journal = journal
                self._acquired = False

            def __enter__(self):
                try:
                    # open() re-binds canonical path / inode and refuses symlink swaps.
                    self.journal._store.open()
                    self.journal._store.lock.acquire()
                    self._acquired = True
                    # Re-verify after lock: substitution during wait must fail closed.
                    self.journal._store._rebind_identity()
                    # C1 §5/§8: install identity/epoch + history residue under OS-lock
                    # for every mutation (including ensure_issuer), including when the
                    # SQLite connection was already open before an in-place restore.
                    self.journal._store.revalidate_live_connection()
                    if require_writable:
                        self.journal._require_writable()
                    return self
                except StorageError as exc:
                    if self._acquired:
                        self.journal._store.lock.release()
                        self._acquired = False
                    raise _map_storage(exc) from exc
                except BaseException:
                    # Validation after acquire must not leave the OS-lock held:
                    # Python skips __exit__ when __enter__ raises.
                    if self._acquired:
                        self.journal._store.lock.release()
                        self._acquired = False
                    raise

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
                if self._acquired:
                    self.journal._store.lock.release()
                    self._acquired = False

        return _Guard(self)

    def _require_open_readable(self) -> None:
        try:
            self._store.open()
        except StorageError as exc:
            raise _map_storage(exc) from exc

    def _assert_write_flags(self) -> None:
        """Fail closed unless every safety flag exists with an exact allowed value.

        Absent or malformed flags must not be treated as permission to write
        (C1 §5: restore/drift/partial-copy stays writes-off until reconcile).
        """
        allowed = frozenset({"0", "1"})
        quarantine = self._get_meta("quarantine")
        restore_drift = self._get_meta("restore_drift")
        writes_enabled = self._get_meta("writes_enabled")
        missing = [
            key
            for key, value in (
                ("quarantine", quarantine),
                ("restore_drift", restore_drift),
                ("writes_enabled", writes_enabled),
            )
            if value is None
        ]
        invalid = {
            key: value
            for key, value in (
                ("quarantine", quarantine),
                ("restore_drift", restore_drift),
                ("writes_enabled", writes_enabled),
            )
            if value is not None and value not in allowed
        }
        if missing or invalid:
            details = {
                "missing_safety_meta": missing,
                "invalid_safety_meta": invalid,
                "partial_copy": True,
            }
            try:
                with self._store.transaction():
                    for key, value in (
                        ("quarantine", "1"),
                        ("writes_enabled", "0"),
                        ("restore_drift", "1"),
                    ):
                        self._store.execute(
                            "INSERT INTO meta(key, value) VALUES(?, ?) "
                            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (key, value),
                        )
                    self._append_history(
                        "",
                        "",
                        "quarantine",
                        {"reason": "missing_or_invalid_safety_meta", **details},
                    )
            except (StorageError, JournalError):
                pass
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "journal safety metadata missing or invalid; writes refused until reconcile",
                storage_kind="corrupt",
                details=details,
            )
        if quarantine == "1":
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "journal is quarantined; writes refused",
                storage_kind="corrupt",
            )
        if restore_drift == "1":
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "journal restore/drift detected; writes refused until reconcile",
                storage_kind="unavailable",
            )
        if writes_enabled != "1":
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "journal writes are disabled",
                storage_kind="unavailable",
            )

    def _require_writable(self) -> None:
        self._require_open_readable()
        self._assert_write_flags()
        if self._get_meta("issuer_id") is None:
            if self._has_prior_durable_identity():
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "durable issuer lost; writes refused until reconcile",
                    storage_kind="unavailable",
                    details={"prior_durable_identity": True},
                )
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "issuer_id missing from durable journal",
                storage_kind="unavailable",
            )

    def _has_prior_durable_identity(self) -> bool:
        """True when the DB is not a blank install (ops/receipts/mappings/history)."""
        for table in ("operations", "receipts", "source_mappings", "history"):
            row = self._store.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            if row is not None:
                return True
        return False

    def _mark_issuer_lost(self, *, inside_transaction: bool = False) -> None:
        """Persist fail-closed flags after issuer meta loss without wiping history."""

        def _apply() -> None:
            self._store.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("restore_drift", "1"),
            )
            self._store.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("writes_enabled", "0"),
            )
            self._append_history(
                "",
                "",
                "issuer_lost",
                {"prior_durable_identity": True},
            )

        try:
            if inside_transaction:
                _apply()
            else:
                with self._store.transaction():
                    _apply()
        except StorageError as exc:
            raise _map_storage(exc) from exc

    def _require_issuer(self, issuer_id: str) -> str:
        durable = self._get_meta("issuer_id")
        if durable is None:
            if self._has_prior_durable_identity():
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "durable issuer lost; writes refused until reconcile",
                    storage_kind="unavailable",
                    details={"prior_durable_identity": True},
                )
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "issuer_id missing from durable journal",
                storage_kind="unavailable",
            )
        durable = validate_uuid_v4(durable, label="issuer_id")
        if durable != issuer_id:
            raise JournalError(
                Outcome.CONFLICT,
                "operation issuer_id does not match durable issuer",
                details={"durable": durable},
            )
        return durable

    def _get_meta(self, key: str) -> str | None:
        row = self._store.execute(
            "SELECT value FROM meta WHERE key = ?",
            (key,),
        ).fetchone()
        return None if row is None else str(row["value"])

    def _append_history(
        self,
        issuer_id: str,
        operation_id: str,
        event_type: str,
        detail: Mapping[str, Any],
    ) -> None:
        self._store.execute(
            """
            INSERT INTO history(issuer_id, operation_id, event_type, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                issuer_id,
                operation_id,
                event_type,
                _canonical_payload(detail),
                _utc_now(),
            ),
        )

    def _normalize_source(
        self, source_kind: str | None, source_key: str | None
    ) -> tuple[str | None, str | None]:
        if source_kind is None and source_key is None:
            return None, None
        if source_kind is None or source_key is None:
            raise JournalError(
                Outcome.VALIDATION_ERROR,
                "source_kind and source_key must be provided together",
            )
        if not isinstance(source_kind, str) or not source_kind or len(source_kind) > OPAQUE_SOURCE_MAX_LEN:
            raise JournalError(Outcome.VALIDATION_ERROR, "invalid source_kind")
        if not isinstance(source_key, str) or not source_key or len(source_key) > OPAQUE_SOURCE_MAX_LEN:
            raise JournalError(Outcome.VALIDATION_ERROR, "invalid source_key")
        validate_confirmed_text(source_kind, label="source_kind")
        validate_confirmed_text(source_key, label="source_key")
        return source_kind, source_key

    def _load_source_mapping(self, source_kind: str, source_key: str) -> tuple[str, str] | None:
        row = self._store.execute(
            """
            SELECT issuer_id, operation_id FROM source_mappings
            WHERE source_kind = ? AND source_key = ?
            """,
            (source_kind, source_key),
        ).fetchone()
        if row is None:
            return None
        return str(row["issuer_id"]), str(row["operation_id"])

    def _load_operations_by_source(
        self, source_kind: str, source_key: str
    ) -> list[OperationRecord]:
        rows = self._store.execute(
            """
            SELECT issuer_id, operation_id FROM operations
            WHERE source_kind = ? AND source_key = ?
            ORDER BY created_at ASC, operation_id ASC
            """,
            (source_kind, source_key),
        ).fetchall()
        out: list[OperationRecord] = []
        for row in rows:
            record = self._load_operation(str(row["issuer_id"]), str(row["operation_id"]))
            if record is not None:
                out.append(record)
        return out

    def _quarantine_partial_source(
        self,
        *,
        reason: str,
        source_kind: str,
        source_key: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        payload = {
            "partial_copy": True,
            "source_kind": source_kind,
            "source_key": source_key,
            **dict(details or {}),
        }
        try:
            self._quarantine_unlocked(reason=reason, details=payload)
        except JournalError:
            pass
        raise JournalError(
            Outcome.DEPENDENCY_UNAVAILABLE,
            "source/operations mapping inconsistent; writes off until reconcile",
            storage_kind="corrupt",
            details=payload,
        )

    def _assert_operation_source_consistent(self, record: OperationRecord) -> None:
        """Require side-table mapping whenever an operation retains source fields."""
        if record.source_kind is None and record.source_key is None:
            return
        if record.source_kind is None or record.source_key is None:
            self._quarantine_partial_source(
                reason="operation_source_fields_incomplete",
                source_kind=str(record.source_kind or ""),
                source_key=str(record.source_key or ""),
                details={
                    "issuer_id": record.issuer_id,
                    "operation_id": record.operation_id,
                },
            )
        mapped = self._load_source_mapping(record.source_kind, record.source_key)
        if mapped is None or mapped != (record.issuer_id, record.operation_id):
            self._quarantine_partial_source(
                reason="source_mapping_missing_partial_copy",
                source_kind=record.source_kind,
                source_key=record.source_key,
                details={
                    "issuer_id": record.issuer_id,
                    "operation_id": record.operation_id,
                    "mapped": None if mapped is None else {
                        "issuer_id": mapped[0],
                        "operation_id": mapped[1],
                    },
                },
            )

    def _resolve_source_binding(
        self,
        source_kind: str,
        source_key: str,
        *,
        durable_issuer: str,
        operation_id: str,
    ) -> OperationRecord | None:
        """Enforce source uniqueness via mappings and retained operations fields.

        A missing source_mappings row while operations still carry the source is a
        partial copy: quarantine, preserve receipts/history, refuse a new intent.
        """
        mapped = self._load_source_mapping(source_kind, source_key)
        by_ops = self._load_operations_by_source(source_kind, source_key)

        if mapped is not None:
            mapped_op = self._load_operation(mapped[0], mapped[1])
            if mapped_op is None:
                self._quarantine_partial_source(
                    reason="source_mapping_without_operation",
                    source_kind=source_kind,
                    source_key=source_key,
                    details={"mapped_issuer_id": mapped[0], "mapped_operation_id": mapped[1]},
                )
            if (
                mapped_op.source_kind != source_kind
                or mapped_op.source_key != source_key
            ):
                self._quarantine_partial_source(
                    reason="source_mapping_operation_fields_mismatch",
                    source_kind=source_kind,
                    source_key=source_key,
                    details={
                        "mapped_operation_id": mapped_op.operation_id,
                        "operation_source_kind": mapped_op.source_kind,
                        "operation_source_key": mapped_op.source_key,
                    },
                )
            if len(by_ops) != 1 or by_ops[0].key != mapped_op.key:
                self._quarantine_partial_source(
                    reason="source_uniqueness_inconsistent",
                    source_kind=source_kind,
                    source_key=source_key,
                    details={
                        "mapped_operation_id": mapped_op.operation_id,
                        "operations": [
                            {"issuer_id": op.issuer_id, "operation_id": op.operation_id}
                            for op in by_ops
                        ],
                    },
                )
            if mapped_op.operation_id != operation_id or mapped_op.issuer_id != durable_issuer:
                raise JournalError(
                    Outcome.CONFLICT,
                    "source mapping already bound to another operation key",
                    details={
                        "source_kind": source_kind,
                        "bound_operation_id": mapped_op.operation_id,
                    },
                )
            return mapped_op

        if by_ops:
            self._quarantine_partial_source(
                reason="source_mapping_missing_partial_copy",
                source_kind=source_kind,
                source_key=source_key,
                details={
                    "operations": [
                        {"issuer_id": op.issuer_id, "operation_id": op.operation_id}
                        for op in by_ops
                    ],
                    "receipts_preserved": any(op.receipt is not None for op in by_ops),
                },
            )
        return None

    def _assert_intent_envelope_intact(self, record: OperationRecord) -> None:
        """Refuse first dispatch when the immutable intent envelope drifted.

        Validates marker grammar against issuer/op IDs and a canonical digest of
        the whole envelope (P/target/M/payload/frozen/source/confirmation), then
        cross-checks retained intent_prepared history (C1 §3/§5).
        """
        row = self._store.execute(
            """
            SELECT issuer_id, operation_id, op_type, project_id, target_id, marker,
                   payload_json, payload_digest, frozen_due_json, source_kind,
                   source_key, confirmed_at
            FROM operations
            WHERE issuer_id = ? AND operation_id = ?
            """,
            (record.issuer_id, record.operation_id),
        ).fetchone()
        if row is None:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "operation row missing during intent envelope integrity check",
                storage_kind="corrupt",
                details={"mixed_copy": True},
            )
        issuer_id = str(row["issuer_id"])
        operation_id = str(row["operation_id"])
        op_type = str(row["op_type"])
        project_id = int(row["project_id"])
        target_id = None if row["target_id"] is None else int(row["target_id"])
        marker = str(row["marker"])
        raw = str(row["payload_json"])
        stored_digest = str(row["payload_digest"])
        frozen_json = None if row["frozen_due_json"] is None else str(row["frozen_due_json"])
        source_kind = None if row["source_kind"] is None else str(row["source_kind"])
        source_key = None if row["source_key"] is None else str(row["source_key"])
        confirmed_at = str(row["confirmed_at"])

        if marker != record.marker:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "operation marker does not match loaded record; refusing first dispatch",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "sha256_matches_payload": False,
                    "stored_marker": marker,
                    "record_marker": record.marker,
                },
            )
        if op_type == "create":
            expected_marker = build_marker(issuer_id, operation_id)
            if marker != expected_marker:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "create marker does not match issuer/operation IDs; refusing first dispatch",
                    storage_kind="corrupt",
                    details={
                        "mixed_copy": True,
                        "sha256_matches_payload": False,
                        "stored_marker": marker,
                        "expected_marker": expected_marker,
                    },
                )
        elif op_type in {"update", "complete"}:
            # Retained verified task M, or empty for unmarked manual (C1 §3).
            if marker != "" and MARKER_RE.fullmatch(marker) is None:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "update/complete marker is not a valid correlation marker; "
                    "refusing first dispatch",
                    storage_kind="corrupt",
                    details={
                        "mixed_copy": True,
                        "sha256_matches_payload": False,
                        "stored_marker": marker,
                        "op_type": op_type,
                    },
                )
        elif marker != "" and MARKER_RE.fullmatch(marker) is None:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "operation marker is not a valid correlation marker; refusing first dispatch",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "sha256_matches_payload": False,
                    "stored_marker": marker,
                    "op_type": op_type,
                },
            )

        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise TypeError("payload_json must be a JSON object")
            canonical_payload = _canonical_payload(parsed)
            if frozen_json is not None:
                frozen_parsed = json.loads(frozen_json)
                if not isinstance(frozen_parsed, dict):
                    raise TypeError("frozen_due_json must be a JSON object")
                canonical_frozen = _canonical_payload(frozen_parsed)
                if canonical_frozen != frozen_json:
                    raise ValueError("frozen_due_json is not canonical")
        except (TypeError, ValueError, json.JSONDecodeError, JournalError) as exc:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "intent envelope fields are not canonical confirmed intent",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "sha256_matches_payload": False,
                    "payload_error": str(exc),
                },
            ) from exc

        if canonical_payload != raw:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "payload_json is not canonical; refusing first dispatch",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "sha256_matches_payload": False,
                },
            )

        recomputed = _envelope_digest(
            issuer_id=issuer_id,
            operation_id=operation_id,
            op_type=op_type,
            project_id=project_id,
            target_id=target_id,
            marker=marker,
            payload_json=raw,
            frozen_due_json=frozen_json,
            source_kind=source_kind,
            source_key=source_key,
            confirmed_at=confirmed_at,
        )
        record_recomputed = _envelope_digest(
            issuer_id=record.issuer_id,
            operation_id=record.operation_id,
            op_type=record.op_type,
            project_id=record.project_id,
            target_id=record.target_id,
            marker=record.marker,
            payload_json=_canonical_payload(record.payload),
            frozen_due_json=(
                None if record.frozen_due is None else _canonical_payload(record.frozen_due)
            ),
            source_kind=record.source_kind,
            source_key=record.source_key,
            confirmed_at=record.confirmed_at,
        )
        matches = (
            recomputed == stored_digest
            and record.payload_digest == stored_digest
            and record_recomputed == stored_digest
            and record.op_type == op_type
            and record.project_id == project_id
            and record.target_id == target_id
            and record.confirmed_at == confirmed_at
            and record.source_kind == source_kind
            and record.source_key == source_key
        )
        if not matches:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "immutable intent envelope does not match stored digest; refusing first dispatch",
                storage_kind="corrupt",
                details={
                    "mixed_copy": True,
                    "sha256_matches_payload": False,
                    "stored_digest": stored_digest,
                    "recomputed_digest": recomputed,
                },
            )

        events = self._store.execute(
            """
            SELECT detail_json FROM history
            WHERE issuer_id = ? AND operation_id = ? AND event_type = ?
            ORDER BY id ASC
            """,
            (record.issuer_id, record.operation_id, "intent_prepared"),
        ).fetchall()
        for event in events:
            detail = json.loads(event["detail_json"])
            history_digest = detail.get("payload_digest")
            disagreements: dict[str, Any] = {}
            if history_digest is not None and str(history_digest) != stored_digest:
                disagreements["history_digest"] = str(history_digest)
            for field, expected in (
                ("marker", marker),
                ("op_type", op_type),
                ("project_id", project_id),
                ("target_id", target_id),
                ("frozen_due_json", frozen_json),
                ("source_kind", source_kind),
                ("source_key", source_key),
                ("confirmed_at", confirmed_at),
            ):
                if field not in detail:
                    continue
                if detail.get(field) != expected:
                    disagreements[field] = detail.get(field)
            if disagreements:
                raise JournalError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "intent history disagrees with operations immutable envelope",
                    storage_kind="corrupt",
                    details={
                        "mixed_copy": True,
                        "sha256_matches_payload": False,
                        "stored_digest": stored_digest,
                        "history_disagreements": disagreements,
                    },
                )

    def _resolve_intent_marker(
        self,
        *,
        durable_issuer: str,
        operation_id: str,
        op_type: str,
        target_id: int | None,
    ) -> str:
        """Bind envelope M: mint on create; retain verified task M otherwise.

        Update/complete must not mint from the new operation_id. When a prior
        receipt exists for the target, reuse its observed marker; unmarked
        manual targets keep an empty sentinel (C1 §3).
        """
        if op_type == "create":
            return build_marker(durable_issuer, operation_id)
        if op_type in {"update", "complete"}:
            if target_id is None:
                raise JournalError(
                    Outcome.VALIDATION_ERROR,
                    f"{op_type} intent requires target_id",
                )
            return self._verified_target_marker(target_id)
        return ""

    def _verified_target_marker(self, target_id: int) -> str:
        """Return the earliest verified receipt marker for a Vikunja task ID."""
        row = self._store.execute(
            """
            SELECT marker FROM receipts
            WHERE vikunja_id = ?
            ORDER BY created_at ASC, operation_id ASC
            LIMIT 1
            """,
            (target_id,),
        ).fetchone()
        if row is None:
            return ""
        marker = str(row["marker"])
        if marker == "":
            return ""
        if MARKER_RE.fullmatch(marker) is None:
            raise JournalError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "stored receipt marker is not a valid correlation marker",
                storage_kind="corrupt",
                details={"vikunja_id": target_id, "marker": marker},
            )
        return marker

    def _load_operation(self, issuer_id: str, operation_id: str) -> OperationRecord | None:
        row = self._store.execute(
            """
            SELECT * FROM operations
            WHERE issuer_id = ? AND operation_id = ?
            """,
            (issuer_id, operation_id),
        ).fetchone()
        if row is None:
            return None
        receipt_row = self._store.execute(
            """
            SELECT * FROM receipts
            WHERE issuer_id = ? AND operation_id = ?
            """,
            (issuer_id, operation_id),
        ).fetchone()
        receipt = None
        if receipt_row is not None:
            receipt = {
                "vikunja_id": int(receipt_row["vikunja_id"]),
                "project_id": int(receipt_row["project_id"]),
                "marker": str(receipt_row["marker"]),
                "effect_json": str(receipt_row["effect_json"]),
                "effect": json.loads(receipt_row["effect_json"]),
                "witnessed_at": str(receipt_row["witnessed_at"]),
                "created_at": str(receipt_row["created_at"]),
            }
        frozen = None
        if row["frozen_due_json"] is not None:
            frozen = json.loads(row["frozen_due_json"])
        return OperationRecord(
            issuer_id=str(row["issuer_id"]),
            operation_id=str(row["operation_id"]),
            op_type=str(row["op_type"]),
            project_id=int(row["project_id"]),
            target_id=None if row["target_id"] is None else int(row["target_id"]),
            marker=str(row["marker"]),
            payload=json.loads(row["payload_json"]),
            payload_digest=str(row["payload_digest"]),
            state=OperationState(str(row["state"])),
            delivery_state=DeliveryState(str(row["delivery_state"])),
            frozen_due=frozen,
            source_kind=None if row["source_kind"] is None else str(row["source_kind"]),
            source_key=None if row["source_key"] is None else str(row["source_key"]),
            confirmed_at=str(row["confirmed_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            receipt=receipt,
        )

    def _replay_or_conflict(
        self,
        existing: OperationRecord,
        *,
        op_type: str,
        project_id: int,
        target_id: int | None,
        payload_digest: str,
        payload: dict[str, Any],
        frozen_due: dict[str, Any] | None,
        source_kind: str | None,
        source_key: str | None,
    ) -> OperationRecord:
        same = (
            existing.op_type == op_type
            and existing.project_id == project_id
            and existing.target_id == target_id
            and existing.payload_digest == payload_digest
        )
        if same and frozen_due is not None:
            same = existing.frozen_due == frozen_due
        if same and source_kind is not None:
            same = existing.source_kind == source_kind and existing.source_key == source_key
        if same:
            # Matching key returns existing state/receipt; never re-arms HTTP.
            return existing
        # Incompatible payload requires a durable conflict mark (write path).
        self._require_writable()
        conflicted = self.mark_conflict(
            existing.issuer_id,
            existing.operation_id,
            reason="incompatible_payload",
            details={
                "existing_digest": existing.payload_digest,
                "new_digest": payload_digest,
            },
        )
        raise JournalError(
            Outcome.CONFLICT,
            "incompatible payload/target for existing operation key; 0 HTTP",
            details={
                "issuer_id": conflicted.issuer_id,
                "operation_id": conflicted.operation_id,
                "state": conflicted.state.value,
            },
        )
