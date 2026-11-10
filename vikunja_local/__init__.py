"""Internal Vikunja stock API helpers, durable journal, and recovery dispatcher.

External MCP/Hermes production writers arrive in later stages; the controlled
link callback in the dispatcher is not a production Hermes bridge.
"""

from .api import (
    ApiError,
    Outcome,
    TaskResult,
    TokenRef,
    VikunjaClient,
    build_create_description,
    extract_marker,
    html_escape_user_portion,
)
from .dates import (
    MSK,
    TARGET_DUE_PRECISION,
    ConfirmedDue,
    DueKind,
    canonicalize_rfc3339_instant,
    confirm_date_only,
    confirm_datetime,
    confirm_relative_msk,
    is_absent_due,
    normalize_due_value,
    require_rfc3339_instant,
    require_target_due_second_precision,
    same_instant,
    same_stock_persisted_instant,
)
from .dispatcher import DispatchResult, IntegrationDispatcher
from .journal import (
    DeliveryState,
    DispatchCriticalSection,
    IntegrationJournal,
    JournalError,
    OperationRecord,
    OperationState,
    new_issuer_id,
    new_operation_id,
)
from .reconcile import (
    ReconcileResult,
    apply_create_reconcile_to_journal,
    exact_marker_scan,
    reconcile_create_marker,
    reconcile_unknown_mutation,
)
from .storage import JOURNAL_SQLITE_USER_VERSION, StorageError

__all__ = [
    "MSK",
    "TARGET_DUE_PRECISION",
    "ApiError",
    "ConfirmedDue",
    "DeliveryState",
    "DispatchCriticalSection",
    "DispatchResult",
    "DueKind",
    "IntegrationDispatcher",
    "IntegrationJournal",
    "JOURNAL_SQLITE_USER_VERSION",
    "JournalError",
    "OperationRecord",
    "OperationState",
    "Outcome",
    "ReconcileResult",
    "StorageError",
    "TaskResult",
    "TokenRef",
    "VikunjaClient",
    "apply_create_reconcile_to_journal",
    "build_create_description",
    "canonicalize_rfc3339_instant",
    "confirm_date_only",
    "confirm_datetime",
    "confirm_relative_msk",
    "exact_marker_scan",
    "extract_marker",
    "html_escape_user_portion",
    "is_absent_due",
    "new_issuer_id",
    "new_operation_id",
    "normalize_due_value",
    "reconcile_create_marker",
    "reconcile_unknown_mutation",
    "require_rfc3339_instant",
    "require_target_due_second_precision",
    "same_instant",
    "same_stock_persisted_instant",
]
