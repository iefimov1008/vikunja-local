"""Internal Vikunja stock API helpers and durable journal for EA-010 synthetic checks.

External MCP/Hermes writers arrive in later stages; this package is not a public
writer surface yet.
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
from .storage import JOURNAL_SQLITE_USER_VERSION, StorageError

__all__ = [
    "MSK",
    "TARGET_DUE_PRECISION",
    "ApiError",
    "ConfirmedDue",
    "DeliveryState",
    "DispatchCriticalSection",
    "DueKind",
    "IntegrationJournal",
    "JOURNAL_SQLITE_USER_VERSION",
    "JournalError",
    "OperationRecord",
    "OperationState",
    "Outcome",
    "StorageError",
    "TaskResult",
    "TokenRef",
    "VikunjaClient",
    "build_create_description",
    "canonicalize_rfc3339_instant",
    "confirm_date_only",
    "confirm_datetime",
    "confirm_relative_msk",
    "extract_marker",
    "html_escape_user_portion",
    "is_absent_due",
    "new_issuer_id",
    "new_operation_id",
    "normalize_due_value",
    "require_rfc3339_instant",
    "require_target_due_second_precision",
    "same_instant",
    "same_stock_persisted_instant",
]
