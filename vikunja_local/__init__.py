"""Internal Vikunja stock API helpers for EA-010 synthetic checks.

Until s03/s04 land, this package is not an external writer surface.
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

__all__ = [
    "MSK",
    "TARGET_DUE_PRECISION",
    "ApiError",
    "ConfirmedDue",
    "DueKind",
    "Outcome",
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
    "normalize_due_value",
    "require_rfc3339_instant",
    "require_target_due_second_precision",
    "same_instant",
    "same_stock_persisted_instant",
]
