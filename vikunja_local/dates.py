"""Confirmed due-date helpers for Europe/Moscow (C1 §7)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
ZERO_DUE_UTC = datetime(1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

# Caller-frozen due targets are whole-second only. Admitted stock v2.8.0 persists
# due_date at second precision; nonzero fractional targets must not be frozen or
# dispatched. Server-generated recurring postimage/read-back may still carry
# sub-second wire values and are compared with same_stock_persisted_instant().
TARGET_DUE_PRECISION = "second"

# Frozen offset-bearing RFC3339 instants only (C1 §6/§7). Rejects ISO basic dates,
# week dates, missing seconds, and offsets with seconds that fromisoformat accepts.
_RFC3339_INSTANT_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"[Tt]"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?P<fraction>\.\d+)?"
    r"(?P<offset>[Zz]|[+-]\d{2}:\d{2})$"
)


class DueKind(str, Enum):
    NONE = "none"
    DATE = "date"
    DATETIME = "datetime"


@dataclass(frozen=True)
class ConfirmedDue:
    """Frozen absolute due produced at confirmation time."""

    kind: DueKind
    confirmed_at: datetime
    timezone_name: str
    expression: str
    absolute: datetime | None

    def wire_value(self) -> str | None:
        if self.kind is DueKind.NONE or self.absolute is None:
            return None
        return format_rfc3339_instant(self.absolute)


def _as_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("confirmed_at/datetime must be timezone-aware")
    return value


def require_target_due_second_precision(value: datetime) -> datetime:
    """Reject nonzero fractional seconds on caller-frozen due targets.

    Stock v2.8.0 truncates due_date to whole seconds. Freezing a sub-second
    absolute would violate C1 §6/§7 instant equality after read-back.
    """
    aware = _as_aware(value)
    if aware.microsecond:
        raise ValueError(
            "due_date fractional seconds are unsupported; freeze whole-second instants only"
        )
    return aware


def confirm_date_only(
    day: date | str,
    *,
    confirmed_at: datetime,
    expression: str | None = None,
) -> ConfirmedDue:
    """Date-only YYYY-MM-DD becomes end of that MSK day (23:59:59+03:00)."""
    if isinstance(day, str):
        day = date.fromisoformat(day)
    confirmed_at = _as_aware(confirmed_at)
    absolute = datetime.combine(day, time(23, 59, 59), tzinfo=MSK)
    return ConfirmedDue(
        kind=DueKind.DATE,
        confirmed_at=confirmed_at,
        timezone_name="Europe/Moscow",
        expression=expression or day.isoformat(),
        absolute=absolute,
    )


def confirm_datetime(
    value: datetime,
    *,
    confirmed_at: datetime,
    expression: str | None = None,
) -> ConfirmedDue:
    value = require_target_due_second_precision(value)
    confirmed_at = _as_aware(confirmed_at)
    return ConfirmedDue(
        kind=DueKind.DATETIME,
        confirmed_at=confirmed_at,
        timezone_name="Europe/Moscow",
        expression=expression or format_rfc3339_instant(value),
        absolute=value,
    )


def confirm_relative_msk(
    expression: str,
    *,
    confirmed_at: datetime,
    days: int,
) -> ConfirmedDue:
    """Resolve a relative day count in MSK at confirmation and freeze it."""
    confirmed_at = _as_aware(confirmed_at)
    local = confirmed_at.astimezone(MSK)
    target = local.date() + timedelta(days=days)
    return confirm_date_only(target, confirmed_at=confirmed_at, expression=expression)


def _to_utc_instant(value: datetime) -> datetime:
    """Convert an aware datetime to UTC; out-of-range conversions are validation errors."""
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    try:
        return aware.astimezone(timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("due_date UTC conversion out of range") from exc


def format_rfc3339_instant(value: datetime) -> str:
    """Canonical offset-bearing RFC3339 wire form for a frozen target instant.

    Emits whole-second precision only; nonzero fractional seconds are unsupported
    for caller-frozen dues (TARGET_DUE_PRECISION).
    """
    aware = require_target_due_second_precision(value)
    offset = aware.utcoffset()
    if offset is None:
        raise ValueError("due_date must include a timezone offset")
    if offset.total_seconds() == 0:
        base = aware.astimezone(timezone.utc).replace(tzinfo=None)
        return base.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    # Preserve the caller's offset; never emit offset-with-seconds.
    seconds = int(offset.total_seconds())
    sign = "+" if seconds >= 0 else "-"
    seconds = abs(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, sec = divmod(rem, 60)
    if sec:
        raise ValueError("due_date offset must be minute-resolution")
    naive = aware.replace(tzinfo=None)
    text = naive.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{text}{sign}{hours:02d}:{minutes:02d}"


def _fraction_digits(fraction: str) -> str:
    """Return digits after the leading '.' from an RFC3339 fraction group."""
    if not fraction:
        return ""
    return fraction[1:] if fraction.startswith(".") else fraction


def _fraction_has_nonzero_digit(fraction: str) -> bool:
    return any(ch != "0" for ch in _fraction_digits(fraction))


def canonicalize_rfc3339_instant(value: str) -> str:
    """Validate accepted RFC3339 grammar and return the canonical wire form.

    Nonzero fractional seconds are rejected before any HTTP write and before
    lossy datetime conversion (fromisoformat truncates beyond microseconds).
    Zero fractions (e.g. ``.000``, ``.000000000``) canonicalize to whole-second form.
    """
    if not isinstance(value, str):
        raise ValueError("due_date must be a string")
    text = value.strip()
    if not text:
        raise ValueError("due_date must be non-empty")
    match = _RFC3339_INSTANT_RE.fullmatch(text)
    if match is None:
        raise ValueError("due_date must be an offset-bearing RFC3339 instant")
    fraction = match.group("fraction") or ""
    # TARGET_DUE_PRECISION=second: any nonzero source fraction is unsupported,
    # including sub-microsecond tails that fromisoformat would silently drop.
    if _fraction_has_nonzero_digit(fraction):
        raise ValueError(
            "due_date fractional seconds are unsupported; freeze whole-second instants only"
        )
    return format_rfc3339_instant(require_rfc3339_instant(value))


def is_absent_due(value: object) -> bool:
    """Treat omission/null/validated zero-date as no deadline.

    C1 §7: only omission, explicit null, and a validated zero instant are absence.
    Blank/whitespace strings are malformed, not absence.
    """
    if value is None:
        return True
    if isinstance(value, str):
        if value.strip() == "":
            return False
        try:
            parsed = require_rfc3339_instant(value)
        except ValueError:
            return False
        return is_absent_due(parsed)
    if isinstance(value, datetime):
        try:
            return _to_utc_instant(value) == ZERO_DUE_UTC
        except ValueError:
            return False
    return False


def require_rfc3339_instant(value: str) -> datetime:
    """Parse a frozen absolute RFC3339 instant with an explicit timezone offset.

    Date-only values, ISO basic/week dates, missing seconds, offsets with seconds,
    and timezone-less local datetimes are rejected (C1 §6/§7).
    """
    if not isinstance(value, str):
        raise ValueError("due_date must be a string")
    text = value.strip()
    if not text:
        raise ValueError("due_date must be non-empty")
    match = _RFC3339_INSTANT_RE.fullmatch(text)
    if match is None:
        raise ValueError("due_date must be an offset-bearing RFC3339 instant")
    fraction = match.group("fraction") or ""
    digits = _fraction_digits(fraction)
    # datetime.fromisoformat truncates beyond six fractional digits. Reject any
    # nonzero digit that would be lost so read-back/compare cannot silently shift.
    if len(digits) > 6 and any(ch != "0" for ch in digits[6:]):
        raise ValueError("due_date fractional seconds exceed representable precision")
    if digits and not _fraction_has_nonzero_digit(fraction):
        # All-zero fractions (including >6 digits) parse as whole seconds.
        fraction = ""
    elif len(digits) > 6:
        fraction = "." + digits[:6]
    offset = match.group("offset")
    if offset in ("Z", "z"):
        normalized = (
            f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
            f"T{match.group('hour')}:{match.group('minute')}:{match.group('second')}"
            f"{fraction}+00:00"
        )
    else:
        normalized = (
            f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
            f"T{match.group('hour')}:{match.group('minute')}:{match.group('second')}"
            f"{fraction}{offset}"
        )
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("due_date must be a valid RFC3339 instant") from exc
    if parsed.tzinfo is None:
        raise ValueError("due_date must include a timezone offset")
    # Reject calendar-invalid combinations the regex cannot see (e.g. month 13).
    _to_utc_instant(parsed)
    return parsed


def normalize_due_value(value: object) -> datetime | None:
    """Return an aware instant, or None when the wire value means absence.

    Wire strings must be offset-bearing RFC3339 (C1 §6/§7). Blank strings and other
    malformed values raise so callers can map them to UNKNOWN after a dispatched write.
    """
    if value is None:
        return None
    if isinstance(value, str):
        if value.strip() == "":
            raise ValueError("due_date blank string is not absence")
        parsed = require_rfc3339_instant(value)
        if _to_utc_instant(parsed) == ZERO_DUE_UTC:
            return None
        return parsed
    if isinstance(value, datetime):
        utc = _to_utc_instant(value)
        if utc == ZERO_DUE_UTC:
            return None
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    raise TypeError(f"Unsupported due value type: {type(value)!r}")


def same_instant(left: object, right: object) -> bool:
    """Compare dues by instant/absence, not timezone string form.

    Unreadable values raise ValueError so API callers can retain mutation_sent and
    report UNKNOWN without retrying (C1 §4/§6).
    """
    try:
        a = normalize_due_value(left)
        b = normalize_due_value(right)
    except (TypeError, ValueError) as exc:
        raise ValueError("due_date comparison failed on unreadable value") from exc
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a.astimezone(timezone.utc) == b.astimezone(timezone.utc)


def same_stock_persisted_instant(left: object, right: object) -> bool:
    """Compare stock-persisted timestamps at verified second precision.

    Pinned v2.8.0 may emit sub-second values on PATCH while mandatory GET returns the
    same persisted second truncated (seen for mode=2 occurrence/reminder/done_at).
    Conflict detection still rejects distinct seconds; no retry is implied.
    """
    try:
        a = normalize_due_value(left)
        b = normalize_due_value(right)
    except (TypeError, ValueError) as exc:
        raise ValueError("due_date comparison failed on unreadable value") from exc
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a.astimezone(timezone.utc).replace(microsecond=0) == b.astimezone(timezone.utc).replace(
        microsecond=0
    )
