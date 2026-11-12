"""Authorized durable Hermes meeting-accept → Vikunja delivery bridge (C2 / I8).

in_process only: real server-owner principal, exact Origin, revision and explicit
due confirmation are enforced before accept/dispatch. Bool admission/accepted=true
is never proof. Durable acceptance + outbox survive the crash gap; replay returns
the same event/receipt; lost backlink is restored without create/re-accept.
Reading/recover of legacy Markdown/accepted never auto-dispatches Vikunja.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import stat
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, NoReturn
from urllib.parse import quote

from .api import Outcome, TokenRef, VikunjaClient, validate_uuid_v4
from .dates import ConfirmedDue, DueKind, confirm_date_only
from .dispatcher import CrashHook, DispatchResult, IntegrationDispatcher
from .journal import IntegrationJournal, new_operation_id

SOURCE_KIND = "hermes_meeting_accept_v1"
TRACKER_TARGET = "vikunja"
OUTBOX_PENDING = "pending"
OUTBOX_UNKNOWN = "unknown"
OUTBOX_LINKED = "linked"
OUTBOX_CONFLICT = "conflict"
HEX64_RE_LEN = 64
OUTBOX_LOCK_NAME = ".vikunja_outbox.lock"

CrashPoint = str
CrashHookBridge = Callable[[CrashPoint], None]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_event_id() -> str:
    return str(uuid.uuid4())


def validate_hex64(value: str, *, label: str = "source_version") -> str:
    if not isinstance(value, str) or len(value) != HEX64_RE_LEN:
        raise BridgeError("validation", f"{label} must be 64 lowercase hex chars")
    if any(ch not in "0123456789abcdef" for ch in value):
        raise BridgeError("validation", f"{label} must be 64 lowercase hex chars")
    return value


def validate_due_iso_or_null(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise BridgeError("validation", "due must be YYYY-MM-DD or null")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise BridgeError("validation", "due must be YYYY-MM-DD or null") from exc
    return value


def confirmed_due_from_accept(
    due: str | None,
    *,
    confirmed_at: datetime | None = None,
) -> ConfirmedDue:
    """Freeze accepted due at confirmation time (C1 §7). Meeting date is not due."""
    at = confirmed_at or datetime.now(timezone.utc).replace(microsecond=0)
    if due is None:
        return ConfirmedDue(
            kind=DueKind.NONE,
            confirmed_at=at,
            timezone_name="Europe/Moscow",
            expression="",
            absolute=None,
        )
    return confirm_date_only(due, confirmed_at=at)


def frozen_due_payload(due: ConfirmedDue) -> dict[str, Any]:
    """Canonical confirmed-due envelope shared with journal prepare_intent."""
    return {
        "kind": due.kind.value,
        "timezone_name": due.timezone_name,
        "expression": due.expression,
        "wire": due.wire_value(),
        "confirmed_at": due.confirmed_at.isoformat(),
    }


def confirmed_due_from_frozen(frozen: Mapping[str, Any]) -> ConfirmedDue:
    """Rebuild ConfirmedDue from a persisted acceptance/journal envelope."""
    if not isinstance(frozen, Mapping):
        raise BridgeError("validation", "frozen_due must be an object")
    kind_raw = frozen.get("kind")
    try:
        kind = DueKind(str(kind_raw))
    except ValueError as exc:
        raise BridgeError("validation", "frozen_due.kind invalid") from exc
    confirmed_raw = frozen.get("confirmed_at")
    if not isinstance(confirmed_raw, str) or not confirmed_raw:
        raise BridgeError("validation", "frozen_due.confirmed_at required")
    confirmed_at = datetime.fromisoformat(confirmed_raw.replace("Z", "+00:00"))
    if confirmed_at.tzinfo is None:
        raise BridgeError("validation", "frozen_due.confirmed_at must be offset-aware")
    expression = str(frozen.get("expression") or "")
    timezone_name = str(frozen.get("timezone_name") or "Europe/Moscow")
    wire = frozen.get("wire")
    absolute: datetime | None = None
    if wire is not None:
        if not isinstance(wire, str) or not wire:
            raise BridgeError("validation", "frozen_due.wire invalid")
        absolute = datetime.fromisoformat(wire.replace("Z", "+00:00"))
        if absolute.tzinfo is None:
            raise BridgeError("validation", "frozen_due.wire must be offset-aware")
    if kind is DueKind.NONE:
        absolute = None
    elif absolute is None:
        raise BridgeError("validation", "frozen_due.wire required for dated due")
    return ConfirmedDue(
        kind=kind,
        confirmed_at=confirmed_at,
        timezone_name=timezone_name,
        expression=expression,
        absolute=absolute,
    )


def assert_frozen_matches_accepted_due(
    confirmed: ConfirmedDue,
    due: str | None,
    *,
    code: str = "unavailable",
    message: str = "outbox accepted payload invalid",
) -> None:
    """Fail closed when frozen absolute/MSK policy diverges from accepted due.

    C2 / NFR-11 / FR-EA010-05: recompute canonical confirm_date_only(accepted due,
    original confirmed_at) and require the persisted wire + Europe/Moscow policy
    to match. Never rewrite due or wire to reconcile drift into another date.
    """
    def _fail(exc: Exception | None = None) -> NoReturn:
        if exc is None:
            raise BridgeError(code, message)
        raise BridgeError(code, message) from exc

    if confirmed.kind is DueKind.NONE:
        if due is not None:
            _fail()
        return
    if confirmed.kind is DueKind.DATE:
        if due is None or confirmed.expression != due:
            _fail()
        if confirmed.timezone_name != "Europe/Moscow":
            _fail()
        try:
            canonical = confirm_date_only(due, confirmed_at=confirmed.confirmed_at)
        except (TypeError, ValueError) as exc:
            _fail(exc)
        if confirmed.wire_value() != canonical.wire_value():
            _fail()
        return
    # Accept path freezes YYYY-MM-DD or null only; other kinds are damaged state.
    _fail()


def acceptance_payload_digest(
    *,
    title: str,
    due: str | None,
    source_version: str,
    tracker_target: str = TRACKER_TARGET,
) -> str:
    envelope = {
        "due": due,
        "source_version": source_version,
        "title": title,
        "tracker_target": tracker_target,
    }
    raw = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def proposal_source_url(*, hermes_origin: str, proposal_id: str) -> str:
    """Navigable Hermes deep-link to a concrete proposal (#pt-proposal-<id>)."""
    origin = hermes_origin.rstrip("/")
    if not origin.startswith("http://") and not origin.startswith("https://"):
        raise BridgeError("validation", "hermes_origin must be http(s)")
    safe_id = quote(str(proposal_id), safe="-_.~")
    return f"{origin}/#pt-proposal-{safe_id}"


def verified_meeting_provenance(
    calendar_record: Mapping[str, Any] | None,
) -> tuple[str | None, str | None, str | None]:
    """Freeze title/starts_at/timezone only from verified calendar projection.

    Inventory meeting summary/first_seen are not calendar provenance (C2 / BR-05).
    """
    if not isinstance(calendar_record, Mapping):
        return None, None, None
    if calendar_record.get("verification_status") != "verified":
        return None, None, None
    title = calendar_record.get("title")
    starts_at = calendar_record.get("starts_at")
    timezone_name = calendar_record.get("timezone")
    if not isinstance(title, str) or not title.strip():
        return None, None, None
    if not isinstance(starts_at, str) or not starts_at.strip():
        return None, None, None
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        return None, None, None
    return title.strip(), starts_at.strip(), timezone_name.strip()


def format_meeting_when(*, starts_at: str, timezone_name: str) -> str:
    return f"{starts_at} ({timezone_name})"


def meeting_provenance_ready(user_portion: str) -> bool:
    """True when frozen portion carries required meeting title and time (FR-EA010-06)."""
    if not isinstance(user_portion, str) or not user_portion:
        return False
    if "Meeting calendar: unavailable" in user_portion:
        return False
    has_title = any(line.startswith("Meeting title:") and line[len("Meeting title:") :].strip() for line in user_portion.splitlines())
    has_when = any(line.startswith("Meeting time:") and line[len("Meeting time:") :].strip() for line in user_portion.splitlines())
    return has_title and has_when


def meeting_user_portion(
    *,
    proposal_id: str,
    meeting_id: str,
    meeting_title: str | None,
    meeting_when: str | None,
    quote: str | None,
    version: str,
    source_url: str | None = None,
    calendar_unavailable: bool = False,
) -> str:
    """Brief context for Vikunja description; never a full transcript."""
    lines = [
        f"Hermes proposal: {proposal_id}",
        f"Meeting: {meeting_id}",
        f"Source version: {version}",
    ]
    if source_url:
        lines.append(f"Source: [Open proposal]({source_url})")
        lines.append(f"Source URL: {source_url}")
    if meeting_title:
        lines.append(f"Meeting title: {meeting_title}")
    if meeting_when:
        lines.append(f"Meeting time: {meeting_when}")
    elif calendar_unavailable:
        lines.append("Meeting calendar: unavailable")
    if quote:
        brief = quote.strip()
        if len(brief) > 500:
            brief = brief[:497] + "..."
        lines.append(f"Context: {brief}")
    return "\n".join(lines)


def fsync_directory(directory: Path) -> None:
    """Durably sync a directory entry after create/replace/unlink (checked)."""
    dir_fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def durable_clear_path(path: Path) -> None:
    """Unlink a recovery journal and fsync its parent so the clear is durable."""
    target = Path(path)
    target.unlink(missing_ok=True)
    fsync_directory(target.parent)


def bound_outbox_details(details: Mapping[str, Any], *, limit: int = 24) -> dict[str, Any]:
    """Persist a bounded JSON-safe subset of delivery/reconcile details."""
    out: dict[str, Any] = {}
    for index, (key, value) in enumerate(details.items()):
        if index >= limit:
            out["_truncated"] = True
            break
        label = str(key)[:64]
        if isinstance(value, bool) or value is None:
            out[label] = value
        elif isinstance(value, int) and not isinstance(value, bool):
            out[label] = value
        elif isinstance(value, str):
            out[label] = value[:500]
        elif isinstance(value, list):
            clipped: list[Any] = []
            for item in value[:20]:
                if isinstance(item, bool) or item is None:
                    clipped.append(item)
                elif isinstance(item, int) and not isinstance(item, bool):
                    clipped.append(item)
                elif isinstance(item, str):
                    clipped.append(item[:120])
            out[label] = clipped
    return out


_OBSOLETE_UI_DIAGNOSTIC_REASONS = frozenset(
    {"prepared_without_dispatch", "no_source_mapping"}
)
_OBSOLETE_UI_DIAGNOSTIC_KEYS = (
    "cannot_first_deliver",
    "ui_reconcile",
    "forbid_create",
    "receipt_preserved",
)


def stamp_verified_outbox_details(
    details: Mapping[str, Any] | None,
    *,
    path: str,
    checked_at: str | None = None,
) -> dict[str, Any]:
    """Clear stale no-first-HTTP diagnostics when delivery/link is verified (UX §4–5)."""
    checked = checked_at or _utc_now()
    cleaned = dict(details or {})
    reason = cleaned.get("reason")
    if isinstance(reason, str) and reason in _OBSOLETE_UI_DIAGNOSTIC_REASONS:
        cleaned.pop("reason", None)
    for key in _OBSOLETE_UI_DIAGNOSTIC_KEYS:
        cleaned.pop(key, None)
    cleaned["last_explicit_check"] = checked
    cleaned["last_check_outcome"] = Outcome.CONFIRMED.value
    cleaned["last_check_path"] = path
    return cleaned


def stamp_failed_check_outbox_details(
    details: Mapping[str, Any] | None,
    *,
    outcome: Outcome | str,
    path: str,
    recovered_details: Mapping[str, Any] | None = None,
    checked_at: str | None = None,
) -> dict[str, Any]:
    """Persist last_check_* so Core.snapshot keeps nonlinked recovery availability."""
    checked = checked_at or _utc_now()
    raw = outcome.value if isinstance(outcome, Outcome) else str(outcome)
    merged = {
        **dict(details or {}),
        "api_outcome": raw,
        "last_explicit_check": checked,
        "last_check_outcome": raw,
        "last_check_path": path,
        **bound_outbox_details(recovered_details or {}),
    }
    reason = merged.get("reason")
    if isinstance(reason, str) and reason.strip():
        merged["reason"] = reason.strip()[:120]
    else:
        src = (recovered_details or {}).get("reason")
        if isinstance(src, str) and src.strip():
            merged["reason"] = src.strip()[:120]
        elif raw in {
            Outcome.AUTH_DENIED.value,
            Outcome.SCOPE_DENIED.value,
            Outcome.DEPENDENCY_UNAVAILABLE.value,
        }:
            merged["reason"] = raw[:120]
    return merged


def tracker_availability_projection(
    *,
    outcome: Outcome | str | None = None,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Bounded public availability for proposal.tracker (FR-EA010-21/23, ADR §7, UX §5).

    Historical outbox state/task_id/url/event_id stay unchanged. Confirmed live checks
    keep the C2 four-field tracker; failed/unknown/conflict checks expose a compact
    availability object so replay and snapshot are not identical to a healthy link.
    """
    merged = dict(details or {})
    raw: str | None
    if isinstance(outcome, Outcome):
        raw = outcome.value
    elif isinstance(outcome, str) and outcome.strip():
        raw = outcome.strip()
    else:
        stored = merged.get("last_check_outcome")
        raw = stored.strip() if isinstance(stored, str) and stored.strip() else None
    if raw is None:
        return None
    if raw == Outcome.CONFIRMED.value:
        return None
    if raw == Outcome.CONFLICT.value:
        status = "conflict"
    elif raw in {Outcome.AUTH_DENIED.value, Outcome.SCOPE_DENIED.value}:
        status = "denied"
    else:
        status = "unavailable"
    projection: dict[str, Any] = {
        "status": status,
        "outcome": raw[:64],
        "last_check_outcome": raw[:64],
    }
    reason = merged.get("reason")
    if isinstance(reason, str) and reason.strip():
        projection["reason"] = reason.strip()[:120]
    checked = merged.get("last_explicit_check") or merged.get("checked_at")
    if isinstance(checked, str) and checked.strip():
        projection["checked_at"] = checked.strip()[:64]
    return projection


class BridgeError(RuntimeError):
    def __init__(self, code: str, message: str, *, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass
class TrackerView:
    event_id: str
    state: str
    task_id: int | None = None
    url: str | None = None
    operation_id: str | None = None
    payload_digest: str | None = None
    availability: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "event_id": self.event_id,
            "state": self.state,
            "task_id": self.task_id,
            "url": self.url,
        }
        if self.availability is not None:
            out["availability"] = dict(self.availability)
        return out


@dataclass
class OutboxRecord:
    event_id: str
    proposal_id: str
    payload_digest: str
    title: str
    due: str | None
    source_version: str
    state: str
    user_portion: str
    created_at: str
    updated_at: str
    operation_id: str | None = None
    task_id: int | None = None
    url: str | None = None
    frozen_due: dict[str, Any] | None = None
    source_url: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def tracker(
        self,
        *,
        live_outcome: Outcome | str | None = None,
        live_details: Mapping[str, Any] | None = None,
    ) -> TrackerView:
        merged = dict(self.details or {})
        if live_details:
            merged.update(dict(live_details))
        return TrackerView(
            event_id=self.event_id,
            state=self.state,
            task_id=self.task_id,
            url=self.url,
            operation_id=self.operation_id,
            payload_digest=self.payload_digest,
            availability=tracker_availability_projection(
                outcome=live_outcome,
                details=merged,
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "proposal_id": self.proposal_id,
            "payload_digest": self.payload_digest,
            "title": self.title,
            "due": self.due,
            "source_version": self.source_version,
            "state": self.state,
            "user_portion": self.user_portion,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "operation_id": self.operation_id,
            "task_id": self.task_id,
            "url": self.url,
            "frozen_due": None if self.frozen_due is None else dict(self.frozen_due),
            "source_url": self.source_url,
            "details": dict(self.details),
        }


@dataclass
class BridgeConfig:
    journal_path: Path
    api_origin: str
    project_id: int
    token_ref: TokenRef
    task_url_template: str
    hermes_origin: str
    page_size: int = 2
    crash_hook: CrashHookBridge | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "BridgeConfig":
        journal = Path(str(raw.get("journal_path") or ""))
        if not journal.is_absolute():
            raise BridgeError("validation", "journal_path must be absolute")
        token = TokenRef.from_mapping(raw.get("token_ref") or {})
        template = str(raw.get("task_url_template") or "")
        if "{id}" not in template:
            raise BridgeError("validation", "task_url_template must contain {id}")
        project_id = raw.get("project_id")
        if not isinstance(project_id, int) or isinstance(project_id, bool) or project_id <= 0:
            raise BridgeError("validation", "project_id must be a positive int")
        origin = str(raw.get("api_origin") or "").rstrip("/")
        if not origin.startswith("http://") and not origin.startswith("https://"):
            raise BridgeError("validation", "api_origin must be http(s)")
        hermes_origin = str(raw.get("hermes_origin") or "").rstrip("/")
        if not hermes_origin.startswith("http://") and not hermes_origin.startswith("https://"):
            raise BridgeError("validation", "hermes_origin must be http(s)")
        return cls(
            journal_path=journal,
            api_origin=origin,
            project_id=project_id,
            token_ref=token,
            task_url_template=template,
            hermes_origin=hermes_origin,
            page_size=int(raw.get("page_size") or 2),
        )


class HermesOutboxStore:
    """Durable Vikunja outbox keyed by proposal_id / event_id (Hermes-side).

    Stored as a JSON file under the synthetic Core runtime. Every read-modify-write
    runs under a process-safe fcntl lock shared with Core acceptance (same lock
    file beside the outbox). Atomic replace alone is not a lock.
    """

    SCHEMA = "ea010-hermes-vikunja-outbox/v1"

    def __init__(self, path: Path):
        if not path.is_absolute():
            raise BridgeError("validation", "outbox path must be absolute")
        self.path = path
        self.lock_path = path.parent / OUTBOX_LOCK_NAME

    def _empty(self) -> dict[str, Any]:
        return {"schema": self.SCHEMA, "by_proposal": {}, "by_event": {}}

    @contextlib.contextmanager
    def _lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            st = os.fstat(fd)
            if st.st_uid != os.getuid() or st.st_nlink != 1 or not stat.S_ISREG(st.st_mode):
                raise BridgeError("unavailable", "unsafe outbox lock file")
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    @staticmethod
    def _verify_accepted_payload_raw(raw: Mapping[str, Any]) -> None:
        """Fail closed when a ledger row diverges from its acceptance digest/envelope.

        Recomputes the C2 acceptance digest from title/due/source_version and checks
        the frozen due envelope — including absolute wire + Europe/Moscow policy —
        against canonical confirm_date_only(accepted due, original confirmed_at).
        user_portion must be a string (cross-index equality covers divergence between
        by_proposal and by_event). Never rewrites due/wire to reconcile drift.
        """
        title = raw.get("title")
        due_raw = raw.get("due")
        source_version = raw.get("source_version")
        payload_digest = raw.get("payload_digest")
        user_portion = raw.get("user_portion")
        if not isinstance(title, str) or not title.strip():
            raise BridgeError("unavailable", "outbox accepted payload invalid")
        if not isinstance(payload_digest, str) or not payload_digest:
            raise BridgeError("unavailable", "outbox accepted payload invalid")
        if user_portion is not None and not isinstance(user_portion, str):
            raise BridgeError("unavailable", "outbox accepted payload invalid")
        try:
            due = validate_due_iso_or_null(None if due_raw is None else str(due_raw))
            source_version = validate_hex64(str(source_version or ""))
        except BridgeError as exc:
            raise BridgeError("unavailable", "outbox accepted payload invalid") from exc
        expected = acceptance_payload_digest(
            title=title, due=due, source_version=source_version
        )
        if expected != payload_digest:
            raise BridgeError("unavailable", "outbox accepted payload invalid")
        frozen = raw.get("frozen_due")
        if frozen is None:
            return
        try:
            confirmed = confirmed_due_from_frozen(frozen)
            assert_frozen_matches_accepted_due(confirmed, due)
        except BridgeError as exc:
            if exc.code == "unavailable" and "accepted payload" in str(exc):
                raise
            raise BridgeError("unavailable", "outbox accepted payload invalid") from exc

    @staticmethod
    def _validate_indexes(data: Mapping[str, Any]) -> None:
        """Require typed indexes and cross-index identity (FR-EA010-21 fail-closed).

        Never invent missing by_proposal/by_event maps: a readable v1 ledger with a
        dropped index would otherwise project healthy linked rows while deliver by
        event_id cannot locate the same binding. Full row equality covers title/due/
        source_version/frozen_due/user_portion drift; each row must also match its
        acceptance digest and frozen envelope.
        """
        if "by_proposal" not in data or "by_event" not in data:
            raise BridgeError("unavailable", "outbox schema mismatch")
        by_proposal = data["by_proposal"]
        by_event = data["by_event"]
        if not isinstance(by_proposal, dict) or not isinstance(by_event, dict):
            raise BridgeError("unavailable", "outbox schema mismatch")
        for proposal_id, row in by_proposal.items():
            if not isinstance(proposal_id, str) or not proposal_id or not isinstance(row, dict):
                raise BridgeError("unavailable", "outbox index inconsistent")
            event_id = row.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise BridgeError("unavailable", "outbox index inconsistent")
            if row.get("proposal_id") != proposal_id:
                raise BridgeError("unavailable", "outbox index inconsistent")
            event_row = by_event.get(event_id)
            if not isinstance(event_row, dict):
                raise BridgeError("unavailable", "outbox index inconsistent")
            # Complete accepted payload + link fields must match across indexes.
            if event_row != row:
                raise BridgeError("unavailable", "outbox index inconsistent")
            HermesOutboxStore._verify_accepted_payload_raw(row)
        for event_id, row in by_event.items():
            if not isinstance(event_id, str) or not event_id or not isinstance(row, dict):
                raise BridgeError("unavailable", "outbox index inconsistent")
            if row.get("event_id") != event_id:
                raise BridgeError("unavailable", "outbox index inconsistent")
            proposal_id = row.get("proposal_id")
            if not isinstance(proposal_id, str) or not proposal_id:
                raise BridgeError("unavailable", "outbox index inconsistent")
            prop_row = by_proposal.get(proposal_id)
            if not isinstance(prop_row, dict) or prop_row.get("event_id") != event_id:
                raise BridgeError("unavailable", "outbox index inconsistent")

    def _load_unlocked(self, *, create: bool = False) -> dict[str, Any]:
        """Load the durable outbox ledger.

        Missing file is not an empty successful ledger for readers: raise
        unavailable/outbox_missing so Vikunja-identified accepts cannot be
        projected as legacy. Writers pass create=True for first insert.
        Incomplete or cross-index-inconsistent ledgers are rejected — never
        silently defaulted to empty maps that hide a damaged event index.
        """
        if not self.path.exists():
            if create:
                return self._empty()
            raise BridgeError("unavailable", "outbox_missing")
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("schema") != self.SCHEMA:
            raise BridgeError("unavailable", "outbox schema mismatch")
        self._validate_indexes(data)
        return data

    def _save_unlocked(self, data: dict[str, Any]) -> None:
        data = dict(data)
        data["schema"] = self.SCHEMA
        data["updated_at"] = _utc_now()
        self.path.parent.mkdir(mode=0o700, exist_ok=True)
        tmp = self.path.with_name(self.path.name + f".tmp-{os.getpid()}")
        payload = (json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        replaced = False
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
            replaced = True
            # Directory entry durability: file fsync alone is not enough after rename.
            fsync_directory(self.path.parent)
            os.chmod(self.path, 0o600)
        finally:
            if not replaced and tmp.exists():
                tmp.unlink(missing_ok=True)

    def load(self) -> dict[str, Any]:
        with self._lock():
            return self._load_unlocked()

    def save(self, data: dict[str, Any]) -> None:
        with self._lock():
            self._save_unlocked(data)

    def get_by_proposal(self, proposal_id: str) -> OutboxRecord | None:
        """Return a row when present.

        A missing ledger file maps to None so first-accept/register can create the
        file via mutate/upsert. Callers that already recorded Vikunja delivery
        identity must treat None as local-state unavailability (Core projection).
        """
        with self._lock():
            try:
                raw = self._load_unlocked()["by_proposal"].get(proposal_id)
            except BridgeError as exc:
                if exc.code == "unavailable" and "outbox_missing" in str(exc):
                    return None
                raise
            return None if raw is None else self._row(raw)

    def get_by_event(self, event_id: str) -> OutboxRecord | None:
        with self._lock():
            try:
                raw = self._load_unlocked()["by_event"].get(event_id)
            except BridgeError as exc:
                if exc.code == "unavailable" and "outbox_missing" in str(exc):
                    return None
                raise
            return None if raw is None else self._row(raw)

    def list_outbox(self) -> list[OutboxRecord]:
        with self._lock():
            try:
                data = self._load_unlocked()
            except BridgeError as exc:
                if exc.code == "unavailable" and "outbox_missing" in str(exc):
                    return []
                raise
            return [self._row(raw) for raw in data["by_event"].values()]

    def upsert(self, record: OutboxRecord) -> OutboxRecord:
        with self._lock():
            data = self._load_unlocked(create=True)
            existing = data["by_proposal"].get(record.proposal_id)
            if existing is not None and existing.get("event_id") != record.event_id:
                raise BridgeError(
                    "conflict",
                    "proposal already bound to another event_id",
                    details={"existing": existing.get("event_id"), "requested": record.event_id},
                )
            payload = record.to_payload()
            data["by_proposal"][record.proposal_id] = payload
            data["by_event"][record.event_id] = payload
            self._save_unlocked(data)
            return record

    def mutate(self, mutator: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        """Reload/merge/save under the shared outbox lock (process-safe RMW)."""
        with self._lock():
            data = self._load_unlocked(create=True)
            mutator(data)
            self._save_unlocked(data)
            return data

    @staticmethod
    def _row(raw: Mapping[str, Any]) -> OutboxRecord:
        frozen = raw.get("frozen_due")
        return OutboxRecord(
            event_id=str(raw["event_id"]),
            proposal_id=str(raw["proposal_id"]),
            payload_digest=str(raw["payload_digest"]),
            title=str(raw["title"]),
            due=None if raw.get("due") is None else str(raw["due"]),
            source_version=str(raw["source_version"]),
            state=str(raw["state"]),
            user_portion=str(raw.get("user_portion") or ""),
            created_at=str(raw["created_at"]),
            updated_at=str(raw["updated_at"]),
            operation_id=None if raw.get("operation_id") is None else str(raw["operation_id"]),
            task_id=None if raw.get("task_id") is None else int(raw["task_id"]),
            url=None if raw.get("url") is None else str(raw["url"]),
            frozen_due=None if frozen is None else dict(frozen),
            source_url=None if raw.get("source_url") is None else str(raw["source_url"]),
            details=dict(raw.get("details") or {}),
        )


@dataclass
class DeliveryResult:
    outcome: Outcome
    record: OutboxRecord | None = None
    dispatch: DispatchResult | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return (
            self.outcome is Outcome.CONFIRMED
            and self.record is not None
            and self.record.state == OUTBOX_LINKED
            and self.record.task_id is not None
        )

    def public_tracker(self) -> dict[str, Any] | None:
        """Router/Core projection: historical link fields + failed live-check availability."""
        if self.record is None:
            return None
        return self.record.tracker(
            live_outcome=self.outcome,
            live_details=self.details,
        ).as_dict()


class HermesBridge:
    """Mini adapter: durable outbox + IntegrationDispatcher for meeting accepts."""

    def __init__(self, config: BridgeConfig, outbox: HermesOutboxStore):
        self.config = config
        self.outbox = outbox
        self._crash = config.crash_hook

    def _fire(self, point: CrashPoint) -> None:
        if self._crash is not None:
            self._crash(point)

    @staticmethod
    def _link_callback_conflict(link_result: Mapping[str, Any] | None) -> bool:
        raw = dict(link_result or {})
        return (
            raw.get("conflict") is True
            or str(raw.get("error") or "") == "incompatible_existing_link"
        )

    def _receipt_backlink_conflict_result(
        self,
        event_id: str,
        record: OutboxRecord,
        *,
        path: str,
        dispatch: DispatchResult | None,
        receipt_task_id: int | None,
    ) -> DeliveryResult | None:
        """Map durable/callback incompatible backlink to CONFLICT (C2), not UNKNOWN."""
        link_result = dict((dispatch.link_result if dispatch is not None else None) or {})
        if record.state != OUTBOX_CONFLICT and not self._link_callback_conflict(link_result):
            return None
        preserved_task_id = record.task_id
        preserved_url = record.url
        checked_at = _utc_now()
        holder: dict[str, Any] = {}

        def mark_incompatible(data: dict[str, Any]) -> None:
            current_raw = data["by_event"].get(event_id)
            if current_raw is None:
                raise BridgeError("unavailable", "outbox_missing")
            row = HermesOutboxStore._row(current_raw)
            # Preserve the incompatible existing link; expose conflict durably.
            row.state = OUTBOX_CONFLICT
            row.task_id = preserved_task_id
            row.url = preserved_url
            row.updated_at = checked_at
            row.details = {
                **row.details,
                "reason": "incompatible_existing_link",
                "existing_task_id": preserved_task_id,
                "receipt_task_id": receipt_task_id,
                "last_explicit_check": checked_at,
                "last_check_outcome": Outcome.CONFLICT.value,
                "last_check_path": path,
                "forbid_create": True,
                "api_outcome": Outcome.CONFLICT.value,
                **bound_outbox_details(
                    {
                        "link_result": link_result,
                        **dict((dispatch.details if dispatch is not None else None) or {}),
                    }
                ),
            }
            payload_row = row.to_payload()
            data["by_proposal"][row.proposal_id] = payload_row
            data["by_event"][row.event_id] = payload_row
            holder["record"] = row

        try:
            self.outbox.mutate(mark_incompatible)
        except BridgeError as exc:
            if not (exc.code == "unavailable" and "outbox_missing" in str(exc)):
                raise
        updated = holder.get("record") or self.outbox.get_by_event(event_id) or record
        return DeliveryResult(
            outcome=Outcome.CONFLICT,
            record=updated,
            dispatch=dispatch,
            details={
                "path": path,
                "reason": "incompatible_existing_link",
                "forbid_create": True,
                "existing_task_id": preserved_task_id,
                "receipt_task_id": receipt_task_id,
                "historical_task_id": preserved_task_id,
                "historical_url": preserved_url,
                **dict((dispatch.details if dispatch is not None else None) or {}),
            },
        )

    def _client(self) -> VikunjaClient:
        return VikunjaClient(
            api_origin=self.config.api_origin,
            project_id=self.config.project_id,
            token_ref=self.config.token_ref,
            page_size=self.config.page_size,
            max_items_per_page=50,
        )

    def _task_url(self, task_id: int) -> str:
        return self.config.task_url_template.format(id=task_id)

    def _saved_link_incompatible(self, record: OutboxRecord, receipt_task_id: int) -> bool:
        """True when saved task_id/url disagree with the admitted receipt identity.

        Missing URL alone is a lost-backlink restore case, not conflict. A non-empty
        URL must equal the admitted template for the confirmed receipt ID (C2).
        """
        if record.task_id not in (None, receipt_task_id):
            return True
        if record.url and record.url != self._task_url(receipt_task_id):
            return True
        return False

    def _verify_linked_outbox(
        self,
        event_id: str,
        record: OutboxRecord,
        *,
        path: str,
        journal: IntegrationJournal | None = None,
    ) -> DeliveryResult:
        """Explicit linked recovery: live receipt/access/marker check; never recreate.

        Historical acceptance, event_id, and task_id/url stay on the outbox row.
        Deleted/inaccessible targets propagate unavailable/unknown/conflict from the
        dispatcher instead of cached CONFIRMED (AC-EA010-16/19, FR-EA010-21/23).
        """
        owns_journal = journal is None
        active = journal
        if owns_journal:
            active = IntegrationJournal(self.config.journal_path)
            active.open()
        assert active is not None
        try:
            mapped = active.lookup_source(SOURCE_KIND, event_id)
            if mapped is None:
                # Persist failed/no-mapping check so Core.snapshot() keeps availability
                # after UI recover+refresh (FR-EA010-20/21/23, C5 0.3).
                checked_at = _utc_now()

                def mark_nomap(row: OutboxRecord) -> None:
                    row.updated_at = checked_at
                    row.details = {
                        **row.details,
                        "reason": "no_source_mapping",
                        "last_explicit_check": checked_at,
                        "last_check_outcome": Outcome.UNKNOWN.value,
                        "last_check_path": path,
                        "forbid_create": True,
                        "receipt_preserved": True,
                    }

                updated = self._monotonic_outbox_apply(
                    event_id, mark_nomap, allow_linked_fill=True
                )
                return DeliveryResult(
                    outcome=Outcome.UNKNOWN,
                    record=updated or record,
                    details={
                        "path": path,
                        "reason": "no_source_mapping",
                        "forbid_create": True,
                        "receipt_preserved": True,
                        "historical_task_id": record.task_id,
                        "historical_url": record.url,
                        "last_explicit_check": checked_at,
                        "last_check_outcome": Outcome.UNKNOWN.value,
                    },
                )
            client = self._client()

            def link_cb(payload: Mapping[str, Any]) -> Mapping[str, Any]:
                task_id = int(payload["vikunja_id"])
                if self._saved_link_incompatible(record, task_id):
                    return {
                        "ok": False,
                        "error": "incompatible_existing_link",
                        "conflict": True,
                    }
                return {
                    "ok": True,
                    "url": record.url or self._task_url(task_id),
                    "task_id": task_id,
                    "production_bridge": True,
                }

            dispatcher = IntegrationDispatcher(
                active,
                client,
                link_callback=link_cb,
                crash_hook=None,
            )
            recovered = dispatcher.recover_create(mapped.issuer_id, mapped.operation_id)
            current = self.outbox.get_by_event(event_id) or record
            checked_at = _utc_now()
            link_result = dict(recovered.link_result or {})
            receipt_id = recovered.vikunja_id
            identity_mismatch = (
                receipt_id is not None
                and self._saved_link_incompatible(current, int(receipt_id))
            )
            callback_conflict = (
                link_result.get("conflict") is True
                or str(link_result.get("error") or "") == "incompatible_existing_link"
            )
            # Receipt CONFIRMED alone is not backlink proof (C2 / FR-EA010-04/07/21).
            incompatible_existing = identity_mismatch or callback_conflict
            if incompatible_existing:
                preserved_task_id = current.task_id
                preserved_url = current.url
                holder: dict[str, Any] = {}

                def mark_incompatible(data: dict[str, Any]) -> None:
                    current_raw = data["by_event"].get(event_id)
                    if current_raw is None:
                        raise BridgeError("unavailable", "outbox_missing")
                    row = HermesOutboxStore._row(current_raw)
                    # Preserve the incompatible existing link; expose conflict durably.
                    row.state = OUTBOX_CONFLICT
                    row.task_id = preserved_task_id
                    row.url = preserved_url
                    row.updated_at = checked_at
                    row.details = {
                        **row.details,
                        "reason": "incompatible_existing_link",
                        "existing_task_id": preserved_task_id,
                        "receipt_task_id": receipt_id,
                        "last_explicit_check": checked_at,
                        "last_check_outcome": Outcome.CONFLICT.value,
                        "last_check_path": path,
                        "forbid_create": True,
                        "api_outcome": Outcome.CONFLICT.value,
                        **bound_outbox_details(
                            {
                                "link_result": link_result,
                                **dict(recovered.details or {}),
                            }
                        ),
                    }
                    payload_row = row.to_payload()
                    data["by_proposal"][row.proposal_id] = payload_row
                    data["by_event"][row.event_id] = payload_row
                    holder["record"] = row

                try:
                    self.outbox.mutate(mark_incompatible)
                except BridgeError as exc:
                    if not (exc.code == "unavailable" and "outbox_missing" in str(exc)):
                        raise
                updated = holder.get("record") or self.outbox.get_by_event(event_id) or current
                return DeliveryResult(
                    outcome=Outcome.CONFLICT,
                    record=updated,
                    dispatch=recovered,
                    details={
                        "path": path,
                        "reason": "incompatible_existing_link",
                        "forbid_create": True,
                        "receipt_preserved": True,
                        "live_validated": False,
                        "existing_task_id": preserved_task_id,
                        "receipt_task_id": receipt_id,
                        "historical_task_id": preserved_task_id,
                        "historical_url": preserved_url,
                        **dict(recovered.details or {}),
                    },
                )

            link_ok = recovered.linked or link_result.get("ok") is True
            if recovered.ok and recovered.vikunja_id is not None and link_ok:

                def mark_verified(row: OutboxRecord) -> None:
                    row.updated_at = checked_at
                    row.details = stamp_verified_outbox_details(
                        row.details, path=path, checked_at=checked_at
                    )

                updated = self._monotonic_outbox_apply(
                    event_id, mark_verified, allow_linked_fill=True
                )
                return DeliveryResult(
                    outcome=Outcome.CONFIRMED,
                    record=updated or current,
                    dispatch=recovered,
                    details={"path": path, "live_validated": True},
                )

            def mark_check_failed(row: OutboxRecord) -> None:
                # Never downgrade linked/conflict identity; only stamp the check.
                row.updated_at = checked_at
                row.details = {
                    **row.details,
                    "last_explicit_check": checked_at,
                    "last_check_outcome": recovered.outcome.value,
                    "last_check_path": path,
                    "api_outcome": recovered.outcome.value,
                    **bound_outbox_details(recovered.details or {}),
                }

            updated = self._monotonic_outbox_apply(
                event_id, mark_check_failed, allow_linked_fill=True
            )
            outcome = (
                recovered.outcome
                if recovered.outcome is not Outcome.CONFIRMED
                else Outcome.UNKNOWN
            )
            return DeliveryResult(
                outcome=outcome,
                record=updated or current,
                dispatch=recovered,
                details={
                    "path": path,
                    "forbid_create": True,
                    "receipt_preserved": True,
                    "historical_task_id": current.task_id,
                    "historical_url": current.url,
                    **dict(recovered.details or {}),
                },
            )
        finally:
            if owns_journal:
                active.close()

    def _monotonic_outbox_apply(
        self,
        event_id: str,
        mutator: Callable[[OutboxRecord], None],
        *,
        allow_linked_fill: bool = False,
    ) -> OutboxRecord | None:
        """RMW under outbox lock; never downgrade linked/conflict to pending/unknown."""
        holder: dict[str, Any] = {}

        def apply(data: dict[str, Any]) -> None:
            current_raw = data["by_event"].get(event_id)
            if current_raw is None:
                raise BridgeError("unavailable", "outbox_missing")
            current = HermesOutboxStore._row(current_raw)
            if current.state == OUTBOX_CONFLICT:
                holder["record"] = current
                holder["skipped"] = True
                return
            if current.state == OUTBOX_LINKED:
                if allow_linked_fill:
                    before = current.to_payload()
                    mutator(current)
                    # Refuse regressing a linked row.
                    if current.state != OUTBOX_LINKED or current.task_id is None:
                        current = HermesOutboxStore._row(before)
                        holder["record"] = current
                        holder["skipped"] = True
                        return
                else:
                    holder["record"] = current
                    holder["skipped"] = True
                    return
            mutator(current)
            if current.state not in {OUTBOX_PENDING, OUTBOX_UNKNOWN, OUTBOX_LINKED, OUTBOX_CONFLICT}:
                raise BridgeError("validation", f"invalid outbox state {current.state}")
            payload_row = current.to_payload()
            data["by_proposal"][current.proposal_id] = payload_row
            data["by_event"][current.event_id] = payload_row
            holder["record"] = current

        try:
            self.outbox.mutate(apply)
        except BridgeError as exc:
            if exc.code == "unavailable" and "outbox_missing" in str(exc):
                return None
            raise
        return holder.get("record")

    @staticmethod
    def _source_mapping_bound_reason(details: Mapping[str, Any] | None) -> bool:
        reason = str((details or {}).get("reason") or "")
        return "source mapping already bound" in reason

    def _resolve_frozen_due(
        self,
        record: OutboxRecord,
        *,
        due: ConfirmedDue | None = None,
        journal_record: Any | None = None,
    ) -> ConfirmedDue:
        if due is not None:
            resolved = due
        elif journal_record is not None and getattr(journal_record, "frozen_due", None):
            resolved = confirmed_due_from_frozen(journal_record.frozen_due)
        elif record.frozen_due:
            resolved = confirmed_due_from_frozen(record.frozen_due)
        else:
            # Legacy rows without a frozen envelope: freeze once and persist.
            resolved = confirmed_due_from_accept(record.due)
            record.frozen_due = frozen_due_payload(resolved)
            record.updated_at = _utc_now()
            self.outbox.upsert(record)
        # Re-check absolute wire + MSK against accepted due before any intent/HTTP.
        # Never rewrite due/wire to reconcile persisted-state drift into another date.
        assert_frozen_matches_accepted_due(resolved, record.due)
        return resolved

    def register_acceptance(
        self,
        *,
        proposal_id: str,
        title: str,
        due: str | None,
        source_version: str,
        user_portion: str,
        event_id: str | None = None,
        frozen_due: Mapping[str, Any] | None = None,
        source_url: str | None = None,
    ) -> OutboxRecord:
        """Durable accept+outbox row. Call under Hermes Core lock before network."""
        title = title.strip()
        if not title:
            raise BridgeError("validation", "title required")
        due = validate_due_iso_or_null(due)
        source_version = validate_hex64(source_version)
        digest = acceptance_payload_digest(
            title=title, due=due, source_version=source_version
        )
        existing = self.outbox.get_by_proposal(proposal_id)
        if existing is not None:
            if existing.payload_digest != digest:
                raise BridgeError(
                    "conflict",
                    "incompatible accept payload for existing proposal outbox",
                    details={
                        "existing_digest": existing.payload_digest,
                        "new_digest": digest,
                    },
                )
            # Identical replay: return previous event/receipt without mutation.
            return existing
        now = _utc_now()
        if frozen_due is None:
            frozen_payload = frozen_due_payload(confirmed_due_from_accept(due))
        else:
            # Validate shape + absolute MSK policy against accepted due; persist
            # canonical copy. Refuse drifted wire instead of rewriting due.
            rebuilt = confirmed_due_from_frozen(frozen_due)
            assert_frozen_matches_accepted_due(
                rebuilt,
                due,
                code="validation",
                message="frozen_due does not match accepted due",
            )
            frozen_payload = frozen_due_payload(rebuilt)
        if source_url is None and self.config.hermes_origin:
            source_url = proposal_source_url(
                hermes_origin=self.config.hermes_origin, proposal_id=proposal_id
            )
        record = OutboxRecord(
            event_id=validate_uuid_v4(event_id or new_event_id(), label="event_id"),
            proposal_id=proposal_id,
            payload_digest=digest,
            title=title,
            due=due,
            source_version=source_version,
            state=OUTBOX_PENDING,
            user_portion=user_portion,
            created_at=now,
            updated_at=now,
            frozen_due=frozen_payload,
            source_url=source_url,
        )
        saved = self.outbox.upsert(record)
        # Durable acceptance+outbox committed; network must not have started yet.
        self._fire("after_acceptance_before_dispatch")
        return saved

    @staticmethod
    def _unavailable_delivery(exc: BridgeError) -> DeliveryResult:
        """Map durable ledger unavailability to a bounded delivery miss (no cached link)."""
        message = str(exc).lower()
        if "schema" in message:
            reason = "outbox_schema_mismatch"
        elif "accepted payload" in message:
            reason = "outbox_accepted_payload_invalid"
        elif "index" in message:
            reason = "outbox_index_inconsistent"
        else:
            reason = "outbox_missing"
        return DeliveryResult(
            outcome=Outcome.VALIDATION_ERROR,
            details={"reason": reason},
        )

    @staticmethod
    def _assert_accepted_record_intact(record: OutboxRecord) -> None:
        """Re-verify consumed outbox row before preparing a journal create intent."""
        HermesOutboxStore._verify_accepted_payload_raw(record.to_payload())

    def deliver(self, event_id: str, *, due: ConfirmedDue | None = None) -> DeliveryResult:
        """Network delivery outside the Hermes transaction. Idempotent on event_id."""
        event_id = validate_uuid_v4(event_id, label="event_id")
        try:
            record = self.outbox.get_by_event(event_id)
        except BridgeError as exc:
            if exc.code == "unavailable":
                return self._unavailable_delivery(exc)
            raise
        if record is None:
            return DeliveryResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "outbox_missing"},
            )
        if record.state == OUTBOX_LINKED and record.task_id is not None and record.url:
            # Live receipt/access/marker check — never trust cached linked success.
            return self._verify_linked_outbox(event_id, record, path="already_linked")
        if record.state == OUTBOX_CONFLICT:
            return DeliveryResult(
                outcome=Outcome.CONFLICT,
                record=record,
                details={"path": "durable_conflict"},
            )
        # Fail closed on digest/frozen/index damage before any journal intent or create.
        try:
            self._assert_accepted_record_intact(record)
        except BridgeError as exc:
            if exc.code == "unavailable":
                return self._unavailable_delivery(exc)
            raise
        # FR-EA010-06 / BR-05: never create a task with permanently missing meeting facts.
        if not meeting_provenance_ready(record.user_portion):
            return DeliveryResult(
                outcome=Outcome.VALIDATION_ERROR,
                record=record,
                details={
                    "reason": "meeting_provenance_unavailable",
                    "resend_forbidden": False,
                },
            )

        journal = IntegrationJournal(self.config.journal_path)
        journal.open()
        try:
            issuer = journal.ensure_issuer()
            existing = journal.lookup_source(SOURCE_KIND, event_id)
            frozen = self._resolve_frozen_due(record, due=due, journal_record=existing)
            client = self._client()

            def link_cb(payload: Mapping[str, Any]) -> Mapping[str, Any]:
                self._fire("after_receipt_before_backlink")
                task_id = int(payload["vikunja_id"])
                url = self._task_url(task_id)

                def apply(data: dict[str, Any]) -> None:
                    current_raw = data["by_event"].get(event_id)
                    if current_raw is None:
                        raise BridgeError("unavailable", "outbox_missing")
                    current = HermesOutboxStore._row(current_raw)
                    if self._saved_link_incompatible(current, task_id):
                        current.state = OUTBOX_CONFLICT
                        current.updated_at = _utc_now()
                        current.details = {
                            **current.details,
                            "reason": "incompatible_existing_link",
                            "existing_task_id": current.task_id,
                            "new_task_id": task_id,
                        }
                        payload_row = current.to_payload()
                        data["by_proposal"][current.proposal_id] = payload_row
                        data["by_event"][current.event_id] = payload_row
                        return
                    checked_at = _utc_now()
                    current.task_id = task_id
                    current.url = url
                    current.state = OUTBOX_LINKED
                    current.operation_id = str(payload.get("operation_id") or current.operation_id)
                    current.updated_at = checked_at
                    current.details = stamp_verified_outbox_details(
                        current.details, path="deliver_link", checked_at=checked_at
                    )
                    payload_row = current.to_payload()
                    data["by_proposal"][current.proposal_id] = payload_row
                    data["by_event"][current.event_id] = payload_row

                try:
                    self.outbox.mutate(apply)
                except BridgeError as exc:
                    if exc.code == "unavailable" and "outbox_missing" in str(exc):
                        return {"ok": False, "error": "outbox_missing"}
                    raise
                current = self.outbox.get_by_event(event_id)
                if current is None:
                    return {"ok": False, "error": "outbox_missing"}
                if current.state == OUTBOX_CONFLICT:
                    return {"ok": False, "error": "incompatible_existing_link", "conflict": True}
                return {"ok": True, "url": url, "task_id": task_id, "production_bridge": True}

            crash_hook: CrashHook | None = None
            if self._crash is not None:
                outer = self._crash

                def crash_hook(point: str) -> None:  # noqa: ANN202
                    outer(point)

            dispatcher = IntegrationDispatcher(
                journal,
                client,
                link_callback=link_cb,
                crash_hook=crash_hook,
            )
            # Acceptance/outbox already durable. Crash here is before adapter claim/HTTP.
            self._fire("before_dispatch")
            # Concurrent winners may have claimed the source / linked while we paused.
            record = self.outbox.get_by_event(event_id) or record
            if record.state == OUTBOX_LINKED and record.task_id is not None and record.url:
                return self._verify_linked_outbox(
                    event_id,
                    record,
                    path="already_linked_after_pause",
                    journal=journal,
                )
            if record.state == OUTBOX_CONFLICT:
                return DeliveryResult(
                    outcome=Outcome.CONFLICT,
                    record=record,
                    details={"path": "durable_conflict_after_pause"},
                )
            existing = journal.lookup_source(SOURCE_KIND, event_id)
            frozen = self._resolve_frozen_due(record, due=due, journal_record=existing)

            def resolve_operation_id() -> str | None:
                mapped = journal.lookup_source(SOURCE_KIND, event_id)
                if mapped is not None:
                    return mapped.operation_id
                current = self.outbox.get_by_event(event_id) or record
                if current.operation_id:
                    if current.state != OUTBOX_PENDING:
                        return None
                    return current.operation_id
                return new_operation_id()

            operation_id = resolve_operation_id()
            if operation_id is None:
                def mark_missing(current: OutboxRecord) -> None:
                    current.state = OUTBOX_UNKNOWN
                    current.updated_at = _utc_now()
                    current.details = {
                        **current.details,
                        "reason": "journal_source_missing",
                        "resend_forbidden": True,
                    }

                updated = self._monotonic_outbox_apply(event_id, mark_missing)
                return DeliveryResult(
                    outcome=Outcome.UNKNOWN,
                    record=updated or self.outbox.get_by_event(event_id) or record,
                    details={"reason": "journal_source_missing", "resend_forbidden": True},
                )

            def invoke(op_id: str) -> DispatchResult:
                return dispatcher.execute_create(
                    issuer_id=issuer,
                    operation_id=op_id,
                    title=record.title,
                    due=frozen,
                    user_portion=record.user_portion,
                    source_kind=SOURCE_KIND,
                    source_key=event_id,
                )

            result = invoke(operation_id)
            if result.outcome is Outcome.CONFLICT and self._source_mapping_bound_reason(result.details):
                rebound = journal.lookup_source(SOURCE_KIND, event_id)
                if rebound is not None and rebound.operation_id != operation_id:
                    operation_id = rebound.operation_id
                    result = invoke(operation_id)
                    record = self.outbox.get_by_event(event_id) or record
                    # Fall through: linked outbox alone is not confirmed success.

            # Persist operation_id only after prepare_intent may have committed.
            bound_op = None
            if result.operation is not None:
                bound_op = result.operation.operation_id
            elif (result.details or {}).get("bound_operation_id"):
                bound_op = str(result.details["bound_operation_id"])
            else:
                rebound = journal.lookup_source(SOURCE_KIND, event_id)
                if rebound is not None:
                    bound_op = rebound.operation_id
                else:
                    bound_op = operation_id

            def remember_op(current: OutboxRecord) -> None:
                if bound_op:
                    current.operation_id = str(bound_op)
                if current.frozen_due is None and frozen is not None:
                    current.frozen_due = frozen_due_payload(frozen)
                current.updated_at = _utc_now()

            remembered = self._monotonic_outbox_apply(event_id, remember_op, allow_linked_fill=True)
            if remembered is not None:
                record = remembered
            record = self.outbox.get_by_event(event_id) or record

            if result.ok and result.vikunja_id is not None:
                if record.state != OUTBOX_LINKED:

                    def mark_receipt(current: OutboxRecord) -> None:
                        current.task_id = int(result.vikunja_id)  # type: ignore[arg-type]
                        current.operation_id = str(bound_op or operation_id)
                        if current.state not in {OUTBOX_LINKED, OUTBOX_CONFLICT}:
                            current.state = OUTBOX_UNKNOWN
                        current.updated_at = _utc_now()

                    updated = self._monotonic_outbox_apply(event_id, mark_receipt, allow_linked_fill=True)
                    if updated is not None:
                        record = updated
                record = self.outbox.get_by_event(event_id) or record
                conflict = self._receipt_backlink_conflict_result(
                    event_id,
                    record,
                    path="deliver",
                    dispatch=result,
                    receipt_task_id=int(result.vikunja_id),
                )
                if conflict is not None:
                    return conflict
                return DeliveryResult(
                    outcome=Outcome.CONFIRMED if record.state == OUTBOX_LINKED else Outcome.UNKNOWN,
                    record=self.outbox.get_by_event(event_id),
                    dispatch=result,
                    details={"path": "deliver", "linked": result.linked},
                )
            if result.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED, Outcome.VALIDATION_ERROR}:

                def mark_refusal(current: OutboxRecord) -> None:
                    current.state = OUTBOX_UNKNOWN
                    current.updated_at = _utc_now()
                    current.details = {
                        **current.details,
                        "api_outcome": result.outcome.value,
                        "definitive_refusal": True,
                    }

                updated = self._monotonic_outbox_apply(event_id, mark_refusal)
                return DeliveryResult(
                    outcome=result.outcome,
                    record=updated or self.outbox.get_by_event(event_id) or record,
                    dispatch=result,
                    details={"path": "definitive_refusal"},
                )
            if result.outcome is Outcome.CONFLICT:

                def mark_conflict(current: OutboxRecord) -> None:
                    current.state = OUTBOX_CONFLICT
                    current.operation_id = str(bound_op or current.operation_id or operation_id)
                    current.updated_at = _utc_now()
                    current.details = {
                        **current.details,
                        "api_outcome": result.outcome.value,
                        **bound_outbox_details(result.details or {}),
                    }

                updated = self._monotonic_outbox_apply(event_id, mark_conflict)
                return DeliveryResult(
                    outcome=Outcome.CONFLICT,
                    record=updated or self.outbox.get_by_event(event_id) or record,
                    dispatch=result,
                    details={"path": "conflict", "forbid_create": True, **dict(result.details or {})},
                )

            def mark_unknown(current: OutboxRecord) -> None:
                current.state = OUTBOX_UNKNOWN
                current.operation_id = str(bound_op or current.operation_id or operation_id)
                current.updated_at = _utc_now()
                current.details = {
                    **current.details,
                    "api_outcome": result.outcome.value,
                    **bound_outbox_details(result.details or {}),
                }

            updated = self._monotonic_outbox_apply(event_id, mark_unknown)
            return DeliveryResult(
                outcome=Outcome.UNKNOWN,
                record=updated or self.outbox.get_by_event(event_id) or record,
                dispatch=result,
                details={"path": "uncertain", "forbid_create": True},
            )
        finally:
            journal.close()

    def recover(self, event_id: str, *, allow_first_http: bool = True) -> DeliveryResult:
        """Recover one explicit outbox row. Never walks legacy Markdown accepts.

        allow_first_http=True preserves s06 first-deliver for missing mapping /
        prepared_without_dispatch. UI reconcile (C5 0.3) passes False so those
        paths only report unknown without PUT/create.
        """
        event_id = validate_uuid_v4(event_id, label="event_id")
        try:
            record = self.outbox.get_by_event(event_id)
        except BridgeError as exc:
            if exc.code == "unavailable":
                return self._unavailable_delivery(exc)
            raise
        if record is None:
            return DeliveryResult(
                outcome=Outcome.VALIDATION_ERROR,
                details={"reason": "outbox_missing"},
            )
        if record.state == OUTBOX_LINKED and record.task_id and record.url:
            return self._verify_linked_outbox(event_id, record, path="recover_linked")
        if record.state == OUTBOX_CONFLICT:
            return DeliveryResult(outcome=Outcome.CONFLICT, record=record)
        # Fail closed on digest/frozen absolute drift before recover create/HTTP.
        try:
            self._assert_accepted_record_intact(record)
        except BridgeError as exc:
            if exc.code == "unavailable":
                return self._unavailable_delivery(exc)
            raise

        journal = IntegrationJournal(self.config.journal_path)
        journal.open()
        try:
            mapped = journal.lookup_source(SOURCE_KIND, event_id)
            client = self._client()

            def link_cb(payload: Mapping[str, Any]) -> Mapping[str, Any]:
                task_id = int(payload["vikunja_id"])
                url = self._task_url(task_id)

                def apply(data: dict[str, Any]) -> None:
                    current_raw = data["by_event"].get(event_id)
                    if current_raw is None:
                        raise BridgeError("unavailable", "outbox_missing")
                    current = HermesOutboxStore._row(current_raw)
                    if self._saved_link_incompatible(current, task_id):
                        current.state = OUTBOX_CONFLICT
                        current.updated_at = _utc_now()
                        current.details = {
                            **current.details,
                            "reason": "incompatible_existing_link",
                            "existing_task_id": current.task_id,
                            "new_task_id": task_id,
                        }
                        payload_row = current.to_payload()
                        data["by_proposal"][current.proposal_id] = payload_row
                        data["by_event"][current.event_id] = payload_row
                        return
                    checked_at = _utc_now()
                    current.task_id = task_id
                    current.url = url
                    current.state = OUTBOX_LINKED
                    current.updated_at = checked_at
                    current.details = stamp_verified_outbox_details(
                        current.details, path="recover_link", checked_at=checked_at
                    )
                    payload_row = current.to_payload()
                    data["by_proposal"][current.proposal_id] = payload_row
                    data["by_event"][current.event_id] = payload_row

                try:
                    self.outbox.mutate(apply)
                except BridgeError as exc:
                    if exc.code == "unavailable" and "outbox_missing" in str(exc):
                        return {"ok": False, "error": "outbox_missing"}
                    raise
                current = self.outbox.get_by_event(event_id)
                if current is None:
                    return {"ok": False, "error": "outbox_missing"}
                if current.state == OUTBOX_CONFLICT:
                    return {"ok": False, "error": "incompatible_existing_link", "conflict": True}
                return {"ok": True, "url": url, "task_id": task_id, "production_bridge": True}

            dispatcher = IntegrationDispatcher(
                journal,
                client,
                link_callback=link_cb,
                crash_hook=None,
            )
            if mapped is None:
                # Acceptance survived but adapter never prepared — first deliver only.
                if record.state in {OUTBOX_PENDING, OUTBOX_UNKNOWN} and record.task_id is None:
                    if allow_first_http:
                        return self.deliver(event_id)
                    checked_at = _utc_now()

                    def mark_pending_nomap(row: OutboxRecord) -> None:
                        row.updated_at = checked_at
                        row.details = {
                            **row.details,
                            "reason": "no_source_mapping",
                            "last_explicit_check": checked_at,
                            "last_check_outcome": Outcome.UNKNOWN.value,
                            "last_check_path": "recover_ui_nofirst",
                            "forbid_create": True,
                            "cannot_first_deliver": True,
                            "ui_reconcile": True,
                        }

                    updated = self._monotonic_outbox_apply(
                        event_id, mark_pending_nomap, allow_linked_fill=True
                    )
                    return DeliveryResult(
                        outcome=Outcome.UNKNOWN,
                        record=updated or record,
                        details={
                            "reason": "no_source_mapping",
                            "forbid_create": True,
                            "cannot_first_deliver": True,
                            "ui_reconcile": True,
                            "last_explicit_check": checked_at,
                            "last_check_outcome": Outcome.UNKNOWN.value,
                        },
                    )
                checked_at = _utc_now()

                def mark_linked_nomap(row: OutboxRecord) -> None:
                    row.updated_at = checked_at
                    row.details = {
                        **row.details,
                        "reason": "no_source_mapping",
                        "last_explicit_check": checked_at,
                        "last_check_outcome": Outcome.UNKNOWN.value,
                        "last_check_path": "recover",
                        "forbid_create": True,
                    }

                updated = self._monotonic_outbox_apply(
                    event_id, mark_linked_nomap, allow_linked_fill=True
                )
                return DeliveryResult(
                    outcome=Outcome.UNKNOWN,
                    record=updated or record,
                    details={
                        "reason": "no_source_mapping",
                        "forbid_create": True,
                        "last_explicit_check": checked_at,
                        "last_check_outcome": Outcome.UNKNOWN.value,
                    },
                )
            recovered = dispatcher.recover_create(mapped.issuer_id, mapped.operation_id)
            record = self.outbox.get_by_event(event_id) or record
            if (
                recovered.details.get("allows_first_http")
                and recovered.details.get("reason") == "prepared_without_dispatch"
            ):
                # Safe first HTTP was never claimed — resume with the original
                # persisted envelope / operation ID (no new acceptance).
                if not allow_first_http:
                    checked_at = _utc_now()

                    def mark_prepared(row: OutboxRecord) -> None:
                        row.updated_at = checked_at
                        row.details = {
                            **row.details,
                            "reason": "prepared_without_dispatch",
                            "last_explicit_check": checked_at,
                            "last_check_outcome": Outcome.UNKNOWN.value,
                            "last_check_path": "recover_ui_nofirst",
                            "forbid_create": True,
                            "cannot_first_deliver": True,
                            "ui_reconcile": True,
                        }

                    updated = self._monotonic_outbox_apply(
                        event_id, mark_prepared, allow_linked_fill=True
                    )
                    return DeliveryResult(
                        outcome=Outcome.UNKNOWN,
                        record=updated or record,
                        details={
                            "reason": "prepared_without_dispatch",
                            "forbid_create": True,
                            "cannot_first_deliver": True,
                            "ui_reconcile": True,
                            "last_explicit_check": checked_at,
                            "last_check_outcome": Outcome.UNKNOWN.value,
                        },
                    )
                if record.operation_id is None:

                    def remember_mapped(current: OutboxRecord) -> None:
                        current.operation_id = mapped.operation_id
                        current.updated_at = _utc_now()

                    self._monotonic_outbox_apply(event_id, remember_mapped, allow_linked_fill=True)
                return self.deliver(event_id)
            if recovered.ok and recovered.vikunja_id is not None:
                if record.state != OUTBOX_LINKED:
                    # Receipt present, backlink missing — restore link only.
                    self._fire("recover_backlink_only")
                    link_cb(
                        {
                            "vikunja_id": recovered.vikunja_id,
                            "issuer_id": mapped.issuer_id,
                            "operation_id": mapped.operation_id,
                        }
                    )
                    record = self.outbox.get_by_event(event_id) or record
                conflict = self._receipt_backlink_conflict_result(
                    event_id,
                    record,
                    path="recover_create",
                    dispatch=recovered,
                    receipt_task_id=int(recovered.vikunja_id),
                )
                if conflict is not None:
                    return conflict
                checked_at = _utc_now()

                def mark_recovered_ok(row: OutboxRecord) -> None:
                    row.updated_at = checked_at
                    row.details = stamp_verified_outbox_details(
                        row.details, path="recover_create", checked_at=checked_at
                    )

                verified = self._monotonic_outbox_apply(
                    event_id, mark_recovered_ok, allow_linked_fill=True
                )
                record = verified or self.outbox.get_by_event(event_id) or record
                return DeliveryResult(
                    outcome=Outcome.CONFIRMED if record.state == OUTBOX_LINKED else Outcome.UNKNOWN,
                    record=record,
                    dispatch=recovered,
                    details={"path": "recover_create", "create_http_attempts": recovered.create_http_attempts},
                )
            if recovered.outcome is Outcome.CONFLICT:
                checked_at = _utc_now()

                def mark_conflict(current: OutboxRecord) -> None:
                    current.state = OUTBOX_CONFLICT
                    current.operation_id = mapped.operation_id
                    current.updated_at = checked_at
                    current.details = stamp_failed_check_outbox_details(
                        current.details,
                        outcome=recovered.outcome,
                        path="recover_conflict",
                        recovered_details=recovered.details,
                        checked_at=checked_at,
                    )

                updated = self._monotonic_outbox_apply(event_id, mark_conflict)
                return DeliveryResult(
                    outcome=Outcome.CONFLICT,
                    record=updated or self.outbox.get_by_event(event_id) or record,
                    dispatch=recovered,
                    details={
                        "path": "recover_conflict",
                        "forbid_create": True,
                        **dict(recovered.details or {}),
                    },
                )

            checked_at = _utc_now()

            def mark_unresolved(current: OutboxRecord) -> None:
                if current.state not in {OUTBOX_LINKED, OUTBOX_CONFLICT}:
                    current.state = OUTBOX_UNKNOWN
                current.operation_id = mapped.operation_id
                current.updated_at = checked_at
                current.details = stamp_failed_check_outbox_details(
                    current.details,
                    outcome=recovered.outcome,
                    path="recover_unresolved",
                    recovered_details=recovered.details,
                    checked_at=checked_at,
                )

            updated = self._monotonic_outbox_apply(event_id, mark_unresolved, allow_linked_fill=True)
            return DeliveryResult(
                outcome=recovered.outcome if recovered.outcome is not Outcome.CONFIRMED else Outcome.UNKNOWN,
                record=updated or self.outbox.get_by_event(event_id) or record,
                dispatch=recovered,
                details={"path": "recover_unresolved", "forbid_create": True, **dict(recovered.details or {})},
            )
        finally:
            journal.close()

    def recover_all_outbox(self) -> list[DeliveryResult]:
        """Iterate only explicit Vikunja outbox rows — never legacy accepted Markdown."""
        results: list[DeliveryResult] = []
        for record in self.outbox.list_outbox():
            if record.state == OUTBOX_CONFLICT:
                results.append(DeliveryResult(outcome=Outcome.CONFLICT, record=record))
                continue
            # Linked rows go through recover → live receipt check (no cached success).
            results.append(self.recover(record.event_id))
        return results


def ensure_bridge_issuer(journal_path: Path, issuer_id: str | None = None) -> str:
    """Load durable journal issuer; mint only when journal has no issuer yet.

    Pass through None so a second default call reuses the durable issuer instead
    of minting a fresh proposed id that fails issuer_id mismatch checks.
    """
    journal = IntegrationJournal(journal_path)
    journal.open()
    try:
        return journal.ensure_issuer(issuer_id)
    finally:
        journal.close()
