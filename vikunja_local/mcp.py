"""MCP 2025-06-18 stdio/JSON-RPC adapter for EA-010 (C3 0.1).

All mutations go through IntegrationDispatcher with source_kind=mcp_confirmed_v1.
Tool names, schemas, and error wire follow the accepted C3 contract only.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, Mapping, TextIO
from urllib.parse import urlsplit

from .api import (
    ApiError,
    Outcome,
    TaskResult,
    TokenRef,
    VikunjaClient,
    _normalize_api_origin,
    validate_uuid_v4,
)
from .dates import (
    ConfirmedDue,
    confirm_date_only,
    confirm_datetime,
    format_rfc3339_instant,
    is_absent_due,
    require_rfc3339_instant,
)
from .dispatcher import DispatchResult, IntegrationDispatcher
from .journal import (
    DeliveryState,
    IntegrationJournal,
    JournalError,
    OperationRecord,
    OperationState,
    new_operation_id,
)

PROTOCOL_VERSION = "2025-06-18"
SERVER_NAME = "ea010-vikunja"
SERVER_VERSION = "1.0.0"
SOURCE_KIND = "mcp_confirmed_v1"
COMMAND_META_KEY = "ea010/command"
MAX_LINE_BYTES = 256 * 1024
MAX_SAFE_INT = 9007199254740991
TITLE_MAX_CODEPOINTS = 250
DESCRIPTION_MAX_CODEPOINTS = 16384

UUID_V4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

WRITE_TOOL_NAMES = frozenset({"ea010_create", "ea010_update", "ea010_complete"})
READ_TOOL_NAMES = frozenset({"ea010_find", "ea010_reconcile"})
ALL_TOOL_NAMES = WRITE_TOOL_NAMES | READ_TOOL_NAMES

STATUS_CONFIRMED = "confirmed"
STATUS_NOT_FOUND = "not_found"
STATUS_UNKNOWN = "unknown"
STATUS_CONFLICT = "conflict"
STATUS_AMBIGUITY = "ambiguity"
STATUS_DENIED = "denied"
STATUS_UNAVAILABLE = "unavailable"
STATUS_INVALID_INPUT = "invalid_input"
STATUS_INCOMPLETE_SCAN = "incomplete_scan"

LINK_VERIFIED = "verified"
LINK_UNAVAILABLE = "unavailable"
LINK_NOT_APPLICABLE = "not_applicable"

REASON_AUTHORIZATION = "authorization"
REASON_PROJECT_SCOPE = "project_scope"
REASON_MISSING_COMMAND_CONTEXT = "missing_command_context"
REASON_TRANSPORT = "transport"
REASON_STORAGE = "storage"
REASON_MARKER_CONFLICT = "marker_conflict"
REASON_IMMUTABLE_CONFLICT = "immutable_conflict"
REASON_UNRESOLVED_TARGET = "unresolved_target"
REASON_INSUFFICIENT_EVIDENCE = "insufficient_evidence"
REASON_INVALID_ARGUMENT = "invalid_argument"
REASON_NOT_FOUND = "not_found"

JSONRPC_PARSE = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL = -32603


class McpError(Exception):
    """Adapter-level validation or configuration error."""

    def __init__(self, message: str, *, reason: str | None = None):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class McpConfig:
    """Closed MCP settings: C1 §9 schema plus C3 profile fields."""

    schema_version: int
    api_origin: str
    project_id: int
    token_ref: TokenRef
    journal_path: Path
    page_size: int
    max_items_per_page: int
    profile: str  # "write" | "read_only"
    browser_origin: str | None
    issuer_id: str | None
    trusted_write_client: bool = False

    @property
    def lists_write_tools(self) -> bool:
        return self.profile == "write"

    @property
    def accepts_write_commands(self) -> bool:
        """Mutations require write profile and a proven trusted host path."""
        return self.profile == "write" and self.trusted_write_client


def load_config(path: Path | str) -> McpConfig:
    """Load closed JSON settings (C1 §9 + C3 profile). Fail closed on drift."""
    cfg_path = Path(path)
    raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise McpError("config root must be an object")
    # Keys starting with '_' are documentation-only in example recipes.
    raw = {k: v for k, v in raw.items() if not str(k).startswith("_")}
    unknown = set(raw) - {
        "schema_version",
        "api_origin",
        "project_id",
        "token_ref",
        "journal_path",
        "page_size",
        "max_items_per_page",
        "profile",
        "browser_origin",
        "issuer_id",
        "trusted_write_client",
    }
    if unknown:
        raise McpError(f"unknown config fields: {sorted(unknown)}")

    if "schema_version" not in raw:
        raise McpError("schema_version is required")
    schema_version = raw["schema_version"]
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise McpError("schema_version must be an int")
    if schema_version != 1:
        raise McpError(f"unsupported schema_version: {schema_version}")

    api_origin_raw = raw.get("api_origin")
    if not isinstance(api_origin_raw, str) or not api_origin_raw.strip():
        raise McpError("api_origin must be a non-empty string")
    try:
        api_origin = _normalize_api_origin(api_origin_raw)
    except ApiError as exc:
        raise McpError(str(exc)) from exc

    project_id = raw.get("project_id")
    if not isinstance(project_id, int) or isinstance(project_id, bool) or project_id <= 0:
        raise McpError("project_id must be a positive int")

    if "token_ref" not in raw:
        raise McpError("token_ref is required")
    token_ref_raw = raw["token_ref"]
    if not isinstance(token_ref_raw, dict):
        raise McpError("token_ref must be an object")
    try:
        token_ref = TokenRef.from_mapping(token_ref_raw)
    except ApiError as exc:
        raise McpError(str(exc)) from exc

    journal_path = raw.get("journal_path")
    if not isinstance(journal_path, str) or not journal_path.strip():
        raise McpError("journal_path must be an absolute path string")
    journal = Path(journal_path)
    if not journal.is_absolute():
        raise McpError("journal_path must be absolute")

    if "max_items_per_page" not in raw:
        raise McpError("max_items_per_page is required")
    max_items_per_page = raw["max_items_per_page"]
    if (
        not isinstance(max_items_per_page, int)
        or isinstance(max_items_per_page, bool)
        or max_items_per_page < 1
    ):
        raise McpError("max_items_per_page must be a positive int")

    if "page_size" not in raw:
        raise McpError("page_size is required")
    page_size = raw["page_size"]
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
        raise McpError("page_size must be a positive int")
    page_size_limit = min(100, max_items_per_page)
    if page_size > page_size_limit:
        raise McpError(
            f"page_size must be in 1..{page_size_limit} (min(100, max_items_per_page))"
        )

    profile = raw.get("profile", "read_only")
    if profile not in {"write", "read_only"}:
        raise McpError("profile must be write or read_only")
    browser_origin = raw.get("browser_origin")
    if browser_origin is not None:
        browser_origin = _validate_browser_origin(browser_origin)

    issuer_id = raw.get("issuer_id")
    if issuer_id is not None:
        issuer_id = validate_uuid_v4(str(issuer_id), label="issuer_id")

    if "trusted_write_client" in raw:
        trusted_raw = raw["trusted_write_client"]
        if not isinstance(trusted_raw, bool):
            raise McpError("trusted_write_client must be a JSON boolean")
        trusted = trusted_raw
    else:
        trusted = False
    return McpConfig(
        schema_version=schema_version,
        api_origin=api_origin,
        project_id=project_id,
        token_ref=token_ref,
        journal_path=journal,
        page_size=page_size,
        max_items_per_page=max_items_per_page,
        profile=profile,
        browser_origin=browser_origin,
        issuer_id=issuer_id,
        trusted_write_client=trusted,
    )


def _is_wire_text(value: str) -> bool:
    """True when a wire string is UTF-8-encodable without NUL or unpaired surrogates."""
    if "\x00" in value:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _reject_nul(value: str, *, label: str) -> str:
    """Reject NUL and invalid Unicode (C3); name retained for call-site stability."""
    if "\x00" in value:
        raise McpError(f"{label} must not contain NUL", reason=REASON_INVALID_ARGUMENT)
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise McpError(
            f"{label} must be valid UTF-8 without unpaired surrogates",
            reason=REASON_INVALID_ARGUMENT,
        ) from exc
    return value


def _validate_browser_origin(value: Any) -> str:
    """Admit only an absolute http(s) origin (no path/query/fragment/credentials)."""
    if not isinstance(value, str) or not value.strip():
        raise McpError("browser_origin must be null or non-empty string")
    raw = value.strip()
    lowered = raw.lower()
    if "example" in lowered or "changeme" in lowered:
        raise McpError("browser_origin placeholder rejected; omit or set a verified origin")
    parts = urlsplit(raw)
    if parts.scheme not in {"http", "https"}:
        raise McpError("browser_origin must be an http(s) origin")
    if not parts.hostname:
        raise McpError("browser_origin must include a hostname")
    if parts.username is not None or parts.password is not None:
        raise McpError("browser_origin must not include credentials")
    if parts.path not in ("", "/"):
        raise McpError("browser_origin must not include a path")
    if parts.query or parts.fragment:
        raise McpError("browser_origin must not include query or fragment")
    # Rebuild a canonical origin without a trailing slash.
    host = parts.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port is not None:
        return f"{parts.scheme}://{host}:{parts.port}"
    return f"{parts.scheme}://{host}"


def _codepoints(value: str) -> int:
    return len(value)


def _is_jsonrpc_id(value: Any) -> bool:
    """JSON-RPC 2.0 request id: string, number, or null — never bool/object/array.

    String ids must be valid UTF-8 wire text: unpaired surrogates (e.g. JSON
    ``\\ud800``) are rejected before any tool/dispatcher admission.
    """
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    if isinstance(value, str):
        return _is_wire_text(value)
    return False


def _optional_test_crash_hook() -> Callable[[str], None] | None:
    """Isolated-suite only: SIGKILL at a named dispatcher barrier when env is set."""
    point = (os.environ.get("EA010_MCP_TEST_CRASH_POINT") or "").strip()
    if not point:
        return None
    ready_path = (os.environ.get("EA010_MCP_TEST_CRASH_READY") or "").strip()

    def _hook(reached: str) -> None:
        if reached != point:
            return
        if ready_path:
            Path(ready_path).write_text(
                json.dumps({"pid": os.getpid(), "point": reached}),
                encoding="utf-8",
            )
        os.kill(os.getpid(), signal.SIGKILL)

    return _hook


def _require_object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise McpError(f"{label} must be an object", reason=REASON_INVALID_ARGUMENT)
    return value


def _is_safe_task_id(value: Any) -> bool:
    """True when value is an integer in the C3 wire range 1..MAX_SAFE_INT."""
    return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= MAX_SAFE_INT


def _task_id_schema() -> dict[str, Any]:
    """Advertised JSON Schema fragment for every input/output task ID."""
    return {"type": "integer", "minimum": 1, "maximum": MAX_SAFE_INT}


def _require_safe_int(value: Any, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise McpError(f"{label} must be an integer", reason=REASON_INVALID_ARGUMENT)
    if value < 1 or value > MAX_SAFE_INT:
        raise McpError(f"{label} out of allowed range", reason=REASON_INVALID_ARGUMENT)
    return value


def _require_uuid_v4(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not UUID_V4_RE.fullmatch(value):
        raise McpError(f"{label} must be lowercase UUIDv4", reason=REASON_INVALID_ARGUMENT)
    return value


def _parse_confirmed_at(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise McpError("confirmed_at must be RFC3339 with explicit offset", reason=REASON_INVALID_ARGUMENT)
    _reject_nul(value, label="confirmed_at")
    try:
        return require_rfc3339_instant(value)
    except ValueError as exc:
        raise McpError(
            "confirmed_at must be RFC3339 with explicit offset",
            reason=REASON_INVALID_ARGUMENT,
        ) from exc


def _parse_due(value: Any, *, confirmed_at: datetime) -> ConfirmedDue:
    due = _require_object(value, label="due")
    unknown = set(due) - {"kind", "value"}
    if unknown:
        raise McpError(f"due unknown fields: {sorted(unknown)}", reason=REASON_INVALID_ARGUMENT)
    kind = due.get("kind")
    raw = due.get("value")
    if kind == "date":
        if not isinstance(raw, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            raise McpError("due.date value must be YYYY-MM-DD", reason=REASON_INVALID_ARGUMENT)
        _reject_nul(raw, label="due.value")
        try:
            return confirm_date_only(raw, confirmed_at=confirmed_at)
        except ValueError as exc:
            raise McpError(
                "due.date value is not a valid calendar date",
                reason=REASON_INVALID_ARGUMENT,
            ) from exc
    if kind == "datetime":
        if not isinstance(raw, str):
            raise McpError("due.datetime value must be RFC3339 string", reason=REASON_INVALID_ARGUMENT)
        _reject_nul(raw, label="due.value")
        try:
            instant = require_rfc3339_instant(raw)
            return confirm_datetime(instant, confirmed_at=confirmed_at, expression=raw)
        except ValueError as exc:
            raise McpError(
                "due.datetime must be RFC3339 with explicit offset and whole-second precision",
                reason=REASON_INVALID_ARGUMENT,
            ) from exc
    raise McpError("due.kind must be date or datetime", reason=REASON_INVALID_ARGUMENT)


def _validate_title(value: Any) -> str:
    if not isinstance(value, str):
        raise McpError("title must be a string", reason=REASON_INVALID_ARGUMENT)
    _reject_nul(value, label="title")
    if not value.strip():
        raise McpError("title must be non-empty after whitespace check", reason=REASON_INVALID_ARGUMENT)
    if _codepoints(value) < 1 or _codepoints(value) > TITLE_MAX_CODEPOINTS:
        raise McpError("title length out of range", reason=REASON_INVALID_ARGUMENT)
    return value


def _validate_description(value: Any) -> str:
    if not isinstance(value, str):
        raise McpError("description must be a string", reason=REASON_INVALID_ARGUMENT)
    _reject_nul(value, label="description")
    if _codepoints(value) > DESCRIPTION_MAX_CODEPOINTS:
        raise McpError("description length out of range", reason=REASON_INVALID_ARGUMENT)
    return value


def _due_schema() -> dict[str, Any]:
    return {
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "value"],
                "properties": {
                    "kind": {"const": "date"},
                    "value": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
                },
            },
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "value"],
                "properties": {
                    "kind": {"const": "datetime"},
                    "value": {"type": "string"},
                },
            },
        ]
    }


def _output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["status", "operation_ref", "task", "link_state"],
        "properties": {
            "status": {
                "type": "string",
                "enum": [
                    STATUS_CONFIRMED,
                    STATUS_NOT_FOUND,
                    STATUS_UNKNOWN,
                    STATUS_CONFLICT,
                    STATUS_AMBIGUITY,
                    STATUS_DENIED,
                    STATUS_UNAVAILABLE,
                    STATUS_INVALID_INPUT,
                    STATUS_INCOMPLETE_SCAN,
                ],
            },
            "operation_ref": {"type": ["string", "null"]},
            "task": {
                "oneOf": [
                    {"type": "null"},
                    {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["id", "title", "done", "due_at", "url"],
                        "properties": {
                            "id": _task_id_schema(),
                            "title": {"type": "string"},
                            "done": {"type": "boolean"},
                            "due_at": {"type": ["string", "null"]},
                            "url": {"type": ["string", "null"]},
                        },
                    },
                ]
            },
            "link_state": {
                "type": "string",
                "enum": [LINK_VERIFIED, LINK_UNAVAILABLE, LINK_NOT_APPLICABLE],
            },
            "reason": {
                "type": "string",
                "enum": [
                    REASON_AUTHORIZATION,
                    REASON_PROJECT_SCOPE,
                    REASON_MISSING_COMMAND_CONTEXT,
                    REASON_TRANSPORT,
                    REASON_STORAGE,
                    REASON_MARKER_CONFLICT,
                    REASON_IMMUTABLE_CONFLICT,
                    REASON_UNRESOLVED_TARGET,
                    REASON_INSUFFICIENT_EVIDENCE,
                    REASON_INVALID_ARGUMENT,
                    REASON_NOT_FOUND,
                ],
            },
        },
    }


def _tool_definitions(*, write_enabled: bool) -> list[dict[str, Any]]:
    out_schema = _output_schema()
    due = _due_schema()
    tools: list[dict[str, Any]] = []
    if write_enabled:
        tools.extend(
            [
                {
                    "name": "ea010_create",
                    "description": "Create a personal Vikunja task via the single integration writer.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["title"],
                        "properties": {
                            "title": {"type": "string", "minLength": 1, "maxLength": TITLE_MAX_CODEPOINTS},
                            "description": {
                                "type": "string",
                                "maxLength": DESCRIPTION_MAX_CODEPOINTS,
                            },
                            "due": due,
                        },
                    },
                    "outputSchema": out_schema,
                    "annotations": {"readOnlyHint": False},
                },
                {
                    "name": "ea010_update",
                    "description": "Update title/description/due of one task; not done via patch.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["task_id", "patch"],
                        "properties": {
                            "task_id": _task_id_schema(),
                            "patch": {
                                "type": "object",
                                "additionalProperties": False,
                                "minProperties": 1,
                                "properties": {
                                    "title": {
                                        "type": "string",
                                        "minLength": 1,
                                        "maxLength": TITLE_MAX_CODEPOINTS,
                                    },
                                    "description": {
                                        "type": "string",
                                        "maxLength": DESCRIPTION_MAX_CODEPOINTS,
                                    },
                                    "due": {"oneOf": [due, {"type": "null"}]},
                                },
                            },
                        },
                    },
                    "outputSchema": out_schema,
                    "annotations": {"readOnlyHint": False},
                },
                {
                    "name": "ea010_complete",
                    "description": "Complete one task occurrence via the single writer.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["task_id"],
                        "properties": {"task_id": _task_id_schema()},
                    },
                    "outputSchema": out_schema,
                    "annotations": {"readOnlyHint": False},
                },
            ]
        )
    tools.extend(
        [
                {
                "name": "ea010_find",
                "description": "Find a task by Vikunja id or prior operation_ref.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    # Draft 2020-12: additionalProperties does not inherit from oneOf
                    # branches, so both properties must be declared at the root.
                    "properties": {
                        "task_id": _task_id_schema(),
                        "operation_ref": {"type": "string"},
                    },
                    "oneOf": [
                        {"required": ["task_id"]},
                        {"required": ["operation_ref"]},
                    ],
                },
                "outputSchema": out_schema,
                "annotations": {"readOnlyHint": True},
            },
            {
                "name": "ea010_reconcile",
                "description": "Reconcile an unknown prior operation without blind retry.",
                "inputSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["operation_ref"],
                    "properties": {"operation_ref": {"type": "string"}},
                },
                "outputSchema": out_schema,
                "annotations": {"readOnlyHint": True},
            },
        ]
    )
    return tools


def _jsonrpc_error(req_id: Any, code: int, message: str, *, data: Any = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def _jsonrpc_result(req_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _tool_result(payload: dict[str, Any], *, is_error: bool) -> dict[str, Any]:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": payload,
        "isError": is_error,
    }


def _task_due_at(task: Mapping[str, Any]) -> str | None:
    raw = task.get("due_date")
    if is_absent_due(raw):
        return None
    if not isinstance(raw, str):
        return None
    try:
        return format_rfc3339_instant(require_rfc3339_instant(raw))
    except ValueError:
        return raw


def _build_link(config: McpConfig, task_id: int) -> tuple[str | None, str]:
    if not _is_safe_task_id(task_id):
        return None, LINK_UNAVAILABLE
    if config.browser_origin:
        return f"{config.browser_origin}/tasks/{task_id}", LINK_VERIFIED
    return None, LINK_UNAVAILABLE


def _task_payload(config: McpConfig, task: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if task is None:
        return None
    task_id = task.get("id")
    if not _is_safe_task_id(task_id):
        return None
    title = task.get("title")
    done = task.get("done")
    if not isinstance(title, str) or not isinstance(done, bool):
        return None
    url, _ = _build_link(config, task_id)
    return {
        "id": task_id,
        "title": title,
        "done": done,
        "due_at": _task_due_at(task),
        "url": url,
    }


_AMBIGUITY_REASONS = frozenset(
    {
        "multiple_exact_matches",
        "multiple_exact_marker_matches",
    }
)


def _history_proves_possible_send(history: list[Mapping[str, Any]] | None) -> bool:
    """Durable history proves HTTP may already have left the adapter."""
    if not history:
        return False
    for event in history:
        event_type = event.get("event_type")
        detail = event.get("detail") if isinstance(event.get("detail"), Mapping) else {}
        if event_type in {"dispatch_started", "receipt"}:
            return True
        if event_type == "unknown" and detail.get("mutation_sent") is True:
            return True
    return False


def _record_proves_possible_send(record: OperationRecord | None) -> bool:
    """Durable operation row proves a dispatch claim or confirmed receipt."""
    if record is None:
        return False
    if record.receipt is not None:
        return True
    if record.delivery_state in {
        DeliveryState.DISPATCH_STARTED,
        DeliveryState.RECEIPT,
        DeliveryState.UNKNOWN,
    }:
        return True
    if record.state in {
        OperationState.DISPATCH_STARTED,
        OperationState.CONFIRMED,
    }:
        return True
    return False


def _send_evidence(
    result: DispatchResult,
    *,
    history: list[Mapping[str, Any]] | None = None,
) -> bool:
    """True when a mutation may already have left the adapter (C3 unknown).

    Prefer durable OperationRecord / history over process-local create counters.
    Transient details remain a supplemental signal for the same process.
    """
    details = result.details or {}
    if details.get("mutation_sent") is True:
        return True
    if _record_proves_possible_send(result.operation):
        return True
    if _history_proves_possible_send(history):
        return True
    # Supplemental same-process create evidence (not sufficient alone for recovery).
    if result.create_http_attempts > 0:
        return True
    if details.get("create_http") is not None:
        return True
    return False


def _missing_task_read(read: TaskResult) -> bool:
    """True when a stock GET maps a gone/missing task (404/410 → validation_error)."""
    if read.outcome is Outcome.EMPTY:
        return True
    if read.http_status in {404, 410}:
        return True
    details = read.details or {}
    if details.get("http_status") in {404, 410}:
        return True
    return False


def _outcome_to_status(
    outcome: Outcome,
    *,
    details: Mapping[str, Any] | None = None,
    send_evidence: bool = False,
) -> str:
    details = details or {}
    detail_reason = str(details.get("reason") or "")
    if outcome is Outcome.CONFIRMED:
        return STATUS_CONFIRMED
    if outcome is Outcome.EMPTY:
        # Pre-send empty/miss is not_found; post-send missing target is unknown.
        if send_evidence:
            return STATUS_UNKNOWN
        return STATUS_NOT_FOUND
    if outcome is Outcome.CONFLICT:
        if detail_reason in _AMBIGUITY_REASONS:
            return STATUS_AMBIGUITY
        return STATUS_CONFLICT
    if outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED}:
        return STATUS_DENIED
    if outcome is Outcome.VALIDATION_ERROR:
        # C3: validation_error → invalid_input. s04 definitive_refusal (HTTP
        # 400/405/415/422 and typed auth/scope/validation refusals) stays
        # invalid_input even when the refused request left the adapter.
        if details.get("definitive_refusal") is True:
            return STATUS_INVALID_INPUT
        # Stock GET 404/410 becomes validation_error. After a possible send
        # (reconcile/recover of deleted/missing targets), that is unresolved
        # outcome — not invalid_argument.
        if send_evidence:
            return STATUS_UNKNOWN
        return STATUS_INVALID_INPUT
    if outcome is Outcome.INCOMPLETE_SCAN:
        return STATUS_INCOMPLETE_SCAN
    if outcome is Outcome.DEPENDENCY_UNAVAILABLE:
        # C3: unavailable before send; unknown after possible send.
        if send_evidence:
            return STATUS_UNKNOWN
        return STATUS_UNAVAILABLE
    return STATUS_UNKNOWN


def _reason_for(
    status: str,
    outcome: Outcome | None,
    details: Mapping[str, Any] | None,
) -> str | None:
    details = details or {}
    detail_reason = str(details.get("reason") or "")
    if status == STATUS_INVALID_INPUT:
        return REASON_INVALID_ARGUMENT
    if status == STATUS_NOT_FOUND:
        return REASON_NOT_FOUND
    if status == STATUS_DENIED:
        if outcome is Outcome.SCOPE_DENIED or "scope" in detail_reason or "project" in detail_reason:
            return REASON_PROJECT_SCOPE
        return REASON_AUTHORIZATION
    if status == STATUS_UNAVAILABLE:
        if "storage" in detail_reason or details.get("storage"):
            return REASON_STORAGE
        return REASON_TRANSPORT
    if status == STATUS_CONFLICT:
        if "marker" in detail_reason and "multiple" not in detail_reason:
            return REASON_MARKER_CONFLICT
        if "unresolved_target" in detail_reason:
            return REASON_UNRESOLVED_TARGET
        return REASON_IMMUTABLE_CONFLICT
    if status in {STATUS_UNKNOWN, STATUS_INCOMPLETE_SCAN, STATUS_AMBIGUITY}:
        return REASON_INSUFFICIENT_EVIDENCE
    return None


def _result_from_parts(
    config: McpConfig,
    *,
    status: str,
    operation_ref: str | None,
    task: Mapping[str, Any] | None,
    reason: str | None = None,
    link_state_override: str | None = None,
) -> dict[str, Any]:
    # Dependency may yield a numeric id outside the C3 wire range. Never advertise
    # it as confirmed; keep durable operation_ref and report an unconfirmed outcome.
    unrepresentable_id = False
    if task is not None:
        raw_id = task.get("id")
        if isinstance(raw_id, int) and not isinstance(raw_id, bool) and not _is_safe_task_id(raw_id):
            unrepresentable_id = True
    if unrepresentable_id and status == STATUS_CONFIRMED:
        status = STATUS_UNKNOWN
        if reason is None:
            reason = REASON_INSUFFICIENT_EVIDENCE
        task = None
    task_obj = _task_payload(config, task)
    if task_obj is None:
        link_state = link_state_override or LINK_NOT_APPLICABLE
        url = None
    else:
        url, link_state = _build_link(config, task_obj["id"])
        task_obj = dict(task_obj)
        task_obj["url"] = url
        if link_state_override is not None:
            link_state = link_state_override
    payload: dict[str, Any] = {
        "status": status,
        "operation_ref": operation_ref,
        "task": task_obj,
        "link_state": link_state,
    }
    if reason is not None:
        payload["reason"] = reason
    return payload


def map_dispatch_to_wire(
    config: McpConfig,
    result: DispatchResult,
    *,
    history: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Table-driven C3 wire mapping from a dispatcher/reconcile result."""
    details = result.details or {}
    send_evidence = _send_evidence(result, history=history)
    status = _outcome_to_status(result.outcome, details=details, send_evidence=send_evidence)
    op_ref = None if result.operation is None else result.operation.operation_id
    if op_ref is None:
        detail_ref = details.get("operation_ref")
        if isinstance(detail_ref, str) and UUID_V4_RE.fullmatch(detail_ref):
            op_ref = detail_ref
    # Pre-intent failures must not invent an operation_ref (any wire status).
    if result.operation is None and not send_evidence:
        op_ref = None
    reason = _reason_for(status, result.outcome, details)
    return _result_from_parts(
        config,
        status=status,
        operation_ref=op_ref,
        task=result.task,
        reason=reason,
    )


def _from_dispatch(
    config: McpConfig,
    result: DispatchResult,
    *,
    history: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    return map_dispatch_to_wire(config, result, history=history)


def _task_result_from_api_error(exc: ApiError) -> TaskResult:
    """Normalize expected VikunjaClient read/transport faults into TaskResult."""
    return TaskResult(
        outcome=exc.outcome,
        details={"reason": str(exc), "api_error": True, **dict(exc.details)},
    )


def _safe_client_read(client: VikunjaClient, task_id: int) -> TaskResult:
    """Read boundary: ApiError is a typed dependency outcome, not an envelope crash."""
    try:
        return client.read(task_id)
    except ApiError as exc:
        return _task_result_from_api_error(exc)


def _result_from_api_error(
    config: McpConfig,
    exc: ApiError,
    *,
    operation_ref: str | None = None,
) -> dict[str, Any]:
    status = _outcome_to_status(exc.outcome, details=exc.details, send_evidence=False)
    reason = _reason_for(status, exc.outcome, exc.details)
    return _result_from_parts(
        config,
        status=status,
        operation_ref=operation_ref,
        task=None,
        reason=reason,
    )


def _result_from_journal_error(
    config: McpConfig,
    exc: JournalError,
    *,
    operation_ref: str | None = None,
    send_evidence: bool = False,
) -> dict[str, Any]:
    """Normalize expected journal/storage faults into C3 tool results (not -32603)."""
    details: dict[str, Any] = {"reason": str(exc), **dict(exc.details)}
    if exc.storage_kind:
        details["storage"] = True
        details["storage_kind"] = exc.storage_kind
    elif "storage" not in details and (
        "storage" in str(exc).lower() or "journal" in str(exc).lower()
    ):
        details["storage"] = True
    status = _outcome_to_status(exc.outcome, details=details, send_evidence=send_evidence)
    reason = _reason_for(status, exc.outcome, details)
    return _result_from_parts(
        config,
        status=status,
        operation_ref=operation_ref,
        task=None,
        reason=reason,
    )


def _optional_operation_ref(arguments: Mapping[str, Any]) -> str | None:
    raw = arguments.get("operation_ref")
    if isinstance(raw, str) and UUID_V4_RE.fullmatch(raw):
        return raw
    return None


class McpServer:
    """Line-delimited JSON-RPC MCP server bound to one dispatcher/journal."""

    def __init__(self, config: McpConfig, *, dispatcher: IntegrationDispatcher | None = None):
        self.config = config
        self.journal = IntegrationJournal(config.journal_path)
        self.journal.open()
        self.issuer_id = self.journal.ensure_issuer(config.issuer_id)
        self.client = VikunjaClient(
            api_origin=config.api_origin,
            project_id=config.project_id,
            token_ref=config.token_ref,
            page_size=config.page_size,
            max_items_per_page=config.max_items_per_page,
        )
        crash_hook = _optional_test_crash_hook()
        self.dispatcher = dispatcher or IntegrationDispatcher(
            self.journal,
            self.client,
            crash_hook=crash_hook,
        )
        self._initialize_succeeded = False
        self._initialized = False
        self._closed = False
        # Adapter-issued operation id resolved for the in-flight write tool call.
        # Retained across JournalError so post-send storage faults keep identity.
        self._active_operation_ref: str | None = None
        self._tools = _tool_definitions(write_enabled=config.lists_write_tools)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.journal.close()
        except Exception:
            pass

    def handle_message(self, message: Mapping[str, Any]) -> dict[str, Any] | None:
        # Envelope strings (jsonrpc/method/id) must be valid Unicode before dispatch.
        for key in ("jsonrpc", "method"):
            raw = message.get(key)
            if isinstance(raw, str) and not _is_wire_text(raw):
                return _jsonrpc_error(None, JSONRPC_INVALID_REQUEST, "invalid Unicode in envelope")
        if message.get("jsonrpc") != "2.0":
            raw_id = message.get("id") if "id" in message else None
            err_id = raw_id if _is_jsonrpc_id(raw_id) else None
            return _jsonrpc_error(err_id, JSONRPC_INVALID_REQUEST, "jsonrpc must be 2.0")
        method = message.get("method")
        is_notification = "id" not in message
        if is_notification:
            req_id = None
        else:
            req_id = message.get("id")
            if not _is_jsonrpc_id(req_id):
                # Invalid envelope: reject before any tool/dispatcher admission.
                return _jsonrpc_error(None, JSONRPC_INVALID_REQUEST, "id must be string, number, or null")
        if not isinstance(method, str) or not method:
            if is_notification:
                return None
            return _jsonrpc_error(req_id, JSONRPC_INVALID_REQUEST, "method required")

        if method == "notifications/initialized":
            if not is_notification:
                return _jsonrpc_error(req_id, JSONRPC_INVALID_REQUEST, "initialized is a notification")
            # Premature initialized (no successful initialize) must not enable tools.
            if not self._initialize_succeeded:
                return None
            if self._initialized:
                # Repeated initialized after ready: ignore without re-entry side effects.
                return None
            self._initialized = True
            return None

        if is_notification:
            # Unknown notifications are ignored per MCP; do not write to stdout.
            return None

        if method == "initialize":
            return self._handle_initialize(req_id, message.get("params"))
        if method == "ping":
            # ping allowed before initialized per C3.
            return _jsonrpc_result(req_id, {})
        if method == "tools/list":
            if not self._initialized:
                return _jsonrpc_error(req_id, JSONRPC_INVALID_PARAMS, "call tools/list after initialized")
            return self._handle_tools_list(req_id, message.get("params"))
        if method == "tools/call":
            if not self._initialized:
                return _jsonrpc_error(req_id, JSONRPC_INVALID_PARAMS, "call tools/call after initialized")
            return self._handle_tools_call(req_id, message.get("params"))
        return _jsonrpc_error(req_id, JSONRPC_METHOD_NOT_FOUND, f"unknown method: {method}")

    def _handle_initialize(self, req_id: Any, params: Any) -> dict[str, Any]:
        try:
            if self._initialized:
                raise McpError("initialize after initialized is forbidden")
            if params is None:
                raise McpError("initialize params required")
            body = _require_object(params, label="params")
            if "protocolVersion" not in body:
                raise McpError("protocolVersion required")
            if not isinstance(body["protocolVersion"], str):
                raise McpError("protocolVersion must be string")
            if "capabilities" not in body:
                raise McpError("capabilities required")
            if not isinstance(body["capabilities"], dict):
                raise McpError("capabilities must be object")
            if "clientInfo" not in body:
                raise McpError("clientInfo required")
            info = _require_object(body["clientInfo"], label="clientInfo")
            if not isinstance(info.get("name"), str) or not isinstance(info.get("version"), str):
                raise McpError("clientInfo.name/version must be strings")
        except McpError as exc:
            return _jsonrpc_error(req_id, JSONRPC_INVALID_PARAMS, str(exc))
        self._initialize_succeeded = True
        # Always advertise the supported version; client decides whether to continue.
        return _jsonrpc_result(
            req_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )

    def _handle_tools_list(self, req_id: Any, params: Any) -> dict[str, Any]:
        body = {} if params is None else params
        try:
            body = _require_object(body, label="params")
            if body.get("cursor") not in (None, ""):
                raise McpError("non-empty cursor is invalid")
            unknown = set(body) - {"cursor"}
            if unknown:
                raise McpError(f"unknown tools/list fields: {sorted(unknown)}")
        except McpError as exc:
            return _jsonrpc_error(req_id, JSONRPC_INVALID_PARAMS, str(exc))
        return _jsonrpc_result(req_id, {"tools": list(self._tools)})

    def _handle_tools_call(self, req_id: Any, params: Any) -> dict[str, Any]:
        try:
            body = _require_object(params, label="params")
            unknown = set(body) - {"name", "arguments", "_meta"}
            if unknown:
                raise McpError(f"unknown tools/call fields: {sorted(unknown)}")
            name = body.get("name")
            if not isinstance(name, str) or name not in ALL_TOOL_NAMES:
                raise McpError("unknown tool")
            if name in WRITE_TOOL_NAMES and name not in {t["name"] for t in self._tools}:
                raise McpError("write tool unavailable in this profile")
            arguments = body.get("arguments", {})
            arguments = _require_object(arguments, label="arguments")
            meta = body.get("_meta")
            if meta is not None:
                meta = _require_object(meta, label="_meta")
        except McpError as exc:
            return _jsonrpc_error(req_id, JSONRPC_INVALID_PARAMS, str(exc))

        self._active_operation_ref = None
        try:
            payload = self._dispatch_tool(name, arguments, meta)
        except McpError as exc:
            reason = exc.reason or REASON_INVALID_ARGUMENT
            status = (
                STATUS_DENIED
                if reason == REASON_MISSING_COMMAND_CONTEXT
                else STATUS_INVALID_INPUT
            )
            payload = _result_from_parts(
                self.config,
                status=status,
                operation_ref=None,
                task=None,
                reason=reason,
            )
        except ApiError as exc:
            # Expected dependency/auth faults are C3 tool results, not -32603.
            payload = _result_from_api_error(
                self.config,
                exc,
                operation_ref=self._active_operation_ref
                or _optional_operation_ref(arguments),
            )
        except JournalError as exc:
            # Expected journal/storage faults stay structured tool outcomes.
            # create/update/complete never carry operation_ref in arguments; keep
            # the adapter-issued id once resolved. After resolve, treat storage
            # faults as possible post-send (C3 unknown), not pre-send unavailable.
            known_ref = self._active_operation_ref or _optional_operation_ref(arguments)
            send_evidence = known_ref is not None and (
                name in WRITE_TOOL_NAMES or self._active_operation_ref is not None
            )
            payload = _result_from_journal_error(
                self.config,
                exc,
                operation_ref=known_ref,
                send_evidence=send_evidence,
            )
        except Exception as exc:  # noqa: BLE001 - map unexpected failures to protocol error
            return _jsonrpc_error(
                req_id,
                JSONRPC_INTERNAL,
                "unexpected adapter failure",
                data={"class": type(exc).__name__},
            )
        finally:
            self._active_operation_ref = None

        is_error = payload.get("status") not in {STATUS_CONFIRMED, STATUS_NOT_FOUND}
        return _jsonrpc_result(req_id, _tool_result(payload, is_error=is_error))

    def _extract_command_context(self, meta: Mapping[str, Any] | None) -> tuple[str, datetime]:
        if not self.config.accepts_write_commands:
            raise McpError(
                "write commands require trusted host binding",
                reason=REASON_MISSING_COMMAND_CONTEXT,
            )
        if meta is None or COMMAND_META_KEY not in meta:
            raise McpError("missing ea010/command context", reason=REASON_MISSING_COMMAND_CONTEXT)
        command = _require_object(meta[COMMAND_META_KEY], label="ea010/command")
        unknown = set(command) - {"command_ref", "confirmed_at"}
        if unknown:
            raise McpError(f"ea010/command unknown fields: {sorted(unknown)}")
        if "command_ref" not in command or "confirmed_at" not in command:
            raise McpError("ea010/command requires command_ref and confirmed_at", reason=REASON_MISSING_COMMAND_CONTEXT)
        command_ref = _require_uuid_v4(command["command_ref"], label="command_ref")
        confirmed_at = _parse_confirmed_at(command["confirmed_at"])
        return command_ref, confirmed_at

    def _resolve_operation_id(self, command_ref: str) -> str:
        existing = self.journal.lookup_source(SOURCE_KIND, command_ref)
        if existing is not None:
            return existing.operation_id
        return new_operation_id()

    def _dispatch_tool(
        self,
        name: str,
        arguments: Mapping[str, Any],
        meta: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if name == "ea010_create":
            return self._tool_create(arguments, meta)
        if name == "ea010_update":
            return self._tool_update(arguments, meta)
        if name == "ea010_complete":
            return self._tool_complete(arguments, meta)
        if name == "ea010_find":
            return self._tool_find(arguments)
        if name == "ea010_reconcile":
            return self._tool_reconcile(arguments)
        raise McpError("unknown tool")

    def _tool_create(self, arguments: Mapping[str, Any], meta: Mapping[str, Any] | None) -> dict[str, Any]:
        command_ref, confirmed_at = self._extract_command_context(meta)
        unknown = set(arguments) - {"title", "description", "due"}
        if unknown:
            raise McpError(f"create unknown fields: {sorted(unknown)}")
        title = _validate_title(arguments.get("title"))
        description = ""
        if "description" in arguments:
            description = _validate_description(arguments.get("description"))
        due = None
        if "due" in arguments:
            due = _parse_due(arguments.get("due"), confirmed_at=confirmed_at)

        # Resolve durable operation_id from source mapping before HTTP.
        # Concurrent first callers that mint distinct IDs: loser re-resolves once.
        result = self._execute_with_source_retry(
            command_ref,
            lambda operation_id: self.dispatcher.execute_create(
                issuer_id=self.issuer_id,
                operation_id=operation_id,
                title=title,
                user_portion=description,
                due=due,
                source_kind=SOURCE_KIND,
                source_key=command_ref,
            ),
        )
        # Prefer verified read-back body when confirmed.
        if result.ok and result.vikunja_id is not None and result.task is None:
            read = self.client.read(result.vikunja_id)
            if read.ok and read.task is not None:
                result = DispatchResult(
                    outcome=result.outcome,
                    operation=result.operation,
                    task=read.task,
                    vikunja_id=result.vikunja_id,
                    create_http_attempts=result.create_http_attempts,
                    linked=result.linked,
                    link_result=result.link_result,
                    details=dict(result.details),
                )
        return self._finalize_dispatch(result)

    def _tool_update(self, arguments: Mapping[str, Any], meta: Mapping[str, Any] | None) -> dict[str, Any]:
        command_ref, confirmed_at = self._extract_command_context(meta)
        unknown = set(arguments) - {"task_id", "patch"}
        if unknown:
            raise McpError(f"update unknown fields: {sorted(unknown)}")
        task_id = _require_safe_int(arguments.get("task_id"), label="task_id")
        patch_in = _require_object(arguments.get("patch"), label="patch")
        unknown_patch = set(patch_in) - {"title", "description", "due"}
        if unknown_patch:
            raise McpError(f"patch unknown fields: {sorted(unknown_patch)}")
        if not patch_in:
            raise McpError("patch must include at least one field")
        patch: dict[str, Any] = {}
        if "title" in patch_in:
            patch["title"] = _validate_title(patch_in["title"])
        if "description" in patch_in:
            patch["description"] = _validate_description(patch_in["description"])
        if "due" in patch_in:
            if patch_in["due"] is None:
                patch["due_date"] = None
            else:
                due = _parse_due(patch_in["due"], confirmed_at=confirmed_at)
                wire = due.wire_value()
                if wire is None:
                    raise McpError("due resolved to empty wire value")
                patch["due_date"] = wire
        result = self._execute_with_source_retry(
            command_ref,
            lambda operation_id: self.dispatcher.execute_update(
                issuer_id=self.issuer_id,
                operation_id=operation_id,
                task_id=task_id,
                patch=patch,
                source_kind=SOURCE_KIND,
                source_key=command_ref,
            ),
        )
        return self._finalize_dispatch(result)

    def _tool_complete(self, arguments: Mapping[str, Any], meta: Mapping[str, Any] | None) -> dict[str, Any]:
        command_ref, _confirmed_at = self._extract_command_context(meta)
        unknown = set(arguments) - {"task_id"}
        if unknown:
            raise McpError(f"complete unknown fields: {sorted(unknown)}")
        task_id = _require_safe_int(arguments.get("task_id"), label="task_id")
        result = self._execute_with_source_retry(
            command_ref,
            lambda operation_id: self.dispatcher.execute_complete(
                issuer_id=self.issuer_id,
                operation_id=operation_id,
                task_id=task_id,
                source_kind=SOURCE_KIND,
                source_key=command_ref,
            ),
        )
        return self._finalize_dispatch(result)

    def _tool_find(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        unknown = set(arguments) - {"task_id", "operation_ref"}
        if unknown:
            raise McpError(f"find unknown fields: {sorted(unknown)}")
        has_id = "task_id" in arguments
        has_ref = "operation_ref" in arguments
        if has_id == has_ref:
            raise McpError("find requires exactly one of task_id or operation_ref")
        if has_ref:
            op_ref = _require_uuid_v4(arguments.get("operation_ref"), label="operation_ref")
            record = self.journal.get_operation(self.issuer_id, op_ref)
            if record is None:
                return _result_from_parts(
                    self.config,
                    status=STATUS_NOT_FOUND,
                    operation_ref=op_ref,
                    task=None,
                    reason=REASON_NOT_FOUND,
                )
            return self._find_from_record(record)

        task_id = _require_safe_int(arguments.get("task_id"), label="task_id")
        read = _safe_client_read(self.client, task_id)
        if _missing_task_read(read):
            # Definite GET miss (stock 404→validation_error) is not_found, not invalid_input.
            return _result_from_parts(
                self.config,
                status=STATUS_NOT_FOUND,
                operation_ref=None,
                task=None,
                reason=REASON_NOT_FOUND,
            )
        status = _outcome_to_status(read.outcome, details=read.details, send_evidence=False)
        reason = _reason_for(status, read.outcome, read.details)
        return _result_from_parts(
            self.config,
            status=status,
            operation_ref=None,
            task=read.task if read.ok else None,
            reason=reason,
        )

    def _find_from_record(self, record: OperationRecord) -> dict[str, Any]:
        if record.receipt is not None and record.receipt.get("vikunja_id") is not None:
            task_id = int(record.receipt["vikunja_id"])
            if not _is_safe_task_id(task_id):
                # Receipt exists but ID cannot be represented on the C3 wire.
                return _result_from_parts(
                    self.config,
                    status=STATUS_UNKNOWN,
                    operation_ref=record.operation_id,
                    task=None,
                    reason=REASON_INSUFFICIENT_EVIDENCE,
                )
            read = _safe_client_read(self.client, task_id)
            if read.ok and read.task is not None:
                return _result_from_parts(
                    self.config,
                    status=STATUS_CONFIRMED,
                    operation_ref=record.operation_id,
                    task=read.task,
                )
            # Previously confirmed linked task is gone/unreadable: keep receipt,
            # never blame the valid operation_ref, and never recreate.
            if _missing_task_read(read) or read.outcome in {
                Outcome.VALIDATION_ERROR,
                Outcome.UNKNOWN,
            }:
                return _result_from_parts(
                    self.config,
                    status=STATUS_UNKNOWN,
                    operation_ref=record.operation_id,
                    task=None,
                    reason=REASON_INSUFFICIENT_EVIDENCE,
                )
            status = _outcome_to_status(read.outcome, details=read.details, send_evidence=False)
            return _result_from_parts(
                self.config,
                status=status if status != STATUS_NOT_FOUND else STATUS_UNKNOWN,
                operation_ref=record.operation_id,
                task=None,
                reason=_reason_for(status, read.outcome, read.details)
                or REASON_INSUFFICIENT_EVIDENCE,
            )
        # Intent exists but outcome not confirmed: ask explicit reconcile.
        return _result_from_parts(
            self.config,
            status=STATUS_UNKNOWN,
            operation_ref=record.operation_id,
            task=None,
            reason=REASON_INSUFFICIENT_EVIDENCE,
        )

    def _tool_reconcile(self, arguments: Mapping[str, Any]) -> dict[str, Any]:
        unknown = set(arguments) - {"operation_ref"}
        if unknown:
            raise McpError(f"reconcile unknown fields: {sorted(unknown)}")
        op_ref = _require_uuid_v4(arguments.get("operation_ref"), label="operation_ref")
        record = self.journal.get_operation(self.issuer_id, op_ref)
        if record is None:
            return _result_from_parts(
                self.config,
                status=STATUS_NOT_FOUND,
                operation_ref=op_ref,
                task=None,
                reason=REASON_NOT_FOUND,
            )
        if record.op_type == "create":
            result = self.dispatcher.recover_create(self.issuer_id, op_ref)
        else:
            result = self.dispatcher.recover_unknown_mutation(self.issuer_id, op_ref)
        # Reconcile may confirm local journal evidence; still not a new PUT/PATCH writer path.
        return self._finalize_dispatch(result)

    def _dispatch_result_from_journal_error(
        self,
        exc: JournalError,
        *,
        operation_id: str,
    ) -> DispatchResult:
        """Map escaping journal faults after a resolved operation id was issued."""
        record = None
        try:
            record = self.journal.get_operation(self.issuer_id, operation_id)
        except JournalError:
            record = None
        details: dict[str, Any] = {
            "phase": "dispatch",
            "reason": str(exc),
            "operation_ref": operation_id,
            **dict(exc.details),
        }
        if exc.storage_kind:
            details["storage"] = True
            details["storage_kind"] = exc.storage_kind
        elif "storage" not in details and (
            "storage" in str(exc).lower() or "journal" in str(exc).lower()
        ):
            details["storage"] = True
        # Conservative: unresolved durable row or proven dispatch ⇒ possible send.
        if record is None or _record_proves_possible_send(record):
            details["mutation_sent"] = True
        return DispatchResult(
            outcome=exc.outcome,
            operation=record,
            details=details,
        )

    def _invoke_with_journal_guard(self, operation_id: str, invoke) -> DispatchResult:
        self._active_operation_ref = operation_id
        try:
            return invoke(operation_id)
        except JournalError as exc:
            # Dispatcher may re-raise when recovery get_operation also fails after
            # a possible HTTP write. Convert at the adapter boundary with the
            # resolved id and conservative post-resolve send evidence.
            return self._dispatch_result_from_journal_error(exc, operation_id=operation_id)

    def _execute_with_source_retry(
        self,
        command_ref: str,
        invoke,
    ) -> DispatchResult:
        operation_id = self._resolve_operation_id(command_ref)
        # Publish resolved adapter-issued id before invoke so escaping JournalError
        # (e.g. receipt fault then get_operation fault) retains wire identity.
        result = self._invoke_with_journal_guard(operation_id, invoke)
        if (
            result.outcome is Outcome.CONFLICT
            and (result.details or {}).get("reason") == "source mapping already bound to another operation key"
        ) or (
            result.outcome is Outcome.CONFLICT
            and "source mapping already bound" in str((result.details or {}).get("reason") or "")
        ):
            rebound = self.journal.lookup_source(SOURCE_KIND, command_ref)
            if rebound is not None and rebound.operation_id != operation_id:
                return self._invoke_with_journal_guard(rebound.operation_id, invoke)
        # JournalError reason text may be in details.reason from prepare_intent.
        reason = str((result.details or {}).get("reason") or "")
        if result.outcome is Outcome.CONFLICT and "source mapping already bound" in reason:
            rebound = self.journal.lookup_source(SOURCE_KIND, command_ref)
            if rebound is not None and rebound.operation_id != operation_id:
                return self._invoke_with_journal_guard(rebound.operation_id, invoke)
        return result

    def _finalize_dispatch(self, result: DispatchResult) -> dict[str, Any]:
        history: list[Mapping[str, Any]] | None = None
        if result.operation is not None:
            try:
                history = self.journal.history(
                    result.operation.issuer_id,
                    result.operation.operation_id,
                )
            except JournalError:
                history = None
        payload = _from_dispatch(self.config, result, history=history)
        # Preserve adapter-issued id only when durable intent/mapping exists or
        # post-send/storage uncertainty requires wire identity. Proven pre-intent
        # failures (no OperationRecord, no send evidence) stay null regardless of
        # wire status — never invent a UUID from _active_operation_ref after
        # resolve-before-prepare (auth/scope/missing-target refusals included).
        if payload.get("operation_ref") is None:
            send_evidence = _send_evidence(result, history=history)
            proven_pre_intent = result.operation is None and not send_evidence
            if not proven_pre_intent:
                detail_ref = (result.details or {}).get("operation_ref")
                if isinstance(detail_ref, str) and UUID_V4_RE.fullmatch(detail_ref):
                    payload = dict(payload)
                    payload["operation_ref"] = detail_ref
                elif self._active_operation_ref is not None:
                    payload = dict(payload)
                    payload["operation_ref"] = self._active_operation_ref
        # Ensure confirmed results expose read-back when dispatcher returned a task.
        if payload["status"] == STATUS_CONFIRMED and payload["task"] is None and result.vikunja_id:
            if not _is_safe_task_id(result.vikunja_id):
                return _result_from_parts(
                    self.config,
                    status=STATUS_UNKNOWN,
                    operation_ref=payload["operation_ref"],
                    task=None,
                    reason=REASON_INSUFFICIENT_EVIDENCE,
                )
            read = _safe_client_read(self.client, result.vikunja_id)
            if read.ok and read.task is not None:
                return _result_from_parts(
                    self.config,
                    status=STATUS_CONFIRMED,
                    operation_ref=payload["operation_ref"],
                    task=read.task,
                )
        return payload

    def serve(
        self,
        stdin: TextIO | BinaryIO | None = None,
        stdout: TextIO | BinaryIO | None = None,
        stderr: TextIO | BinaryIO | None = None,
    ) -> int:
        in_stream = stdin if stdin is not None else sys.stdin.buffer
        out_stream = stdout if stdout is not None else sys.stdout.buffer
        err_stream = stderr if stderr is not None else sys.stderr.buffer

        def write_err(text: str) -> None:
            data = (text + "\n").encode("utf-8")
            if hasattr(err_stream, "write"):
                if isinstance(getattr(err_stream, "encoding", None), str):
                    err_stream.write(text + "\n")  # type: ignore[arg-type]
                else:
                    err_stream.write(data)  # type: ignore[arg-type]
                err_stream.flush()

        def write_out(obj: Mapping[str, Any]) -> None:
            line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
            data = line.encode("utf-8")
            if isinstance(getattr(out_stream, "encoding", None), str):
                out_stream.write(line)  # type: ignore[arg-type]
            else:
                out_stream.write(data)  # type: ignore[arg-type]
            out_stream.flush()

        def write_out_safe(obj: Mapping[str, Any]) -> None:
            """Frame a response; on encode failure emit a null-id error and keep serving.

            Durable journal/dispatcher state is retained so the host can reconnect
            and reconcile any mutation that already committed before the transport fault.
            """
            try:
                write_out(obj)
            except UnicodeEncodeError:
                write_err("response UTF-8 encode failure; durable state retained for recovery")
                try:
                    write_out(_jsonrpc_error(None, JSONRPC_INTERNAL, "response encoding failure"))
                except Exception:  # noqa: BLE001
                    write_err("fallback response encode also failed")

        while True:
            try:
                if hasattr(in_stream, "readline"):
                    raw = in_stream.readline(MAX_LINE_BYTES + 1)
                else:
                    raw = b""
            except Exception as exc:  # noqa: BLE001
                write_err(f"stdin read failure: {type(exc).__name__}")
                return 1
            if raw == b"" or raw == "":
                return 0
            if isinstance(raw, str):
                raw_bytes = raw.encode("utf-8")
            else:
                raw_bytes = raw
            if len(raw_bytes) > MAX_LINE_BYTES or (
                len(raw_bytes) > MAX_LINE_BYTES - 1 and not raw_bytes.endswith(b"\n")
            ):
                # Oversize line: close session without mutation.
                write_err("line exceeds 256 KiB; closing session")
                return 2
            if not raw_bytes.endswith(b"\n"):
                # EOF mid-line: treat as end without processing partial.
                write_err("EOF before complete line")
                return 0
            try:
                text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                write_out_safe(_jsonrpc_error(None, JSONRPC_PARSE, "invalid UTF-8"))
                continue
            text = text[:-1]
            if text.endswith("\r"):
                text = text[:-1]
            if not text.strip():
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError:
                write_out_safe(_jsonrpc_error(None, JSONRPC_PARSE, "parse error"))
                continue
            except ValueError:
                # Python 3.15+ raises ValueError (not JSONDecodeError) for integer
                # conversion limits and similar decoder resource failures. Keep the
                # session alive with a defined parse error (C3 < 256 KiB frames).
                write_out_safe(_jsonrpc_error(None, JSONRPC_PARSE, "parse error"))
                continue
            except RecursionError:
                write_out_safe(_jsonrpc_error(None, JSONRPC_PARSE, "parse error"))
                continue
            if isinstance(message, list):
                write_out_safe(
                    _jsonrpc_error(None, JSONRPC_INVALID_REQUEST, "batched arrays are forbidden")
                )
                continue
            if not isinstance(message, dict):
                write_out_safe(
                    _jsonrpc_error(None, JSONRPC_INVALID_REQUEST, "request must be an object")
                )
                continue
            try:
                response = self.handle_message(message)
            except Exception:  # noqa: BLE001
                write_err("internal handler failure")
                write_err(traceback.format_exc().splitlines()[-1])
                raw_err_id = message.get("id")
                err_id = raw_err_id if _is_jsonrpc_id(raw_err_id) else None
                write_out_safe(_jsonrpc_error(err_id, JSONRPC_INTERNAL, "internal error"))
                continue
            if response is not None:
                write_out_safe(response)


def run_from_config_path(config_path: Path | str) -> int:
    config = load_config(config_path)
    server = McpServer(config)
    try:
        return server.serve()
    finally:
        server.close()
