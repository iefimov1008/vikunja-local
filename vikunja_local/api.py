"""Stock Vikunja API client for EA-010 (C1 §3–7): create/list/read + v2 merge-patch.

No CAS. No v1 POST update. Tokens never appear in argv or logs.
"""

from __future__ import annotations

import http.client
import io
import ipaddress
import json
import math
import re
import socket
import stat
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse
from urllib.request import Request

from .dates import (
    ConfirmedDue,
    DueKind,
    canonicalize_rfc3339_instant,
    is_absent_due,
    normalize_due_value,
    same_instant,
    same_stock_persisted_instant,
)

MARKER_RE = re.compile(
    r"^EA010:v1:"
    r"(?P<issuer>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}):"
    r"(?P<operation>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$"
)
UUID_V4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
HTML_ESCAPE = (
    ("&", "&amp;"),
    ("<", "&lt;"),
    (">", "&gt;"),
    ('"', "&quot;"),
    ("'", "&#39;"),
)
ALLOWED_PATCH_FIELDS = frozenset({"title", "description", "due_date", "done"})
CONNECT_TIMEOUT_S = 3.0
HTTP_TIMEOUT_S = 10.0
SCAN_DEADLINE_S = 30.0
MAX_SCAN_PAGES = 1000
MAX_SCAN_ITEMS = 100000
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class Outcome(str, Enum):
    CONFIRMED = "confirmed"
    UNKNOWN = "unknown"
    CONFLICT = "conflict"
    AUTH_DENIED = "auth_denied"
    SCOPE_DENIED = "scope_denied"
    VALIDATION_ERROR = "validation_error"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    INCOMPLETE_SCAN = "incomplete_scan"
    EMPTY = "empty"


class ApiError(RuntimeError):
    def __init__(self, outcome: Outcome, message: str, *, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.outcome = outcome
        self.details = dict(details or {})


@dataclass(frozen=True)
class TokenRef:
    """Local Mini token reference; secret is never kept in argv/logs."""

    kind: str
    path: Path

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "TokenRef":
        if raw.get("kind") != "file":
            raise ApiError(Outcome.VALIDATION_ERROR, "token_ref.kind must be 'file'")
        path = Path(str(raw.get("path") or ""))
        if not path.is_absolute():
            raise ApiError(Outcome.VALIDATION_ERROR, "token_ref.path must be absolute")
        return cls(kind="file", path=path)

    def load(self) -> str:
        try:
            st = self.path.lstat()
        except OSError as exc:
            raise ApiError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "token file unavailable",
                details={"errno": getattr(exc, "errno", None)},
            ) from exc
        if stat.S_ISLNK(st.st_mode):
            raise ApiError(Outcome.VALIDATION_ERROR, "token file must not be a symlink")
        if not stat.S_ISREG(st.st_mode):
            raise ApiError(Outcome.VALIDATION_ERROR, "token path must be a regular file")
        if st.st_mode & 0o177:
            raise ApiError(
                Outcome.VALIDATION_ERROR,
                "token file mode must be owner-only (0600)",
                details={"mode": oct(st.st_mode & 0o777)},
            )
        parent_mode = self.path.parent.lstat().st_mode
        if parent_mode & 0o077:
            raise ApiError(
                Outcome.VALIDATION_ERROR,
                "token parent directory must be 0700",
                details={"mode": oct(parent_mode & 0o777)},
            )
        text = self.path.read_text(encoding="utf-8")
        if "\n" in text or "\r" in text:
            # One non-empty line without CR/LF, per C1 §9.
            if text.endswith("\n") and text[:-1].count("\n") == 0 and "\r" not in text:
                text = text[:-1]
            else:
                raise ApiError(Outcome.VALIDATION_ERROR, "token file must be a single line")
        if not text:
            raise ApiError(Outcome.VALIDATION_ERROR, "token file is empty")
        return text


@dataclass
class TaskResult:
    outcome: Outcome
    task: dict[str, Any] | None = None
    http_status: int | None = None
    mutation_sent: bool = False
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.CONFIRMED and self.task is not None


def html_escape_user_portion(user_portion: str) -> str:
    if "\x00" in user_portion:
        raise ApiError(Outcome.VALIDATION_ERROR, "user portion must not contain NUL")
    try:
        user_portion.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ApiError(Outcome.VALIDATION_ERROR, "user portion must be valid UTF-8") from exc
    out = user_portion
    for src, dst in HTML_ESCAPE:
        out = out.replace(src, dst)
    return out


def validate_uuid_v4(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not UUID_V4_RE.fullmatch(value):
        raise ApiError(Outcome.VALIDATION_ERROR, f"{label} must be lowercase UUIDv4")
    return value


def build_marker(issuer_id: str, operation_id: str) -> str:
    issuer_id = validate_uuid_v4(issuer_id, label="issuer_id")
    operation_id = validate_uuid_v4(operation_id, label="operation_id")
    return f"EA010:v1:{issuer_id}:{operation_id}"


def build_create_description(issuer_id: str, operation_id: str, user_portion: str = "") -> str:
    marker = build_marker(issuer_id, operation_id)
    return marker + "\n" + html_escape_user_portion(user_portion)


def extract_marker(description: str | None) -> dict[str, str] | None:
    if not isinstance(description, str) or "\n" not in description:
        return None
    header, suffix = description.split("\n", 1)
    match = MARKER_RE.fullmatch(header)
    if not match:
        return None
    return {
        "marker": header,
        "issuer_id": match.group("issuer"),
        "operation_id": match.group("operation"),
        "user_portion": suffix,
    }


def encode_unmarked_description(user_portion: str) -> str:
    """Encode plain-text U for a manual task that has no integration marker.

    C1 §3: apply E(U) for the HTML API and never mint M from caller input.
    """
    encoded = html_escape_user_portion(user_portion)
    if extract_marker(encoded) is not None:
        raise ApiError(
            Outcome.VALIDATION_ERROR,
            "description must not mint an integration marker on an unmarked task",
        )
    return encoded


# Stock Vikunja resetDescriptionChecklist (v2.8.0): uncheck TipTap/input boxes only.
_CHECKLIST_TIPTAP_CHECKED = re.compile(r'(data-checked=")true(")')
_CHECKLIST_INPUT_CHECKED = re.compile(
    r"(<input[^>]*type=[\"']checkbox[\"'][^>]*?)\s+checked(?:=[\"'][^\"']*[\"'])?"
)


def reset_description_checklist(description: str) -> str:
    """Mirror stock checklist reset; other description bytes stay unchanged."""
    out = _CHECKLIST_TIPTAP_CHECKED.sub(r"\1false\2", description)
    out = _CHECKLIST_INPUT_CHECKED.sub(r"\1", out)
    return out


def new_operation_ids() -> tuple[str, str]:
    return str(uuid.uuid4()), str(uuid.uuid4())


def _redact(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 12 and value.count(".") >= 2:
        return "<redacted>"
    return value


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(Outcome.VALIDATION_ERROR, f"{label} must be a positive integer")
    if value <= 0:
        raise ApiError(Outcome.VALIDATION_ERROR, f"{label} must be a positive integer")
    return value


def _cap_timeout(value: float, ceiling: float, *, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ApiError(Outcome.VALIDATION_ERROR, f"{label} must be > 0")
    return float(min(float(value), ceiling))


def _is_loopback_host(host: str) -> bool:
    """Admit only literal loopback addresses (C1 §9 / ADR cleartext boundary)."""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _normalize_api_origin(api_origin: str) -> str:
    """Fail-closed origin: scheme+host[+port], no path/userinfo/query/fragment.

    Cleartext and TLS alike are limited to loopback; do not infer a wider network
    permit from an arbitrary config string before a bearer is attached.
    """
    if not isinstance(api_origin, str) or not api_origin.strip():
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must be a non-empty origin")
    parsed = urlparse(api_origin.strip())
    if parsed.scheme not in ("http", "https"):
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must be http(s) origin")
    if parsed.username is not None or parsed.password is not None or "@" in (parsed.netloc or ""):
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must not contain userinfo")
    if parsed.query or parsed.fragment:
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must not contain query/fragment")
    if parsed.path not in ("", "/"):
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must not contain a path")
    host = parsed.hostname
    if not host:
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin must include a host")
    if not _is_loopback_host(host):
        raise ApiError(
            Outcome.VALIDATION_ERROR,
            "api_origin must use a loopback host",
            details={"host": host},
        )
    # Reconstruct a canonical origin without path/slash/userinfo noise.
    try:
        addr = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ApiError(Outcome.VALIDATION_ERROR, "api_origin host unreadable") from exc
    host_out = f"[{host}]" if addr.version == 6 else host
    port = parsed.port
    if port is None:
        return f"{parsed.scheme}://{host_out}"
    return f"{parsed.scheme}://{host_out}:{port}"


def _same_done_at(left: Any, right: Any) -> bool:
    """Compare done_at at stock second precision (PATCH usecs vs GET truncate)."""
    try:
        return same_stock_persisted_instant(left, right)
    except ValueError:
        return False


class _DeadlineSocket:
    """Proxy that re-applies the absolute monotonic deadline on every recv.

    Idle socket timeouts alone are not enough: a server that drips status/header
    bytes just inside each idle window can overrun C1 §4's exchange ceiling.

    Important: override makefile() so http.client's BufferedReader is bound to
    this proxy. Delegating makefile via __getattr__ would bind SocketIO to the
    raw socket and bypass deadline enforcement during getresponse().
    """

    def __init__(self, sock: socket.socket, deadline: float) -> None:
        self._sock = sock
        self._deadline = deadline

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sock, name)

    def _apply_deadline(self) -> None:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("request budget exhausted")
        self._sock.settimeout(remaining)

    def makefile(self, mode: str = "r", buffering: int | None = None, *args: Any, **kwargs: Any):
        # Mirror socket.socket.makefile but keep reads on this proxy.
        # Increment the real socket's _io_refs so HTTPConnection.close() after
        # Connection: close / HTTP/1.0 does not _real_close the FD while the
        # response BufferedReader still needs it (SocketIO.close decrefs later).
        if buffering is None:
            buffering = -1
        if buffering == 0:
            raise ValueError("makefile buffering must not be 0 for text/binary http use")
        raw = socket.SocketIO(self, mode)  # type: ignore[arg-type]
        self._sock._io_refs += 1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
        if "r" in mode and "w" in mode:
            return io.BufferedRWPair(raw, raw, buffering)
        if "r" in mode:
            return io.BufferedReader(raw, buffering)
        if "w" in mode:
            return io.BufferedWriter(raw, buffering)
        return io.BufferedRWPair(raw, raw, buffering)

    def _decref_socketios(self) -> None:
        self._sock._decref_socketios()

    def close(self) -> None:
        self._sock.close()

    def recv(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recv(*args, **kwargs)

    def recv_into(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recv_into(*args, **kwargs)

    def recvfrom(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recvfrom(*args, **kwargs)

    def recvfrom_into(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recvfrom_into(*args, **kwargs)

    def recvmsg(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recvmsg(*args, **kwargs)

    def recvmsg_into(self, *args: Any, **kwargs: Any) -> Any:
        self._apply_deadline()
        return self._sock.recvmsg_into(*args, **kwargs)


class VikunjaClient:
    """Internal stock client. Not an external writer until s03/s04."""

    def __init__(
        self,
        *,
        api_origin: str,
        project_id: int,
        token_ref: TokenRef,
        page_size: int = 50,
        max_items_per_page: int = 50,
        connect_timeout: float = CONNECT_TIMEOUT_S,
        http_timeout: float = HTTP_TIMEOUT_S,
        token: str | None = None,
    ) -> None:
        # Origin gate before any token retention or Authorization construction (C1 §9).
        origin = _normalize_api_origin(api_origin)
        if not isinstance(project_id, int) or isinstance(project_id, bool) or project_id <= 0:
            raise ApiError(Outcome.VALIDATION_ERROR, "project_id must be > 0")
        if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size < 1:
            raise ApiError(Outcome.VALIDATION_ERROR, "page_size must be >= 1")
        if (
            not isinstance(max_items_per_page, int)
            or isinstance(max_items_per_page, bool)
            or max_items_per_page < 1
        ):
            raise ApiError(Outcome.VALIDATION_ERROR, "max_items_per_page must be >= 1")
        self.api_origin = origin
        self.project_id = project_id
        self.token_ref = token_ref
        self.page_size = min(page_size, max_items_per_page, 100)
        self.max_items_per_page = max_items_per_page
        # Fixed C1 §4 caps; callers cannot raise them.
        self.connect_timeout = _cap_timeout(
            connect_timeout, CONNECT_TIMEOUT_S, label="connect_timeout"
        )
        self.http_timeout = _cap_timeout(http_timeout, HTTP_TIMEOUT_S, label="http_timeout")
        self._token = token

    def _bearer(self) -> str:
        if self._token is None:
            self._token = self.token_ref.load()
        return self._token

    def _request_deadline(self, scan_deadline: float | None) -> float:
        """Absolute monotonic deadline for one HTTP exchange (C1 §4)."""
        now = time.monotonic()
        if scan_deadline is not None and scan_deadline <= now:
            raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "request budget exhausted")
        deadline = now + self.http_timeout
        if scan_deadline is not None:
            deadline = min(deadline, scan_deadline)
        return deadline

    def _remaining_until(self, deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "request budget exhausted")
        return remaining

    def _budget_timeouts(self, deadline: float | None) -> tuple[float, float]:
        """Return (connect_cap, remaining_to_http_deadline) for caps/tests."""
        request_deadline = self._request_deadline(deadline)
        remaining = self._remaining_until(request_deadline)
        connect_t = min(self.connect_timeout, remaining)
        if connect_t <= 0:
            raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "request budget exhausted")
        return connect_t, remaining

    def _set_sock_deadline(self, sock: socket.socket | None, request_deadline: float) -> None:
        if sock is not None:
            sock.settimeout(self._remaining_until(request_deadline))

    def _read_body_until_deadline(
        self,
        sock: socket.socket | _DeadlineSocket | None,
        response: http.client.HTTPResponse,
        request_deadline: float,
    ) -> bytes:
        chunks: list[bytes] = []
        total = 0
        # read1 returns after at most one underlying recv so a slow stream cannot hide
        # inside one Content-Length-sized read() under a single idle sock timeout.
        while True:
            remaining = self._remaining_until(request_deadline)
            if sock is not None:
                try:
                    sock.settimeout(remaining)
                except OSError:
                    # After Connection: close, http.client may _real_close the FD once
                    # Content-Length is satisfied; fall through to read1 for EOF.
                    pass
            try:
                chunk = response.read1(65536)
            except (TimeoutError, socket.timeout) as exc:
                raise ApiError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "network error talking to Vikunja",
                    details={"phase": "body", "reason": str(exc)},
                ) from exc
            except OSError as exc:
                if chunks:
                    break
                raise ApiError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "network error talking to Vikunja",
                    details={"phase": "body", "reason": str(exc)},
                ) from exc
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_RESPONSE_BYTES:
                raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "API response too large")
            chunks.append(chunk)
        return b"".join(chunks)

    def _exchange(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        data: bytes | None,
        connect_timeout: float,
        request_deadline: float,
    ) -> tuple[int, bytes, dict[str, str]]:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ApiError(Outcome.VALIDATION_ERROR, "unsupported URL scheme")
        host = parsed.hostname
        if not host:
            raise ApiError(Outcome.VALIDATION_ERROR, "URL missing host")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        connect_cap = min(connect_timeout, self._remaining_until(request_deadline))
        if parsed.scheme == "https":
            conn: http.client.HTTPConnection = http.client.HTTPSConnection(
                host, port, timeout=connect_cap
            )
        else:
            conn = http.client.HTTPConnection(host, port, timeout=connect_cap)
        sock: socket.socket | None = None
        try:
            try:
                conn.connect()
            except (TimeoutError, socket.timeout, OSError) as exc:
                raise ApiError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "network error talking to Vikunja",
                    details={"phase": "connect", "reason": str(exc)},
                ) from exc
            # Keep the connected socket: getresponse() may clear conn.sock when will_close.
            # Wrap so status/header parsing and body reads share the absolute deadline.
            raw_sock = conn.sock
            deadline_sock: _DeadlineSocket | None = (
                _DeadlineSocket(raw_sock, request_deadline) if raw_sock is not None else None
            )
            if deadline_sock is not None:
                conn.sock = deadline_sock  # type: ignore[assignment]
            try:
                self._set_sock_deadline(raw_sock, request_deadline)
                conn.request(method.upper(), path, body=data, headers=dict(headers))
                self._set_sock_deadline(raw_sock, request_deadline)
                response = conn.getresponse()
                # Prefer the deadline proxy so body recv keeps absolute budget enforcement.
                raw = self._read_body_until_deadline(
                    deadline_sock if deadline_sock is not None else raw_sock,
                    response,
                    request_deadline,
                )
            except ApiError:
                raise
            except (TimeoutError, socket.timeout, OSError, http.client.HTTPException) as exc:
                raise ApiError(
                    Outcome.DEPENDENCY_UNAVAILABLE,
                    "network error talking to Vikunja",
                    details={"phase": "transfer", "reason": str(exc)},
                ) from exc
            headers_out = {k.lower(): v for k, v in response.getheaders()}
            return int(response.status), raw, headers_out
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any | None = None,
        content_type: str | None = "application/json",
        query: Mapping[str, Any] | None = None,
        conditional: bool = False,
        deadline: float | None = None,
    ) -> tuple[int, Any, dict[str, str]]:
        if conditional:
            raise ApiError(Outcome.VALIDATION_ERROR, "conditional HTTP caching is forbidden")
        if method.upper() == "POST" and path.startswith("/api/v1/tasks/"):
            raise ApiError(Outcome.VALIDATION_ERROR, "v1 POST update/complete is forbidden")
        q = ""
        if query:
            parts = []
            for key, value in query.items():
                if isinstance(value, (list, tuple)):
                    for item in value:
                        parts.append(f"{key}={item}")
                else:
                    parts.append(f"{key}={value}")
            q = "?" + "&".join(parts)
        url = f"{self.api_origin}{path}{q}"
        headers = {"Accept": "application/json", "Authorization": "Bearer " + self._bearer()}
        data = None
        if body is not None:
            if content_type is None:
                raise ApiError(Outcome.VALIDATION_ERROR, "content_type required for body")
            headers["Content-Type"] = content_type
            data = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode("utf-8")
        # Keep Request construction for validation/compatibility with unit tests that
        # inspect kwargs; the wire path uses http.client with an absolute HTTP deadline.
        Request(url, data=data, headers=headers, method=method.upper())
        request_deadline = self._request_deadline(deadline)
        connect_timeout = min(self.connect_timeout, self._remaining_until(request_deadline))
        try:
            status, raw, headers_out = self._exchange(
                method,
                url,
                headers=headers,
                data=data,
                connect_timeout=connect_timeout,
                request_deadline=request_deadline,
            )
        except ApiError:
            raise
        except Exception as exc:  # noqa: BLE001 — normalize all transport faults
            raise ApiError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "network error talking to Vikunja",
                details={"reason": str(exc)},
            ) from exc
        if status in REDIRECT_STATUSES:
            raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "HTTP redirects are forbidden")
        if not raw:
            return status, None, headers_out
        try:
            return status, json.loads(raw.decode("utf-8")), headers_out
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(
                Outcome.DEPENDENCY_UNAVAILABLE,
                "API returned non-JSON body",
            ) from exc

    def _classify_http(self, status: int) -> Outcome | None:
        if status in (401,):
            return Outcome.AUTH_DENIED
        if status in (403,):
            return Outcome.SCOPE_DENIED
        if status in (400, 404, 405, 415, 422):
            return Outcome.VALIDATION_ERROR
        if status in (429,) or status >= 500:
            return Outcome.UNKNOWN
        return None

    def _validate_timestamp_field(
        self,
        value: Any,
        *,
        key: str,
        outcome: Outcome,
        allow_null: bool = True,
    ) -> None:
        """Null or validated RFC3339/zero; blank strings are never absence (C1 §7)."""
        if value is None:
            if allow_null:
                return
            raise ApiError(outcome, f"{key} unreadable", details={"key": key, "reason": "null"})
        if not isinstance(value, str):
            raise ApiError(outcome, f"{key} unreadable", details={"key": key, "reason": "type"})
        if value.strip() == "":
            raise ApiError(outcome, f"{key} unreadable", details={"key": key, "reason": "blank"})
        try:
            normalize_due_value(value)
        except (TypeError, ValueError) as exc:
            raise ApiError(
                outcome,
                f"{key} unreadable",
                details={"key": key, "reason": "malformed"},
            ) from exc

    def _validate_reminders(
        self,
        reminders: Any,
        *,
        outcome: Outcome,
    ) -> None:
        """Stock reminders are null or a list of objects with typed reminder instants."""
        if reminders is None:
            return
        if not isinstance(reminders, list):
            raise ApiError(outcome, "reminders unreadable", details={"reason": "type"})
        for index, item in enumerate(reminders):
            if not isinstance(item, Mapping):
                raise ApiError(
                    outcome,
                    "reminders unreadable",
                    details={"reason": "item_type", "index": index},
                )
            if "reminder" not in item:
                raise ApiError(
                    outcome,
                    "reminders unreadable",
                    details={"reason": "missing_reminder", "index": index},
                )
            self._validate_timestamp_field(
                item["reminder"],
                key="reminders.reminder",
                outcome=outcome,
                allow_null=False,
            )
            if "relative_period" in item:
                period = item["relative_period"]
                if isinstance(period, bool) or not isinstance(period, int):
                    raise ApiError(
                        outcome,
                        "reminders unreadable",
                        details={"reason": "relative_period", "index": index},
                    )
            if "relative_to" in item and item["relative_to"] is not None:
                # Stock absolute reminders use relative_to="" (empty relation).
                # Accept that form; reject only unsupported nonempty relations.
                relative_to = item["relative_to"]
                if relative_to not in ("", "due_date", "start_date", "end_date"):
                    raise ApiError(
                        outcome,
                        "reminders unreadable",
                        details={"reason": "relative_to", "index": index},
                    )

    def _validate_recurrence_fields(
        self,
        task: Mapping[str, Any],
        *,
        outcome: Outcome,
        require_present: bool,
    ) -> None:
        if require_present:
            missing = [key for key in ("repeat_after", "repeat_mode") if key not in task]
            if missing:
                raise ApiError(outcome, "incomplete recurrence fields", details={"missing": missing})
        if "repeat_after" in task:
            repeat_after = task["repeat_after"]
            if isinstance(repeat_after, bool) or not isinstance(repeat_after, int) or repeat_after < 0:
                raise ApiError(outcome, "repeat_after unreadable")
        if "repeat_mode" in task:
            repeat_mode = task["repeat_mode"]
            if isinstance(repeat_mode, bool) or not isinstance(repeat_mode, int):
                raise ApiError(outcome, "repeat_mode unreadable")
            if repeat_mode not in (0, 1, 2):
                raise ApiError(
                    outcome,
                    "unsupported repeat_mode",
                    details={"repeat_mode": repeat_mode},
                )

    def _require_task_object(self, payload: Any, *, expected_id: int | None = None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ApiError(Outcome.UNKNOWN, "task payload is not an object")
        try:
            task_id = _positive_int(payload.get("id"), label="task.id")
        except ApiError as exc:
            raise ApiError(Outcome.UNKNOWN, "task payload missing valid id", details=exc.details) from exc
        if expected_id is not None and task_id != expected_id:
            raise ApiError(
                Outcome.CONFLICT,
                "task id mismatch on read-back",
                details={"expected": expected_id, "got": task_id},
            )
        try:
            project_id = _positive_int(payload.get("project_id"), label="task.project_id")
        except ApiError as exc:
            raise ApiError(
                Outcome.UNKNOWN,
                "task payload missing valid project_id",
                details=exc.details,
            ) from exc
        if project_id != self.project_id:
            raise ApiError(
                Outcome.SCOPE_DENIED,
                "task project_id outside configured scope",
                details={"expected": self.project_id, "got": project_id},
            )
        # C1 §4/§6: typed complete read-back witnesses; missing core fields are unknown.
        for key in ("title", "description", "done", "due_date"):
            if key not in payload:
                raise ApiError(
                    Outcome.UNKNOWN,
                    "task payload incomplete",
                    details={"missing": key},
                )
        if not isinstance(payload["title"], str) or not payload["title"].strip():
            raise ApiError(Outcome.UNKNOWN, "title unreadable")
        if not isinstance(payload["description"], str):
            raise ApiError(Outcome.UNKNOWN, "description unreadable")
        if not isinstance(payload["done"], bool):
            raise ApiError(Outcome.UNKNOWN, "done unreadable")
        self._validate_timestamp_field(
            payload["due_date"],
            key="due_date",
            outcome=Outcome.UNKNOWN,
            allow_null=True,
        )
        for key in ("start_date", "end_date", "done_at"):
            if key not in payload:
                continue
            self._validate_timestamp_field(
                payload[key],
                key=key,
                outcome=Outcome.UNKNOWN,
                allow_null=True,
            )
        if "reminders" in payload:
            self._validate_reminders(payload["reminders"], outcome=Outcome.UNKNOWN)
        try:
            self._validate_recurrence_fields(payload, outcome=Outcome.UNKNOWN, require_present=False)
        except ApiError as exc:
            # Present-but-invalid recurrence on any task witness stays unknown.
            if exc.outcome is Outcome.VALIDATION_ERROR:
                raise ApiError(Outcome.UNKNOWN, str(exc), details=exc.details) from exc
            raise
        return payload

    def _validate_response_timestamps(self, task: Mapping[str, Any]) -> None:
        """Typed offset-bearing timestamps at the response boundary (C1 §4/§6/§7).

        Present keys must be null or a validated RFC3339/zero instant. Blank strings
        are malformed. Missing keys are not inferred as cleared deadlines.
        """
        for key in ("due_date", "start_date", "end_date", "done_at"):
            if key not in task:
                continue
            self._validate_timestamp_field(
                task[key],
                key=key,
                outcome=Outcome.UNKNOWN,
                allow_null=True,
            )
        if "reminders" in task:
            self._validate_reminders(task["reminders"], outcome=Outcome.UNKNOWN)

    def _require_occurrence_dates(
        self,
        task: Mapping[str, Any],
        *,
        outcome: Outcome = Outcome.VALIDATION_ERROR,
        label: str = "incomplete recurrence preimage",
    ) -> None:
        """Typed occurrence fields required for recurring complete (C1 §6)."""
        # due_date and description are required keys; null/zero-due and empty text are
        # legitimate values and must not be conflated with an omitted field.
        required = (
            "done",
            "done_at",
            "start_date",
            "end_date",
            "reminders",
            "due_date",
            "description",
            "repeat_after",
            "repeat_mode",
        )
        missing = [key for key in required if key not in task]
        if missing:
            raise ApiError(outcome, label, details={"missing": missing})
        if not isinstance(task["done"], bool):
            raise ApiError(outcome, "done unreadable")
        if not isinstance(task["description"], str):
            raise ApiError(outcome, "description unreadable")
        self._validate_timestamp_field(
            task["due_date"],
            key="due_date",
            outcome=outcome,
            allow_null=True,
        )
        for key in ("done_at", "start_date", "end_date"):
            self._validate_timestamp_field(
                task[key],
                key=key,
                outcome=outcome,
                allow_null=True,
            )
        self._validate_reminders(task["reminders"], outcome=outcome)
        self._validate_recurrence_fields(task, outcome=outcome, require_present=True)

    def _parse_recurrence(self, task: Mapping[str, Any]) -> bool:
        """Return whether the task is recurring; reject incomplete/unsupported preimage."""
        self._validate_recurrence_fields(
            task,
            outcome=Outcome.VALIDATION_ERROR,
            require_present=True,
        )
        repeat_after = task["repeat_after"]
        repeat_mode = task["repeat_mode"]
        recurring = repeat_after > 0 or repeat_mode == 1
        if recurring:
            self._require_occurrence_dates(task)
        return recurring

    def _is_recurring(self, task: Mapping[str, Any]) -> bool:
        return self._parse_recurrence(task)

    def _reminders_equal(
        self,
        left: Any,
        right: Any,
        *,
        allow_due_relative_instant_drift: bool = False,
    ) -> bool:
        left_list = [] if left is None else left
        right_list = [] if right is None else right
        if left_list == right_list:
            return True
        if not isinstance(left_list, list) or not isinstance(right_list, list):
            return False
        if len(left_list) != len(right_list):
            return False
        for left_item, right_item in zip(left_list, right_list):
            if not isinstance(left_item, Mapping) or not isinstance(right_item, Mapping):
                return False
            if "reminder" not in left_item or "reminder" not in right_item:
                return False
            left_keys = set(left_item) - {"reminder"}
            right_keys = set(right_item) - {"reminder"}
            if left_keys != right_keys:
                return False
            for key in left_keys:
                if left_item.get(key) != right_item.get(key):
                    return False
            # Stock recalculates due-relative reminder instants when due_date changes.
            # Absolute / start / end relations must still match the preimage instant.
            if (
                allow_due_relative_instant_drift
                and left_item.get("relative_to") == "due_date"
                and right_item.get("relative_to") == "due_date"
            ):
                continue
            try:
                if not same_stock_persisted_instant(left_item["reminder"], right_item["reminder"]):
                    return False
            except ValueError:
                return False
        return True

    def _occurrence_matches(self, observed: Mapping[str, Any], postimage: Mapping[str, Any]) -> bool:
        """Compare full typed occurrences; missing keys never compare equal via .get()."""
        required = (
            "id",
            "project_id",
            "done",
            "due_date",
            "done_at",
            "start_date",
            "end_date",
            "repeat_after",
            "repeat_mode",
            "description",
            "reminders",
        )
        for key in required:
            if key not in observed or key not in postimage:
                return False
        try:
            if _positive_int(observed["id"], label="id") != _positive_int(
                postimage["id"], label="id"
            ):
                return False
            if _positive_int(observed["project_id"], label="project_id") != _positive_int(
                postimage["project_id"], label="project_id"
            ):
                return False
        except ApiError:
            return False
        if observed["done"] is not postimage["done"]:
            return False
        try:
            # Stock may keep usecs on PATCH while GET truncates the same second.
            if not same_stock_persisted_instant(observed["due_date"], postimage["due_date"]):
                return False
            if not _same_done_at(observed["done_at"], postimage["done_at"]):
                return False
            if not same_stock_persisted_instant(observed["start_date"], postimage["start_date"]):
                return False
            if not same_stock_persisted_instant(observed["end_date"], postimage["end_date"]):
                return False
        except ValueError:
            return False
        if observed["repeat_after"] != postimage["repeat_after"]:
            return False
        if observed["repeat_mode"] != postimage["repeat_mode"]:
            return False
        if observed["description"] != postimage["description"]:
            return False
        if not self._reminders_equal(observed["reminders"], postimage["reminders"]):
            return False
        return True

    def _occurrence_receipt(self, task: Mapping[str, Any]) -> dict[str, Any]:
        """Fields preserved for future durable receipt after recurring complete.

        Omitted keys are listed under ``_missing`` instead of being silently
        collapsed to null/absent via ``.get()``.
        """
        keys = (
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
        receipt: dict[str, Any] = {}
        missing: list[str] = []
        for key in keys:
            if key in task:
                receipt[key] = task[key]
            else:
                missing.append(key)
        if missing:
            receipt["_missing"] = missing
        return receipt

    def _marker_preserved(self, description: Any, expected_marker: str | None) -> bool:
        extracted = extract_marker(description if isinstance(description, str) else None)
        if expected_marker is None:
            return extracted is None
        return extracted is not None and extracted["marker"] == expected_marker

    def _recurrence_immutable(self, observed: Mapping[str, Any], preimage: Mapping[str, Any]) -> bool:
        """repeat_after / repeat_mode must remain equal to the saved preimage."""
        if "repeat_after" not in observed or "repeat_mode" not in observed:
            return False
        if "repeat_after" not in preimage or "repeat_mode" not in preimage:
            return False
        return (
            observed["repeat_after"] == preimage["repeat_after"]
            and observed["repeat_mode"] == preimage["repeat_mode"]
        )

    def _recurring_description_delta_allowed(
        self,
        preimage_description: Any,
        observed_description: Any,
        expected_marker: str | None,
    ) -> bool:
        """Allow equal text or stock checklist-only reset; reject other context drift."""
        if preimage_description == observed_description:
            return True
        if not isinstance(preimage_description, str) or not isinstance(observed_description, str):
            return False
        if not self._marker_preserved(observed_description, expected_marker):
            return False
        return reset_description_checklist(preimage_description) == observed_description

    def read(self, task_id: int, *, format: str = "html", deadline: float | None = None) -> TaskResult:
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ApiError(Outcome.VALIDATION_ERROR, "task_id must be > 0")
        status, body, _ = self._request(
            "GET",
            f"/api/v2/tasks/{task_id}",
            query={"format": format},
            deadline=deadline,
        )
        denied = self._classify_http(status)
        if denied is not None:
            return TaskResult(outcome=denied, http_status=status, details={"body": _redact(body)})
        if status != 200:
            if status == 304:
                # Unconditional GET already requested; a 304 without body is unknown.
                return TaskResult(outcome=Outcome.UNKNOWN, http_status=status)
            return TaskResult(outcome=Outcome.UNKNOWN, http_status=status, details={"body": body})
        try:
            task = self._require_task_object(body, expected_id=task_id)
            self._validate_response_timestamps(task)
        except ApiError as exc:
            return TaskResult(
                outcome=exc.outcome if exc.outcome is not Outcome.VALIDATION_ERROR else Outcome.UNKNOWN,
                http_status=status,
                details={"reason": str(exc), **exc.details},
            )
        return TaskResult(outcome=Outcome.CONFIRMED, task=task, http_status=status)

    def create(
        self,
        *,
        title: str,
        issuer_id: str,
        operation_id: str,
        user_portion: str = "",
        due: ConfirmedDue | None = None,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> TaskResult:
        if not isinstance(title, str) or not title.strip():
            raise ApiError(Outcome.VALIDATION_ERROR, "title must be a non-empty string")
        description = build_create_description(issuer_id, operation_id, user_portion)
        expected_marker = build_marker(issuer_id, operation_id)
        payload: dict[str, Any] = {"title": title, "description": description}
        expected_due: str | None = None
        if due is not None:
            if due.kind is DueKind.NONE:
                pass
            else:
                try:
                    # Freeze/wire must already be whole-second; canonicalize rejects
                    # nonzero fractional targets before any HTTP (F1 / C1 §6/§7).
                    raw_wire = due.wire_value()
                    if raw_wire is None:
                        raise ApiError(Outcome.VALIDATION_ERROR, "confirmed due missing absolute value")
                    wire = canonicalize_rfc3339_instant(raw_wire)
                except ValueError as exc:
                    raise ApiError(
                        Outcome.VALIDATION_ERROR,
                        "due_date fractional seconds are unsupported; freeze whole-second instants only",
                        details={"reason": str(exc)},
                    ) from exc
                payload["due_date"] = wire
                expected_due = wire
        if extra_fields:
            for key, value in extra_fields.items():
                if key in {"title", "description", "due_date", "id", "project_id"}:
                    raise ApiError(Outcome.VALIDATION_ERROR, f"extra_fields may not override {key}")
                payload[key] = value
        try:
            status, body, _ = self._request(
                "PUT",
                f"/api/v1/projects/{self.project_id}/tasks",
                body=payload,
            )
        except ApiError as exc:
            # Write may have left the process; never expose as clean pre-dispatch failure.
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                mutation_sent=True,
                details={"phase": "create", "reason": str(exc), "outcome": exc.outcome.value},
            )
        if status != 201:
            denied = self._classify_http(status)
            if denied is not None:
                return TaskResult(
                    outcome=denied,
                    http_status=status,
                    mutation_sent=True,
                    details={"body": body, "phase": "create"},
                )
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=status,
                mutation_sent=True,
                details={"body": body, "phase": "create"},
            )
        try:
            created = self._require_task_object(body)
            task_id = _positive_int(created["id"], label="created.id")
        except ApiError as exc:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=status,
                mutation_sent=True,
                details={"phase": "create_response", "reason": str(exc)},
            )
        try:
            read_back = self.read(task_id)
        except ApiError as exc:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=status,
                mutation_sent=True,
                details={
                    "create_http": status,
                    "phase": "create_read_back",
                    "reason": str(exc),
                    "task_id": task_id,
                },
            )
        if not read_back.ok or read_back.task is None:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={
                    "create_http": status,
                    "read_back": read_back.details,
                    "phase": "create_read_back",
                    "task_id": task_id,
                },
            )
        task = read_back.task
        if task.get("title") != title:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={"reason": "title_mismatch_on_read_back", "task_id": task_id},
            )
        extracted = extract_marker(task.get("description"))
        if not extracted or extracted["marker"] != expected_marker:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={"reason": "marker_mismatch_on_read_back", "task_id": task_id},
            )
        if extracted["user_portion"] != html_escape_user_portion(user_portion):
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={"reason": "context_mismatch_on_read_back", "task_id": task_id},
            )
        try:
            if "due_date" not in task:
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": "due_missing_on_read_back", "task_id": task_id},
                )
            if expected_due is not None:
                if not same_instant(task["due_date"], expected_due):
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "due_mismatch_on_read_back", "task_id": task_id},
                    )
            elif not is_absent_due(task["due_date"]):
                # Omission / DueKind.NONE must confirm absence, not an unexpected deadline.
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": "due_unexpected_on_read_back", "task_id": task_id},
                )
        except ValueError as exc:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={
                    "reason": "due_unreadable_on_read_back",
                    "task_id": task_id,
                    "error": str(exc),
                },
            )
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=task,
            http_status=read_back.http_status,
            mutation_sent=True,
            details={"create_http": status, "marker": expected_marker, "task_id": task_id},
        )

    def list_project_tasks(self) -> TaskResult:
        """Stable ascending id traversal until an empty page (C1 §4)."""
        page = 1
        collected: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        expected_total: int | None = None
        effective_per_page: int | None = None
        expected_total_pages: int | None = None
        pages_read = 0
        last_id: int | None = None
        deadline = time.monotonic() + SCAN_DEADLINE_S
        while True:
            if time.monotonic() >= deadline:
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    details={"reason": "scan_deadline", "pages_read": pages_read, "collected": len(collected)},
                )
            if pages_read >= MAX_SCAN_PAGES or len(collected) >= MAX_SCAN_ITEMS:
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    details={"pages_read": pages_read, "collected": len(collected)},
                )
            try:
                status, body, _ = self._request(
                    "GET",
                    f"/api/v2/projects/{self.project_id}/tasks",
                    query={
                        "page": page,
                        "per_page": self.page_size,
                        "sort_by": "id",
                        "order_by": "asc",
                        "format": "html",
                    },
                    deadline=deadline,
                )
            except ApiError as exc:
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    details={"reason": str(exc), "page": page, "phase": "scan_request"},
                )
            denied = self._classify_http(status)
            if denied is not None:
                return TaskResult(outcome=denied, http_status=status, details={"page": page})
            if status != 200 or not isinstance(body, dict):
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    http_status=status,
                    details={"page": page, "body": body},
                )
            items = body.get("items")
            total = body.get("total")
            resp_page = body.get("page")
            per_page = body.get("per_page")
            total_pages = body.get("total_pages")
            if not isinstance(items, list):
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "items"})
            if isinstance(total, bool) or not isinstance(total, int) or total < 0:
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "total"})
            # C1 §4: page must be a positive non-boolean integer before equality.
            # JSON true/1.0 must not coerce into a successful first-page match.
            if (
                isinstance(resp_page, bool)
                or not isinstance(resp_page, int)
                or resp_page < 1
                or resp_page != page
            ):
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "page"})
            if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page <= 0:
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "per_page"})
            if isinstance(total_pages, bool) or not isinstance(total_pages, int) or total_pages < 0:
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "total_pages"})
            expected_pages = math.ceil(total / per_page) if total > 0 else 0
            if total_pages != expected_pages:
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    details={
                        "reason": "total_pages_mismatch",
                        "total": total,
                        "per_page": per_page,
                        "total_pages": total_pages,
                        "expected_total_pages": expected_pages,
                    },
                )
            if expected_total is None:
                expected_total = total
                effective_per_page = per_page
                expected_total_pages = total_pages
            elif (
                total != expected_total
                or per_page != effective_per_page
                or total_pages != expected_total_pages
            ):
                return TaskResult(
                    outcome=Outcome.INCOMPLETE_SCAN,
                    details={"reason": "totals_drift", "page": page},
                )
            if len(items) > per_page:
                return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "page_overflow"})
            for item in items:
                if not isinstance(item, dict):
                    return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "item"})
                try:
                    task_id = _positive_int(item.get("id"), label="item.id")
                    project_id = _positive_int(item.get("project_id"), label="item.project_id")
                except ApiError:
                    return TaskResult(
                        outcome=Outcome.INCOMPLETE_SCAN,
                        details={"reason": "item_schema", "page": page},
                    )
                # C1 §4: every scanned row needs a present string description before
                # complete/empty/exact-one. Missing/null/non-string are unreadable;
                # an actual empty string is a valid present value.
                if "description" not in item or not isinstance(item["description"], str):
                    return TaskResult(
                        outcome=Outcome.INCOMPLETE_SCAN,
                        details={"reason": "item_description", "page": page, "item_id": task_id},
                    )
                if project_id != self.project_id:
                    return TaskResult(
                        outcome=Outcome.SCOPE_DENIED,
                        details={"got_project_id": project_id},
                    )
                if task_id in seen_ids:
                    return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "duplicate_id"})
                if last_id is not None and task_id <= last_id:
                    return TaskResult(outcome=Outcome.INCOMPLETE_SCAN, details={"reason": "not_asc"})
                seen_ids.add(task_id)
                collected.append(item)
                last_id = task_id
            pages_read += 1
            if not items:
                break
            page += 1
        if expected_total is not None and len(seen_ids) != expected_total:
            return TaskResult(
                outcome=Outcome.INCOMPLETE_SCAN,
                details={"unique_ids": len(seen_ids), "total": expected_total},
            )
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            http_status=200,
            details={
                "items": collected,
                "total": expected_total if expected_total is not None else 0,
                "pages_read": pages_read,
                "effective_per_page": effective_per_page,
                "total_pages": expected_total_pages,
            },
        )

    def find_by_marker(self, marker: str) -> TaskResult:
        listed = self.list_project_tasks()
        if listed.outcome is not Outcome.CONFIRMED:
            return listed
        matches: list[dict[str, Any]] = []
        for item in listed.details.get("items") or []:
            extracted = extract_marker(item.get("description"))
            if extracted and extracted["marker"] == marker:
                matches.append(item)
        if not matches:
            return TaskResult(outcome=Outcome.EMPTY, details={"marker": marker})
        if len(matches) > 1:
            return TaskResult(
                outcome=Outcome.CONFLICT,
                details={"marker": marker, "ids": [m.get("id") for m in matches]},
            )
        try:
            task_id = _positive_int(matches[0].get("id"), label="match.id")
        except ApiError:
            return TaskResult(outcome=Outcome.UNKNOWN, details={"reason": "match_id", "marker": marker})
        read_back = self.read(task_id)
        if not read_back.ok or read_back.task is None:
            return TaskResult(
                outcome=Outcome.UNKNOWN if read_back.outcome is Outcome.CONFIRMED else read_back.outcome,
                http_status=read_back.http_status,
                details={"marker": marker, "reason": "read_back_failed", **read_back.details},
            )
        extracted = extract_marker(read_back.task.get("description"))
        if not extracted or extracted["marker"] != marker:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=read_back.task,
                http_status=read_back.http_status,
                details={"marker": marker, "reason": "marker_lost_on_read_back"},
            )
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=read_back.task,
            http_status=read_back.http_status,
            details={"marker": marker},
        )

    def _validate_patch(self, patch: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(patch, Mapping) or not patch:
            raise ApiError(Outcome.VALIDATION_ERROR, "patch must be a non-empty object")
        out: dict[str, Any] = {}
        for key, value in patch.items():
            if key not in ALLOWED_PATCH_FIELDS:
                raise ApiError(Outcome.VALIDATION_ERROR, f"unsupported patch field: {key}")
            if key == "title":
                if not isinstance(value, str) or not value.strip():
                    raise ApiError(Outcome.VALIDATION_ERROR, "title patch must be non-empty string")
                out[key] = value
            elif key == "description":
                # Plain-text user portion U_new; trusted rebuild of M+LF+E(U) happens in update().
                if not isinstance(value, str):
                    raise ApiError(Outcome.VALIDATION_ERROR, "description patch must be string")
                out[key] = value
            elif key == "due_date":
                if value is None:
                    out[key] = None
                elif isinstance(value, str):
                    # Reject accidental zero-date writes; absence is null.
                    # Blank strings are malformed, not absence (C1 §7).
                    if value.strip() == "":
                        raise ApiError(
                            Outcome.VALIDATION_ERROR,
                            "due_date must be an offset-bearing RFC3339 instant",
                            details={"reason": "blank"},
                        )
                    if is_absent_due(value):
                        raise ApiError(
                            Outcome.VALIDATION_ERROR,
                            "use due_date=null to clear; zero-date is not a write value",
                        )
                    try:
                        # Strict grammar + canonical wire before any HTTP (C1 §6/§7).
                        out[key] = canonicalize_rfc3339_instant(value)
                    except ValueError as exc:
                        raise ApiError(
                            Outcome.VALIDATION_ERROR,
                            "due_date must be an offset-bearing RFC3339 instant",
                            details={"reason": str(exc)},
                        ) from exc
                else:
                    raise ApiError(Outcome.VALIDATION_ERROR, "due_date must be RFC3339 string or null")
            elif key == "done":
                if not isinstance(value, bool):
                    raise ApiError(Outcome.VALIDATION_ERROR, "done must be bool")
                out[key] = value
        # C1 §6: complete is a selective {done:true} PATCH only — no combined field edits.
        if out.get("done") is True and len(out) > 1:
            raise ApiError(
                Outcome.VALIDATION_ERROR,
                "complete must be a separate {done:true} PATCH",
                details={"fields": sorted(out)},
            )
        return out

    def update(self, task_id: int, patch: Mapping[str, Any]) -> TaskResult:
        try:
            body = self._validate_patch(patch)
        except ApiError as exc:
            return TaskResult(
                outcome=exc.outcome,
                mutation_sent=False,
                details={"phase": "validate", "reason": str(exc), **exc.details},
            )
        pre = self.read(task_id)
        if not pre.ok or pre.task is None:
            return TaskResult(
                outcome=pre.outcome if pre.outcome is not Outcome.CONFIRMED else Outcome.UNKNOWN,
                http_status=pre.http_status,
                details={"phase": "preimage", **pre.details},
            )
        preimage = pre.task
        recurring = False
        recurrence_error: str | None = None
        try:
            recurring = self._parse_recurrence(preimage)
        except ApiError as exc:
            recurrence_error = str(exc)
            if body.get("done") is True:
                # Complete requires a complete recurrence preimage before dispatch.
                return TaskResult(
                    outcome=Outcome.VALIDATION_ERROR,
                    http_status=pre.http_status,
                    mutation_sent=False,
                    details={"phase": "preimage", "reason": recurrence_error},
                )
        recurring_complete = body.get("done") is True and recurring
        preimage_marker = None
        extracted_pre = extract_marker(preimage.get("description"))
        if extracted_pre:
            preimage_marker = extracted_pre["marker"]

        # C1 §3/§6: description PATCH is plain-text U_new.
        # Marked: rebuild verified M+LF+E(U_new); empty U still preserves M.
        # Unmarked manual task: E(U) only; never mint a marker from caller input.
        if "description" in body:
            try:
                if preimage_marker is not None:
                    body["description"] = (
                        preimage_marker + "\n" + html_escape_user_portion(body["description"])
                    )
                else:
                    body["description"] = encode_unmarked_description(body["description"])
            except ApiError as exc:
                return TaskResult(
                    outcome=exc.outcome,
                    mutation_sent=False,
                    http_status=pre.http_status,
                    details={"phase": "validate", "reason": str(exc), **exc.details},
                )

        # C1 §6: already at desired done=true → fresh fact, no mutation (complete and update).
        if body.get("done") is True and preimage.get("done") is True:
            return TaskResult(
                outcome=Outcome.CONFIRMED,
                task=preimage,
                http_status=pre.http_status,
                mutation_sent=False,
                details={"noop": True, "recurring": recurring},
            )

        mutation_sent = False
        postimage: dict[str, Any] | None = None
        try:
            status, response_body, _ = self._request(
                "PATCH",
                f"/api/v2/tasks/{task_id}",
                body=body,
                content_type="application/merge-patch+json",
            )
            mutation_sent = True
        except ApiError as exc:
            # Once a mutation may have left the process, never blind-retry; report unknown.
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                mutation_sent=True,
                details={
                    "reason": str(exc),
                    "phase": "patch",
                    "task_id": task_id,
                    "preimage_outcome": exc.outcome.value,
                },
            )
        if status not in (200, 304):
            denied = self._classify_http(status)
            if denied is not None:
                return TaskResult(
                    outcome=denied,
                    http_status=status,
                    mutation_sent=mutation_sent,
                    details={"body": response_body, "preimage_id": preimage.get("id"), "phase": "patch"},
                )
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=status,
                mutation_sent=mutation_sent,
                details={"body": response_body, "phase": "patch", "task_id": task_id},
            )
        postimage_error: str | None = None
        if status == 200:
            try:
                candidate = self._require_task_object(response_body, expected_id=task_id)
            except ApiError as exc:
                postimage = None
                postimage_error = str(exc)
            else:
                try:
                    self._validate_response_timestamps(candidate)
                except ApiError as exc:
                    # Keep the object for later witness checks; mandatory GET still runs.
                    postimage = candidate
                    postimage_error = str(exc)
                else:
                    postimage = candidate

        # 200 or 304 both require an unconditional GET read-back.
        try:
            read_back = self.read(task_id)
        except ApiError as exc:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=status,
                mutation_sent=True,
                details={
                    "patch_http": status,
                    "phase": "read_back",
                    "reason": str(exc),
                    "task_id": task_id,
                },
            )
        if not read_back.ok or read_back.task is None:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                http_status=read_back.http_status,
                mutation_sent=mutation_sent,
                details={
                    "patch_http": status,
                    "reason": "read_back_failed",
                    "task_id": task_id,
                    **({"postimage_error": postimage_error} if postimage_error else {}),
                    **read_back.details,
                },
            )
        task = read_back.task
        if postimage_error is not None:
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={
                    "patch_http": status,
                    "reason": "postimage_unreadable",
                    "task_id": task_id,
                    "postimage_error": postimage_error,
                },
            )

        try:
            if "title" in body and task.get("title") != body["title"]:
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": "title_mismatch", "task_id": task_id},
                )
            if "description" in body and task.get("description") != body["description"]:
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": "description_mismatch", "task_id": task_id},
                )
            if "due_date" in body:
                # Target-field witness must be present and typed before compare
                # (nonrecurring and recurring); missing ≠ explicit null/zero.
                if "due_date" not in task:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "due_missing_on_read_back", "task_id": task_id},
                    )
                if body["due_date"] is None:
                    if not is_absent_due(task["due_date"]):
                        return TaskResult(
                            outcome=Outcome.UNKNOWN,
                            task=task,
                            http_status=read_back.http_status,
                            mutation_sent=True,
                            details={"reason": "due_not_cleared", "task_id": task_id},
                        )
                # Caller-frozen target: exact instant equality (same policy as create).
                # Server-generated recurring postimage tolerance stays on
                # same_stock_persisted_instant() elsewhere; never for frozen targets.
                elif not same_instant(task["due_date"], body["due_date"]):
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "due_mismatch", "task_id": task_id},
                    )

            recurring_transition = False
            if "done" in body:
                if recurring_complete:
                    if status == 304:
                        # Full typed GET occurrence required before a 304 no-op receipt.
                        try:
                            self._require_occurrence_dates(
                                task,
                                outcome=Outcome.UNKNOWN,
                                label="incomplete recurring GET occurrence",
                            )
                        except ApiError as exc:
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "incomplete_recurring_witness",
                                    "patch_http": status,
                                    "task_id": task_id,
                                    **exc.details,
                                },
                            )
                        # 304 does not prove an occurrence advance; require a full no-op witness.
                        if not self._occurrence_matches(task, preimage):
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "recurring_304_occurrence_mismatch",
                                    "patch_http": status,
                                    "task_id": task_id,
                                },
                            )
                        if preimage_marker is not None and not self._marker_preserved(
                            task.get("description"), preimage_marker
                        ):
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "marker_lost_on_recurring_304",
                                    "patch_http": status,
                                    "task_id": task_id,
                                },
                            )
                        recurring_transition = False
                    elif status == 200:
                        # C1 §6: validate full typed response postimage and fresh GET
                        # occurrence before comparing or issuing a receipt.
                        if postimage is None:
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "recurring_postimage_invalid",
                                    "patch_http": status,
                                    "task_id": task_id,
                                    **(
                                        {"postimage_error": postimage_error}
                                        if postimage_error
                                        else {}
                                    ),
                                },
                            )
                        try:
                            self._require_occurrence_dates(
                                postimage,
                                outcome=Outcome.UNKNOWN,
                                label="incomplete recurring response postimage",
                            )
                            self._require_occurrence_dates(
                                task,
                                outcome=Outcome.UNKNOWN,
                                label="incomplete recurring GET occurrence",
                            )
                        except ApiError as exc:
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "incomplete_recurring_witness",
                                    "patch_http": status,
                                    "task_id": task_id,
                                    **exc.details,
                                },
                            )
                        if not self._occurrence_matches(task, postimage):
                            return TaskResult(
                                outcome=Outcome.UNKNOWN,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "recurring_postimage_not_witnessed",
                                    "patch_http": status,
                                    "task_id": task_id,
                                },
                            )
                        # Matching GET==postimage is not enough: immutable recurrence and the
                        # allowed description delta are judged against the saved preimage.
                        if not self._recurrence_immutable(task, preimage) or (
                            not self._recurrence_immutable(postimage, preimage)
                        ):
                            return TaskResult(
                                outcome=Outcome.CONFLICT,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "recurring_immutable_fields_changed",
                                    "patch_http": status,
                                    "task_id": task_id,
                                },
                            )
                        if not self._recurring_description_delta_allowed(
                            preimage.get("description"),
                            task.get("description"),
                            preimage_marker,
                        ):
                            return TaskResult(
                                outcome=Outcome.CONFLICT,
                                task=task,
                                http_status=read_back.http_status,
                                mutation_sent=True,
                                details={
                                    "reason": "recurring_description_drift",
                                    "patch_http": status,
                                    "task_id": task_id,
                                },
                            )
                        # C1 §6: a receipt is an observed occurrence transition.
                        # Matching GET==postimage with immutable fields intact is not
                        # enough when the full typed occurrence equals the preimage
                        # (no due/done_at/start/end/reminders/description/done effect).
                        # Preserve no-op rather than claiming a transition; never retry.
                        if self._occurrence_matches(task, preimage):
                            recurring_transition = False
                        else:
                            recurring_transition = True
                    else:
                        return TaskResult(
                            outcome=Outcome.UNKNOWN,
                            task=task,
                            http_status=read_back.http_status,
                            mutation_sent=True,
                            details={"reason": "unexpected_recurring_status", "patch_http": status},
                        )
                elif task.get("done") is not body["done"]:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "done_mismatch", "task_id": task_id},
                    )

            # Omitted fields must match preimage except allowed recurring complete side effects.
            # Missing keys never compare equal via .get() — incomplete witnesses stay unknown.
            for key in ("title", "description", "done"):
                if key in body:
                    continue
                if key not in task or key not in preimage:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": f"omitted_{key}_missing", "task_id": task_id},
                    )
                if key == "done":
                    if task["done"] is preimage["done"]:
                        continue
                    if (
                        recurring_complete
                        and status == 200
                        and postimage is not None
                        and "done" in postimage
                        and task["done"] is postimage["done"]
                    ):
                        continue
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "omitted_done_changed", "task_id": task_id},
                    )
                if task[key] == preimage[key]:
                    continue
                if key == "description" and recurring_complete and status == 200 and postimage is not None:
                    # Stock may reset HTML checklist; allow only that delta vs preimage, and
                    # require GET to witness the same postimage description.
                    if (
                        "description" in postimage
                        and task["description"] == postimage["description"]
                        and self._recurring_description_delta_allowed(
                            preimage["description"],
                            task["description"],
                            preimage_marker,
                        )
                    ):
                        continue
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": f"omitted_{key}_changed", "task_id": task_id},
                )
            if "due_date" not in body:
                if "due_date" not in task or "due_date" not in preimage:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "omitted_due_missing", "task_id": task_id},
                    )
                if not same_stock_persisted_instant(task["due_date"], preimage["due_date"]):
                    if not recurring_complete:
                        return TaskResult(
                            outcome=Outcome.UNKNOWN,
                            task=task,
                            http_status=read_back.http_status,
                            mutation_sent=True,
                            details={"reason": "omitted_due_changed", "task_id": task_id},
                        )
                    if status == 304:
                        return TaskResult(
                            outcome=Outcome.UNKNOWN,
                            task=task,
                            http_status=read_back.http_status,
                            mutation_sent=True,
                            details={"reason": "due_changed_after_304", "task_id": task_id},
                        )
                    if (
                        postimage is None
                        or "due_date" not in postimage
                        or not same_stock_persisted_instant(task["due_date"], postimage["due_date"])
                    ):
                        return TaskResult(
                            outcome=Outcome.UNKNOWN,
                            task=task,
                            http_status=read_back.http_status,
                            mutation_sent=True,
                            details={"reason": "due_not_witnessed", "task_id": task_id},
                        )

            # C1 §6: ordinary updates must also preserve omitted recurrence/occurrence
            # fields against the preimage. Detect observed drift (not CAS). Recurring
            # complete may advance start/end/reminders when the PATCH postimage witnesses
            # the same values; due-relative reminder instants may move when due_date is
            # explicitly patched (stock updateRelativeReminderDates).
            for key in ("repeat_after", "repeat_mode"):
                if key in body:
                    continue
                if key not in task and key not in preimage:
                    continue
                if key not in task or key not in preimage:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": f"omitted_{key}_missing", "task_id": task_id},
                    )
                if task[key] == preimage[key]:
                    continue
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": f"omitted_{key}_changed", "task_id": task_id},
                )
            for key in ("start_date", "end_date"):
                if key in body:
                    continue
                if key not in task and key not in preimage:
                    continue
                if key not in task or key not in preimage:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": f"omitted_{key}_missing", "task_id": task_id},
                    )
                if same_stock_persisted_instant(task[key], preimage[key]):
                    continue
                if (
                    recurring_complete
                    and status == 200
                    and postimage is not None
                    and key in postimage
                    and same_stock_persisted_instant(task[key], postimage[key])
                ):
                    continue
                return TaskResult(
                    outcome=Outcome.UNKNOWN,
                    task=task,
                    http_status=read_back.http_status,
                    mutation_sent=True,
                    details={"reason": f"omitted_{key}_changed", "task_id": task_id},
                )
            if "reminders" not in body:
                if "reminders" not in task and "reminders" not in preimage:
                    pass
                elif "reminders" not in task or "reminders" not in preimage:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "omitted_reminders_missing", "task_id": task_id},
                    )
                elif self._reminders_equal(task["reminders"], preimage["reminders"]):
                    pass
                elif (
                    recurring_complete
                    and status == 200
                    and postimage is not None
                    and "reminders" in postimage
                    and self._reminders_equal(task["reminders"], postimage["reminders"])
                ):
                    pass
                elif "due_date" in body and self._reminders_equal(
                    task["reminders"],
                    preimage["reminders"],
                    allow_due_relative_instant_drift=True,
                ):
                    pass
                else:
                    return TaskResult(
                        outcome=Outcome.UNKNOWN,
                        task=task,
                        http_status=read_back.http_status,
                        mutation_sent=True,
                        details={"reason": "omitted_reminders_changed", "task_id": task_id},
                    )
        except ValueError as exc:
            # Malformed dates after dispatch must not escape; retain mutation_sent.
            return TaskResult(
                outcome=Outcome.UNKNOWN,
                task=task,
                http_status=read_back.http_status,
                mutation_sent=True,
                details={
                    "reason": "timestamp_compare_failed",
                    "task_id": task_id,
                    "error": str(exc),
                    "patch_http": status,
                },
            )

        details: dict[str, Any] = {
            "patch_http": status,
            "task_id": task_id,
            "preimage": {
                "id": preimage.get("id"),
                **self._occurrence_receipt(preimage),
            },
        }
        if recurring_complete:
            details["recurring"] = True
            details["recurring_transition"] = recurring_transition
            if status == 304:
                # Reached only after full occurrence matched preimage (no-op witness).
                details["noop_304"] = True
            if postimage is not None:
                details["postimage"] = self._occurrence_receipt(postimage)
            details["occurrence"] = self._occurrence_receipt(task)
        return TaskResult(
            outcome=Outcome.CONFIRMED,
            task=task,
            http_status=read_back.http_status,
            mutation_sent=True,
            details=details,
        )

    def complete(self, task_id: int) -> TaskResult:
        """Send at most one {"done": true} PATCH. Recurring may stay done=false."""
        pre = self.read(task_id)
        if not pre.ok or pre.task is None:
            return TaskResult(
                outcome=pre.outcome if pre.outcome is not Outcome.CONFIRMED else Outcome.UNKNOWN,
                http_status=pre.http_status,
                details={"phase": "preimage", **pre.details},
            )
        preimage = pre.task
        try:
            recurring = self._parse_recurrence(preimage)
        except ApiError as exc:
            return TaskResult(
                outcome=Outcome.VALIDATION_ERROR,
                http_status=pre.http_status,
                mutation_sent=False,
                details={"phase": "preimage", "reason": str(exc)},
            )
        if preimage.get("done") is True:
            # Already done (recurring or not): return fresh fact without a new mutation.
            return TaskResult(
                outcome=Outcome.CONFIRMED,
                task=preimage,
                http_status=pre.http_status,
                mutation_sent=False,
                details={"noop": True, "recurring": recurring},
            )
        # Exactly one mutation attempt; never loop until done=true.
        result = self.update(task_id, {"done": True})
        if result.task is not None or result.details:
            result.details.setdefault("recurring", recurring)
            result.details.setdefault("due_before", preimage.get("due_date"))
            if result.task is not None:
                result.details.setdefault("due_after", result.task.get("due_date"))
                result.details.setdefault("done_after", result.task.get("done"))
            result.details.setdefault(
                "preimage",
                {
                    "id": preimage.get("id"),
                    **self._occurrence_receipt(preimage),
                },
            )
        return result
