"""Unit and focused checks for stock API client and MSK dates (s02)."""

from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vikunja_local.api import (  # noqa: E402
    ALLOWED_PATCH_FIELDS,
    ApiError,
    Outcome,
    TokenRef,
    VikunjaClient,
    build_create_description,
    build_marker,
    encode_unmarked_description,
    extract_marker,
    html_escape_user_portion,
    reset_description_checklist,
)
from vikunja_local.dates import (  # noqa: E402
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

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")


class DateUnitTests(unittest.TestCase):
    def test_date_only_becomes_msk_end_of_day(self) -> None:
        confirmed = datetime(2026, 11, 9, 10, 0, tzinfo=MSK)
        due = confirm_date_only("2026-11-30", confirmed_at=confirmed)
        self.assertEqual(due.kind, DueKind.DATE)
        self.assertEqual(due.absolute.isoformat(), "2026-11-30T23:59:59+03:00")
        self.assertEqual(due.wire_value(), "2026-11-30T23:59:59+03:00")

    def test_relative_frozen_at_confirmation_not_replay(self) -> None:
        confirmed = datetime(2026, 11, 9, 23, 50, tzinfo=MSK)
        due = confirm_relative_msk("завтра", confirmed_at=confirmed, days=1)
        self.assertEqual(due.absolute.date(), date(2026, 11, 10))
        # Replay after midnight must keep frozen absolute.
        later = datetime(2026, 11, 10, 0, 30, tzinfo=MSK)
        self.assertEqual(due.absolute, confirm_date_only("2026-11-10", confirmed_at=later).absolute)

    def test_zero_date_is_absence(self) -> None:
        self.assertTrue(is_absent_due(None))
        self.assertTrue(is_absent_due("0001-01-01T00:00:00Z"))
        self.assertTrue(is_absent_due("0001-01-01T00:00:00+00:00"))
        self.assertTrue(is_absent_due("0001-01-01T03:00:00+03:00"))
        self.assertIsNone(normalize_due_value("0001-01-01T00:00:00Z"))
        self.assertTrue(same_instant(None, "0001-01-01T00:00:00Z"))
        self.assertFalse(is_absent_due("2026-11-30T20:59:59Z"))
        # C1 §7: blank/whitespace is not omission/null/zero absence.
        self.assertFalse(is_absent_due(""))
        self.assertFalse(is_absent_due("   "))
        with self.assertRaises(ValueError):
            normalize_due_value("")
        with self.assertRaises(ValueError):
            normalize_due_value("   ")

    def test_malformed_zero_date_is_not_absence(self) -> None:
        """Prefix lookalikes are not stock zero dates (C1 §6/§7)."""
        bad = "0001-01-01T00:00:00-not-a-date"
        self.assertFalse(is_absent_due(bad))
        with self.assertRaises(ValueError):
            normalize_due_value(bad)
        with self.assertRaises(ValueError):
            require_rfc3339_instant(bad)

    def test_malformed_wire_date_raises_on_compare(self) -> None:
        with self.assertRaises(ValueError):
            normalize_due_value("invalid-date")
        with self.assertRaises(ValueError):
            same_instant("invalid-date", "2026-12-01T20:59:59Z")
        with self.assertRaises(ValueError):
            require_rfc3339_instant("invalid-date")
        with self.assertRaises(ValueError):
            # Offset pushes year-1 below the representable UTC range.
            require_rfc3339_instant("0001-01-01T00:00:00+14:00")

    def test_datetime_confirmation_preserves_instant(self) -> None:
        value = datetime(2026, 11, 30, 6, 0, tzinfo=timezone.utc)
        due = confirm_datetime(value, confirmed_at=datetime.now(tz=MSK), expression="explicit")
        self.assertEqual(due.kind, DueKind.DATETIME)
        self.assertEqual(due.absolute, value)

    def test_fractional_target_due_rejected_before_freeze(self) -> None:
        """Caller-frozen dues are whole-second only (stock truncates; F1)."""
        self.assertEqual(TARGET_DUE_PRECISION, "second")
        fractional = datetime(2026, 12, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            require_target_due_second_precision(fractional)
        with self.assertRaises(ValueError):
            confirm_datetime(fractional, confirmed_at=datetime.now(tz=MSK))
        with self.assertRaises(ValueError):
            canonicalize_rfc3339_instant("2026-12-01T12:00:00.123456Z")
        # Sub-microsecond nonzero tails must not truncate into an accepted second.
        for bad in (
            "2026-12-01T12:00:00.0000001Z",
            "2026-12-01T12:00:00.000000999Z",
            "2026-12-01T12:00:00.000000001Z",
        ):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    canonicalize_rfc3339_instant(bad)
                with self.assertRaises(ValueError):
                    require_rfc3339_instant(bad)
        # Zero fractions (including >6 digits) still canonicalize to whole-second wire.
        for zero in (
            "2026-12-01T12:00:00.000Z",
            "2026-12-01T12:00:00.000000Z",
            "2026-12-01T12:00:00.000000000Z",
        ):
            with self.subTest(value=zero):
                self.assertEqual(
                    canonicalize_rfc3339_instant(zero),
                    "2026-12-01T12:00:00Z",
                )
        # Read-back parsers still accept server fractional strings within microseconds.
        parsed = require_rfc3339_instant("2026-12-01T12:00:00.123456Z")
        self.assertEqual(parsed.microsecond, 123456)
        self.assertFalse(same_instant(parsed, "2026-12-01T12:00:00Z"))
        self.assertTrue(same_stock_persisted_instant(parsed, "2026-12-01T12:00:00Z"))

    def test_require_rfc3339_instant_rejects_naive_and_date_only(self) -> None:
        self.assertEqual(
            require_rfc3339_instant("2026-11-30T23:59:59+03:00"),
            datetime(2026, 11, 30, 23, 59, 59, tzinfo=MSK),
        )
        self.assertEqual(
            require_rfc3339_instant("2026-11-30T20:59:59Z").tzinfo,
            timezone.utc,
        )
        self.assertEqual(
            canonicalize_rfc3339_instant("2026-11-30t20:59:59z"),
            "2026-11-30T20:59:59Z",
        )
        for bad in (
            "tomorrow",
            "2026-11-30",
            "2026-11-30T20:59:59",
            "",
            "20261130T205959Z",
            "2026-W49-1T20:59:59Z",
            "2026-11-30T20:59Z",
            "2026-11-30T20:59:59+00:00:30",
        ):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    require_rfc3339_instant(bad)

    def test_stock_persisted_second_precision(self) -> None:
        """Pinned stock may truncate PATCH usecs on mandatory GET (mode=2)."""
        patch_due = "2026-11-11T00:18:48.972809+03:00"
        get_due = "2026-11-10T21:18:48Z"
        self.assertTrue(same_stock_persisted_instant(patch_due, get_due))
        self.assertFalse(same_instant(patch_due, get_due))
        self.assertFalse(
            same_stock_persisted_instant(patch_due, "2026-11-10T21:18:49Z")
        )


class MarkerUnitTests(unittest.TestCase):
    def test_marker_and_html_escape_in_same_create_description(self) -> None:
        issuer = "11111111-1111-4111-8111-111111111111"
        operation = "22222222-2222-4222-8222-222222222222"
        description = build_create_description(issuer, operation, 'A & B <C> "D" \'E\'')
        self.assertTrue(description.startswith(f"EA010:v1:{issuer}:{operation}\n"))
        self.assertIn("&amp;", description)
        self.assertIn("&lt;C&gt;", description)
        extracted = extract_marker(description)
        self.assertIsNotNone(extracted)
        assert extracted is not None
        self.assertEqual(extracted["marker"], build_marker(issuer, operation))
        self.assertEqual(extracted["user_portion"], html_escape_user_portion('A & B <C> "D" \'E\''))

    def test_extract_rejects_uppercase_or_missing_lf(self) -> None:
        self.assertIsNone(extract_marker("EA010:V1:x:y\nsuffix"))
        self.assertIsNone(extract_marker("EA010:v1:11111111-1111-4111-8111-111111111111:"
                                         "22222222-2222-4222-8222-222222222222"))


class TokenRefUnitTests(unittest.TestCase):
    def test_loads_owner_only_file_without_logging_secret(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            os.chmod(root, 0o700)
            path = root / "token"
            secret = "synthetic-token-value"
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, secret.encode())
            finally:
                os.close(fd)
            os.chmod(path, 0o600)
            loaded = TokenRef(kind="file", path=path).load()
            self.assertEqual(loaded, secret)

    def test_rejects_group_readable_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            os.chmod(root, 0o700)
            path = root / "token"
            path.write_text("x", encoding="utf-8")
            os.chmod(path, 0o640)
            with self.assertRaises(ApiError) as ctx:
                TokenRef(kind="file", path=path).load()
            self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)


class ClientUnitTests(unittest.TestCase):
    def _client(self, token_path: Path) -> VikunjaClient:
        return VikunjaClient(
            api_origin="http://127.0.0.1:19345",
            project_id=2,
            token_ref=TokenRef(kind="file", path=token_path),
            token="synthetic-in-memory-only",
            page_size=1,
            max_items_per_page=50,
        )

    def _token_file(self) -> Path:
        root = Path(tempfile.mkdtemp())
        os.chmod(root, 0o700)
        path = root / "t"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, b"tok")
        finally:
            os.close(fd)
        os.chmod(path, 0o600)
        self.addCleanup(lambda: __import__("shutil").rmtree(root, ignore_errors=True))
        return path

    def _recurring_task(self, **overrides: object) -> dict:
        marker = build_marker(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )
        task = {
            "id": 7,
            "project_id": 2,
            "title": "r",
            "description": marker + "\ncontext",
            "due_date": "2026-12-01T20:59:59Z",
            "done": False,
            "done_at": "0001-01-01T00:00:00Z",
            "start_date": "2026-12-01T10:00:00Z",
            "end_date": "2026-12-01T11:00:00Z",
            "repeat_after": 86400,
            "repeat_mode": 0,
            "reminders": [],
        }
        task.update(overrides)
        return task

    def test_plaintext_nonloopback_origin_rejected(self) -> None:
        """HTTP cleartext outside Mini loopback is fail-closed before bearer use."""
        token = self._token_file()
        for origin in (
            "http://198.51.100.10",
            "http://198.51.100.10:3456",
            "https://198.51.100.10",
            "http://127.0.0.1:19345/api",
            "http://127.0.0.1:19345?x=1",
        ):
            with self.subTest(origin=origin):
                with self.assertRaises(ApiError) as ctx:
                    VikunjaClient(
                        api_origin=origin,
                        project_id=2,
                        token_ref=TokenRef(kind="file", path=token),
                        token="synthetic-must-not-be-sent",
                    )
                self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)

    def test_missing_due_readback_stays_unknown(self) -> None:
        """Absent due_date key is not an explicit null clear witness (C1 §4/§6)."""
        client = self._client(self._token_file())
        base = {
            "id": 7,
            "project_id": 2,
            "title": "old",
            "description": "plain",
            "done": False,
            "due_date": "2026-11-30T20:59:59Z",
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        missing = {k: v for k, v in base.items() if k != "due_date"}
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, base, {}), (200, missing, {}), (200, missing, {})],
        ) as wire:
            result = client.update(7, {"due_date": None})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)

    def test_malformed_zero_readback_stays_unknown(self) -> None:
        """Malformed zero-prefix due must not confirm a null clear (C1 §6/§7)."""
        client = self._client(self._token_file())
        base = {
            "id": 7,
            "project_id": 2,
            "title": "old",
            "description": "plain",
            "done": False,
            "due_date": "2026-11-30T20:59:59Z",
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        bad = dict(base, due_date="0001-01-01T00:00:00-not-a-date")
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, base, {}), (200, bad, {}), (200, bad, {})],
        ) as wire:
            result = client.update(7, {"due_date": None})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)

    def test_explicit_null_due_still_confirms_when_witnessed(self) -> None:
        client = self._client(self._token_file())
        base = {
            "id": 7,
            "project_id": 2,
            "title": "old",
            "description": "plain",
            "done": False,
            "due_date": "2026-11-30T20:59:59Z",
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        cleared = dict(base, due_date=None)
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, base, {}), (200, cleared, {}), (200, cleared, {})],
        ):
            result = client.update(7, {"due_date": None})
        self.assertTrue(result.ok)
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.CONFIRMED)

    def test_patch_allows_only_explicit_fields(self) -> None:
        client = self._client(self._token_file())
        with self.assertRaises(ApiError):
            client._validate_patch({"priority": 1})
        with self.assertRaises(ApiError):
            client._validate_patch({"title": None})
        with self.assertRaises(ApiError):
            client._validate_patch({"done": "true"})
        with self.assertRaises(ApiError) as ctx:
            client._validate_patch({"done": True, "title": "changed"})
        self.assertIn("separate", str(ctx.exception))
        body = client._validate_patch({"due_date": None, "title": "x"})
        self.assertEqual(body, {"due_date": None, "title": "x"})
        self.assertEqual(ALLOWED_PATCH_FIELDS, frozenset({"title", "description", "due_date", "done"}))
        ok_due = client._validate_patch({"due_date": "2026-11-30T23:59:59+03:00"})
        self.assertEqual(ok_due["due_date"], "2026-11-30T23:59:59+03:00")

    def test_invalid_due_date_fails_before_http(self) -> None:
        client = self._client(self._token_file())
        for bad in (
            "tomorrow",
            "2026-11-30",
            "2026-11-30T20:59:59",
            "20261130T205959Z",
            "2026-W49-1T20:59:59Z",
            "2026-11-30T20:59Z",
            "2026-11-30T20:59:59+00:00:30",
            "2026-12-01T12:00:00.123456Z",
            "",
            "   ",
        ):
            with self.subTest(value=bad):
                with self.assertRaises(ApiError) as ctx:
                    client._validate_patch({"due_date": bad})
                self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)
                with mock.patch.object(client, "_request") as wire:
                    result = client.update(7, {"due_date": bad})
                self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
                self.assertFalse(result.mutation_sent)
                wire.assert_not_called()

    def test_fractional_create_and_update_rejected_before_http(self) -> None:
        """Create/update share the same whole-second target gate (F1)."""
        client = self._client(self._token_file())
        fractional = "2026-12-01T12:00:00.123456Z"
        with mock.patch.object(client, "_request") as wire:
            result = client.update(7, {"due_date": fractional})
        self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
        self.assertFalse(result.mutation_sent)
        wire.assert_not_called()

        # Independent update regression: sub-microsecond nonzero must not mutate.
        for bad in (
            "2026-12-01T12:00:00.0000001Z",
            "2026-12-01T12:00:00.000000999Z",
        ):
            with self.subTest(value=bad):
                with mock.patch.object(client, "_request") as wire:
                    result = client.update(7, {"due_date": bad})
                self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
                self.assertFalse(result.mutation_sent)
                wire.assert_not_called()

        hand_built = ConfirmedDue(
            kind=DueKind.DATETIME,
            confirmed_at=datetime(2026, 11, 9, 12, 0, tzinfo=MSK),
            timezone_name="Europe/Moscow",
            expression=fractional,
            absolute=datetime(2026, 12, 1, 12, 0, 0, 123456, tzinfo=timezone.utc),
        )
        with mock.patch.object(client, "_request") as wire:
            with self.assertRaises(ApiError) as ctx:
                client.create(
                    title="frac",
                    issuer_id="11111111-1111-4111-8111-111111111111",
                    operation_id="22222222-2222-4222-8222-222222222222",
                    due=hand_built,
                )
        self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)
        wire.assert_not_called()

    def test_update_target_due_uses_exact_instant_not_stock_truncate(self) -> None:
        """After validation, update confirms frozen targets via same_instant (F1)."""
        client = self._client(self._token_file())
        marker = build_create_description(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
            "",
        )
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": marker,
            "done": False,
            "due_date": "0001-01-01T00:00:00Z",
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        # Truncated stock persistence of a whole-second target must confirm.
        ok = dict(pre, due_date="2026-12-01T12:00:00Z")
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, ok, {}), (200, ok, {})],
        ) as wire:
            confirmed = client.update(7, {"due_date": "2026-12-01T12:00:00Z"})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertTrue(confirmed.ok)
        self.assertTrue(same_instant(confirmed.task["due_date"], "2026-12-01T12:00:00Z"))

        # Exact-instant mismatch stays unknown (no microsecond-dropping shortcut).
        truncated = dict(pre, due_date="2026-12-01T12:00:00Z")
        # Bypass _validate_patch by feeding a body that would formerly match via
        # same_stock_persisted_instant while failing same_instant.
        with mock.patch.object(client, "_validate_patch", return_value={"due_date": "2026-12-01T12:00:00.123456Z"}):
            with mock.patch.object(
                client,
                "_request",
                side_effect=[(200, pre, {}), (200, truncated, {}), (200, truncated, {})],
            ):
                mismatched = client.update(7, {"due_date": "ignored"})
        self.assertEqual(mismatched.outcome, Outcome.UNKNOWN)
        self.assertEqual(mismatched.details.get("reason"), "due_mismatch")
        self.assertFalse(same_instant(truncated["due_date"], "2026-12-01T12:00:00.123456Z"))
        self.assertTrue(
            same_stock_persisted_instant(truncated["due_date"], "2026-12-01T12:00:00.123456Z")
        )

    def test_blank_due_response_stays_unknown(self) -> None:
        """Blank due_date is not null/zero absence on read-back (C1 §7)."""
        client = self._client(self._token_file())
        base = {
            "id": 7,
            "project_id": 2,
            "title": "old",
            "description": "plain",
            "done": False,
            "due_date": "2026-11-30T20:59:59Z",
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        blank = dict(base, due_date="")
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, base, {}), (200, blank, {}), (200, blank, {})],
        ) as wire:
            result = client.update(7, {"due_date": None})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)

    def test_incomplete_core_fields_block_title_only_confirm(self) -> None:
        """Missing description/done/due_date must not confirm via .get() equality."""
        client = self._client(self._token_file())
        incomplete = {"id": 7, "project_id": 2, "title": "old"}
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, incomplete, {})],
        ) as wire:
            result = client.update(7, {"title": "new"})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET"])
        self.assertFalse(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)

    def test_malformed_recurring_response_stays_unknown(self) -> None:
        """Faulty typed recurrence/reminder witnesses must not confirm (C1 §4/§6)."""
        client = self._client(self._token_file())
        pre = self._recurring_task()
        bad = dict(
            pre,
            done=False,
            due_date="2026-12-02T20:59:59Z",
            repeat_mode=False,
            repeat_after=86400.0,
            reminders=["not-a-reminder"],
        )
        with mock.patch.object(
            client,
            "_request",
            # complete() pre-reads, then update() GET/PATCH/GET.
            side_effect=[(200, pre, {}), (200, pre, {}), (200, bad, {}), (200, bad, {})],
        ) as wire:
            result = client.complete(7)
        self.assertEqual(
            [c.args[0] for c in wire.call_args_list],
            ["GET", "GET", "PATCH", "GET"],
        )
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)

    def test_mode2_stock_precision_witness_confirms(self) -> None:
        """mode=2 PATCH usecs vs GET truncated second must witness once."""
        client = self._client(self._token_file())
        pre = self._recurring_task(repeat_mode=2, repeat_after=86400)
        post = dict(
            pre,
            done=False,
            due_date="2026-11-11T00:18:48.972809+03:00",
            done_at="2026-11-10T00:18:48.972811+03:00",
        )
        got = dict(
            pre,
            done=False,
            due_date="2026-11-10T21:18:48Z",
            done_at="2026-11-09T21:18:48Z",
        )
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (200, post, {}), (200, got, {})],
        ) as wire:
            result = client.complete(7)
        self.assertEqual(
            [c.args[0] for c in wire.call_args_list],
            ["GET", "GET", "PATCH", "GET"],
        )
        self.assertTrue(result.ok)
        self.assertTrue(result.mutation_sent)
        self.assertTrue(result.details.get("recurring_transition"))

    def test_forbids_v1_post_update_path(self) -> None:
        client = self._client(self._token_file())
        with self.assertRaises(ApiError) as ctx:
            client._request("POST", "/api/v1/tasks/1", body={"done": True})
        self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)

    def test_304_requires_get_without_retry_patch(self) -> None:
        """304 read-back uses a genuine field PATCH; done=true on already-done is a no-op."""
        client = self._client(self._token_file())
        preimage = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": "0001-01-01T00:00:00Z",
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        calls: list[tuple[str, str]] = []

        def fake_request(method, path, **kwargs):
            calls.append((method, path))
            if method == "GET" and path.endswith("/7"):
                return 200, preimage, {}
            if method == "PATCH":
                self.assertEqual(kwargs.get("body"), {"title": "t"})
                return 304, None, {}
            raise AssertionError((method, path, kwargs))

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.update(7, {"title": "t"})
        self.assertTrue(result.ok)
        self.assertTrue(result.mutation_sent)
        self.assertEqual([c[0] for c in calls], ["GET", "PATCH", "GET"])
        self.assertEqual(result.details.get("patch_http"), 304)

    def test_update_done_true_on_already_done_is_noop(self) -> None:
        client = self._client(self._token_file())
        preimage = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": None,
            "done": True,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        with mock.patch.object(client, "_request", side_effect=[(200, preimage, {})]) as wire:
            result = client.update(7, {"done": True})
        self.assertTrue(result.ok)
        self.assertFalse(result.mutation_sent)
        self.assertTrue(result.details.get("noop"))
        self.assertFalse(any(args[0][0] == "PATCH" for args in wire.call_args_list))

    def test_recurring_complete_sends_one_patch_even_if_done_false(self) -> None:
        client = self._client(self._token_file())
        preimage = self._recurring_task(id=9)
        postimage = dict(preimage)
        postimage["due_date"] = "2026-12-02T20:59:59Z"
        postimage["done"] = False
        postimage["done_at"] = "2026-11-09T20:00:00Z"
        patch_calls = {"n": 0}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                # First GET preimage, later GET postimage.
                if patch_calls["n"] == 0:
                    return 200, preimage, {}
                return 200, postimage, {}
            if method == "PATCH":
                patch_calls["n"] += 1
                body = kwargs.get("body")
                self.assertEqual(body, {"done": True})
                self.assertEqual(kwargs.get("content_type"), "application/merge-patch+json")
                return 200, postimage, {}
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.complete(9)
        self.assertTrue(result.ok)
        self.assertEqual(patch_calls["n"], 1)
        self.assertIs(result.task.get("done"), False)
        self.assertTrue(result.details.get("recurring"))
        self.assertEqual(result.details["occurrence"]["start_date"], preimage["start_date"])
        self.assertEqual(result.details["occurrence"]["end_date"], preimage["end_date"])

    def test_network_error_is_dependency_unavailable_not_empty(self) -> None:
        client = self._client(self._token_file())
        with mock.patch.object(client, "_request", side_effect=ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "network")):
            with self.assertRaises(ApiError) as ctx:
                client.read(1)
            self.assertEqual(ctx.exception.outcome, Outcome.DEPENDENCY_UNAVAILABLE)

    def test_unknown_mutation_does_not_retry(self) -> None:
        client = self._client(self._token_file())
        preimage = {
            "id": 3,
            "project_id": 2,
            "title": "t",
            "description": "EA010:v1:11111111-1111-4111-8111-111111111111:"
            "22222222-2222-4222-8222-222222222222\n",
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        calls = {"patch": 0}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                return 200, preimage, {}
            if method == "PATCH":
                calls["patch"] += 1
                raise ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "timeout after dispatch")
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.update(3, {"title": "x"})
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)
        self.assertEqual(calls["patch"], 1)

    def test_create_verifies_title_context_and_due(self) -> None:
        client = self._client(self._token_file())
        issuer = "11111111-1111-4111-8111-111111111111"
        operation = "22222222-2222-4222-8222-222222222222"
        desc = build_create_description(issuer, operation, "confirmed & context")
        due = confirm_date_only("2026-11-30", confirmed_at=datetime(2026, 11, 9, tzinfo=MSK))
        created = {
            "id": 7,
            "project_id": 2,
            "title": "synthetic",
            "description": desc,
            "due_date": due.wire_value(),
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        wrong = dict(created)
        wrong["title"] = "DIFFERENT"
        wrong["description"] = desc.split("\n", 1)[0] + "\nCHANGED"
        wrong["due_date"] = None
        with mock.patch.object(
            client, "_request", side_effect=[(201, created, {}), (200, wrong, {})]
        ) as call:
            result = client.create(
                title="synthetic",
                issuer_id=issuer,
                operation_id=operation,
                user_portion="confirmed & context",
                due=due,
            )
        self.assertFalse(result.ok)
        self.assertEqual(call.call_count, 2)

    def test_create_without_due_must_verify_absence(self) -> None:
        client = self._client(self._token_file())
        issuer = "11111111-1111-4111-8111-111111111111"
        operation = "22222222-2222-4222-8222-222222222222"
        created = {
            "id": 7,
            "project_id": 2,
            "title": "synthetic",
            "description": build_create_description(issuer, operation),
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
            "due_date": "2026-12-01T20:59:59Z",
            "done_at": "0001-01-01T00:00:00Z",
            "start_date": None,
            "end_date": None,
            "reminders": [],
        }
        with mock.patch.object(
            client, "_request", side_effect=[(201, created, {}), (200, created, {})]
        ) as wire:
            result = client.create(title="synthetic", issuer_id=issuer, operation_id=operation)
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["PUT", "GET"])
        self.assertFalse(any(c.args[0] == "PATCH" for c in wire.call_args_list))

    def test_done_false_is_verified(self) -> None:
        client = self._client(self._token_file())
        preimage = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": None,
            "done": True,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        with mock.patch.object(
            client, "_request", side_effect=[(200, preimage, {}), (200, preimage, {}), (200, preimage, {})]
        ):
            result = client.update(7, {"done": False})
        self.assertFalse(result.ok)

    def test_recurring_304_is_not_confirmed_transition(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task()
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (304, None, {}), (200, pre, {})],
        ):
            result = client.complete(7)
        self.assertTrue(result.ok)
        self.assertFalse(result.details.get("recurring_transition", False))
        self.assertTrue(result.details.get("noop_304"))

    def test_recurring_200_unchanged_occurrence_is_not_transition(self) -> None:
        """F2: identical preimage/postimage/GET must not claim recurring_transition."""
        client = self._client(self._token_file())
        pre = self._recurring_task(done=False)
        patch_calls = {"n": 0}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                return 200, pre, {}
            if method == "PATCH":
                patch_calls["n"] += 1
                self.assertEqual(kwargs.get("body"), {"done": True})
                # Four complete typed responses with the same unchanged occurrence.
                return 200, pre, {}
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.complete(7)
        self.assertEqual(patch_calls["n"], 1)
        self.assertTrue(result.ok)
        self.assertTrue(result.mutation_sent)
        self.assertTrue(result.details.get("recurring"))
        self.assertFalse(result.details.get("recurring_transition", False))
        self.assertNotEqual(result.details.get("noop_304"), True)

    def test_recurring_304_done_at_mismatch_is_unknown(self) -> None:
        """C1 §6: 304 without a full occurrence no-op witness cannot be confirmed."""
        client = self._client(self._token_file())
        pre = self._recurring_task()
        observed = dict(pre)
        observed["done_at"] = "2026-11-09T20:00:00Z"
        patch_calls = {"n": 0}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                if patch_calls["n"] == 0:
                    return 200, pre, {}
                return 200, observed, {}
            if method == "PATCH":
                patch_calls["n"] += 1
                return 304, None, {}
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.complete(7)
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertEqual(result.details.get("reason"), "recurring_304_occurrence_mismatch")
        self.assertEqual(patch_calls["n"], 1)
        self.assertNotEqual(result.details.get("noop_304"), True)

    def test_description_patch_preserves_verified_marker(self) -> None:
        """Empty U and substituted headers must keep the verified preimage marker."""
        client = self._client(self._token_file())
        issuer = "11111111-1111-4111-8111-111111111111"
        operation = "22222222-2222-4222-8222-222222222222"
        marker = build_marker(issuer, operation)
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": marker + "\nold context",
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        fake_other = build_marker(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        )

        for user_portion, expected_suffix in (
            ("", ""),
            (fake_other + "\nsubstituted", html_escape_user_portion(fake_other + "\nsubstituted")),
            ('A & B', "A &amp; B"),
        ):
            with self.subTest(user_portion=user_portion):
                expected_description = marker + "\n" + expected_suffix
                after = dict(pre)
                after["description"] = expected_description
                captured: dict[str, object] = {}

                def fake_request(method, path, **kwargs):
                    if method == "GET":
                        return 200, after if captured.get("patched") else pre, {}
                    if method == "PATCH":
                        captured["patched"] = True
                        captured["body"] = kwargs.get("body")
                        return 200, after, {}
                    raise AssertionError(method)

                with mock.patch.object(client, "_request", side_effect=fake_request):
                    result = client.update(7, {"description": user_portion})
                self.assertTrue(result.ok, result.details)
                self.assertEqual(captured["body"], {"description": expected_description})
                extracted = extract_marker(result.task["description"])
                self.assertIsNotNone(extracted)
                assert extracted is not None
                self.assertEqual(extracted["marker"], marker)
                self.assertEqual(extracted["user_portion"], expected_suffix)

    def test_unmarked_description_escapes_and_rejects_marker_mint(self) -> None:
        """C1 §3: unmarked manual tasks use E(U) and must not mint an integration header."""
        plain = "<b>plain & context</b>"
        self.assertEqual(encode_unmarked_description(plain), html_escape_user_portion(plain))
        self.assertIsNone(extract_marker(encode_unmarked_description(plain)))

        issuer = "11111111-1111-4111-8111-111111111111"
        operation = "22222222-2222-4222-8222-222222222222"
        mint = build_marker(issuer, operation) + "\nsmuggled"
        with self.assertRaises(ApiError) as ctx:
            encode_unmarked_description(mint)
        self.assertEqual(ctx.exception.outcome, Outcome.VALIDATION_ERROR)

        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "manual",
            "description": "owner typed note",
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        escaped = html_escape_user_portion(plain)
        after = dict(pre)
        after["description"] = escaped
        captured: dict[str, object] = {}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                return 200, after if captured.get("patched") else pre, {}
            if method == "PATCH":
                captured["patched"] = True
                captured["body"] = kwargs.get("body")
                return 200, after, {}
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            ok = client.update(7, {"description": plain})
        self.assertTrue(ok.ok, ok.details)
        self.assertEqual(captured["body"], {"description": escaped})
        self.assertIsNone(extract_marker(ok.task["description"]))

        with mock.patch.object(client, "_request", side_effect=[(200, pre, {})]) as wire:
            denied = client.update(7, {"description": mint})
        self.assertEqual(denied.outcome, Outcome.VALIDATION_ERROR)
        self.assertFalse(denied.mutation_sent)
        self.assertFalse(any(args[0][0] == "PATCH" for args in wire.call_args_list))

    def test_omitted_done_mismatch_cannot_confirm_title_edit(self) -> None:
        """C1 §6: title-only PATCH must not confirm when observed done diverges."""
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "old",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        after = dict(pre)
        after["title"] = "new"
        after["done"] = True
        gets = {"n": 0}

        def fake_request(method, path, **kwargs):
            if method == "GET":
                gets["n"] += 1
                if gets["n"] == 1:
                    return 200, pre, {}
                return 200, after, {}
            if method == "PATCH":
                self.assertEqual(kwargs.get("body"), {"title": "new"})
                return 200, after, {}
            raise AssertionError(method)

        with mock.patch.object(client, "_request", side_effect=fake_request):
            result = client.update(7, {"title": "new"})
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertEqual(result.details.get("reason"), "omitted_done_changed")

    def test_absolute_reminder_empty_relative_to_is_readable(self) -> None:
        """Stock absolute reminders use relative_to=\"\"; must not block read/complete."""
        client = self._client(self._token_file())
        task = {
            "id": 7,
            "project_id": 2,
            "title": "with-reminder",
            "description": "synthetic absolute reminder",
            "due_date": "2026-12-01T12:00:00Z",
            "done": False,
            "done_at": None,
            "start_date": None,
            "end_date": None,
            "repeat_after": 0,
            "repeat_mode": 0,
            "reminders": [
                {
                    "reminder": "2026-12-01T11:00:00Z",
                    "relative_period": 0,
                    "relative_to": "",
                }
            ],
        }
        with mock.patch.object(client, "_request", return_value=(200, task, {})):
            observed = client.read(7)
        self.assertTrue(observed.ok, observed.details)
        self.assertEqual(observed.task["reminders"][0]["relative_to"], "")

        done = dict(task, done=True, done_at="2026-11-09T20:00:00Z")
        with mock.patch.object(
            client,
            "_request",
            # complete() reads once, then update() reads again before the single PATCH.
            side_effect=[(200, task, {}), (200, task, {}), (200, done, {}), (200, done, {})],
        ) as wire:
            completed = client.complete(7)
        self.assertEqual(
            [c.args[0] for c in wire.call_args_list],
            ["GET", "GET", "PATCH", "GET"],
        )
        self.assertTrue(completed.ok, completed.details)
        self.assertTrue(completed.mutation_sent)
        self.assertEqual(sum(1 for c in wire.call_args_list if c.args[0] == "PATCH"), 1)

        with self.assertRaises(ApiError) as ctx:
            client._validate_reminders(
                [{"reminder": "2026-12-01T11:00:00Z", "relative_to": "unsupported"}],
                outcome=Outcome.UNKNOWN,
            )
        self.assertEqual(ctx.exception.details.get("reason"), "relative_to")

    def test_title_only_omitted_occurrence_drift_is_unknown(self) -> None:
        """C1 §6: title-only PATCH must not confirm when omitted occurrence fields drift."""
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "synthetic",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": "2026-12-01T12:00:00Z",
            "done": False,
            "done_at": None,
            "start_date": "2026-12-01T10:00:00Z",
            "end_date": "2026-12-01T11:00:00Z",
            "repeat_after": 86400,
            "repeat_mode": 0,
            "reminders": [],
        }
        drifted = dict(
            pre,
            title="renamed",
            repeat_mode=1,
            repeat_after=123,
            start_date="2026-12-05T10:00:00Z",
            end_date="2026-12-05T11:00:00Z",
            reminders=[{"reminder": "2026-12-04T10:00:00Z", "relative_period": 0, "relative_to": ""}],
        )
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, drifted, {}), (200, drifted, {})],
        ) as wire:
            result = client.update(7, {"title": "renamed"})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertTrue(result.mutation_sent)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertIn(
            result.details.get("reason"),
            {
                "omitted_repeat_mode_changed",
                "omitted_repeat_after_changed",
                "omitted_start_date_changed",
                "omitted_end_date_changed",
                "omitted_reminders_changed",
            },
        )
        # No retry after observed drift.
        self.assertEqual(sum(1 for c in wire.call_args_list if c.args[0] == "PATCH"), 1)

    def test_due_patch_allows_due_relative_reminder_side_effect(self) -> None:
        """Stock may recalculate due-relative reminders when due_date is patched."""
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "relative-reminder",
            "description": "plain",
            "due_date": "2026-12-01T12:00:00Z",
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
            "reminders": [
                {
                    "reminder": "2026-12-01T11:00:00Z",
                    "relative_period": -3600,
                    "relative_to": "due_date",
                },
                {
                    "reminder": "2026-11-30T09:00:00Z",
                    "relative_period": 0,
                    "relative_to": "",
                },
            ],
        }
        after = dict(
            pre,
            due_date="2026-12-15T12:00:00Z",
            reminders=[
                {
                    "reminder": "2026-12-15T11:00:00Z",
                    "relative_period": -3600,
                    "relative_to": "due_date",
                },
                {
                    "reminder": "2026-11-30T09:00:00Z",
                    "relative_period": 0,
                    "relative_to": "",
                },
            ],
        )
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, after, {}), (200, after, {})],
        ):
            ok = client.update(7, {"due_date": "2026-12-15T12:00:00Z"})
        self.assertTrue(ok.ok, ok.details)

        abs_drift = dict(
            after,
            reminders=[
                {
                    "reminder": "2026-12-15T11:00:00Z",
                    "relative_period": -3600,
                    "relative_to": "due_date",
                },
                {
                    "reminder": "2026-12-01T09:00:00Z",
                    "relative_period": 0,
                    "relative_to": "",
                },
            ],
        )
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, abs_drift, {}), (200, abs_drift, {})],
        ) as wire:
            bad = client.update(7, {"due_date": "2026-12-15T12:00:00Z"})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertEqual(bad.outcome, Outcome.UNKNOWN)
        self.assertEqual(bad.details.get("reason"), "omitted_reminders_changed")
        self.assertEqual(sum(1 for c in wire.call_args_list if c.args[0] == "PATCH"), 1)

    def test_recurring_matched_response_and_get_still_rejects_preimage_drift(self) -> None:
        """GET==postimage is insufficient when recurrence/context drifted from preimage."""
        client = self._client(self._token_file())
        pre = self._recurring_task()
        marker = extract_marker(pre["description"])
        assert marker is not None

        drifted = dict(pre)
        drifted["due_date"] = "2026-12-02T20:59:59Z"
        drifted["done_at"] = "2026-11-09T20:00:00Z"
        drifted["repeat_after"] = 123
        drifted["repeat_mode"] = 1
        drifted["description"] = marker["marker"] + "\nreplaced context"
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (200, drifted, {}), (200, drifted, {})],
        ):
            result = client.complete(7)
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, Outcome.CONFLICT)
        self.assertEqual(result.details.get("reason"), "recurring_immutable_fields_changed")
        self.assertTrue(result.mutation_sent)
        self.assertNotEqual(result.details.get("recurring_transition"), True)

        checklist_pre = self._recurring_task(
            description=marker["marker"]
            + '\n<p data-checked="true">keep</p><input type="checkbox" checked>'
        )
        context_drift = dict(checklist_pre)
        context_drift["due_date"] = "2026-12-02T20:59:59Z"
        context_drift["done_at"] = "2026-11-09T20:00:00Z"
        context_drift["description"] = marker["marker"] + "\nreplaced non-checklist context"
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (200, checklist_pre, {}),
                (200, checklist_pre, {}),
                (200, context_drift, {}),
                (200, context_drift, {}),
            ],
        ):
            drifted_desc = client.complete(7)
        self.assertEqual(drifted_desc.outcome, Outcome.CONFLICT)
        self.assertEqual(drifted_desc.details.get("reason"), "recurring_description_drift")

        allowed = dict(checklist_pre)
        allowed["due_date"] = "2026-12-02T20:59:59Z"
        allowed["done_at"] = "2026-11-09T20:00:00Z"
        allowed["description"] = reset_description_checklist(checklist_pre["description"])
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (200, checklist_pre, {}),
                (200, checklist_pre, {}),
                (200, allowed, {}),
                (200, allowed, {}),
            ],
        ):
            ok = client.complete(7)
        self.assertTrue(ok.ok, ok.details)
        self.assertTrue(ok.details.get("recurring_transition"))

    def test_recurring_200_postimage_must_agree_with_get(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task()
        post = dict(pre)
        post["due_date"] = "2026-12-02T20:59:59Z"
        post["done_at"] = "2026-11-09T20:00:00Z"
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (200, post, {}), (200, pre, {})],
        ):
            result = client.complete(7)
        self.assertFalse(result.ok)

    def test_recurring_already_done_must_not_patch(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task(done=True)
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (304, None, {}), (200, pre, {})],
        ) as call:
            result = client.complete(7)
        self.assertTrue(result.ok)
        self.assertFalse(result.mutation_sent)
        self.assertFalse(any(args[0][0] == "PATCH" for args in call.call_args_list))

    def test_incomplete_recurrence_preimage_must_not_patch(self) -> None:
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "r",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": "2026-12-01T20:59:59Z",
            "done": False,
        }
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (200, pre, {}),
                (200, pre, {}),
                (200, dict(pre, done=True, repeat_after=0, repeat_mode=0), {}),
                (200, dict(pre, done=True, repeat_after=0, repeat_mode=0), {}),
            ],
        ) as call:
            result = client.complete(7)
        self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
        self.assertFalse(result.mutation_sent)
        self.assertFalse(any(args[0][0] == "PATCH" for args in call.call_args_list))

    def test_incomplete_recurring_witness_cannot_confirm(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task()
        post = {
            k: v
            for k, v in pre.items()
            if k not in ("done", "done_at", "due_date", "start_date", "end_date", "reminders")
        }
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, post, {}), (200, post, {})],
        ) as wire:
            result = client.update(7, {"done": True})
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertFalse(result.ok)
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)

    def test_malformed_date_after_patch_returns_unknown(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task()
        pre["repeat_after"] = 0
        post = dict(pre, title="changed", due_date="invalid-date")
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, post, {}), (200, post, {})],
        ) as wire:
            try:
                result = client.update(7, {"title": "changed"})
            except Exception as exc:  # pragma: no cover - must not escape
                self.fail(f"expected UNKNOWN TaskResult, got {type(exc).__name__}")
        self.assertEqual([c.args[0] for c in wire.call_args_list], ["GET", "PATCH", "GET"])
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)

    def test_recurring_checklist_reset_allowed_when_postimage_matches(self) -> None:
        client = self._client(self._token_file())
        marker = build_marker(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )
        pre = self._recurring_task(
            description=marker + '\n<ul><li data-checked="true">item</li></ul>',
        )
        post = dict(pre)
        post["due_date"] = "2026-12-02T20:59:59Z"
        post["done_at"] = "2026-11-09T20:00:00Z"
        post["description"] = marker + '\n<ul><li data-checked="false">item</li></ul>'
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, pre, {}), (200, post, {}), (200, post, {})],
        ):
            result = client.complete(7)
        self.assertTrue(result.ok)
        self.assertTrue(result.details.get("recurring_transition"))
        self.assertIn('data-checked="false"', result.task["description"])

    def test_recurring_get_must_witness_start_end_and_recurrence(self) -> None:
        client = self._client(self._token_file())
        for field, value in (
            ("start_date", "2027-01-01T00:00:00Z"),
            ("end_date", "2027-01-01T00:00:00Z"),
            ("repeat_after", 172800),
            ("repeat_mode", 2),
            ("reminders", [{"relative_period": 3600}]),
        ):
            with self.subTest(field=field):
                pre = self._recurring_task()
                post = dict(pre)
                post["due_date"] = "2026-12-02T20:59:59Z"
                post["done_at"] = "2026-11-09T20:00:00Z"
                observed = dict(post)
                observed[field] = value
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[
                        (200, pre, {}),
                        (200, pre, {}),
                        (200, post, {}),
                        (200, observed, {}),
                    ],
                ):
                    result = client.complete(7)
                self.assertFalse(result.ok)

    def test_missing_occurrence_fields_must_block_recurring_patch(self) -> None:
        client = self._client(self._token_file())
        # Core task fields are rejected at the typed read boundary as unknown.
        for key in ("done", "due_date", "description"):
            with self.subTest(missing=key):
                pre = self._recurring_task()
                pre.pop(key)
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[(200, pre, {}), (200, pre, {}), (200, pre, {}), (200, pre, {})],
                ) as wire:
                    result = client.complete(7)
                self.assertEqual(result.outcome, Outcome.UNKNOWN)
                self.assertFalse(result.mutation_sent)
                self.assertFalse(any(args[0][0] == "PATCH" for args in wire.call_args_list))
        # Recurrence-only occurrence keys remain a pre-dispatch validation_error.
        for key in ("done_at", "start_date", "end_date", "reminders"):
            with self.subTest(missing=key):
                pre = self._recurring_task()
                pre.pop(key)
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[(200, pre, {}), (200, pre, {}), (200, pre, {}), (200, pre, {})],
                ) as wire:
                    result = client.complete(7)
                self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
                self.assertFalse(result.mutation_sent)
                self.assertFalse(any(args[0][0] == "PATCH" for args in wire.call_args_list))

        # Legitimate null/zero due and empty description are present keys, not omissions.
        for pre in (
            self._recurring_task(due_date=None),
            self._recurring_task(due_date="0001-01-01T00:00:00Z"),
            self._recurring_task(description=build_marker(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
            )
            + "\n"),
        ):
            post = dict(pre)
            post["due_date"] = "2026-12-02T20:59:59Z"
            post["done_at"] = "2026-11-09T20:00:00Z"
            with self.subTest(due=pre.get("due_date"), description=pre.get("description")):
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[(200, pre, {}), (200, pre, {}), (200, post, {}), (200, post, {})],
                ) as wire:
                    result = client.complete(7)
                self.assertTrue(result.ok, result.details)
                self.assertTrue(any(args[0][0] == "PATCH" for args in wire.call_args_list))

    def test_recurring_complete_body_must_be_separate(self) -> None:
        client = self._client(self._token_file())
        pre = self._recurring_task()
        post = dict(pre)
        post["title"] = "changed"
        post["due_date"] = "2026-12-02T20:59:59Z"
        post["done_at"] = "2026-11-09T20:00:00Z"
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, pre, {}), (200, post, {}), (200, post, {})],
        ) as wire:
            result = client.update(7, {"done": True, "title": "changed"})
        bodies = [
            args.kwargs.get("body")
            for args in wire.call_args_list
            if args.args and args.args[0] == "PATCH"
        ]
        self.assertEqual(bodies, [])
        self.assertEqual(result.outcome, Outcome.VALIDATION_ERROR)
        self.assertFalse(result.mutation_sent)

    def test_find_rechecks_marker_after_scan(self) -> None:
        client = self._client(self._token_file())
        marker = build_marker(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )
        listed = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": marker + "\n",
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        page1 = {
            "items": [listed],
            "total": 1,
            "page": 1,
            "per_page": 1,
            "total_pages": 1,
        }
        page2 = {"items": [], "total": 1, "page": 2, "per_page": 1, "total_pages": 1}
        lost = dict(listed)
        lost["description"] = "marker removed"
        with mock.patch.object(
            client,
            "_request",
            side_effect=[(200, page1, {}), (200, page2, {}), (200, lost, {})],
        ):
            result = client.find_by_marker(marker)
        self.assertFalse(result.ok)

    def test_scan_requires_string_description_before_empty_or_match(self) -> None:
        """F1: missing/null/non-string description cannot become EMPTY/exact-one."""
        client = self._client(self._token_file())
        marker = build_marker(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )

        def envelope(items: list[dict], *, total: int | None = None) -> dict:
            count = total if total is not None else len(items)
            return {
                "items": items,
                "total": count,
                "page": 1,
                "per_page": 1,
                "total_pages": 1 if count else 0,
            }

        empty_page = {"items": [], "total": 1, "page": 2, "per_page": 1, "total_pages": 1}
        # Present empty string is readable and may still yield EMPTY for a marker miss.
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (
                    200,
                    envelope([{"id": 7, "project_id": 2, "description": ""}], total=1),
                    {},
                ),
                (200, empty_page, {}),
            ],
        ):
            empty_ok = client.find_by_marker(marker)
        self.assertEqual(empty_ok.outcome, Outcome.EMPTY)

        for bad_description in (
            {"id": 7, "project_id": 2},  # missing
            {"id": 7, "project_id": 2, "description": None},
            {"id": 7, "project_id": 2, "description": ["not", "a", "string"]},
            {"id": 7, "project_id": 2, "description": 12},
        ):
            with self.subTest(description=bad_description.get("description", "<missing>")):
                pages = [
                    (200, envelope([bad_description], total=1), {}),
                    (200, empty_page, {}),
                ]
                with mock.patch.object(client, "_request", side_effect=list(pages)):
                    listed = client.list_project_tasks()
                self.assertEqual(listed.outcome, Outcome.INCOMPLETE_SCAN)
                self.assertEqual(listed.details.get("reason"), "item_description")
                # find_by_marker must not collapse the unreadable page into EMPTY.
                with mock.patch.object(client, "_request", side_effect=list(pages)):
                    found = client.find_by_marker(marker)
                self.assertEqual(found.outcome, Outcome.INCOMPLETE_SCAN)
                self.assertNotEqual(found.outcome, Outcome.EMPTY)

        # A second matching marker must not be concealed by an unreadable sibling row.
        match = {
            "id": 7,
            "project_id": 2,
            "description": marker + "\n",
        }
        sibling_unreadable = {"id": 8, "project_id": 2, "description": None}
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (
                    200,
                    {
                        "items": [match, sibling_unreadable],
                        "total": 2,
                        "page": 1,
                        "per_page": 2,
                        "total_pages": 1,
                    },
                    {},
                ),
                (
                    200,
                    {"items": [], "total": 2, "page": 2, "per_page": 2, "total_pages": 1},
                    {},
                ),
            ],
        ):
            concealed = client.find_by_marker(marker)
        self.assertEqual(concealed.outcome, Outcome.INCOMPLETE_SCAN)
        self.assertNotEqual(concealed.outcome, Outcome.CONFIRMED)
        self.assertNotEqual(concealed.outcome, Outcome.EMPTY)

    def test_paging_rejects_malformed_page_at_list_and_find(self) -> None:
        """C1 §4: page must be a positive non-boolean int; true/1.0 are incomplete."""
        client = self._client(self._token_file())
        marker = build_marker(
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        )

        for bad_page in (True, 1.0, "1", 0, -1, None):
            envelope = {
                "items": [],
                "total": 0,
                "page": bad_page,
                "per_page": 1,
                "total_pages": 0,
            }
            with self.subTest(page=bad_page):
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[(200, envelope, {})],
                ):
                    listed = client.list_project_tasks()
                self.assertEqual(listed.outcome, Outcome.INCOMPLETE_SCAN)
                self.assertEqual(listed.details.get("reason"), "page")
                self.assertNotEqual(listed.outcome, Outcome.EMPTY)
                with mock.patch.object(
                    client,
                    "_request",
                    side_effect=[(200, envelope, {})],
                ):
                    found = client.find_by_marker(marker)
                self.assertEqual(found.outcome, Outcome.INCOMPLETE_SCAN)
                self.assertEqual(found.details.get("reason"), "page")
                self.assertNotEqual(found.outcome, Outcome.EMPTY)

    def test_paging_rejects_invalid_total_pages_and_cross_page_order(self) -> None:
        client = self._client(self._token_file())

        def task(task_id: int) -> dict:
            return {
                "id": task_id,
                "project_id": 2,
                "title": "t",
                "description": "x\n",
                "due_date": None,
                "done": False,
                "repeat_after": 0,
                "repeat_mode": 0,
            }

        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (
                    200,
                    {
                        "items": [task(7)],
                        "total": 1,
                        "page": 1,
                        "per_page": 1,
                        "total_pages": 999,
                    },
                    {},
                ),
                (
                    200,
                    {"items": [], "total": 1, "page": 2, "per_page": 1, "total_pages": 999},
                    {},
                ),
            ],
        ):
            bad_pages = client.list_project_tasks()
        self.assertEqual(bad_pages.outcome, Outcome.INCOMPLETE_SCAN)

        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (
                    200,
                    {
                        "items": [task(9)],
                        "total": 2,
                        "page": 1,
                        "per_page": 1,
                        "total_pages": 2,
                    },
                    {},
                ),
                (
                    200,
                    {
                        "items": [task(7)],
                        "total": 2,
                        "page": 2,
                        "per_page": 1,
                        "total_pages": 2,
                    },
                    {},
                ),
                (
                    200,
                    {"items": [], "total": 2, "page": 3, "per_page": 1, "total_pages": 2},
                    {},
                ),
            ],
        ):
            bad_order = client.list_project_tasks()
        self.assertEqual(bad_order.outcome, Outcome.INCOMPLETE_SCAN)

    def test_missing_project_blocks_patch(self) -> None:
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "title": "t",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (200, pre, {}),
                (200, dict(pre, project_id=2, title="new"), {}),
                (200, dict(pre, project_id=2, title="new"), {}),
            ],
        ) as call:
            client.update(7, {"title": "new"})
        self.assertFalse(any(args[0][0] == "PATCH" for args in call.call_args_list))

    def test_create_transport_failure_is_unknown_mutation(self) -> None:
        client = self._client(self._token_file())
        with mock.patch.object(
            client,
            "_request",
            side_effect=ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "synthetic response loss"),
        ):
            try:
                result = client.create(
                    title="synthetic",
                    issuer_id="11111111-1111-4111-8111-111111111111",
                    operation_id="22222222-2222-4222-8222-222222222222",
                )
            except ApiError as exc:
                self.fail("dispatched create escaped as " + exc.outcome.value + " without mutation_sent")
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)

    def test_update_readback_transport_failure_is_unknown_mutation(self) -> None:
        client = self._client(self._token_file())
        pre = {
            "id": 7,
            "project_id": 2,
            "title": "t",
            "description": build_create_description(
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                "",
            ),
            "due_date": None,
            "done": False,
            "repeat_after": 0,
            "repeat_mode": 0,
        }
        with mock.patch.object(
            client,
            "_request",
            side_effect=[
                (200, pre, {}),
                (304, None, {}),
                ApiError(Outcome.DEPENDENCY_UNAVAILABLE, "synthetic GET failure"),
            ],
        ):
            try:
                result = client.update(7, {"title": "new"})
            except ApiError as exc:
                self.fail(
                    "post-dispatch GET escaped as " + exc.outcome.value + " without mutation_sent"
                )
        self.assertEqual(result.outcome, Outcome.UNKNOWN)
        self.assertTrue(result.mutation_sent)

    def test_timeouts_are_capped_and_separate(self) -> None:
        client = VikunjaClient(
            api_origin="http://127.0.0.1:19345",
            project_id=2,
            token_ref=TokenRef(kind="file", path=self._token_file()),
            token="synthetic-in-memory-only",
            connect_timeout=30,
            http_timeout=60,
        )
        self.assertLessEqual(client.connect_timeout, 3.0)
        self.assertLessEqual(client.http_timeout, 10.0)
        connect_t, read_t = client._budget_timeouts(None)
        self.assertLessEqual(connect_t, 3.0)
        self.assertLessEqual(read_t, 10.0)
        self.assertNotEqual(connect_t, max(30.0, 60.0))

    def _run_fault_port_deadline_case(
        self,
        *,
        handler_cls: type[BaseHTTPRequestHandler],
        port: int = 19346,
    ) -> tuple[str, float]:
        probe = socket.socket()
        try:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                self.fail("fault port 19346 already in use")
        finally:
            probe.close()

        server = HTTPServer(("127.0.0.1", port), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = VikunjaClient(
            api_origin=f"http://127.0.0.1:{port}",
            project_id=2,
            token_ref=TokenRef(kind="file", path=self._token_file()),
            token="synthetic-in-memory-only",
        )
        start = time.monotonic()
        try:
            try:
                result = client.read(7)
                observed = result.outcome.value
            except ApiError as exc:
                observed = exc.outcome.value
            elapsed = time.monotonic() - start
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            check = socket.socket()
            check.settimeout(1)
            free = check.connect_ex(("127.0.0.1", port)) != 0
            check.close()
            self.assertTrue(free)
            self.assertFalse(thread.is_alive())
        return observed, elapsed

    def test_slow_streaming_body_respects_http_deadline(self) -> None:
        """C1 §4: absolute HTTP ceiling, not per-recv idle timeout (fault port 19346)."""
        payload = json.dumps(
            {
                "id": 7,
                "project_id": 2,
                "title": "synthetic",
                "description": "",
                "done": False,
                "repeat_after": 0,
                "repeat_mode": 0,
                "due_date": None,
            }
        ).encode()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                cuts = [0, len(payload) // 4, len(payload) // 2, 3 * len(payload) // 4, len(payload)]
                try:
                    for n in range(4):
                        if n:
                            time.sleep(4)
                        self.wfile.write(payload[cuts[n] : cuts[n + 1]])
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return

        observed, elapsed = self._run_fault_port_deadline_case(handler_cls=Handler)
        self.assertNotEqual(observed, Outcome.CONFIRMED.value)
        self.assertGreaterEqual(elapsed, 2.0)
        self.assertLessEqual(elapsed, 11.0)
        self.assertEqual(observed, Outcome.DEPENDENCY_UNAVAILABLE.value)

    def test_slow_streaming_headers_respect_http_deadline(self) -> None:
        """C1 §4: dripped status/header bytes must not overrun the exchange ceiling."""

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def handle_one_request(self) -> None:  # noqa: N802
                try:
                    self.raw_requestline = self.rfile.readline(65537)
                    if not self.raw_requestline:
                        self.close_connection = True
                        return
                    if not self.parse_request():
                        return
                    # Bypass BaseHTTPRequestHandler header helpers: drip raw bytes.
                    chunks = [
                        b"HTTP/1.1 200 OK\r\n",
                        b"Content-Type: application/json\r\n",
                        b"Content-Length: 2\r\n",
                        b"X-Slow-1: a\r\n",
                        b"X-Slow-2: b\r\n",
                        b"X-Slow-3: c\r\n",
                        b"X-Slow-4: d\r\n",
                        b"X-Slow-5: e\r\n",
                        b"X-Slow-6: f\r\n",
                        b"X-Slow-7: g\r\n",
                        b"X-Slow-8: h\r\n",
                        b"X-Slow-9: i\r\n",
                        b"X-Slow-10: j\r\n",
                        b"X-Slow-11: k\r\n",
                        b"X-Slow-12: l\r\n",
                        b"\r\n",
                        b"{}",
                    ]
                    for idx, chunk in enumerate(chunks):
                        if idx:
                            time.sleep(1.0)
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                    self.close_connection = True

        observed, elapsed = self._run_fault_port_deadline_case(handler_cls=Handler)
        self.assertNotEqual(observed, Outcome.CONFIRMED.value)
        self.assertGreaterEqual(elapsed, 2.0)
        self.assertLessEqual(elapsed, 11.0)
        self.assertEqual(observed, Outcome.DEPENDENCY_UNAVAILABLE.value)

    def test_fragmented_connection_close_success_keeps_socket_for_body(self) -> None:
        """Connection: close must not Bad-FD the response body before consumption."""
        payload = json.dumps(
            {
                "id": 7,
                "project_id": 2,
                "title": "synthetic",
                "description": "",
                "done": False,
                "repeat_after": 0,
                "repeat_mode": 0,
                "due_date": None,
            }
        ).encode()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *_args: object) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                self.end_headers()
                time.sleep(0.15)
                mid = max(1, len(payload) // 2)
                try:
                    self.wfile.write(payload[:mid])
                    self.wfile.flush()
                    time.sleep(0.05)
                    self.wfile.write(payload[mid:])
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return

        observed, elapsed = self._run_fault_port_deadline_case(handler_cls=Handler)
        self.assertEqual(observed, Outcome.CONFIRMED.value)
        self.assertGreaterEqual(elapsed, 0.15)
        self.assertLess(elapsed, 2.0)


class ApiSuiteRegistrationTests(unittest.TestCase):
    def test_api_suite_registered(self) -> None:
        from tests.support.suites import list_suites, get_suite

        self.assertIn("api", list_suites())
        runner = get_suite("api")
        self.assertTrue(callable(runner))


if __name__ == "__main__":
    unittest.main()
