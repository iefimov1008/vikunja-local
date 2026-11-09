#!/usr/bin/env python3
"""EA-010 verification runner. Suites fail closed; no skip-as-success."""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.support.stand import (  # noqa: E402
    StandError,
    append_journal,
    assert_suite_result_contract,
    load_profile,
    validate_journal_path,
)
from tests.support.suites import get_suite, list_suites  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EA-010 isolated verification runner")
    parser.add_argument("--suite", required=True, help="Suite name (unknown/empty => failure)")
    parser.add_argument(
        "--mode",
        default="isolated",
        help="Execution mode (s01 supports isolated only)",
    )
    parser.add_argument(
        "--journal",
        default="",
        help="Optional argv/rc journal path under approved run-root",
    )
    return parser.parse_args(argv)


def _resolve_journal(profile, args: argparse.Namespace, *, suite_name: str, failure: bool) -> Path:
    if args.journal:
        return validate_journal_path(profile, args.journal)
    leaf = f"failure-{os.getpid()}.json" if failure else f"{suite_name}-{os.getpid()}.json"
    return validate_journal_path(profile, Path("journals") / leaf)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv if argv is None else argv)
    # Preserve real process argv for the journal even when main() is called with a list.
    command = argv[1:] if argv and argv[0].endswith("ea010_verify.py") else argv
    try:
        args = parse_args(command)
    except SystemExit as exc:
        code = int(exc.code or 1)
        return code if code != 0 else 1

    suite_name = args.suite
    mode = args.mode
    rc = 1
    payload: dict = {"ok": False}
    journal_path: Path | None = None
    profile = None
    try:
        if not suite_name or not str(suite_name).strip():
            raise StandError("Suite must be specified; empty suite is a failure")
        runner = get_suite(suite_name)
        profile = load_profile()
        # Containment/Git/symlink checks happen before any suite execution or write.
        journal_path = _resolve_journal(profile, args, suite_name=str(suite_name), failure=False)
        result = runner(mode=mode, worktree=ROOT, argv=["python3", "scripts/ea010_verify.py", *command])
        result = assert_suite_result_contract(result)
        payload = {"ok": True, "result": result, "known_suites": list_suites()}
        print(json.dumps(payload, indent=2, sort_keys=True))
        rc = 0
        append_journal(
            journal_path,
            argv=["python3", "scripts/ea010_verify.py", *command],
            rc=rc,
            suite=suite_name,
            mode=mode,
            profile=profile,
            extra={
                "run_id": (result or {}).get("run_id"),
                "synthetic_ids": (result or {}).get("synthetic_ids"),
                "source": (result or {}).get("source"),
                "executed_count": (result or {}).get("executed_count"),
            },
        )
        return rc
    except StandError as exc:
        payload = {"ok": False, "error": str(exc), "known_suites": list_suites()}
        print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        rc = 2
    except Exception as exc:  # noqa: BLE001 - surface unexpected failures as runner errors
        payload = {
            "ok": False,
            "error": f"unexpected: {exc}",
            "traceback": traceback.format_exc(),
            "known_suites": list_suites(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        rc = 1
    try:
        if profile is None:
            profile = load_profile()
        if journal_path is None:
            journal_path = _resolve_journal(
                profile,
                args,
                suite_name=str(getattr(args, "suite", "failure") or "failure"),
                failure=True,
            )
        else:
            journal_path = validate_journal_path(profile, journal_path)
        append_journal(
            journal_path,
            argv=["python3", "scripts/ea010_verify.py", *command],
            rc=rc,
            suite=str(getattr(args, "suite", "")),
            mode=str(getattr(args, "mode", "")),
            profile=profile,
            extra={"error": payload.get("error")},
        )
    except Exception:
        pass
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
