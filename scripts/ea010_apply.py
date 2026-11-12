#!/usr/bin/env python3
"""EA-010 Hermes three-patch one-shot rehearsal tool (s08).

Default mode is dry-run. Each invocation creates one disposable temporary tree
under the approved stand run-root, runs the admitted mode, and deletes that tree
on every outcome. There is no production apply path, no persistent contour, and
no cross-invocation apply/repeat/undo state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local.lifecycle import (  # noqa: E402
    LifecycleError,
    MODES,
    run_exact_three_patch_rehearsal,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worktree",
        default=str(ROOT),
        help="Absolute path to the vikunja-local worktree that publishes the three patches",
    )
    parser.add_argument(
        "--mode",
        default="dry-run",
        choices=MODES,
        help="dry-run (default) | rehearse (apply/hash/repeat/reverse/hash then delete)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    worktree = Path(args.worktree)
    if not worktree.is_absolute():
        print(json.dumps({"ok": False, "error": "worktree path must be absolute"}, sort_keys=True))
        return 2
    try:
        result = run_exact_three_patch_rehearsal(worktree, mode=args.mode)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except LifecycleError as exc:
        details = dict(exc.details or {})
        # Only claim deletion when the library verified absence (NFR-18).
        contour_deleted = bool(details.get("contour_deleted", False))
        payload = {
            "ok": False,
            "error": str(exc),
            "code": exc.code,
            "details": details,
            "contour_deleted": contour_deleted,
        }
        contour = details.get("contour_root")
        if contour:
            payload["contour_root"] = str(contour)
        if details.get("cleanup_remaining"):
            payload["cleanup_remaining"] = details["cleanup_remaining"]
        print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
