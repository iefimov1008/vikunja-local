#!/usr/bin/env python3
"""EA-010 manual consistent copy / isolated restore CLI (s09).

One-shot operator helper only. No scheduler, retention, RPO/RTO, or working-Mini
overwrite. Secrets stay in a local 0600 tree and are never printed.
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

from vikunja_local.restore import (  # noqa: E402
    ContourPaths,
    RestoreError,
    create_manual_copy,
    restore_into_empty_target,
    verify_consistent_copy,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    copy_p = sub.add_parser("copy", help="Create one manual consistent copy from a stopped synthetic contour")
    copy_p.add_argument("--source-root", required=True, help="Absolute synthetic contour root")
    copy_p.add_argument("--dest", required=True, help="Absolute empty destination directory")
    copy_p.add_argument(
        "--journal-name",
        default="integration.sqlite3",
        help="Journal filename under source-root/journal/",
    )

    verify_p = sub.add_parser("verify", help="Verify an existing consistent copy")
    verify_p.add_argument("--copy", required=True, help="Absolute copy directory")

    restore_p = sub.add_parser(
        "restore",
        help="Restore a verified copy into a separate empty synthetic target (writes off)",
    )
    restore_p.add_argument("--copy", required=True, help="Absolute verified copy directory")
    restore_p.add_argument("--target", required=True, help="Absolute empty synthetic target")
    restore_p.add_argument("--listen-host", default="", help="Optional listen host rewrite for config")
    restore_p.add_argument("--listen-port", type=int, default=0, help="Optional listen port rewrite")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "copy":
            source = ContourPaths.from_stand_root(args.source_root, journal_name=args.journal_name)
            manifest = create_manual_copy(source, args.dest)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "command": "copy",
                        "copy_id": manifest["copy_id"],
                        "dest": args.dest,
                        "scheduler": False,
                        "rpo": None,
                        "rto": None,
                        "retention": None,
                        "mini_local_copy_not_device_loss_protection": True,
                        "components": sorted(manifest["components"]),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "verify":
            result = verify_consistent_copy(args.copy)
            print(json.dumps({"ok": True, "command": "verify", **result}, indent=2, sort_keys=True))
            return 0
        if args.command == "restore":
            host = args.listen_host or None
            port = args.listen_port or None
            result = restore_into_empty_target(
                args.copy,
                args.target,
                listen_host=host,
                listen_port=port,
            )
            # Never dump secrets or real task bodies.
            safe = {
                "ok": True,
                "command": "restore",
                "target": result["target"],
                "copy_id": result["copy_id"],
                "writes_enabled": result["writes_enabled"],
                "restore_drift": result["restore_drift"],
                "admitted": result["admitted"],
                "mapping_count": len((result.get("links") or {}).get("mappings") or []),
                "receipt_count": len((result.get("links") or {}).get("receipts") or []),
                "working_contour_restore": False,
                "scheduler": False,
                "mini_local_copy_not_device_loss_protection": True,
            }
            print(json.dumps(safe, indent=2, sort_keys=True))
            return 0
        print(json.dumps({"ok": False, "error": f"unknown command {args.command!r}"}, sort_keys=True))
        return 2
    except RestoreError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(exc),
                    "code": exc.code,
                    "details": {
                        key: value
                        for key, value in exc.details.items()
                        if key
                        not in {
                            "expected",
                            "observed",
                            "service.secret",
                            "password",
                            "token",
                        }
                    },
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"unexpected: {exc}"}, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
