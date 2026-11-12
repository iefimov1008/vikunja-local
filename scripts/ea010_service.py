#!/usr/bin/env python3
"""EA-010 service helper (s08). Read-only / describe-only; never touches working Mini service."""

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
    SUPERVISOR_LABEL,
    LifecycleError,
    describe_post_login,
    existing_service_preflight,
    load_service_config,
    read_only_bindings_preflight,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="Absolute path to ea010-service-config/v1 JSON (synthetic)",
    )
    parser.add_argument(
        "operation",
        choices=("preflight", "describe-post-login", "supervisor-policy", "bindings-preflight"),
        help="Read-only / dry operations only",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    if not config_path.is_absolute():
        print(json.dumps({"ok": False, "error": "config path must be absolute"}, sort_keys=True))
        return 2
    try:
        config = load_service_config(config_path)
        if args.operation == "describe-post-login":
            result = describe_post_login(config)
        elif args.operation == "supervisor-policy":
            result = {
                "label": SUPERVISOR_LABEL,
                "reuse_existing_supervisor": True,
                "forbid_second_supervisor": True,
                "working_service_mutations": False,
                "note": "Tools refuse a second LaunchAgent label and do not mutate production launchd.",
            }
        elif args.operation == "bindings-preflight":
            observed = config.get("observed_read_only_bindings_doc") or {}
            path = observed.get("path")
            if not path:
                raise LifecycleError("observed_read_only_bindings_doc.path required", code="bindings_path")
            result = read_only_bindings_preflight(path)
        else:
            if str(config["paths"].get("contour_root", "")).startswith("/absolute/path/"):
                raise LifecycleError(
                    "example placeholder paths cannot run preflight; use a synthetic absolute config",
                    code="placeholder_config",
                )
            result = existing_service_preflight(config)
            if result.get("ok") is False:
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "operation": args.operation,
                            "result": result,
                            "error": "preflight_failed",
                            "code": "preflight_failed",
                        },
                        indent=2,
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                )
                return 2
        print(json.dumps({"ok": True, "operation": args.operation, "result": result}, indent=2, sort_keys=True))
        return 0
    except LifecycleError as exc:
        print(
            json.dumps(
                {"ok": False, "error": str(exc), "code": exc.code, "details": exc.details},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
