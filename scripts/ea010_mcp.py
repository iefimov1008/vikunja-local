#!/usr/bin/env python3
"""EA-010 MCP stdio entrypoint (C3 0.1).

Example Mini argv (absolute paths only; secrets stay in owner config):
  <absolute-python> <absolute-install>/scripts/ea010_mcp.py --config <absolute-owner-config>

MacBook recipe uses an approved fixed remote launcher over SSH stdio; this
process always runs on Mini against the local journal/API. Live Codex/SSH is
separate admitted QA — not fabricated here.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local.mcp import McpError, run_from_config_path  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EA-010 Vikunja MCP stdio server")
    parser.add_argument(
        "--config",
        required=True,
        help="Absolute path to owner-local MCP JSON config (outside Git secrets)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    if not config_path.is_absolute():
        print("config path must be absolute", file=sys.stderr)
        return 2
    if not config_path.is_file():
        print("config file not found", file=sys.stderr)
        return 2
    try:
        return int(run_from_config_path(config_path))
    except McpError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"startup failure: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
