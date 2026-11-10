"""Suite registration for ea010_verify. Unknown/empty suites fail closed."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .stand import StandError, load_profile, run_stand_cycle

SuiteRunner = Callable[..., dict[str, Any]]

SUITE_REGISTRY: dict[str, SuiteRunner] = {}


def _api_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    from .api_suite import run_api_suite

    return run_api_suite(mode=mode, worktree=worktree, argv=argv)


def _journal_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    from .journal_suite import run_journal_suite

    return run_journal_suite(mode=mode, worktree=worktree, argv=argv)


def _recovery_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    from .recovery_suite import run_recovery_suite

    return run_recovery_suite(mode=mode, worktree=worktree, argv=argv)


def register_suite(name: str, runner: SuiteRunner) -> None:
    if not name or not str(name).strip():
        raise StandError("Suite name must be non-empty")
    if name in SUITE_REGISTRY:
        raise StandError(f"Suite already registered: {name}")
    SUITE_REGISTRY[name] = runner


def list_suites() -> list[str]:
    return sorted(SUITE_REGISTRY)


def get_suite(name: str | None) -> SuiteRunner:
    if name is None or not str(name).strip():
        raise StandError("Suite must be specified; empty suite is a failure")
    if name not in SUITE_REGISTRY:
        raise StandError(
            f"Unknown suite {name!r}; known={list_suites()}"
        )
    return SUITE_REGISTRY[name]


def _stand_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite stand supports only isolated mode, got {mode!r}")
    profile = load_profile()
    result = run_stand_cycle(profile, worktree=worktree, revoke_token=True)
    if not result.get("readiness") or result["readiness"].get("http") != 200:
        raise StandError("Stand suite readiness was not HTTP 200")
    if not result.get("synthetic_ids"):
        raise StandError("Stand suite produced no synthetic IDs")
    if not result.get("revoke_readback"):
        raise StandError("Stand suite missing token revoke read-back")
    if not (result.get("cleanup") or {}).get("confirmed"):
        raise StandError("Stand suite cleanup was not confirmed")
    result["argv"] = list(argv)
    result["suite"] = "stand"
    result["mode"] = mode
    # Common runner contract: nonempty executed scenario count.
    result["executed_count"] = 1
    return result


register_suite("stand", _stand_suite)
register_suite("api", _api_suite)
register_suite("journal", _journal_suite)
register_suite("recovery", _recovery_suite)
