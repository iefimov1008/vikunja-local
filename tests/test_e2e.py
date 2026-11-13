"""Unit/self-checks for s10 E2E harness and approved-pilot fail-closed gate."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from tests.support.e2e_suite import (
    ALL_COMPONENT_SUITES,
    DevelopmentRunGuard,
    _source_digest,
    run_all_suite,
)
from tests.support.pilot_suite import (
    EXECUTOR_ENV,
    MANUAL_CHECKPOINTS,
    PERMIT_ENV,
    PERMIT_SCHEMA,
    RESOURCES_SCHEMA,
    PilotRefused,
    default_checkpoint_status,
    describe_fail_closed_contract,
    is_permit_bound_operational_executor,
    load_permit,
    permit_path_from_env,
    run_approved_pilot,
    snapshot_files,
    validate_checkpoint_receipts,
)
from tests.support.suites import list_suites


ROOT = Path(__file__).resolve().parents[1]
# Wide window so unit tests are not coupled to wall-clock inside a one-hour slot.
VALID_WINDOW = {
    "starts_at": "2026-01-01T00:00:00+00:00",
    "ends_at": "2027-01-01T00:00:00+00:00",
}


def _profile_bound_resources(**overrides: object) -> dict:
    """Resources that bind to the live C7 isolated profile run_root."""
    from tests.support.stand import load_profile

    profile = load_profile()
    base = {
        "schema": RESOURCES_SCHEMA,
        "service_bindings": [
            {
                "label": "local.cto.vikunja",
                "listen_host": str(profile.listen_host),
                "port": int(profile.ports["vikunja"]),
            }
        ],
        "owned_paths": [str(Path(profile.run_root).resolve())],
        "run_root": str(Path(profile.run_root).resolve()),
    }
    base.update(overrides)
    return base  # type: ignore[return-value]


def _resources(**overrides: object) -> dict:
    base = {
        "schema": RESOURCES_SCHEMA,
        "service_bindings": [{"label": "local.cto.vikunja"}],
        "owned_paths": ["/tmp/ea010-synthetic-only"],
        "run_root": "/tmp/ea010-synthetic-only",
    }
    base.update(overrides)
    return base  # type: ignore[return-value]


def _permit(resources: dict, **overrides: object) -> dict:
    material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode()
    payload = {
        "schema": PERMIT_SCHEMA,
        "project_id": "vikunja-local-ea010-install-integration",
        "window": dict(VALID_WINDOW),
        "authorized_actions": ["all", "suite:all", "stand"],
        "executor": "coordinator-pilot",
        "resources_manifest": resources,
        "resources_manifest_sha256": hashlib.sha256(material).hexdigest(),
        "allow_live_pilot": False,
    }
    payload.update(overrides)
    return payload


def _verified_receipts() -> dict[str, dict[str, object]]:
    return {
        name: {"verified": True, "receipt_id": f"receipt-{name}", "attested_at": "2026-11-13T00:00:00Z"}
        for name in MANUAL_CHECKPOINTS
    }


def _write_permit(path: Path, permit: dict) -> Path:
    path.write_text(json.dumps(permit), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


class PilotFailClosedTests(unittest.TestCase):
    def test_contract_documents_not_run_checkpoints(self) -> None:
        contract = describe_fail_closed_contract()
        self.assertEqual(contract["mode"], "approved-pilot")
        self.assertEqual(contract["permit_env"], PERMIT_ENV)
        self.assertEqual(contract["executor_env"], EXECUTOR_ENV)
        self.assertIn("operational_executor", contract)
        self.assertIn("exact_authorization", contract)
        self.assertIn("receipt_vs_execution", contract)
        self.assertIn("owner_synthetic_acceptance", contract["manual_checkpoints"])
        self.assertEqual(contract["checkpoint_default"], "not-run")
        self.assertIn("non-zero", contract["unexecuted_pilot"])
        checkpoints = default_checkpoint_status()
        for name, row in checkpoints.items():
            self.assertEqual(row["status"], "not-run", msg=name)

    def test_missing_permit_env_refuses_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            probe = Path(tmp) / "probe"
            probe.mkdir()
            marker = probe / "before.txt"
            marker.write_text("keep\n", encoding="utf-8")
            before = snapshot_files([probe])
            with self.assertRaises(PilotRefused) as ctx:
                run_approved_pilot(
                    mode="approved-pilot",
                    worktree=ROOT,
                    argv=["python3", "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                    env={},
                    probe_roots=[probe],
                )
            self.assertEqual(ctx.exception.status, "not-run")
            self.assertFalse((ctx.exception.details or {}).get("mutations", True))
            after = {str(p) for p in probe.rglob("*") if p.is_file()}
            self.assertEqual(after, before[str(probe)])

    def test_missing_permit_file_refuses(self) -> None:
        missing = Path("/tmp/ea010-missing-permit-does-not-exist.json")
        with self.assertRaises(PilotRefused) as ctx:
            run_approved_pilot(
                mode="approved-pilot",
                worktree=ROOT,
                argv=["--suite", "all", "--mode", "approved-pilot"],
                env={PERMIT_ENV: str(missing)},
            )
        self.assertEqual(ctx.exception.status, "not-run")

    def test_invalid_permit_schema_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "permit.json"
            path.write_text(json.dumps({"schema": "wrong"}), encoding="utf-8")
            with self.assertRaises(PilotRefused):
                load_permit(path)

    def test_placeholder_permit_fields_refused(self) -> None:
        resources = {
            "schema": RESOURCES_SCHEMA,
            "service_bindings": [],
            "owned_paths": [],
            "run_root": None,
        }
        material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode()
        permit = {
            "schema": PERMIT_SCHEMA,
            "project_id": "vikunja-local-ea010-install-integration",
            "window": None,
            "authorized_actions": [],
            "executor": None,
            "resources_manifest": resources,
            "resources_manifest_sha256": hashlib.sha256(material).hexdigest(),
            "allow_live_pilot": True,
            "owner_action_receipts": {"unrelated": True},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "permit.json"
            path.write_text(json.dumps(permit), encoding="utf-8")
            os.chmod(path, 0o600)
            with self.assertRaises(PilotRefused) as ctx:
                load_permit(path)
            self.assertEqual(ctx.exception.status, "not-run")
            with self.assertRaises(PilotRefused) as ctx2:
                run_approved_pilot(
                    mode="approved-pilot",
                    worktree=ROOT,
                    argv=["--suite", "all", "--mode", "approved-pilot"],
                    env={PERMIT_ENV: str(path)},
                )
            self.assertNotEqual(ctx2.exception.status, "ok")
            self.assertEqual((ctx2.exception.details or {}).get("executed_count", 0), 0)

    def test_null_service_binding_refused_with_zero_runner_calls(self) -> None:
        resources = _resources(service_bindings=[None])  # type: ignore[list-item]
        material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode()
        # Rebuild sha after typed mutation shape used by helper.
        resources = {
            "schema": RESOURCES_SCHEMA,
            "service_bindings": [None],
            "owned_paths": ["/tmp/ea010-synthetic-only"],
            "run_root": "/tmp/ea010-synthetic-only",
        }
        material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            authorized_actions=["all"],
            resources_manifest_sha256=hashlib.sha256(material).hexdigest(),
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            calls: list[dict] = []

            def _fake_suite(**kwargs):  # noqa: ANN003
                calls.append(dict(kwargs))
                return {"executed_count": 1, "mode": kwargs.get("mode"), "cleanup": {"confirmed": True}}

            with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                with self.assertRaises(PilotRefused) as ctx:
                    run_approved_pilot(
                        mode="approved-pilot",
                        worktree=ROOT,
                        argv=["--suite", "all", "--mode", "approved-pilot"],
                        env={PERMIT_ENV: str(path), EXECUTOR_ENV: "coordinator-pilot"},
                        suite="all",
                    )
            self.assertEqual(ctx.exception.status, "not-run")
            self.assertEqual(calls, [])
            self.assertEqual((ctx.exception.details or {}).get("runner_calls", 0), 0)

    def test_expired_and_malformed_windows_refuse_with_zero_runner_calls(self) -> None:
        resources = _profile_bound_resources()
        for window in (
            {"starts_at": "2000-01-01T00:00:00Z", "ends_at": "2000-01-01T01:00:00Z"},
            {"starts_at": "not-a-date", "ends_at": "also-not-a-date"},
            {"starts_at": "2026-12-01T00:00:00Z", "ends_at": "2026-11-01T00:00:00Z"},
        ):
            with self.subTest(window=window):
                permit = _permit(
                    resources,
                    window=window,
                    allow_live_pilot=True,
                    owner_action_receipts=_verified_receipts(),
                    authorized_actions=["all"],
                )
                with tempfile.TemporaryDirectory() as tmp:
                    path = _write_permit(Path(tmp) / "permit.json", permit)
                    calls: list[dict] = []

                    def _fake_suite(**kwargs):  # noqa: ANN003
                        calls.append(dict(kwargs))
                        return {"executed_count": 1, "mode": kwargs.get("mode")}

                    with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                        with self.assertRaises(PilotRefused) as ctx:
                            run_approved_pilot(
                                mode="approved-pilot",
                                worktree=ROOT,
                                argv=["--suite", "all", "--mode", "approved-pilot"],
                                env={PERMIT_ENV: str(path), EXECUTOR_ENV: "coordinator-pilot"},
                                suite="all",
                                now=datetime(2026, 11, 13, 1, 30, tzinfo=timezone.utc),
                            )
                    self.assertEqual(ctx.exception.status, "not-run")
                    self.assertEqual(calls, [])

    def test_readiness_only_action_refuses_suite_all_with_zero_runner_calls(self) -> None:
        resources = _profile_bound_resources()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            authorized_actions=["readiness-only"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            calls: list[dict] = []

            def _fake_suite(**kwargs):  # noqa: ANN003
                calls.append(dict(kwargs))
                return {"executed_count": 1, "mode": kwargs.get("mode")}

            with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                with self.assertRaises(PilotRefused) as ctx:
                    run_approved_pilot(
                        mode="approved-pilot",
                        worktree=ROOT,
                        argv=["--suite", "all", "--mode", "approved-pilot"],
                        env={PERMIT_ENV: str(path), EXECUTOR_ENV: "coordinator-pilot"},
                        suite="all",
                    )
            self.assertEqual(ctx.exception.status, "not-run")
            self.assertIn("authorized_actions", str(ctx.exception).lower())
            self.assertEqual(calls, [])

    def test_unrelated_run_root_refuses_with_zero_runner_calls(self) -> None:
        resources = _resources()  # /tmp — not C7 profile root
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            authorized_actions=["all"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            calls: list[dict] = []

            def _fake_suite(**kwargs):  # noqa: ANN003
                calls.append(dict(kwargs))
                return {"executed_count": 1, "mode": kwargs.get("mode")}

            with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                with self.assertRaises(PilotRefused) as ctx:
                    run_approved_pilot(
                        mode="approved-pilot",
                        worktree=ROOT,
                        argv=["--suite", "all", "--mode", "approved-pilot"],
                        env={PERMIT_ENV: str(path), EXECUTOR_ENV: "coordinator-pilot"},
                        suite="all",
                    )
            self.assertEqual(ctx.exception.status, "not-run")
            self.assertIn("run_root", str(ctx.exception).lower())
            self.assertEqual(calls, [])

    def test_valid_permit_without_live_flag_still_not_run(self) -> None:
        resources = _resources()
        permit = _permit(resources, allow_live_pilot=False)
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            with self.assertRaises(PilotRefused) as ctx:
                run_approved_pilot(
                    mode="approved-pilot",
                    worktree=ROOT,
                    argv=["--mode", "approved-pilot"],
                    env={PERMIT_ENV: str(path)},
                )
            self.assertEqual(ctx.exception.status, "not-run")
            self.assertIn("live pilot", str(ctx.exception).lower())
            self.assertIn("allow_live_pilot", str((ctx.exception.details or {}).get("reason", "")).lower())

    def test_unrelated_receipts_refused(self) -> None:
        resources = _resources()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts={"unrelated": True},
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            with self.assertRaises(PilotRefused) as ctx:
                run_approved_pilot(
                    mode="approved-pilot",
                    worktree=ROOT,
                    argv=["--suite", "all", "--mode", "approved-pilot"],
                    env={PERMIT_ENV: str(path)},
                )
            self.assertEqual(ctx.exception.status, "not-run")
            details = ctx.exception.details or {}
            self.assertIn("owner_synthetic_acceptance", details.get("missing_checkpoints") or [])

    def test_verified_receipts_still_nonzero_when_unexecuted(self) -> None:
        resources = _profile_bound_resources()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            authorized_actions=["all"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            calls: list[dict] = []

            def _fake_suite(**kwargs):  # noqa: ANN003
                calls.append(dict(kwargs))
                return {"executed_count": 1}

            with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                with self.assertRaises(PilotRefused) as ctx:
                    run_approved_pilot(
                        mode="approved-pilot",
                        worktree=ROOT,
                        argv=["--suite", "all", "--mode", "approved-pilot"],
                        env={PERMIT_ENV: str(path)},
                        suite="all",
                    )
            self.assertEqual(ctx.exception.status, "authorised-not-executed-by-coding-worker")
            self.assertEqual((ctx.exception.details or {}).get("executed_count"), 0)
            self.assertFalse((ctx.exception.details or {}).get("mutations", True))
            self.assertEqual(calls, [])

    def test_operational_executor_does_not_relabel_isolated_suite(self) -> None:
        resources = _profile_bound_resources()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            executor="coordinator-pilot",
            authorized_actions=["stand", "all"],
        )
        self.assertTrue(
            is_permit_bound_operational_executor(
                permit, {EXECUTOR_ENV: "coordinator-pilot"}
            )
        )
        self.assertFalse(is_permit_bound_operational_executor(permit, {}))
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            calls: list[dict] = []

            def _fake_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict:
                calls.append({"mode": mode, "worktree": str(worktree), "argv": list(argv)})
                return {
                    "suite": "stand",
                    "mode": mode,
                    "argv": list(argv),
                    "executed_count": 1,
                    "run_id": "abc123",
                    "cleanup": {"confirmed": True},
                    "synthetic_ids": {"project_id": 1},
                    "source": {"sha256": "0" * 64},
                }

            with mock.patch("tests.support.suites.get_suite", return_value=_fake_suite):
                with self.assertRaises(PilotRefused) as ctx:
                    run_approved_pilot(
                        mode="approved-pilot",
                        worktree=ROOT,
                        argv=["--suite", "stand", "--mode", "approved-pilot"],
                        env={PERMIT_ENV: str(path), EXECUTOR_ENV: "coordinator-pilot"},
                        suite="stand",
                    )
            self.assertEqual(
                ctx.exception.status,
                "authorised-not-executed-operational-contour-unverified",
            )
            self.assertEqual(calls, [])
            details = ctx.exception.details or {}
            self.assertEqual(details.get("executed_count"), 0)
            self.assertEqual(details.get("runner_calls"), 0)
            checkpoints = details.get("checkpoints") or {}
            for name in MANUAL_CHECKPOINTS:
                self.assertEqual(checkpoints[name]["status"], "receipt-present-not-executed")

    def test_mismatched_executor_identity_still_refuses(self) -> None:
        resources = _profile_bound_resources()
        permit = _permit(
            resources,
            allow_live_pilot=True,
            owner_action_receipts=_verified_receipts(),
            executor="coordinator-pilot",
            authorized_actions=["stand"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_permit(Path(tmp) / "permit.json", permit)
            with self.assertRaises(PilotRefused) as ctx:
                run_approved_pilot(
                    mode="approved-pilot",
                    worktree=ROOT,
                    argv=["--suite", "stand", "--mode", "approved-pilot"],
                    env={PERMIT_ENV: str(path), EXECUTOR_ENV: "other-executor"},
                    suite="stand",
                )
            self.assertEqual(ctx.exception.status, "authorised-not-executed-by-coding-worker")

    def test_validate_checkpoint_receipts_requires_each_named(self) -> None:
        with self.assertRaises(PilotRefused):
            validate_checkpoint_receipts({"unrelated": {"verified": True, "receipt_id": "x"}})
        ok = validate_checkpoint_receipts(_verified_receipts())
        self.assertEqual(set(ok), set(MANUAL_CHECKPOINTS))

    def test_permit_path_from_env(self) -> None:
        self.assertIsNone(permit_path_from_env({}))
        self.assertEqual(
            permit_path_from_env({PERMIT_ENV: "/abs/permit.json"}),
            Path("/abs/permit.json"),
        )

    def test_wrong_mode_rejected(self) -> None:
        with self.assertRaises(Exception):
            run_approved_pilot(mode="isolated", worktree=ROOT, argv=[])


class SuiteRegistryTests(unittest.TestCase):
    def test_all_and_e2e_registered(self) -> None:
        known = set(list_suites())
        self.assertIn("e2e", known)
        self.assertIn("all", known)
        for name in ALL_COMPONENT_SUITES:
            self.assertIn(name, known, msg=name)

    def test_component_order_covers_created_suites(self) -> None:
        # all composes real suites; does not invent names beyond registry.
        known = set(list_suites()) - {"all"}
        self.assertEqual(set(ALL_COMPONENT_SUITES), known)

    def test_source_digest_nonempty_for_candidate(self) -> None:
        from tests.support.stand import implementation_source_digest

        digest = _source_digest(ROOT)
        full = implementation_source_digest(ROOT)
        self.assertEqual(len(digest["sha256"]), 64)
        self.assertTrue(digest["head"])
        # Primary fields bind the full candidate revision (not the six-path allowlist).
        self.assertEqual(digest["sha256"], full["sha256"])
        self.assertEqual(digest["diff_sha256"], full["diff_sha256"])
        harness = digest.get("harness_fingerprint") or {}
        self.assertIsInstance(harness, dict)
        self.assertEqual(len(harness.get("sha256") or ""), 64)
        self.assertIn("tests/support/e2e_suite.py", harness.get("paths") or [])
        # Changing stand/hermes suites must move the full digest; harness may stay put.
        self.assertTrue(
            any(
                p.endswith(("stand.py", "hermes_ux_suite.py", "hermes_bridge_suite.py", "e2e_suite.py"))
                for p in (full.get("untracked_implementation_files") or [])
            )
            or digest["diff_sha256"] != harness.get("diff_sha256"),
            "full digest must reflect implementation files beyond the harness allowlist",
        )

    def test_run_all_rejects_missing_cleanup_confirmed(self) -> None:
        def _fake_runner(*, mode: str, worktree: Path, argv: list[str]) -> dict:
            return {"executed_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            run_root = Path(tmp) / "run-root"
            run_root.mkdir()
            fake_profile = mock.Mock()
            fake_profile.run_root = run_root
            fake_profile.sensitive_file_mode = 0o600
            fake_profile.ports = {"vikunja": 19345, "fault_proxy": 19346, "synthetic_hermes": 19347}
            with mock.patch("tests.support.e2e_suite.load_profile", return_value=fake_profile):
                with mock.patch("tests.support.suites.get_suite", return_value=_fake_runner):
                    with mock.patch(
                        "tests.support.suites.list_suites",
                        return_value=list(ALL_COMPONENT_SUITES) + ["all"],
                    ):
                        with self.assertRaises(Exception) as ctx:
                            run_all_suite(mode="isolated", worktree=ROOT, argv=["--suite", "all"])
            self.assertIn("cleanup.confirmed", str(ctx.exception))
            # Failure/interruption receipt must survive outside disposable stand roots.
            journals = list((run_root / "journals").glob("all-*/stand.json"))
            self.assertTrue(journals, "expected durable stand failure receipt")
            payload = json.loads(journals[0].read_text(encoding="utf-8"))
            self.assertEqual(payload.get("status"), "failed")
            self.assertEqual(payload.get("argv")[3], "stand")
            aggregate = list((run_root / "journals").glob("all-*/aggregate.json"))
            self.assertTrue(aggregate)
            agg = json.loads(aggregate[0].read_text(encoding="utf-8"))
            self.assertIsNotNone(agg.get("interruption"))
            progress = list((run_root / "journals").glob("all-*/progress.json"))
            self.assertTrue(progress, "expected durable in-progress receipt")

    def test_run_all_persists_component_receipt_before_advance(self) -> None:
        calls: list[str] = []

        def _fake_runner(*, mode: str, worktree: Path, argv: list[str]) -> dict:
            name = argv[argv.index("--suite") + 1]
            calls.append(name)
            if name == "api":
                raise RuntimeError("controlled api failure after stand")
            return {
                "executed_count": 2,
                "run_id": f"run-{name}",
                "cleanup": {"confirmed": True, "ports_clear": True},
                "source": {"sha256": "a" * 64, "head": "deadbeef"},
                "binary": {"sha256": "b" * 64},
                "synthetic_ids": {"project_id": 7, "task_id": 9},
                "scenario_names": [f"{name}-ok"],
            }

        with tempfile.TemporaryDirectory() as tmp:
            run_root = Path(tmp) / "run-root"
            run_root.mkdir()
            fake_profile = mock.Mock()
            fake_profile.run_root = run_root
            fake_profile.sensitive_file_mode = 0o600
            fake_profile.ports = {"vikunja": 19345, "fault_proxy": 19346, "synthetic_hermes": 19347}
            with mock.patch("tests.support.e2e_suite.load_profile", return_value=fake_profile):
                with mock.patch("tests.support.suites.get_suite", return_value=_fake_runner):
                    with mock.patch(
                        "tests.support.suites.list_suites",
                        return_value=list(ALL_COMPONENT_SUITES) + ["all"],
                    ):
                        with self.assertRaises(RuntimeError):
                            run_all_suite(mode="isolated", worktree=ROOT, argv=["--suite", "all"])
            self.assertEqual(calls[:2], ["stand", "api"])
            stand_receipt = list((run_root / "journals").glob("all-*/stand.json"))
            self.assertTrue(stand_receipt)
            stand_payload = json.loads(stand_receipt[0].read_text(encoding="utf-8"))
            self.assertEqual(stand_payload.get("status"), "completed")
            self.assertEqual(stand_payload.get("executed_count"), 2)
            self.assertEqual(stand_payload.get("run_id"), "run-stand")
            self.assertEqual((stand_payload.get("source") or {}).get("sha256"), "a" * 64)
            self.assertEqual((stand_payload.get("synthetic_ids") or {}).get("project_id"), 7)
            self.assertTrue((stand_payload.get("cleanup") or {}).get("confirmed"))
            api_receipt = list((run_root / "journals").glob("all-*/api.json"))
            self.assertTrue(api_receipt)
            api_payload = json.loads(api_receipt[0].read_text(encoding="utf-8"))
            self.assertEqual(api_payload.get("status"), "failed")
            self.assertIn("controlled api failure", api_payload.get("error") or "")
            progress = list((run_root / "journals").glob("all-*/progress.json"))
            self.assertTrue(progress)
            progress_payload = json.loads(progress[0].read_text(encoding="utf-8"))
            self.assertEqual(progress_payload.get("phase"), "failed")
            aggregate = list((run_root / "journals").glob("all-*/aggregate.json"))
            self.assertTrue(aggregate)
            self.assertEqual(
                json.loads(aggregate[0].read_text(encoding="utf-8")).get("terminal_phase"),
                "failed",
            )

    def test_run_all_sigterm_between_suites_persists_aggregate(self) -> None:
        import signal
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-sigterm-") as tmp:
            base = Path(tmp)
            run_root = base / "run-root"
            run_root.mkdir()
            child = base / "child.py"
            child.write_text(
                "\n".join(
                    [
                        "import os, signal, sys, pathlib",
                        "from unittest import mock",
                        "sys.path.insert(0, sys.argv[1])",
                        "from tests.support.e2e_suite import run_all_suite",
                        "profile = mock.Mock()",
                        "profile.run_root = pathlib.Path(sys.argv[2])",
                        "profile.sensitive_file_mode = 0o600",
                        "profile.ports = {'vikunja': 19345, 'fault_proxy': 19346, 'synthetic_hermes': 19347}",
                        "profile.run_root.mkdir(exist_ok=True)",
                        "def runner(**kwargs):",
                        "    return {",
                        "        'executed_count': 1,",
                        "        'run_id': 'synthetic-no-service-started',",
                        "        'cleanup': {'confirmed': True},",
                        "        'source': {'sha256': '0' * 64},",
                        "    }",
                        "def pause(_seconds):",
                        "    os.kill(os.getpid(), signal.SIGTERM)",
                        "with mock.patch('tests.support.e2e_suite.load_profile', return_value=profile), \\",
                        "     mock.patch('tests.support.suites.get_suite', return_value=runner), \\",
                        "     mock.patch('tests.support.e2e_suite.time.sleep', side_effect=pause):",
                        "    run_all_suite(",
                        "        mode='isolated',",
                        "        worktree=pathlib.Path(sys.argv[1]),",
                        "        argv=['python3', 'scripts/ea010_verify.py', '--suite', 'all', '--mode', 'isolated'],",
                        "    )",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            proc = subprocess.run(
                [sys.executable, str(child), str(ROOT), str(run_root)],
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
            self.assertEqual(proc.returncode, -signal.SIGTERM)
            aggregates = list(run_root.rglob("aggregate.json"))
            self.assertTrue(aggregates, "expected aggregate.json after SIGTERM")
            agg = json.loads(aggregates[0].read_text(encoding="utf-8"))
            self.assertIsNotNone(agg.get("interruption"))
            self.assertEqual((agg.get("interruption") or {}).get("rc"), -signal.SIGTERM)
            self.assertIn("SIGTERM", str((agg.get("interruption") or {}).get("signal") or {}))
            progress = list(run_root.rglob("progress.json"))
            self.assertTrue(progress, "expected durable progress receipt for SIGKILL reconciliation")
            stand = list(run_root.rglob("stand.json"))
            self.assertTrue(stand)

    def test_development_guard_compares_baseline_and_entrypoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stand_dir = root / "stand"
            stand_dir.mkdir()
            process_dir = root / "process"
            process_dir.mkdir()
            marker = root / "docs" / "process" / "t_606468bd.md"
            marker.parent.mkdir(parents=True)
            marker.write_text("marker\n", encoding="utf-8")
            fake_stand = mock.Mock()
            fake_stand.run_dir = stand_dir
            guard = DevelopmentRunGuard(root, fake_stand, process_dir=process_dir)
            guard.start()
            # UUID-named record with forbidden content must be observed.
            dirty = stand_dir / f"{'a' * 32}.json"
            dirty.write_text('{"op":"development_flow_start"}\n', encoding="utf-8")
            # External process-dir dispatch record.
            (process_dir / "devflow-dispatch-record.json").write_text(
                '{"development_task_register": true}\n', encoding="utf-8"
            )
            # Marker mutation after baseline.
            time_before = marker.stat().st_mtime_ns
            marker.write_text("marker-changed\n", encoding="utf-8")
            self.assertNotEqual(marker.stat().st_mtime_ns, time_before)
            with self.assertRaises(Exception) as ctx:
                guard.assert_clean()
            msg = str(ctx.exception).lower()
            self.assertTrue(
                "development" in msg or "process marker" in msg or "process-dir" in msg,
                msg=msg,
            )

    def test_development_guard_detects_string_and_sequence_subprocess_argv(self) -> None:
        """String/bytes shell argv must not be character-split out of forbidden tokens."""
        import subprocess

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-guard-argv-") as tmp:
            root = Path(tmp)
            stand_dir = root / "stand"
            stand_dir.mkdir()
            process_dir = root / "process"
            process_dir.mkdir()
            (root / "docs" / "process").mkdir(parents=True)
            (root / "docs" / "process" / "t_606468bd.md").write_text("marker\n", encoding="utf-8")
            fake_stand = mock.Mock()
            fake_stand.run_dir = stand_dir
            guard = DevelopmentRunGuard(root, fake_stand, process_dir=process_dir)
            guard.start()
            guard.channel("mcp")
            try:
                self.assertTrue(
                    guard._argv_forbidden(["/bin/true", "development_flow_start"]),
                )
                self.assertTrue(
                    guard._argv_forbidden("/usr/bin/true development_flow_start"),
                )
                self.assertTrue(
                    guard._argv_forbidden(b"/usr/bin/true development_flow_start"),
                )
                self.assertTrue(
                    guard._argv_forbidden(Path("/usr/bin/true development_flow_start")),
                )
                subprocess.run("/usr/bin/true development_flow_start", shell=True, check=False)
                subprocess.run(
                    ["/usr/bin/true", "development_task_register"],
                    check=False,
                )
                proc = subprocess.Popen(
                    b"/usr/bin/true coding-agent",
                    shell=True,
                )
                proc.wait()
                proc2 = subprocess.Popen(
                    ["/usr/bin/true", "devflow_dispatch"],
                )
                proc2.wait()
                self.assertGreaterEqual(len(guard.subprocess_hits), 4)
                vias = {hit.get("via") for hit in guard.subprocess_hits}
                self.assertIn("subprocess.run", vias)
                self.assertIn("subprocess.Popen", vias)
                recorded = " ".join(
                    " ".join(hit.get("argv") or []) for hit in guard.subprocess_hits
                ).lower()
                self.assertIn("development_flow_start", recorded)
                self.assertIn("development_task_register", recorded)
                with self.assertRaises(Exception) as ctx:
                    guard.assert_clean()
                self.assertIn("development process launches", str(ctx.exception).lower())
            finally:
                guard.stop()

    def test_run_all_active_stand_exception_preserves_receipt_evidence(self) -> None:
        """Real API stand RuntimeError after fixtures: durable IDs/cleanup + failed progress."""
        import subprocess
        import sys

        from tests.support.stand import listeners, load_profile

        profile = load_profile()
        helper_root: Path | None = None
        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-active-exc-") as tmp:
            helper_root = Path(tmp)
            marker = helper_root / "ready.json"
            child = helper_root / "active-exception-child.py"
            child.write_text(
                "\n".join(
                    [
                        "import json, sys",
                        "from pathlib import Path",
                        "sys.path.insert(0, sys.argv[1])",
                        "import tests.support.e2e_suite as suite",
                        "from tests.support.stand import IsolatedStand",
                        "suite.ALL_COMPONENT_SUITES = ('api',)",
                        "original = IsolatedStand.create_synthetic_session",
                        "def boom(self, *args, **kwargs):",
                        "    result = original(self, *args, **kwargs)",
                        "    Path(sys.argv[2]).write_text(",
                        "        json.dumps({",
                        "            'run_id': self.run_id,",
                        "            'synthetic_ids': dict(self.synthetic_ids) or result,",
                        "        }),",
                        "        encoding='utf-8',",
                        "    )",
                        "    raise RuntimeError('controlled failure after service writes')",
                        "IsolatedStand.create_synthetic_session = boom",
                        "suite.run_all_suite(",
                        "    mode='isolated',",
                        "    worktree=Path(sys.argv[1]),",
                        "    argv=['python3', sys.argv[0], sys.argv[1], sys.argv[2]],",
                        ")",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
            proc = subprocess.run(
                [sys.executable, str(child), str(ROOT), str(marker)],
                capture_output=True,
                text=True,
                env=env,
                timeout=180,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertTrue(marker.exists(), "expected ready marker after synthetic session")
            ready_payload = json.loads(marker.read_text(encoding="utf-8"))
            journals = list(Path(profile.run_root).glob("journals/all-*/api.json"))
            self.assertTrue(journals, "expected api component receipt")
            api_payload = None
            receipt_root = None
            for path in sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True):
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("run_id") == ready_payload.get("run_id"):
                    api_payload = payload
                    receipt_root = path.parent
                    break
            if api_payload is None:
                receipt_root = sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True)[0].parent
                api_payload = json.loads((receipt_root / "api.json").read_text(encoding="utf-8"))
            self.assertEqual(api_payload.get("status"), "failed")
            self.assertEqual(api_payload.get("run_id"), ready_payload.get("run_id"))
            self.assertTrue(api_payload.get("synthetic_ids"))
            self.assertIsNotNone(api_payload.get("binary"))
            cleanup = api_payload.get("cleanup") or {}
            self.assertIsInstance(cleanup, dict)
            self.assertTrue(cleanup.get("confirmed") is True)
            progress = json.loads((receipt_root / "progress.json").read_text(encoding="utf-8"))
            self.assertEqual(progress.get("phase"), "failed")
            aggregate = json.loads((receipt_root / "aggregate.json").read_text(encoding="utf-8"))
            self.assertEqual(aggregate.get("terminal_phase"), "failed")
            for port in profile.ports.values():
                self.assertFalse(listeners(int(port)), f"port {port} still busy after exception")
            run_dir = Path(profile.run_root) / str(ready_payload.get("run_id"))
            self.assertFalse(
                (run_dir / "resource-manifest.json").exists(),
                "owned manifest must not survive exceptional teardown",
            )
        self.assertIsNotNone(helper_root)
        self.assertFalse(
            helper_root.exists(),
            f"helper root must be removed after active-exception case: {helper_root}",
        )

    def test_run_all_active_stand_sigterm_preserves_receipt_evidence(self) -> None:
        """Real API stand SIGTERM: raw rc=143, terminal progress, live IDs/cleanup."""
        import signal
        import subprocess
        import sys
        import time

        from tests.support.stand import listeners, load_profile

        profile = load_profile()
        helper_root: Path | None = None
        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-active-sig-") as tmp:
            helper_root = Path(tmp)
            ready = helper_root / "ready.json"
            child = helper_root / "active-signal-child.py"
            child.write_text(
                "\n".join(
                    [
                        "import json, signal, sys",
                        "from pathlib import Path",
                        "sys.path.insert(0, sys.argv[1])",
                        "import tests.support.e2e_suite as suite",
                        "from tests.support.stand import IsolatedStand",
                        "suite.ALL_COMPONENT_SUITES = ('api',)",
                        "original = IsolatedStand.create_synthetic_session",
                        "def ready_pause(self, *args, **kwargs):",
                        "    result = original(self, *args, **kwargs)",
                        "    Path(sys.argv[2]).write_text(",
                        "        json.dumps({'run_id': self.run_id, 'synthetic_ids': dict(self.synthetic_ids) or result}),",
                        "        encoding='utf-8',",
                        "    )",
                        "    signal.pause()",
                        "    return result",
                        "IsolatedStand.create_synthetic_session = ready_pause",
                        "suite.run_all_suite(",
                        "    mode='isolated',",
                        "    worktree=Path(sys.argv[1]),",
                        "    argv=['python3', sys.argv[0], sys.argv[1], sys.argv[2]],",
                        ")",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
            proc = subprocess.Popen(
                [sys.executable, str(child), str(ROOT), str(ready)],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline and not ready.exists():
                    if proc.poll() is not None:
                        out, err = proc.communicate()
                        self.fail(
                            f"child exited before ready: rc={proc.returncode}\n"
                            f"stdout={out[-2000:]}\nstderr={err[-2000:]}"
                        )
                    time.sleep(0.1)
                self.assertTrue(ready.exists(), "expected ready marker after synthetic session")
                ready_payload = json.loads(ready.read_text(encoding="utf-8"))
                os.kill(proc.pid, signal.SIGTERM)
                try:
                    proc.communicate(timeout=90)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=10)
                    self.fail("active-stand child did not exit after SIGTERM")
                self.assertEqual(proc.returncode, 128 + signal.SIGTERM)
                journals = list(Path(profile.run_root).glob("journals/all-*/api.json"))
                self.assertTrue(journals, "expected api component receipt")
                # Prefer the receipt matching this run_id when multiple exist.
                api_payload = None
                for path in sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True):
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if payload.get("run_id") == ready_payload.get("run_id"):
                        api_payload = payload
                        receipt_root = path.parent
                        break
                if api_payload is None:
                    receipt_root = sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True)[0].parent
                    api_payload = json.loads((receipt_root / "api.json").read_text(encoding="utf-8"))
                self.assertEqual(api_payload.get("rc"), 128 + signal.SIGTERM)
                self.assertEqual(api_payload.get("run_id"), ready_payload.get("run_id"))
                self.assertTrue(api_payload.get("synthetic_ids"))
                cleanup = api_payload.get("cleanup") or {}
                self.assertIsInstance(cleanup, dict)
                self.assertTrue(cleanup.get("confirmed") is True)
                progress = json.loads((receipt_root / "progress.json").read_text(encoding="utf-8"))
                self.assertEqual(progress.get("phase"), "interrupted-by-signal")
                aggregate = json.loads((receipt_root / "aggregate.json").read_text(encoding="utf-8"))
                self.assertEqual(aggregate.get("terminal_phase"), "interrupted-by-signal")
                self.assertEqual((aggregate.get("interruption") or {}).get("rc"), 128 + signal.SIGTERM)
                for port in profile.ports.values():
                    self.assertFalse(listeners(int(port)), f"port {port} still busy after signal")
                run_dir = Path(profile.run_root) / str(ready_payload.get("run_id"))
                self.assertFalse(
                    (run_dir / "resource-manifest.json").exists(),
                    "owned manifest must not survive active-stand SIGTERM",
                )
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
        self.assertIsNotNone(helper_root)
        self.assertFalse(
            helper_root.exists(),
            f"helper root must be removed after active-signal case: {helper_root}",
        )

    def _run_reexec_signal_case(self, *, suite: str, signum: int) -> None:
        """Real reexec suite + aggregate signal: cleanup + durable component receipt."""
        import signal as signal_mod
        import subprocess
        import sys
        import time

        from tests.support.stand import listeners, load_profile

        profile = load_profile()
        vikunja_port = int(profile.ports["vikunja"])
        signame = signal_mod.Signals(signum).name
        helper_root: Path | None = None
        with tempfile.TemporaryDirectory(
            prefix=f"EA010-SYNTHETIC-reexec-{suite}-{signame.lower()}-"
        ) as tmp:
            helper_root = Path(tmp)
            work = helper_root
            child = work / "reexec-signal-child.py"
            session_ready = work / "session-ready.json"
            child.write_text(
                "\n".join(
                    [
                        "import sys",
                        "from pathlib import Path",
                        "sys.path.insert(0, sys.argv[1])",
                        "import tests.support.e2e_suite as s",
                        f"s.ALL_COMPONENT_SUITES = ({suite!r},)",
                        "s.run_all_suite(",
                        "    mode='isolated',",
                        "    worktree=Path(sys.argv[1]),",
                        "    argv=['python3', sys.argv[0], sys.argv[1]],",
                        ")",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                # Force the reexec path even when the outer interpreter already has FastAPI.
                "EA010_FORCE_HERMES_REEXEC": "1",
                "EA010_STAND_SESSION_READY": str(session_ready),
            }
            proc = subprocess.Popen(
                [sys.executable, str(child), str(ROOT)],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            inner_pid = None
            ready_payload = None
            try:
                deadline = time.monotonic() + 240
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        out, err = proc.communicate()
                        self.fail(
                            f"reexec child exited before live stand: rc={proc.returncode}\n"
                            f"stdout={out[-2000:]}\nstderr={err[-2000:]}"
                        )
                    try:
                        listening = listeners(vikunja_port)
                    except Exception:
                        listening = set()
                    if listening:
                        children = []
                        try:
                            out = subprocess.check_output(
                                ["/bin/ps", "-o", "pid=,ppid=", "-ax"],
                                text=True,
                            )
                            for line in out.splitlines():
                                parts = line.split()
                                if len(parts) >= 2 and parts[1] == str(proc.pid):
                                    children.append(int(parts[0]))
                        except Exception:
                            children = []
                        if children:
                            inner_pid = children[0]
                    if inner_pid is not None and session_ready.is_file():
                        ready_payload = json.loads(session_ready.read_text(encoding="utf-8"))
                        if ready_payload.get("run_id") and ready_payload.get("synthetic_ids"):
                            break
                    time.sleep(0.2)
                self.assertIsNotNone(inner_pid, f"expected {suite} reexec child PID")
                self.assertTrue(listeners(vikunja_port), f"expected live Vikunja before {signame}")
                self.assertTrue(
                    ready_payload and ready_payload.get("synthetic_ids"),
                    f"expected synthetic session before {signame}; ready={ready_payload}",
                )
                manifests_before = {
                    str(p) for p in Path(profile.run_root).glob("*/resource-manifest.json")
                }
                os.kill(proc.pid, signum)
                try:
                    proc.communicate(timeout=120)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=10)
                    self.fail(f"reexec aggregate did not exit after {signame}")
                self.assertEqual(proc.returncode, -signum)
                still = subprocess.run(
                    ["/bin/ps", "-p", str(inner_pid), "-o", "pid="],
                    capture_output=True,
                    text=True,
                )
                self.assertFalse(
                    (still.stdout or "").strip(),
                    f"reexec inner PID {inner_pid} still alive after aggregate {signame}",
                )
                for name, port in profile.ports.items():
                    self.assertFalse(
                        listeners(int(port)),
                        f"port {name}={port} still busy after aggregate {signame}",
                    )
                manifests_after = {
                    str(p) for p in Path(profile.run_root).glob("*/resource-manifest.json")
                }
                created = manifests_after - manifests_before
                self.assertEqual(
                    created,
                    set(),
                    f"new owned manifests survived aggregate {signame}: {sorted(created)}",
                )
                progress_files = list(Path(profile.run_root).glob("journals/all-*/progress.json"))
                self.assertTrue(progress_files)
                newest = max(progress_files, key=lambda p: p.stat().st_mtime)
                receipt_root = newest.parent
                progress = json.loads(newest.read_text(encoding="utf-8"))
                self.assertEqual(progress.get("phase"), "interrupted-by-signal")
                self.assertEqual(progress.get("current_suite"), suite)
                cancel = ((progress.get("signal") or {}).get("component_cancel") or {})
                self.assertTrue(
                    cancel.get("children") or cancel.get("error") is None,
                    f"expected component_cancel evidence in progress.signal: {cancel}",
                )
                # s10 requires durable component receipts for signal runs too.
                component_path = receipt_root / f"{suite}.json"
                self.assertTrue(
                    component_path.is_file(),
                    f"expected {suite}.json receipt after reexec {signame}; "
                    f"dir={sorted(p.name for p in receipt_root.iterdir())}",
                )
                component = json.loads(component_path.read_text(encoding="utf-8"))
                self.assertEqual(component.get("status"), "interrupted-by-signal")
                self.assertEqual(component.get("run_id"), ready_payload.get("run_id"))
                component_ids = component.get("synthetic_ids") or {}
                ready_ids = ready_payload.get("synthetic_ids") or {}
                self.assertTrue(component_ids, f"missing synthetic_ids in {suite} receipt: {component}")
                # Bridge may add hermes_write_token_id after create_synthetic_session;
                # require the session-ready IDs to be preserved in the signal receipt.
                for key, value in ready_ids.items():
                    self.assertEqual(
                        component_ids.get(key),
                        value,
                        f"synthetic_ids[{key!r}] mismatch in {suite} receipt: {component_ids}",
                    )
                cleanup = component.get("cleanup") or {}
                self.assertIsInstance(cleanup, dict)
                self.assertTrue(
                    cleanup.get("confirmed") is True,
                    f"expected cleanup.confirmed in {suite} receipt: {cleanup}",
                )
                children = cancel.get("children") or []
                self.assertTrue(
                    any(isinstance(c, dict) and c.get("signal_evidence") for c in children),
                    f"expected ingested signal_evidence on component_cancel children: {cancel}",
                )
                aggregate = json.loads((receipt_root / "aggregate.json").read_text(encoding="utf-8"))
                self.assertEqual(aggregate.get("terminal_phase"), "interrupted-by-signal")
                self.assertIn(suite, (aggregate.get("components") or {}))
                self.assertEqual(
                    ((aggregate.get("components") or {}).get(suite) or {}).get("run_id"),
                    component.get("run_id"),
                )
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
                if inner_pid is not None:
                    subprocess.run(
                        ["/bin/kill", "-9", str(inner_pid)],
                        capture_output=True,
                        check=False,
                    )
        self.assertIsNotNone(helper_root)
        self.assertFalse(
            helper_root.exists(),
            f"helper root must be removed after reexec-signal case: {helper_root}",
        )

    def test_run_all_hermes_ux_reexec_sigterm_cleans_owned_tree(self) -> None:
        """Real hermes-ux reexec: aggregate SIGTERM cleans + persists component receipt."""
        import signal

        self._run_reexec_signal_case(suite="hermes-ux", signum=signal.SIGTERM)

    def test_run_all_hermes_ux_reexec_sighup_persists_component_receipt(self) -> None:
        import signal

        self._run_reexec_signal_case(suite="hermes-ux", signum=signal.SIGHUP)

    def test_run_all_hermes_bridge_reexec_sigterm_persists_component_receipt(self) -> None:
        import signal

        self._run_reexec_signal_case(suite="hermes-bridge", signum=signal.SIGTERM)

    def test_run_all_hermes_bridge_reexec_sighup_persists_component_receipt(self) -> None:
        import signal

        self._run_reexec_signal_case(suite="hermes-bridge", signum=signal.SIGHUP)

    def _run_reexec_acquisition_boundary_sigterm(self, *, suite: str) -> None:
        """SIGTERM between Popen and register must still cancel the owned child."""
        import signal
        import subprocess
        import sys
        import time

        from tests.support import stand as stand_mod
        from tests.support.stand import cancel_active_component_procs

        if suite == "hermes-ux":
            from tests.support import hermes_ux_suite as hermes_mod
        elif suite == "hermes-bridge":
            from tests.support import hermes_bridge_suite as hermes_mod
        else:
            raise AssertionError(f"unsupported suite {suite!r}")

        cancel_seen: dict = {}
        child_pid_box: list[int] = []
        prior = signal.signal(signal.SIGTERM, signal.SIG_DFL)

        def handler(signum: int, _frame: object) -> None:
            cancel_seen.clear()
            cancel_seen.update(
                cancel_active_component_procs(signum, wait_timeout=5.0)
            )
            raise SystemExit(128 + signum)

        original_popen = stand_mod.subprocess.Popen
        inner_marker = (
            "run_hermes_ux_suite_inner"
            if suite == "hermes-ux"
            else "run_hermes_bridge_suite_inner"
        )

        def passive_racing_popen(args, **kwargs):  # type: ignore[no-untyped-def]
            argv = [str(a) for a in args]
            # Only race the owned reexec spawn — not preliminary Hermes probes.
            if not any(inner_marker in a for a in argv):
                return original_popen(args, **kwargs)
            # Replace the real reexec payload with a passive sleeper so this
            # acquisition-boundary probe never starts services or live data.
            # Forward kwargs (including preexec_fn) so the child unblocks cancel
            # signals inherited from the parent's temporary mask.
            pass_kwargs = dict(kwargs)
            pass_kwargs["stdout"] = subprocess.DEVNULL
            pass_kwargs["stderr"] = subprocess.DEVNULL
            pass_kwargs.pop("text", None)
            passive = [sys.executable, "-c", "import time; time.sleep(30)"]
            proc = original_popen(passive, **pass_kwargs)
            child_pid_box.append(int(proc.pid))
            os.kill(os.getpid(), signal.SIGTERM)
            return proc

        helper_root: Path | None = None
        with tempfile.TemporaryDirectory(
            prefix=f"EA010-SYNTHETIC-acq-boundary-{suite}-"
        ) as tmp:
            helper_root = Path(tmp)
            marker = helper_root / "unused"
            marker.write_text("synthetic\n", encoding="utf-8")
            signal.signal(signal.SIGTERM, handler)
            try:
                with mock.patch.object(
                    hermes_mod, "_hermes_python", return_value=sys.executable
                ), mock.patch.object(
                    stand_mod.subprocess, "Popen", side_effect=passive_racing_popen
                ), mock.patch.dict(
                    os.environ, {"EA010_FORCE_HERMES_REEXEC": "1"}, clear=False
                ):
                    with self.assertRaises(SystemExit) as raised:
                        if suite == "hermes-ux":
                            hermes_mod.run_hermes_ux_suite(
                                mode="isolated",
                                worktree=ROOT,
                                argv=["python3", "scripts/ea010_verify.py"],
                            )
                        else:
                            hermes_mod.run_hermes_bridge_suite(
                                mode="isolated",
                                worktree=ROOT,
                                argv=["python3", "scripts/ea010_verify.py"],
                            )
                self.assertEqual(raised.exception.code, 128 + signal.SIGTERM)
                self.assertTrue(child_pid_box, "expected passive child pid")
                children = cancel_seen.get("children") or []
                self.assertTrue(
                    children,
                    f"expected registered child in component_cancel: {cancel_seen}",
                )
                entry = children[0]
                self.assertEqual(entry.get("pid"), child_pid_box[0])
                self.assertFalse(
                    entry.get("still_alive"),
                    f"owned child must not survive acquisition-boundary cancel: {entry}",
                )
                # Independent read-back: process table must not retain the child.
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    try:
                        os.kill(child_pid_box[0], 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    try:
                        os.kill(child_pid_box[0], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    self.fail(
                        f"passive child pid {child_pid_box[0]} still alive after cancel"
                    )
            finally:
                signal.signal(signal.SIGTERM, prior)
                for pid in child_pid_box:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        assert helper_root is not None
        self.assertFalse(
            helper_root.exists(),
            f"helper root must be removed after acquisition-boundary case: {helper_root}",
        )

    def test_hermes_ux_reexec_acquisition_boundary_sigterm_cancels_child(self) -> None:
        self._run_reexec_acquisition_boundary_sigterm(suite="hermes-ux")

    def test_hermes_bridge_reexec_acquisition_boundary_sigterm_cancels_child(self) -> None:
        self._run_reexec_acquisition_boundary_sigterm(suite="hermes-bridge")

    def test_spawn_registered_component_proc_sigterm_during_popen_cancels_child(
        self,
    ) -> None:
        """Direct helper: SIGTERM delivered inside Popen still sees a registered child."""
        import signal
        import subprocess
        import sys
        import time

        from tests.support import stand as stand_mod
        from tests.support.stand import (
            cancel_active_component_procs,
            spawn_registered_component_proc,
            unregister_active_component_proc,
        )

        cancel_seen: dict = {}
        prior = signal.signal(signal.SIGTERM, signal.SIG_DFL)
        original_popen = stand_mod.subprocess.Popen

        def racing_popen(args, **kwargs):  # type: ignore[no-untyped-def]
            proc = original_popen(args, **kwargs)
            os.kill(os.getpid(), signal.SIGTERM)
            return proc

        def handler(signum: int, _frame: object) -> None:
            cancel_seen.clear()
            cancel_seen.update(
                cancel_active_component_procs(signum, wait_timeout=5.0)
            )
            raise SystemExit(128 + signum)

        signal.signal(signal.SIGTERM, handler)
        proc = None
        try:
            with mock.patch.object(
                stand_mod.subprocess, "Popen", side_effect=racing_popen
            ):
                with self.assertRaises(SystemExit) as raised:
                    proc = spawn_registered_component_proc(
                        [sys.executable, "-c", "import time; time.sleep(120)"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    if proc is not None:
                        proc.communicate(timeout=1)
            self.assertEqual(raised.exception.code, 128 + signal.SIGTERM)
            children = cancel_seen.get("children") or []
            self.assertTrue(children, f"expected cancel children: {cancel_seen}")
            self.assertFalse(children[0].get("still_alive"))
            pid = children[0].get("pid")
            if isinstance(pid, int):
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
                else:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    self.fail(f"child pid {pid} still alive")
        finally:
            signal.signal(signal.SIGTERM, prior)
            if proc is not None:
                unregister_active_component_proc(proc)
                if proc.poll() is None:
                    try:
                        proc.kill()
                    except OSError:
                        pass

    def _run_reexec_ordinary_failure_case(self, *, suite: str) -> None:
        """Real reexec suite + controlled RuntimeError: durable IDs/cleanup + child rc."""
        import subprocess
        import sys

        from tests.support.stand import (
            SYNTHETIC_FAIL_AFTER_SESSION_ENV,
            listeners,
            load_profile,
        )

        profile = load_profile()
        helper_root: Path | None = None
        with tempfile.TemporaryDirectory(prefix=f"EA010-SYNTHETIC-reexec-fail-{suite}-") as tmp:
            helper_root = Path(tmp)
            work = helper_root
            child = work / "reexec-fail-child.py"
            session_ready = work / "session-ready.json"
            child.write_text(
                "\n".join(
                    [
                        "import sys",
                        "from pathlib import Path",
                        "sys.path.insert(0, sys.argv[1])",
                        "import tests.support.e2e_suite as s",
                        f"s.ALL_COMPONENT_SUITES = ({suite!r},)",
                        "s.run_all_suite(",
                        "    mode='isolated',",
                        "    worktree=Path(sys.argv[1]),",
                        "    argv=['python3', sys.argv[0], sys.argv[1]],",
                        ")",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "EA010_FORCE_HERMES_REEXEC": "1",
                "EA010_STAND_SESSION_READY": str(session_ready),
                SYNTHETIC_FAIL_AFTER_SESSION_ENV: "1",
            }
            proc = subprocess.run(
                [sys.executable, str(child), str(ROOT)],
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertTrue(
                session_ready.is_file(),
                f"expected synthetic session ready marker for {suite}; "
                f"stdout={(proc.stdout or '')[-2000:]}\nstderr={(proc.stderr or '')[-2000:]}",
            )
            ready_payload = json.loads(session_ready.read_text(encoding="utf-8"))
            self.assertTrue(ready_payload.get("run_id"))
            self.assertTrue(ready_payload.get("synthetic_ids"))
            journals = list(Path(profile.run_root).glob(f"journals/all-*/{suite}.json"))
            self.assertTrue(journals, f"expected durable {suite} component receipt")
            component = None
            receipt_root = None
            for path in sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True):
                payload = json.loads(path.read_text(encoding="utf-8"))
                if payload.get("run_id") == ready_payload.get("run_id"):
                    component = payload
                    receipt_root = path.parent
                    break
            if component is None:
                receipt_root = sorted(journals, key=lambda p: p.stat().st_mtime, reverse=True)[0].parent
                component = json.loads((receipt_root / f"{suite}.json").read_text(encoding="utf-8"))
            self.assertEqual(component.get("status"), "failed")
            self.assertEqual(component.get("run_id"), ready_payload.get("run_id"))
            component_ids = component.get("synthetic_ids") or {}
            ready_ids = ready_payload.get("synthetic_ids") or {}
            self.assertTrue(component_ids, f"missing synthetic_ids in {suite} receipt: {component}")
            for key, value in ready_ids.items():
                self.assertEqual(
                    component_ids.get(key),
                    value,
                    f"synthetic_ids[{key!r}] mismatch in {suite} receipt: {component_ids}",
                )
            cleanup = component.get("cleanup") or {}
            self.assertIsInstance(cleanup, dict)
            self.assertTrue(
                cleanup.get("confirmed") is True,
                f"expected cleanup.confirmed in {suite} receipt: {cleanup}",
            )
            self.assertIsInstance(component.get("rc"), int)
            self.assertNotEqual(component.get("rc"), 0)
            progress = json.loads((receipt_root / "progress.json").read_text(encoding="utf-8"))
            self.assertEqual(progress.get("phase"), "failed")
            aggregate = json.loads((receipt_root / "aggregate.json").read_text(encoding="utf-8"))
            self.assertEqual(aggregate.get("terminal_phase"), "failed")
            agg_row = (aggregate.get("components") or {}).get(suite) or {}
            self.assertEqual(agg_row.get("run_id"), component.get("run_id"))
            interruption = aggregate.get("interruption") or {}
            self.assertEqual(interruption.get("run_id"), component.get("run_id"))
            for port in profile.ports.values():
                self.assertFalse(listeners(int(port)), f"port {port} still busy after reexec failure")
            run_dir = Path(profile.run_root) / str(ready_payload.get("run_id"))
            self.assertFalse(
                (run_dir / "resource-manifest.json").exists(),
                "owned manifest must not survive ordinary reexec failure",
            )
        self.assertIsNotNone(helper_root)
        self.assertFalse(
            helper_root.exists(),
            f"helper root must be removed after reexec-fail case: {helper_root}",
        )

    def test_run_all_hermes_ux_reexec_ordinary_failure_preserves_receipt(self) -> None:
        self._run_reexec_ordinary_failure_case(suite="hermes-ux")

    def test_run_all_hermes_bridge_reexec_ordinary_failure_preserves_receipt(self) -> None:
        self._run_reexec_ordinary_failure_case(suite="hermes-bridge")


class VerifyPilotCliTests(unittest.TestCase):
    def _copied_profile_env(self, tmp: Path, *, permit_env: dict[str, str] | None = None) -> tuple[dict[str, str], Path]:
        """Redirect C7 profile run_root to an empty disposable root for CLI refusal probes."""
        from tests.support.stand import DEFAULT_PROFILE_PATH

        src = Path(
            os.environ.get("EA010_ISOLATED_STAND_PROFILE", str(DEFAULT_PROFILE_PATH))
        )
        tmp.mkdir(parents=True, exist_ok=True)
        payload = json.loads(src.read_text(encoding="utf-8"))
        run_root = tmp / "runtime-root"
        run_root.mkdir(parents=True, exist_ok=True)
        payload["run_root"] = str(run_root.resolve())
        profile_path = tmp / "profile-v1.json"
        profile_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        env = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "EA010_ISOLATED_STAND_PROFILE": str(profile_path),
        }
        env.pop(PERMIT_ENV, None)
        if permit_env:
            env.update(permit_env)
        return env, run_root

    def test_verify_approved_pilot_without_permit_nonzero(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-cli-") as tmp:
            env, run_root = self._copied_profile_env(Path(tmp))
            before = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            combined = (proc.stdout or "") + (proc.stderr or "")
            self.assertIn("approved-pilot", combined.lower())
            self.assertIn("not-run", combined.lower())
            self.assertNotIn('"ok": true', combined.lower().replace(" ", ""))
            after = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            self.assertEqual(
                after,
                before,
                f"no-permit CLI must not write under runtime root; created={sorted(after - before)}",
            )
            self.assertFalse(
                (run_root / "journals").exists(),
                "journals/ must remain absent without exact permit",
            )
            compact = combined.lower().replace(" ", "")
            self.assertIn('"runtime_root_writes":false', compact)
            self.assertIn('"ok":false', compact)

    def test_verify_approved_pilot_invalid_permit_no_runtime_root_writes(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-invalid-") as tmp:
            base = Path(tmp)
            permit_path = base / "permit.json"
            permit_path.write_text(json.dumps({"schema": "wrong"}), encoding="utf-8")
            os.chmod(permit_path, 0o600)
            env, run_root = self._copied_profile_env(
                base / "runtime",
                permit_env={PERMIT_ENV: str(permit_path)},
            )
            before = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            after = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            self.assertEqual(
                after,
                before,
                f"invalid-permit CLI must not write under runtime root; created={sorted(after - before)}",
            )
            compact = ((proc.stderr or "") + (proc.stdout or "")).lower().replace(" ", "")
            self.assertIn('"ok":false', compact)
            self.assertIn('"runtime_root_writes":false', compact)
            self.assertNotIn('"ok":true', compact)

    def test_verify_approved_pilot_unknown_suite_nonzero(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-unknown-") as tmp:
            env, run_root = self._copied_profile_env(Path(tmp))
            before_files = {
                str(p.relative_to(run_root)): p.stat().st_mtime_ns
                for p in run_root.rglob("*")
                if p.is_file()
            }
            before_tree = {str(p.relative_to(run_root)) for p in run_root.rglob("*")}
            proc = subprocess.run(
                [
                    sys.executable,
                    "scripts/ea010_verify.py",
                    "--suite",
                    "does-not-exist",
                    "--mode",
                    "approved-pilot",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            combined = (proc.stdout or "") + (proc.stderr or "")
            self.assertIn("unknown suite", combined.lower())
            compact = combined.lower().replace(" ", "")
            self.assertIn('"runtime_root_writes":false', compact)
            self.assertIn('"ok":false', compact)
            after_files = {
                str(p.relative_to(run_root)): p.stat().st_mtime_ns
                for p in run_root.rglob("*")
                if p.is_file()
            }
            after_tree = {str(p.relative_to(run_root)) for p in run_root.rglob("*")}
            self.assertEqual(
                after_tree,
                before_tree,
                f"unknown-suite approved-pilot must not mutate runtime tree; "
                f"added={sorted(after_tree - before_tree)} "
                f"removed={sorted(before_tree - after_tree)}",
            )
            self.assertEqual(after_files, before_files)
            self.assertFalse(
                (run_root / "journals").exists(),
                "journals/ must remain absent for unpermitted unknown-suite refusal",
            )

    def test_verify_approved_pilot_review_evidence_outside_runtime_creates(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-review-ok-") as tmp:
            base = Path(tmp)
            env, run_root = self._copied_profile_env(base / "runtime")
            evidence = base / "outside" / "refusal.json"
            env["EA010_REVIEW_EVIDENCE"] = str(evidence)
            before = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            after = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            self.assertEqual(after, before)
            self.assertTrue(evidence.is_file())
            payload = json.loads(evidence.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("schema"), "ea010-approved-pilot-refusal/v1")
            self.assertEqual(payload.get("rc"), 2)

    def test_verify_approved_pilot_review_evidence_inside_runtime_refused(self) -> None:
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-review-bad-") as tmp:
            base = Path(tmp)
            env, run_root = self._copied_profile_env(base / "runtime")
            existing = run_root / "keep-me.json"
            existing.write_text('{"preserve":true}\n', encoding="utf-8")
            os.chmod(existing, 0o600)
            before_text = existing.read_text(encoding="utf-8")
            before_mtime = existing.stat().st_mtime_ns
            env["EA010_REVIEW_EVIDENCE"] = str(existing)
            before = {
                str(p.relative_to(run_root)): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else b"")
                for p in run_root.rglob("*")
                if p.is_file()
            }
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            after = {
                str(p.relative_to(run_root)): (p.stat().st_mtime_ns, p.read_bytes() if p.is_file() else b"")
                for p in run_root.rglob("*")
                if p.is_file()
            }
            self.assertEqual(after, before)
            self.assertEqual(existing.read_text(encoding="utf-8"), before_text)
            self.assertEqual(existing.stat().st_mtime_ns, before_mtime)
            compact = ((proc.stderr or "") + (proc.stdout or "")).lower().replace(" ", "")
            self.assertIn('"runtime_root_writes":false', compact)
            # Symlink alias into runtime must also leave the target untouched.
            alias_dir = base / "alias-outside"
            alias_dir.mkdir()
            alias = alias_dir / "via-symlink.json"
            alias.symlink_to(existing)
            env["EA010_REVIEW_EVIDENCE"] = str(alias)
            proc2 = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc2.returncode, 2)
            self.assertEqual(existing.read_text(encoding="utf-8"), before_text)
            self.assertEqual(existing.stat().st_mtime_ns, before_mtime)
            self.assertFalse((run_root / "refusal.json").exists())

    def test_verify_approved_pilot_review_evidence_owned_path_refused(self) -> None:
        """Denied permit: distinct owned service file outside C7 run_root stays untouched."""
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-owned-ev-") as tmp:
            base = Path(tmp)
            owned_root = (base / "owned-service").resolve()
            owned_root.mkdir(parents=True, exist_ok=True)
            owned_file = owned_root / "service-state.json"
            owned_file.write_text('{"preserve":"owned"}\n', encoding="utf-8")
            os.chmod(owned_file, 0o600)
            before_text = owned_file.read_text(encoding="utf-8")
            before_mtime = owned_file.stat().st_mtime_ns
            before_mode = owned_file.stat().st_mode & 0o777
            resources = {
                "schema": RESOURCES_SCHEMA,
                "service_bindings": [{"label": "local.cto.vikunja"}],
                "owned_paths": [str(owned_root)],
                "run_root": str(owned_root),
            }
            permit = _permit(resources, allow_live_pilot=False)
            permit_path = _write_permit(base / "permit.json", permit)
            env, run_root = self._copied_profile_env(
                base / "runtime",
                permit_env={PERMIT_ENV: str(permit_path)},
            )
            env["EA010_REVIEW_EVIDENCE"] = str(owned_file)
            before_runtime = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertEqual(owned_file.read_text(encoding="utf-8"), before_text)
            self.assertEqual(owned_file.stat().st_mtime_ns, before_mtime)
            self.assertEqual(owned_file.stat().st_mode & 0o777, before_mode)
            after_runtime = {str(p.relative_to(run_root)) for p in run_root.rglob("*") if p.is_file()}
            self.assertEqual(after_runtime, before_runtime)
            compact = ((proc.stderr or "") + (proc.stdout or "")).lower().replace(" ", "")
            self.assertIn('"mutations":false', compact)
            self.assertIn('"runtime_root_writes":false', compact)

    def test_verify_approved_pilot_review_evidence_hardlink_owned_preserved(self) -> None:
        """Outside hard-link alias must not mutate permit-owned bytes/mode on refusal."""
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-hardlink-ev-") as tmp:
            base = Path(tmp)
            owned_root = (base / "owned").resolve()
            owned_root.mkdir(parents=True, exist_ok=True)
            protected = owned_root / "service-config.json"
            protected.write_text("SYNTHETIC-KEEP\n", encoding="utf-8")
            os.chmod(protected, 0o640)
            before_bytes = protected.read_bytes()
            before_mode = protected.stat().st_mode & 0o777
            before_mtime = protected.stat().st_mtime_ns
            review_dir = base / "review"
            review_dir.mkdir(parents=True, exist_ok=True)
            alias = review_dir / "evidence.json"
            os.link(protected, alias)
            self.assertGreater(protected.stat().st_nlink, 1)
            self.assertEqual(protected.stat().st_ino, alias.stat().st_ino)
            # Invalid-schema permit still contributes owned_paths for containment.
            permit = {
                "schema": "intentionally-invalid",
                "resources_manifest": {
                    "run_root": str((base / "runtime").resolve()),
                    "owned_paths": [str(owned_root)],
                },
            }
            permit_path = base / "permit.json"
            permit_path.write_text(json.dumps(permit), encoding="utf-8")
            os.chmod(permit_path, 0o600)
            env, run_root = self._copied_profile_env(
                base / "runtime",
                permit_env={PERMIT_ENV: str(permit_path)},
            )
            env["EA010_REVIEW_EVIDENCE"] = str(alias)
            before_runtime = {
                str(p.relative_to(run_root)): p.read_bytes()
                for p in run_root.rglob("*")
                if p.is_file()
            }
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertEqual(protected.read_bytes(), before_bytes)
            self.assertEqual(protected.stat().st_mode & 0o777, before_mode)
            self.assertEqual(protected.stat().st_mtime_ns, before_mtime)
            self.assertEqual(alias.read_bytes(), before_bytes)
            self.assertEqual(alias.stat().st_mode & 0o777, before_mode)
            self.assertEqual(protected.stat().st_ino, alias.stat().st_ino)
            after_runtime = {
                str(p.relative_to(run_root)): p.read_bytes()
                for p in run_root.rglob("*")
                if p.is_file()
            }
            self.assertEqual(after_runtime, before_runtime)
            compact = ((proc.stderr or "") + (proc.stdout or "")).lower().replace(" ", "")
            self.assertIn('"mutations":false', compact)
            self.assertIn('"runtime_root_writes":false', compact)
            self.assertEqual(protected.read_text(encoding="utf-8"), "SYNTHETIC-KEEP\n")
            self.assertNotIn("ea010-approved-pilot-refusal", protected.read_text(encoding="utf-8"))

    def test_verify_approved_pilot_review_evidence_parent_swap_owned_preserved(self) -> None:
        """Concurrent rename+symlink of validated review parent must not hit owned files."""
        import contextlib
        import importlib.util
        import io
        import sys

        from tests.support.stand import DEFAULT_PROFILE_PATH

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-parent-swap-") as tmp:
            base = Path(tmp).resolve()
            runtime = base / "runtime"
            runtime.mkdir()
            owned = base / "owned"
            owned.mkdir()
            review = base / "review"
            review.mkdir()
            protected = owned / "service-config.json"
            protected.write_text("SYNTHETIC-PROTECTED", encoding="utf-8")
            os.chmod(protected, 0o640)
            before_bytes = protected.read_bytes()
            before_mode = protected.stat().st_mode & 0o777
            payload = json.loads(DEFAULT_PROFILE_PATH.read_text(encoding="utf-8"))
            payload["run_root"] = str(runtime)
            profile_path = base / "profile.json"
            profile_path.write_text(json.dumps(payload), encoding="utf-8")
            permit_path = base / "invalid-permit.json"
            permit_path.write_text(
                json.dumps(
                    {
                        "schema": "invalid",
                        "resources_manifest": {
                            "run_root": str(runtime),
                            "owned_paths": [str(owned)],
                        },
                    }
                ),
                encoding="utf-8",
            )
            dest = review / "service-config.json"
            spec = importlib.util.spec_from_file_location(
                "ea010_verify_parent_swap",
                ROOT / "scripts" / "ea010_verify.py",
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            original_open = module.os.open
            swapped: list[bool] = []

            def interleaved_open(path, *args, **kwargs):
                # After the parent dirfd is bound, swap the path name on the first
                # exclusive temp create. dirfd openat must keep writing the original
                # inode; a path-only writer would follow the new symlink into owned.
                flags = args[0] if args else int(kwargs.get("flags") or 0)
                name = Path(path).name
                if (
                    name.startswith(".ea010-review-")
                    and (flags & os.O_EXCL)
                    and not swapped
                    and review.exists()
                    and not review.is_symlink()
                ):
                    review.rename(base / "review-original")
                    review.symlink_to(owned, target_is_directory=True)
                    swapped.append(True)
                return original_open(path, *args, **kwargs)

            env = {
                "EA010_ISOLATED_STAND_PROFILE": str(profile_path),
                "EA010_APPROVED_PILOT_PERMIT": str(permit_path),
                "EA010_REVIEW_EVIDENCE": str(dest),
            }
            stderr = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                module.os, "open", side_effect=interleaved_open
            ), contextlib.redirect_stderr(stderr):
                # Keep sys.path identical to the CLI entry for suite discovery.
                if str(ROOT) not in sys.path:
                    sys.path.insert(0, str(ROOT))
                rc = module.main(["--suite", "all", "--mode", "approved-pilot"])
            self.assertEqual(rc, 2)
            self.assertTrue(swapped, "expected deterministic parent swap at first write open")
            self.assertEqual(protected.read_bytes(), before_bytes)
            self.assertEqual(protected.stat().st_mode & 0o777, before_mode)
            self.assertEqual(protected.read_text(encoding="utf-8"), "SYNTHETIC-PROTECTED")
            self.assertNotIn("ea010-approved-pilot-refusal", protected.read_text(encoding="utf-8"))
            refusal = json.loads(stderr.getvalue())
            self.assertFalse(refusal.get("mutations"))
            self.assertFalse(refusal.get("runtime_root_writes"))
            # Evidence may land on the original review inode (via dirfd) or be refused.
            original_dest = base / "review-original" / "service-config.json"
            if original_dest.is_file():
                evidence = json.loads(original_dest.read_text(encoding="utf-8"))
                self.assertEqual(evidence.get("schema"), "ea010-approved-pilot-refusal/v1")
            if dest.exists() and not dest.is_symlink():
                # Path now resolves through the symlink into owned — must stay intact.
                self.assertEqual(dest.read_bytes(), before_bytes)

    def test_verify_approved_pilot_review_evidence_intermediate_ancestor_swap_refused(
        self,
    ) -> None:
        """Intermediate-ancestor symlink into owned before parent fd walk must not write."""
        import contextlib
        import importlib.util
        import io
        import sys

        from tests.support.stand import DEFAULT_PROFILE_PATH

        with tempfile.TemporaryDirectory(
            prefix="EA010-SYNTHETIC-pilot-ancestor-swap-"
        ) as tmp:
            base = Path(tmp).resolve()
            runtime = base / "runtime"
            runtime.mkdir()
            owned = base / "owned"
            owned.mkdir()
            owned_leaf = owned / "leaf"
            owned_leaf.mkdir()
            protected = owned_leaf / "evidence.json"
            protected.write_text("SYNTHETIC-OWNED-KEEP\n", encoding="utf-8")
            os.chmod(protected, 0o640)
            before_bytes = protected.read_bytes()
            before_mode = protected.stat().st_mode & 0o777
            outside = base / "outside"
            branch = outside / "branch"
            leaf = branch / "leaf"
            leaf.mkdir(parents=True)
            dest = leaf / "evidence.json"
            payload = json.loads(DEFAULT_PROFILE_PATH.read_text(encoding="utf-8"))
            payload["run_root"] = str(runtime)
            profile_path = base / "profile.json"
            profile_path.write_text(json.dumps(payload), encoding="utf-8")
            permit_path = base / "invalid-permit.json"
            permit_path.write_text(
                json.dumps(
                    {
                        "schema": "invalid",
                        "resources_manifest": {
                            "run_root": str(runtime),
                            "owned_paths": [str(owned)],
                        },
                    }
                ),
                encoding="utf-8",
            )
            spec = importlib.util.spec_from_file_location(
                "ea010_verify_ancestor_swap",
                ROOT / "scripts" / "ea010_verify.py",
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            original_ensure = module._ensure_parent_dirfd
            swapped: list[bool] = []

            def racing_ensure(path, mode=0o700, forbidden_roots=None):
                # Race after validate_review_evidence_destination accepted the
                # outside path: replace intermediate ancestor with a symlink into
                # owned before the component dirfd walk begins.
                if not swapped and branch.exists() and not branch.is_symlink():
                    branch.rename(base / "branch-original")
                    branch.symlink_to(owned, target_is_directory=True)
                    swapped.append(True)
                return original_ensure(
                    path, mode, forbidden_roots=forbidden_roots
                )

            env = {
                "EA010_ISOLATED_STAND_PROFILE": str(profile_path),
                "EA010_APPROVED_PILOT_PERMIT": str(permit_path),
                "EA010_REVIEW_EVIDENCE": str(dest),
            }
            stderr = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                module, "_ensure_parent_dirfd", side_effect=racing_ensure
            ), contextlib.redirect_stderr(stderr):
                if str(ROOT) not in sys.path:
                    sys.path.insert(0, str(ROOT))
                rc = module.main(["--suite", "all", "--mode", "approved-pilot"])
            self.assertEqual(rc, 2)
            self.assertTrue(swapped, "expected intermediate ancestor swap before walk")
            self.assertEqual(protected.read_bytes(), before_bytes)
            self.assertEqual(protected.stat().st_mode & 0o777, before_mode)
            self.assertEqual(
                protected.read_text(encoding="utf-8"), "SYNTHETIC-OWNED-KEEP\n"
            )
            self.assertNotIn(
                "ea010-approved-pilot-refusal",
                protected.read_text(encoding="utf-8"),
            )
            refusal = json.loads(stderr.getvalue())
            self.assertFalse(refusal.get("mutations"))
            self.assertFalse(refusal.get("runtime_root_writes"))
            # Path now resolves into owned via the swapped ancestor — must stay intact.
            if dest.exists():
                self.assertEqual(dest.read_bytes(), before_bytes)

    def test_verify_approved_pilot_symlink_permit_preserves_owned_evidence_target(self) -> None:
        """Refused symlink permit must still protect owned_paths from review evidence."""
        import subprocess
        import sys

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-symlink-permit-") as tmp:
            base = Path(tmp)
            owned_root = (base / "owned-service").resolve()
            owned_root.mkdir(parents=True, exist_ok=True)
            owned_file = owned_root / "service-state.json"
            owned_file.write_text('{"preserve":true}\n', encoding="utf-8")
            os.chmod(owned_file, 0o600)
            before_bytes = owned_file.read_bytes()
            before_mtime = owned_file.stat().st_mtime_ns
            before_mode = owned_file.stat().st_mode & 0o777
            resources = {
                "schema": RESOURCES_SCHEMA,
                "service_bindings": [{"label": "EA010-SYNTHETIC-service"}],
                "owned_paths": [str(owned_root)],
                "run_root": str(owned_root),
            }
            permit = _permit(resources, allow_live_pilot=False)
            permit_path = _write_permit(base / "permit.json", permit)
            alias = base / "permit-link.json"
            alias.symlink_to(permit_path)
            env, run_root = self._copied_profile_env(
                base / "runtime",
                permit_env={PERMIT_ENV: str(alias)},
            )
            env["EA010_REVIEW_EVIDENCE"] = str(owned_file)
            before_runtime = {
                str(p.relative_to(run_root)): p.read_bytes()
                for p in run_root.rglob("*")
                if p.is_file()
            }
            # Destination outside both C7 and owned roots remains writable only
            # when configured permit roots were established; also probe that a
            # dangling/unreadable configured permit refuses all evidence writes.
            outside = base / "outside" / "refusal.json"
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc.returncode, 2)
            combined = (proc.stderr or "") + (proc.stdout or "")
            self.assertIn("must not be a symlink", combined.lower())
            self.assertEqual(owned_file.read_bytes(), before_bytes)
            self.assertEqual(owned_file.stat().st_mtime_ns, before_mtime)
            self.assertEqual(owned_file.stat().st_mode & 0o777, before_mode)
            after_runtime = {
                str(p.relative_to(run_root)): p.read_bytes()
                for p in run_root.rglob("*")
                if p.is_file()
            }
            self.assertEqual(after_runtime, before_runtime)
            compact = combined.lower().replace(" ", "")
            self.assertIn('"mutations":false', compact)
            self.assertIn('"runtime_root_writes":false', compact)
            self.assertNotEqual(
                json.loads(owned_file.read_text(encoding="utf-8")).get("schema"),
                "ea010-approved-pilot-refusal/v1",
            )
            # Outside destination with established (resolved) permit roots may
            # receive optional evidence; owned target above must stay intact.
            env["EA010_REVIEW_EVIDENCE"] = str(outside)
            proc2 = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc2.returncode, 2)
            self.assertEqual(owned_file.read_bytes(), before_bytes)
            if outside.exists():
                payload = json.loads(outside.read_text(encoding="utf-8"))
                self.assertEqual(payload.get("schema"), "ea010-approved-pilot-refusal/v1")
            # Unresolvable configured permit must refuse every evidence write.
            dangling = base / "dangling-permit.json"
            dangling.symlink_to(base / "missing-permit.json")
            env[PERMIT_ENV] = str(dangling)
            env["EA010_REVIEW_EVIDENCE"] = str(outside)
            if outside.exists():
                outside.unlink()
            proc3 = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertEqual(proc3.returncode, 2)
            self.assertFalse(outside.exists())
            self.assertEqual(owned_file.read_bytes(), before_bytes)

    def test_verify_approved_pilot_review_evidence_invalid_profile_refused(self) -> None:
        """Early profile failure: refuse evidence writes even into configured runtime."""
        import subprocess
        import sys

        from tests.support.stand import DEFAULT_PROFILE_PATH

        with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-pilot-bad-profile-") as tmp:
            base = Path(tmp)
            runtime = (base / "configured-runtime").resolve()
            runtime.mkdir(parents=True, exist_ok=True)
            keep = runtime / "keep-me.json"
            keep.write_text('{"preserve":true}\n', encoding="utf-8")
            os.chmod(keep, 0o600)
            before_text = keep.read_text(encoding="utf-8")
            before_mtime = keep.stat().st_mtime_ns
            before_mode = keep.stat().st_mode & 0o777
            src = Path(
                os.environ.get("EA010_ISOLATED_STAND_PROFILE", str(DEFAULT_PROFILE_PATH))
            )
            payload = json.loads(src.read_text(encoding="utf-8"))
            payload["run_root"] = str(runtime)
            payload["schema"] = "invalid-stand-profile"
            profile_path = base / "broken-profile.json"
            profile_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            # Outside path that would be writable if containment were skipped.
            outside = base / "outside" / "refusal.json"
            env = {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "EA010_ISOLATED_STAND_PROFILE": str(profile_path),
                "EA010_REVIEW_EVIDENCE": str(keep),
            }
            env.pop(PERMIT_ENV, None)
            before_tree = {str(p.relative_to(runtime)) for p in runtime.rglob("*") if p.is_file()}
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(keep.read_text(encoding="utf-8"), before_text)
            self.assertEqual(keep.stat().st_mtime_ns, before_mtime)
            self.assertEqual(keep.stat().st_mode & 0o777, before_mode)
            after_tree = {str(p.relative_to(runtime)) for p in runtime.rglob("*") if p.is_file()}
            self.assertEqual(after_tree, before_tree)
            self.assertFalse(outside.exists())
            # Even a destination outside the broken profile must be refused when
            # containment cannot be established.
            env["EA010_REVIEW_EVIDENCE"] = str(outside)
            proc2 = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertNotEqual(proc2.returncode, 0)
            self.assertFalse(outside.exists())
            self.assertEqual(keep.read_text(encoding="utf-8"), before_text)

    def test_verify_weak_permit_nonzero(self) -> None:
        import subprocess
        import sys

        resources = {
            "schema": RESOURCES_SCHEMA,
            "service_bindings": [],
            "owned_paths": [],
            "run_root": None,
        }
        material = json.dumps(resources, sort_keys=True, separators=(",", ":")).encode()
        permit = {
            "schema": PERMIT_SCHEMA,
            "project_id": "vikunja-local-ea010-install-integration",
            "window": None,
            "authorized_actions": [],
            "executor": None,
            "resources_manifest": resources,
            "resources_manifest_sha256": hashlib.sha256(material).hexdigest(),
            "allow_live_pilot": True,
            "owner_action_receipts": {"unrelated": True},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "permit.json"
            path.write_text(json.dumps(permit), encoding="utf-8")
            os.chmod(path, 0o600)
            env = {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                PERMIT_ENV: str(path),
                "EA010_ISOLATED_STAND_PROFILE": os.environ.get(
                    "EA010_ISOLATED_STAND_PROFILE",
                    "/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json",
                ),
            }
            proc = subprocess.run(
                [sys.executable, "scripts/ea010_verify.py", "--suite", "all", "--mode", "approved-pilot"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env=env,
            )
            self.assertNotEqual(proc.returncode, 0)
            combined = (proc.stdout or "") + (proc.stderr or "")
            self.assertRegex(combined, r'"ok"\s*:\s*false')
            self.assertNotRegex(combined, r'"ok"\s*:\s*true')


if __name__ == "__main__":
    unittest.main()
