"""Unit tests for s08 read-only preflight + one-shot Hermes three-patch rehearsal."""

from __future__ import annotations

import ast
import inspect
import json
import os
import plistlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local.lifecycle import (
    C4_C5_SCOPE_SHA256,
    OBSERVED_BINDINGS_SHA256,
    OWNER_SCOPE_CLARIFICATION_SHA256,
    PATCH_REHEARSAL_PROCEDURE_SHA256,
    REHEARSAL_SCHEMA,
    SESSION_FLAG_NAME,
    SUPERVISOR_LABEL,
    LifecycleError,
    apply_patches,
    delete_owned_contour,
    dry_run,
    existing_service_preflight,
    load_approved_run_root,
    load_published_patch_sequence,
    load_service_config,
    preflight,
    read_only_bindings_preflight,
    reverse_patches,
    run_exact_three_patch_rehearsal,
    seed_disposable_copy,
    sha256_file,
    validate_rehearsal_manifest,
    verify_live_baseline_sources,
)
import vikunja_local.lifecycle as life

ROOT = Path(__file__).resolve().parents[1]


def _temp_contour(prefix: str = "EA010-SYNTHETIC-life-ut-") -> tempfile.TemporaryDirectory[str]:
    run_root = load_approved_run_root()
    run_root.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=str(run_root))


def _write(path: Path, data: bytes | str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = data if isinstance(data, bytes) else data.encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)
    os.chmod(path, mode)


class LifecyclePatchRehearsalTests(unittest.TestCase):
    def test_constants_match_owner_evidence(self) -> None:
        self.assertEqual(C4_C5_SCOPE_SHA256, "772a44d23db6a113a7a57dd02a40abdf51e114917e11b516cebd826e47565def")
        self.assertEqual(OBSERVED_BINDINGS_SHA256, "97fb13487a3437a93a4fb5bfa0460156aa7822c89e7f49c16723b4dd3d3e78b6")
        self.assertEqual(OWNER_SCOPE_CLARIFICATION_SHA256, "f50fa84a380c9538b2dfcc3ab4363ab07cd9fa4775acaa569687ee75317f10d4")
        self.assertEqual(PATCH_REHEARSAL_PROCEDURE_SHA256, "6286618c4fe26a197db1526b1fe31ffd24257c06f62c149409db6d090144ba34")
        self.assertEqual(SUPERVISOR_LABEL, "local.cto.vikunja")
        self.assertEqual(REHEARSAL_SCHEMA, "ea010-hermes-patch-rehearsal/v1")

    def test_scope_regression_forbids_old_engine_symbols(self) -> None:
        module = ROOT / "vikunja_local" / "lifecycle.py"
        tree = ast.parse(module.read_text(encoding="utf-8"))
        forbidden = {
            "IntegrationFile",
            "DemoManifest",
            "SessionSendGate",
            "apply_manifest",
            "undo_manifest",
            "repeat_apply",
            "_copy_protected",
            "_lifecycle_lock",
            "_save_state",
            "_load_state",
            "_restore_changed_sources",
            "assert_writable_manifest_target",
            "load_rehearsal_manifest",
        }
        remaining = sorted(
            node.name
            for node in tree.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in forbidden
        )
        self.assertEqual(remaining, [])
        text = module.read_text(encoding="utf-8")
        self.assertNotIn("STATE_NAME", text)
        self.assertNotIn(".ea010-patch-rehearsal-state.json", text)
        self.assertNotIn("preserve_seed", text)
        self.assertNotIn("preserve_seed", inspect.signature(seed_disposable_copy).parameters)

    def test_load_approved_run_root(self) -> None:
        root = load_approved_run_root()
        self.assertTrue(root.is_absolute())
        self.assertFalse((root / ".git").exists())

    def test_published_sequence_and_live_baseline(self) -> None:
        sequence = load_published_patch_sequence(ROOT)
        self.assertEqual(len(sequence.patches), 3)
        self.assertEqual(
            [p.key for p in sequence.patches],
            ["backend", "ui_readiness", "ui"],
        )
        hashes = verify_live_baseline_sources(sequence)
        self.assertEqual(len(hashes), 3)

    def test_one_shot_rehearsal_apply_repeat_reverse_deletes(self) -> None:
        result = run_exact_three_patch_rehearsal(ROOT, mode="rehearse")
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "rehearse")
        self.assertTrue(result["contour_deleted"])
        self.assertFalse(Path(result["contour_root"]).exists())
        phases = result["phases"]
        self.assertEqual(phases["apply"]["writes_performed"], 3)
        self.assertEqual(phases["repeat"]["writes_performed"], 0)
        self.assertEqual(
            [item["key"] for item in phases["reverse"]["restored"]],
            ["ui", "ui_readiness", "backend"],
        )
        self.assertTrue(phases["reverse"]["dispatch_disabled"])

    def test_in_process_dry_run_apply_reverse(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            dry = dry_run(contour)
            self.assertEqual(dry["writes_performed"], 0)
            self.assertEqual(len(dry["planned"]), 3)
            applied = apply_patches(contour)
            self.assertEqual(applied["writes_performed"], 3)
            repeated = apply_patches(contour)
            self.assertEqual(repeated["writes_performed"], 0)
            before = {str(path): sha256_file(path) for path in contour.preserve}
            reversed_result = reverse_patches(contour)
            self.assertTrue(reversed_result["dispatch_disabled"])
            for source in contour.sequence.sources:
                self.assertEqual(
                    sha256_file(contour.contour_root / source.relative_path),
                    source.from_sha256,
                )
            after = {str(path): sha256_file(path) for path in contour.preserve}
            self.assertEqual(before, after)

    def test_drift_blocks_write(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            target = contour.contour_root / contour.sequence.sources[0].relative_path
            target.write_bytes(b"foreign-drift\n")
            with self.assertRaises(LifecycleError) as ctx:
                apply_patches(contour)
            self.assertEqual(ctx.exception.code, "drift")

    def test_reverse_refuses_ordinary_source_drift(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            apply_patches(contour)
            target = contour.contour_root / contour.sequence.sources[0].relative_path
            foreign = b"owner-edited-after-apply\n"
            target.write_bytes(foreign)
            with self.assertRaises(LifecycleError) as ctx:
                reverse_patches(contour)
            self.assertEqual(ctx.exception.code, "drift")
            self.assertEqual(target.read_bytes(), foreign)

    def test_contour_must_be_under_run_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="EA010-OUTSIDE-") as outside:
            with self.assertRaises(LifecycleError) as ctx:
                seed_disposable_copy(Path(outside) / "child", worktree=ROOT)
            self.assertEqual(ctx.exception.code, "contour_unbound")

    def test_seed_refuses_nonempty_contour(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp) / "fresh", worktree=ROOT)
            with self.assertRaises(LifecycleError) as ctx:
                seed_disposable_copy(contour.contour_root, worktree=ROOT)
            self.assertEqual(ctx.exception.code, "contour_not_fresh")

    def test_cli_dry_run_deletes_contour(self) -> None:
        dry = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "ea010_apply.py"),
                "--worktree",
                str(ROOT),
                "--mode",
                "dry-run",
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            check=False,
        )
        self.assertEqual(dry.returncode, 0, dry.stderr)
        payload = json.loads(dry.stdout)
        self.assertEqual(payload["mode"], "dry-run")
        self.assertEqual(payload["result"]["writes_performed"], 0)
        self.assertTrue(payload["contour_deleted"])
        self.assertFalse(Path(payload["contour_root"]).exists())

    def test_cli_rehearse_deletes_contour(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "ea010_apply.py"),
                "--worktree",
                str(ROOT),
                "--mode",
                "rehearse",
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["mode"], "rehearse")
        self.assertTrue(payload["contour_deleted"])
        self.assertFalse(Path(payload["contour_root"]).exists())
        self.assertEqual(payload["phases"]["repeat"]["writes_performed"], 0)

    def test_cli_failure_discards_own_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="EA010-MISSING-WT-") as missing:
            bogus = Path(missing) / "no-such-worktree"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "ea010_apply.py"),
                    "--worktree",
                    str(bogus),
                    "--mode",
                    "dry-run",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            err = json.loads(proc.stderr)
            self.assertEqual(err["code"], "ui_manifest_missing")
            self.assertIn("contour_deleted", err)
            self.assertTrue(err["contour_deleted"])
            # No leftover synthetic contour under approved run-root from this failure.
            # (tree is created only after worktree validation starts seeding; missing
            # ui-manifest fails inside seed after mkdtemp — must still be deleted.)
            if err.get("contour_root"):
                self.assertFalse(Path(err["contour_root"]).exists())
            if err.get("details", {}).get("contour_root"):
                self.assertFalse(Path(err["details"]["contour_root"]).exists())

    def test_production_contour_forbidden(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            raw = dict(contour.raw)
            raw["contour"] = "production"
            with self.assertRaises(LifecycleError) as ctx:
                validate_rehearsal_manifest(raw, worktree=ROOT)
            self.assertEqual(ctx.exception.code, "contour_forbidden")

    def test_existing_service_preflight_and_second_supervisor(self) -> None:
        with _temp_contour() as tmp:
            root = Path(tmp)
            agents = root / "Library" / "LaunchAgents"
            agents.mkdir(parents=True)
            cfg = root / "config.json"
            _write(cfg, b'{"database":{"path":"stand.db"}}\n')
            binary = root / "bin"
            _write(binary, b"#!/bin/true\n", mode=0o700)
            argv = [str(binary.resolve()), "--config", str(cfg.resolve())]
            plist = agents / f"{SUPERVISOR_LABEL}.plist"
            _write(
                plist,
                plistlib.dumps(
                    {"Label": SUPERVISOR_LABEL, "ProgramArguments": argv, "RunAtLoad": True}
                ),
            )
            rehearsal = root / "rehearsal.json"
            _write(rehearsal, b"{}\n")
            install = root / "installation.json"
            _write(install, b'{"schema":"ea010-vikunja-install/v1"}\n')
            service = {
                "schema": "ea010-service-config/v1",
                "supervisor_label": SUPERVISOR_LABEL,
                "contour": "isolated_synthetic",
                "reuse_existing_supervisor": True,
                "forbid_second_supervisor": True,
                "post_login_pilot": "RunAtLoad after login; FileVault unchanged.",
                "admitted_program_arguments": argv,
                "paths": {
                    "contour_root": str(root.resolve()),
                    "plist": str(plist.resolve()),
                    "config": str(cfg.resolve()),
                    "installation_manifest": str(install.resolve()),
                    "database": str((root / "stand.db").resolve()),
                    "files": str((root / "files").resolve()),
                    "binary": str(binary.resolve()),
                    "rehearsal_manifest": str(rehearsal.resolve()),
                },
                "observed_read_only_bindings_doc": {
                    "path": (
                        "/Users/efimovivan/.local/state/cto-agent-environment/real-flows/"
                        "rf-17bd4e51a4b66a1d1d902871/driver/s08-review/EA-010 C4 observed Mini bindings.md"
                    ),
                    "sha256": OBSERVED_BINDINGS_SHA256,
                },
                "c4_c5_scope": {
                    "path": (
                        "/Users/efimovivan/.local/state/cto-agent-environment/real-flows/"
                        "rf-17bd4e51a4b66a1d1d902871/driver/s08-review/EA-010 C4 C5 lifecycle scope 0.1.md"
                    ),
                    "sha256": C4_C5_SCOPE_SHA256,
                },
            }
            (root / "files").mkdir(exist_ok=True)
            ok = existing_service_preflight(service)
            self.assertTrue(ok["ok"])
            rogue = agents / "rogue.plist"
            _write(rogue, plistlib.dumps({"Label": SUPERVISOR_LABEL, "ProgramArguments": ["/bin/false"]}))
            with self.assertRaises(LifecycleError) as ctx:
                existing_service_preflight(service)
            self.assertEqual(ctx.exception.code, "second_supervisor")

    def test_read_only_bindings_preflight(self) -> None:
        path = Path(
            "/Users/efimovivan/.local/state/cto-agent-environment/real-flows/"
            "rf-17bd4e51a4b66a1d1d902871/driver/s08-review/EA-010 C4 observed Mini bindings.md"
        )
        result = read_only_bindings_preflight(path)
        self.assertTrue(result["ok"])
        self.assertEqual(result["sha256"], OBSERVED_BINDINGS_SHA256)

    def test_service_example_schema_loads(self) -> None:
        example = ROOT / "config" / "service.example.json"
        loaded = load_service_config(example)
        self.assertEqual(loaded["supervisor_label"], SUPERVISOR_LABEL)
        self.assertEqual(loaded["defaults"]["synthetic_only"], True)
        self.assertEqual(
            loaded["admitted_program_arguments"],
            [
                loaded["paths"]["binary"],
                "--config",
                loaded["paths"]["config"],
            ],
        )

    def test_template_missing_argv_incompatible_plist(self) -> None:
        example = json.loads((ROOT / "config" / "service.example.json").read_text(encoding="utf-8"))
        with _temp_contour() as tmp:
            root = Path(tmp)
            agents = root / "Library" / "LaunchAgents"
            agents.mkdir(parents=True)
            plist = agents / f"{SUPERVISOR_LABEL}.plist"
            _write(
                plist,
                plistlib.dumps(
                    {
                        "Label": SUPERVISOR_LABEL,
                        "ProgramArguments": ["/bin/false"],
                        "RunAtLoad": True,
                    }
                ),
            )
            derived = dict(example)
            derived.pop("admitted_program_arguments", None)
            derived["paths"] = {
                "contour_root": str(root.resolve()),
                "plist": str(plist.resolve()),
                "config": str((root / "missing-config.json").resolve()),
                "installation_manifest": str((root / "missing-install.json").resolve()),
                "database": str((root / "missing.db").resolve()),
                "files": str((root / "missing-files").resolve()),
                "binary": str((root / "missing-binary").resolve()),
                "rehearsal_manifest": str((root / "missing-rehearsal.json").resolve()),
            }
            cfg_path = root / "service-from-example.json"
            _write(cfg_path, (json.dumps(derived, indent=2, sort_keys=True) + "\n").encode())
            with self.assertRaises(LifecycleError) as load_ctx:
                load_service_config(cfg_path)
            self.assertEqual(load_ctx.exception.code, "admitted_argv_missing")
            proc = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "ea010_service.py"),
                    "--config",
                    str(cfg_path),
                    "preflight",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0, proc.stdout)
            err = json.loads(proc.stderr)
            self.assertEqual(err["code"], "admitted_argv_missing")

            derived_with_false = dict(derived)
            derived_with_false["admitted_program_arguments"] = ["/bin/false"]
            cfg_false = root / "service-false-argv.json"
            _write(cfg_false, (json.dumps(derived_with_false, indent=2, sort_keys=True) + "\n").encode())
            loaded_false = load_service_config(cfg_false)
            with self.assertRaises(LifecycleError) as pre_ctx:
                existing_service_preflight(loaded_false)
            self.assertEqual(pre_ctx.exception.code, "admitted_argv_binding")

    def test_dispatcher_has_no_lifecycle_gate_import(self) -> None:
        dispatcher = (ROOT / "vikunja_local" / "dispatcher.py").read_text(encoding="utf-8")
        mcp = (ROOT / "vikunja_local" / "mcp.py").read_text(encoding="utf-8")
        hermes = (ROOT / "vikunja_local" / "hermes_bridge.py").read_text(encoding="utf-8")
        self.assertNotIn("vikunja_local.lifecycle", dispatcher)
        self.assertNotIn("dispatch_gate", dispatcher)
        self.assertNotIn("dispatch_gate", mcp)
        self.assertNotIn("dispatch_gate", hermes)

    def test_preflight_ready_before_apply(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            result = preflight(contour)
            self.assertEqual(result["status"], "ready")
            self.assertTrue(result["test_session_send_enabled"])

    def test_session_marker_collision_rejected_before_writes(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            journal = contour.preserve[0]
            journal_bytes = journal.read_bytes()
            preserve_before = {str(path): sha256_file(path) for path in contour.preserve}
            sources_before = {
                source.relative_path: sha256_file(contour.contour_root / source.relative_path)
                for source in contour.sequence.sources
            }
            flag_before = (
                contour.session_flag.read_bytes() if contour.session_flag.exists() else None
            )
            raw = dict(contour.raw)
            raw["test_session"] = {"enabled_flag_path": str(journal)}
            with self.assertRaises(LifecycleError) as ctx:
                validate_rehearsal_manifest(raw, worktree=ROOT)
            self.assertEqual(ctx.exception.code, "session_marker_conflict")
            self.assertEqual(journal.read_bytes(), journal_bytes)
            for path, digest in preserve_before.items():
                self.assertEqual(sha256_file(Path(path)), digest)
            for rel, digest in sources_before.items():
                self.assertEqual(sha256_file(contour.contour_root / rel), digest)
            self.assertEqual(
                contour.session_flag.read_bytes() if contour.session_flag.exists() else None,
                flag_before,
            )
            self.assertEqual(contour.session_flag.name, SESSION_FLAG_NAME)

    def test_partial_failure_discards_without_durable_retry_state(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp), worktree=ROOT)
            dist_dir = contour.contour_root / "plugins/personal-tasks/dashboard/dist"
            before_preserve = {str(path): sha256_file(path) for path in contour.preserve}
            os.chmod(dist_dir, 0o500)
            partial_error: LifecycleError | None = None
            try:
                try:
                    apply_patches(contour)
                except LifecycleError as exc:
                    partial_error = exc
            finally:
                os.chmod(dist_dir, 0o700)
            self.assertIsNotNone(partial_error)
            assert partial_error is not None
            self.assertEqual(partial_error.code, "partial_failure")
            written = list(partial_error.details.get("written_before_failure") or [])
            self.assertEqual([item["key"] for item in written], ["backend", "ui_readiness"])
            self.assertFalse((contour.contour_root / ".ea010-patch-rehearsal-state.json").exists())
            # Contour is neither baseline nor full result → further apply/reverse refuse as drift.
            with self.assertRaises(LifecycleError) as blocked:
                apply_patches(contour)
            self.assertEqual(blocked.exception.code, "drift")
            after_preserve = {str(path): sha256_file(path) for path in contour.preserve}
            self.assertEqual(before_preserve, after_preserve)

    def test_one_shot_partial_failure_deletes_readonly_dist_without_restore(self) -> None:
        """Outer one-shot finally must remove owned 0500 trees without a prior chmod restore."""
        real_seed = seed_disposable_copy
        seeded_root: Path | None = None

        def seed_with_readonly_dist(*args, **kwargs):
            nonlocal seeded_root
            contour = real_seed(*args, **kwargs)
            seeded_root = contour.contour_root
            os.chmod(contour.contour_root / "plugins/personal-tasks/dashboard/dist", 0o500)
            # Intentionally do not restore permissions before the rehearsal finally.
            return contour

        with patch.object(life, "seed_disposable_copy", side_effect=seed_with_readonly_dist):
            with self.assertRaises(LifecycleError) as ctx:
                run_exact_three_patch_rehearsal(ROOT, mode="rehearse")
        self.assertEqual(ctx.exception.code, "partial_failure")
        self.assertTrue(ctx.exception.details.get("contour_deleted"))
        self.assertFalse(Path(ctx.exception.details["contour_root"]).exists())
        self.assertIsNotNone(seeded_root)
        assert seeded_root is not None
        self.assertFalse(seeded_root.exists())

    def test_delete_owned_contour_removes_readonly_dist(self) -> None:
        with _temp_contour() as tmp:
            contour = seed_disposable_copy(Path(tmp) / "owned", worktree=ROOT)
            dist_dir = contour.contour_root / "plugins/personal-tasks/dashboard/dist"
            os.chmod(dist_dir, 0o500)
            # No chmod restore before delete — mirrors the one-shot finally path.
            info = delete_owned_contour(contour.contour_root)
            self.assertTrue(info["contour_deleted"])
            self.assertEqual(info["remaining"], [])
            self.assertFalse(contour.contour_root.exists())

    def test_seed_has_no_arbitrary_preserve_seed_hook(self) -> None:
        with _temp_contour() as tmp:
            with tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-outside-") as outside:
                target = Path(outside) / "owner-config.synthetic"
                target.write_bytes(b"SYNTHETIC ORIGINAL\n")
                with self.assertRaises(TypeError):
                    seed_disposable_copy(  # type: ignore[call-arg]
                        Path(tmp) / "fresh",
                        worktree=ROOT,
                        preserve_seed={str(target): b"SYNTHETIC REPLACEMENT\n"},
                    )
                self.assertEqual(target.read_bytes(), b"SYNTHETIC ORIGINAL\n")


if __name__ == "__main__":
    unittest.main()
