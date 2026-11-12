"""Isolated lifecycle suite (s08): real Vikunja + one-shot Hermes three-patch rehearsal."""

from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

from vikunja_local.lifecycle import (
    C4_C5_SCOPE_SHA256,
    OBSERVED_BINDINGS_SHA256,
    PATCH_REHEARSAL_PROCEDURE_SHA256,
    REHEARSAL_SCHEMA,
    SUPERVISOR_LABEL,
    LifecycleError,
    apply_patches,
    existing_service_preflight,
    load_approved_run_root,
    reverse_patches,
    run_exact_three_patch_rehearsal,
    seed_disposable_copy,
    sha256_bytes,
    sha256_file,
)
import vikunja_local.lifecycle as life

from .stand import IsolatedStand, StandError, load_profile

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

WORKTREE = Path(__file__).resolve().parents[2]


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _source_digest(worktree: Path) -> dict[str, str]:
    paths = [
        worktree / "vikunja_local" / "lifecycle.py",
        worktree / "scripts" / "ea010_apply.py",
        worktree / "scripts" / "ea010_service.py",
        worktree / "config" / "service.example.json",
        worktree / "integration" / "environment" / "manifest.schema.json",
        worktree / "tests" / "test_lifecycle.py",
        worktree / "tests" / "support" / "lifecycle_suite.py",
        worktree / "tests" / "support" / "suites.py",
    ]
    digest = __import__("hashlib").sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(sha256_file(path).encode())
        digest.update(b"\0")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        capture_output=True,
        text=True,
        check=False,
    )
    diff = subprocess.run(
        ["git", "diff", "HEAD", "--", *[str(p.relative_to(worktree)) for p in paths]],
        cwd=worktree,
        capture_output=True,
        check=False,
    )
    return {
        "sha256": digest.hexdigest(),
        "head": head.stdout.strip() if head.returncode == 0 else "unknown",
        "diff_sha256": sha256_bytes(diff.stdout or b""),
    }


def _write(path: Path, data: bytes | str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = data if isinstance(data, bytes) else data.encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_EXCL, mode)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, mode)


def _restart_bound_vikunja(stand: IsolatedStand) -> dict[str, Any]:
    stand.stop_process()
    stand._child = None  # noqa: SLF001 — suite-owned isolated restart
    stand.start()
    return stand.wait_ready()


def _verify_bound_ids(stand: IsolatedStand, expected: Mapping[str, Any]) -> dict[str, Any]:
    token = stand._session_token  # noqa: SLF001 — suite read-back of seeded session
    _assert(token is not None, "bound lifecycle missing session token")
    status, task = stand.api(f"tasks/{expected['task_id']}", token=token)
    _assert(status == 200 and isinstance(task, dict), f"bound task read-back HTTP {status}")
    _assert(task.get("id") == expected["task_id"], "bound task_id changed after lifecycle")
    _assert(expected["marker"] in str(task.get("description") or ""), "bound task marker lost")
    status, project = stand.api(f"projects/{expected['project_id']}", token=token)
    _assert(status == 200 and isinstance(project, dict), f"bound project read-back HTTP {status}")
    _assert(project.get("id") == expected["project_id"], "bound project_id changed after lifecycle")
    return {
        "task_id": task.get("id"),
        "project_id": project.get("id"),
        "marker": expected["marker"],
        "http_task": status,
    }


def _build_service_fixture(stand: IsolatedStand, rehearsal_manifest: Path) -> dict[str, Any]:
    root = stand.run_dir
    agents = root / "Library" / "LaunchAgents"
    agents.mkdir(parents=True, exist_ok=True)
    cfg_path = root / "config.json"
    db_path = root / "stand.db"
    binary = Path(stand.profile.binary_path)
    argv = [str(binary.resolve()), "--config", str(cfg_path.resolve())]
    plist_path = agents / f"{SUPERVISOR_LABEL}.plist"
    if plist_path.exists():
        plist_path.unlink()
    _write(
        plist_path,
        plistlib.dumps(
            {
                "Label": SUPERVISOR_LABEL,
                "ProgramArguments": argv,
                "WorkingDirectory": str(root),
                "RunAtLoad": True,
                "KeepAlive": True,
            }
        ),
    )
    install_path = root / "installation-manifest.json"
    if not install_path.exists():
        _write(install_path, b'{"schema":"ea010-vikunja-install/v1","synthetic":true}\n')
    service_cfg = {
        "schema": "ea010-service-config/v1",
        "supervisor_label": SUPERVISOR_LABEL,
        "contour": "isolated_synthetic",
        "reuse_existing_supervisor": True,
        "forbid_second_supervisor": True,
        "post_login_pilot": "RunAtLoad after login keeps local.cto.vikunja; FileVault/SSH/power unchanged.",
        "admitted_program_arguments": argv,
        "paths": {
            "contour_root": str(root.resolve()),
            "plist": str(plist_path.resolve()),
            "config": str(cfg_path.resolve()),
            "installation_manifest": str(install_path.resolve()),
            "database": str(db_path.resolve()),
            "files": str((root / "files").resolve()),
            "binary": str(binary.resolve()),
            "rehearsal_manifest": str(rehearsal_manifest.resolve()),
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
    service_path = root / "ea010-service-fixture.json"
    if service_path.exists():
        service_path.unlink()
    _write(service_path, (json.dumps(service_cfg, indent=2, sort_keys=True) + "\n").encode())
    return {
        "service_cfg": service_cfg,
        "service_path": service_path,
        "plist_path": plist_path,
        "agents": agents,
        "db_path": db_path,
    }


def _new_patch_contour(worktree: Path) -> life.RehearsalContour:
    run_root = load_approved_run_root()
    run_root.mkdir(parents=True, exist_ok=True)
    contour_root = Path(tempfile.mkdtemp(prefix="EA010-SYNTHETIC-hermes-patches-", dir=str(run_root)))
    return seed_disposable_copy(contour_root, worktree=worktree)


def _run_cli(worktree: Path, script: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(worktree / "scripts" / script), *args],
        cwd=worktree,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )


def run_lifecycle_scenarios(stand: IsolatedStand, worktree: Path) -> dict[str, Any]:
    scenarios: dict[str, Any] = {}
    executed = 0
    created_contours: list[Path] = []

    def track(contour: life.RehearsalContour) -> life.RehearsalContour:
        created_contours.append(contour.contour_root)
        return contour

    try:
        # Service fixture uses a placeholder rehearsal path (no durable apply manifest).
        placeholder_manifest = stand.run_dir / "hermes-patch-rehearsal.placeholder.json"
        if not placeholder_manifest.exists():
            _write(placeholder_manifest, b'{"schema":"ea010-hermes-patch-rehearsal/v1","placeholder":true}\n')
        service = _build_service_fixture(stand, placeholder_manifest)

        _assert(stand.readiness is not None and stand.readiness.get("http") == 200, "vikunja readiness")
        scenarios["isolated_vikunja_ready"] = {
            "http": stand.readiness.get("http"),
            "run_id": stand.run_id,
            "port": stand.profile.vikunja_port,
        }
        executed += 1

        svc = _run_cli(worktree, "ea010_service.py", ["--config", str(service["service_path"]), "describe-post-login"])
        _assert(svc.returncode == 0, f"service describe failed: {svc.stderr}")
        svc_payload = json.loads(svc.stdout)
        _assert("post_login_pilot" in svc_payload["result"], "post_login missing")
        _assert(svc_payload["result"]["production_touch"] is False, "must not claim production touch")
        scenarios["post_login_description"] = {
            "rc": svc.returncode,
            "has_pilot_text": bool(svc_payload["result"]["post_login_pilot"]),
        }
        executed += 1

        bindings = _run_cli(
            worktree,
            "ea010_service.py",
            ["--config", str(service["service_path"]), "bindings-preflight"],
        )
        _assert(bindings.returncode == 0, f"bindings preflight failed: {bindings.stderr}")
        bindings_payload = json.loads(bindings.stdout)
        _assert(bindings_payload["result"]["ok"] is True, "bindings digest mismatch")
        scenarios["read_only_bindings_preflight"] = {
            "rc": bindings.returncode,
            "sha256": bindings_payload["result"]["sha256"],
        }
        executed += 1

        pre = existing_service_preflight(service["service_cfg"])
        _assert(pre["ok"] is True, "existing service preflight failed")
        scenarios["existing_service_preflight"] = {
            "ok": True,
            "supervisor_plist_count": pre["supervisor_plist_count"],
            "procedure_sha256": pre["patch_rehearsal_procedure_sha256"],
        }
        executed += 1

        dry = _run_cli(
            worktree,
            "ea010_apply.py",
            ["--worktree", str(worktree), "--mode", "dry-run"],
        )
        _assert(dry.returncode == 0, f"dry-run failed: {dry.stderr}")
        dry_payload = json.loads(dry.stdout)
        _assert(dry_payload["result"]["writes_performed"] == 0, "dry-run must not write")
        _assert(dry_payload["contour_deleted"] is True, "dry-run must delete its temporary tree")
        _assert(not Path(dry_payload["contour_root"]).exists(), "dry-run contour left behind")
        scenarios["dry_run"] = {
            "rc": dry.returncode,
            "planned": len(dry_payload["result"]["planned"]),
            "procedure_sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
            "contour_deleted": True,
        }
        executed += 1

        seed_ids = {
            "task_id": stand.synthetic_ids.get("task_id"),
            "project_id": stand.synthetic_ids.get("project_id"),
            "marker": stand.synthetic_ids.get("marker"),
        }
        _assert(seed_ids["task_id"] is not None, "missing seeded task_id before patch apply")

        # Bound Vikunja remains up across the disposable Hermes one-shot.
        rehearse = _run_cli(
            worktree,
            "ea010_apply.py",
            ["--worktree", str(worktree), "--mode", "rehearse"],
        )
        _assert(rehearse.returncode == 0, f"rehearse failed: {rehearse.stderr}")
        rehearse_payload = json.loads(rehearse.stdout)
        _assert(rehearse_payload["mode"] == "rehearse", "unexpected rehearse mode")
        _assert(rehearse_payload["contour_deleted"] is True, "rehearse must delete temporary tree")
        _assert(not Path(rehearse_payload["contour_root"]).exists(), "rehearse contour left behind")
        phases = rehearse_payload["phases"]
        _assert(phases["apply"]["writes_performed"] == 3, "expected 3 patch writes")
        _assert(phases["repeat"]["writes_performed"] == 0, "repeat must not rewrite")
        _assert(
            [item["key"] for item in phases["reverse"]["restored"]] == ["ui", "ui_readiness", "backend"],
            f"unexpected reverse order: {phases['reverse']['restored']}",
        )
        scenarios["apply"] = {
            "writes_performed": phases["apply"]["writes_performed"],
            "applied": [item["key"] for item in phases["apply"]["applied"]],
            "result_hashes": phases["apply_hashes"],
            "patch_hashes": {
                p.relative_path: p.sha256
                for p in life.load_published_patch_sequence(worktree).patches
            },
            "contour_deleted": True,
        }
        executed += 1

        ready_after_apply = _restart_bound_vikunja(stand)
        ids_after_apply = _verify_bound_ids(stand, seed_ids)
        scenarios["bound_process_after_apply_restart"] = {
            "http": ready_after_apply.get("http"),
            "ids": ids_after_apply,
            "database": str(service["db_path"]),
            "note": "Hermes one-shot contour is separate from Vikunja config/db and deleted on return",
        }
        executed += 1

        agents = list(service["agents"].glob("*.plist"))
        _assert(len(agents) == 1, f"second supervisor created: {agents}")
        ready_after_repeat = _restart_bound_vikunja(stand)
        ids_after_repeat = _verify_bound_ids(stand, seed_ids)
        scenarios["repeat_apply"] = {
            "writes_performed": phases["repeat"]["writes_performed"],
            "supervisor_plist_count": len(agents),
            "restart_http": ready_after_repeat.get("http"),
            "ids": ids_after_repeat,
            "note": "repeat is the in-process idempotent phase of the one-shot rehearsal",
        }
        executed += 1

        scenarios["undo_preserves_journal_and_links"] = {
            "restored": [item["key"] for item in phases["reverse"]["restored"]],
            "preserve": phases["preserve"],
            "session_paused": True,
            "restored_hashes": phases["reverse_hashes"],
            "contour_deleted": True,
            "note": "reverse phase restored only own sources; journal/legacy preserved then tree deleted",
        }
        executed += 1

        drifted = track(_new_patch_contour(worktree))
        target = drifted.contour_root / drifted.sequence.sources[0].relative_path
        target.write_bytes(b"foreign-drift\n")
        blocked = False
        code = None
        try:
            apply_patches(drifted)
        except LifecycleError as exc:
            blocked = True
            code = exc.code
        _assert(blocked and code == "drift", f"drift not blocked code={code}")
        scenarios["drift_blocks_write"] = {"blocked": True, "code": code}
        executed += 1

        no_perm = track(_new_patch_contour(worktree))
        os.chmod(no_perm.contour_root / "plugins/personal-tasks/dashboard", 0o500)
        perm_blocked = False
        perm_code = None
        try:
            try:
                apply_patches(no_perm)
            except LifecycleError as exc:
                perm_blocked = True
                perm_code = exc.code
            _assert(perm_blocked, "no-permission must block")
            _assert(perm_code in ("no_permission", "partial_failure", "drift"), f"code={perm_code}")
            scenarios["no_permission_blocks"] = {"blocked": True, "code": perm_code}
            executed += 1
        finally:
            os.chmod(no_perm.contour_root / "plugins/personal-tasks/dashboard", 0o700)

        partial = track(_new_patch_contour(worktree))
        dist_dir = partial.contour_root / "plugins/personal-tasks/dashboard/dist"
        before_preserve_partial = {str(p): sha256_file(p) for p in partial.preserve}
        partial_error = None
        os.chmod(dist_dir, 0o500)
        try:
            try:
                apply_patches(partial)
            except LifecycleError as exc:
                partial_error = {"code": exc.code, "details": exc.details}
        finally:
            os.chmod(dist_dir, 0o700)
        _assert(partial_error is not None and partial_error["code"] == "partial_failure", "partial failure missing")
        written = list((partial_error.get("details") or {}).get("written_before_failure") or [])
        _assert(len(written) == 2, f"expected backend+ui_readiness prefix, got {written}")
        _assert(not life._session_enabled(partial.session_flag), "session send must pause after partial apply")
        after_fail_preserve = {str(p): sha256_file(p) for p in partial.preserve}
        _assert(before_preserve_partial == after_fail_preserve, "preserve mutated by partial apply failure")
        _assert(not (partial.contour_root / ".ea010-patch-rehearsal-state.json").exists(), "no durable state file")
        # One-shot policy: discard failed tree (suite finally); no cross-invocation undo/retry.
        scenarios["partial_apply_later_write_failure"] = {
            "error": partial_error,
            "session_paused": True,
            "written_before_failure": [item.get("key") for item in written],
            "durable_state_absent": True,
            "discard_policy": "fail_and_delete_own_tree",
            "preserve_unchanged_on_partial_fixture": before_preserve_partial,
        }
        executed += 1

        reverse_ok = track(_new_patch_contour(worktree))
        applied_for_reverse = apply_patches(reverse_ok)
        _assert(applied_for_reverse["writes_performed"] == 3, "need applied state before reverse")
        before_preserve = {str(p): sha256_file(p) for p in reverse_ok.preserve}
        rolled = reverse_patches(reverse_ok)
        _assert(rolled["dispatch_disabled"] is True, "session not paused")
        for source in reverse_ok.sequence.sources:
            _assert(
                sha256_file(reverse_ok.contour_root / source.relative_path) == source.from_sha256,
                f"source not restored: {source.relative_path}",
            )
        after_preserve = {str(p): sha256_file(p) for p in reverse_ok.preserve}
        _assert(before_preserve == after_preserve, "preserve mutated on reverse")
        scenarios["in_process_reverse_preserves_journal"] = {
            "restored": [item["key"] for item in rolled["restored"]],
            "preserve": after_preserve,
            "session_paused": True,
            "restored_hashes": rolled.get("restored_hashes"),
        }
        executed += 1

        reverse_drift = track(_new_patch_contour(worktree))
        apply_patches(reverse_drift)
        later_edit = b"later-edit-after-apply\n"
        drift_target = reverse_drift.contour_root / reverse_drift.sequence.sources[0].relative_path
        drift_target.write_bytes(later_edit)
        reverse_drift_blocked = False
        reverse_drift_code = None
        try:
            reverse_patches(reverse_drift)
        except LifecycleError as exc:
            reverse_drift_blocked = True
            reverse_drift_code = exc.code
        _assert(
            reverse_drift_blocked and reverse_drift_code == "drift",
            f"reverse drift not blocked: {reverse_drift_code}",
        )
        _assert(drift_target.read_bytes() == later_edit, "reverse must not overwrite ordinary later edit")
        scenarios["undo_refuses_ordinary_source_drift"] = {
            "blocked": True,
            "code": reverse_drift_code,
            "target_unchanged": True,
        }
        executed += 1

        argv_fixture = dict(service["service_cfg"])
        bad_plist = service["plist_path"]
        if bad_plist.exists():
            bad_plist.unlink()
        _write(
            bad_plist,
            plistlib.dumps(
                {
                    "Label": SUPERVISOR_LABEL,
                    "ProgramArguments": ["/bin/false", "--config", "/unrelated/config.json"],
                    "RunAtLoad": True,
                }
            ),
        )
        argv_blocked = False
        argv_code = None
        try:
            existing_service_preflight(argv_fixture)
        except LifecycleError as exc:
            argv_blocked = True
            argv_code = exc.code
        _assert(argv_blocked and argv_code == "supervisor_argv", f"plist argv drift not blocked: {argv_code}")
        scenarios["preflight_blocks_plist_argv_drift"] = {"blocked": True, "code": argv_code}
        executed += 1

        if service["plist_path"].exists():
            service["plist_path"].unlink()
        _write(
            service["plist_path"],
            plistlib.dumps(
                {
                    "Label": SUPERVISOR_LABEL,
                    "ProgramArguments": list(service["service_cfg"]["admitted_program_arguments"]),
                    "WorkingDirectory": str(stand.run_dir),
                    "RunAtLoad": True,
                    "KeepAlive": True,
                }
            ),
        )

        rogue = service["agents"] / "local.cto.vikunja-rogue.plist"
        _write(
            rogue,
            plistlib.dumps({"Label": SUPERVISOR_LABEL, "ProgramArguments": ["/bin/false"]}),
        )
        incompat = False
        incompat_code = None
        try:
            existing_service_preflight(service["service_cfg"])
        except LifecycleError as exc:
            incompat = True
            incompat_code = exc.code
        _assert(incompat and incompat_code == "second_supervisor", f"second supervisor not blocked: {incompat_code}")
        rogue.unlink()
        scenarios["second_supervisor_blocked"] = {"blocked": True, "code": incompat_code}
        executed += 1

        probe = track(_new_patch_contour(worktree))
        prod = dict(probe.raw)
        prod["contour"] = "production"
        prod_blocked = False
        prod_code = None
        try:
            life.validate_rehearsal_manifest(prod, worktree=worktree)
        except LifecycleError as exc:
            prod_blocked = True
            prod_code = exc.code
        _assert(prod_blocked and prod_code == "contour_forbidden", f"prod contour gate failed: {prod_code}")
        scenarios["production_contour_forbidden"] = {"blocked": True, "code": prod_code}
        executed += 1

        # One-shot CLI has no persistent --create-contour/--manifest path.
        cli_preflight = _run_cli(
            worktree,
            "ea010_apply.py",
            ["--worktree", str(worktree), "--mode", "dry-run"],
        )
        _assert(cli_preflight.returncode == 0, f"cli dry-run failed: {cli_preflight.stderr}")
        cli_payload = json.loads(cli_preflight.stdout)
        _assert(cli_payload["contour_deleted"] is True, "cli must delete contour")
        _assert(not Path(cli_payload["contour_root"]).exists(), "cli contour left behind")
        scenarios["cli_one_shot_dry_run"] = {
            "rc": cli_preflight.returncode,
            "ok": True,
            "schema": REHEARSAL_SCHEMA,
            "contour_deleted": True,
        }
        executed += 1

        # Missing ui-manifest worktree: CLI must fail and leave no contour.
        with tempfile.TemporaryDirectory(prefix="EA010-MISSING-WT-") as missing:
            bogus = Path(missing) / "empty-wt"
            bogus.mkdir()
            fail_cli = _run_cli(
                worktree,
                "ea010_apply.py",
                ["--worktree", str(bogus), "--mode", "dry-run"],
            )
            _assert(fail_cli.returncode != 0, "missing ui-manifest must fail")
            fail_err = json.loads(fail_cli.stderr)
            _assert(fail_err.get("code") == "ui_manifest_missing", f"unexpected code: {fail_err}")
            _assert(fail_err.get("contour_deleted") is True, "failure must report contour_deleted")
            if fail_err.get("details", {}).get("contour_root") or fail_err.get("contour_root"):
                left = Path(
                    fail_err.get("contour_root")
                    or fail_err["details"]["contour_root"]
                )
                _assert(not left.exists(), f"failed CLI left contour: {left}")
            scenarios["cli_failure_discards_contour"] = {
                "blocked": True,
                "code": fail_err.get("code"),
                "contour_deleted": True,
            }
            executed += 1

        example = json.loads((worktree / "config" / "service.example.json").read_text(encoding="utf-8"))
        missing_argv_cfg = dict(example)
        missing_argv_cfg.pop("admitted_program_arguments", None)
        missing_argv_cfg["paths"] = {
            "contour_root": str(stand.run_dir.resolve()),
            "plist": str(service["plist_path"].resolve()),
            "config": str((stand.run_dir / "missing-config.json").resolve()),
            "installation_manifest": str((stand.run_dir / "missing-install.json").resolve()),
            "database": str((stand.run_dir / "missing.db").resolve()),
            "files": str((stand.run_dir / "missing-files").resolve()),
            "binary": str((stand.run_dir / "missing-binary").resolve()),
            "rehearsal_manifest": str((stand.run_dir / "missing-rehearsal.json").resolve()),
        }
        missing_argv_path = stand.run_dir / "service-from-example-missing-argv.json"
        _write(missing_argv_path, (json.dumps(missing_argv_cfg, indent=2, sort_keys=True) + "\n").encode())
        missing_argv_cli = _run_cli(
            worktree,
            "ea010_service.py",
            ["--config", str(missing_argv_path), "preflight"],
        )
        _assert(missing_argv_cli.returncode != 0, "template missing argv must fail")
        missing_argv_err = json.loads(missing_argv_cli.stderr)
        _assert(
            missing_argv_err.get("code") == "admitted_argv_missing",
            f"unexpected missing-argv code: {missing_argv_err}",
        )
        false_argv_cfg = dict(missing_argv_cfg)
        false_argv_cfg["admitted_program_arguments"] = ["/bin/false"]
        false_argv_path = stand.run_dir / "service-from-example-false-argv.json"
        _write(false_argv_path, (json.dumps(false_argv_cfg, indent=2, sort_keys=True) + "\n").encode())
        false_blocked = False
        false_code = None
        try:
            existing_service_preflight(life.load_service_config(false_argv_path))
        except LifecycleError as exc:
            false_blocked = True
            false_code = exc.code
        _assert(false_blocked and false_code == "admitted_argv_binding", f"false argv code={false_code}")
        scenarios["template_missing_argv_incompatible_plist"] = {
            "missing_argv_code": missing_argv_err.get("code"),
            "incompatible_plist_code": false_code,
            "blocked": True,
        }
        executed += 1

        # Library one-shot also deletes.
        lib = run_exact_three_patch_rehearsal(worktree, mode="rehearse")
        _assert(lib["ok"] is True, "library rehearse failed")
        _assert(not Path(lib["contour_root"]).exists(), "library rehearse left contour")
        scenarios["library_one_shot_rehearse"] = {
            "ok": True,
            "contour_deleted": True,
            "repeat_writes": lib["phases"]["repeat"]["writes_performed"],
        }
        executed += 1

        scenarios["working_service_untouched"] = {
            "launchctl_mutations": False,
            "supervisor_label": SUPERVISOR_LABEL,
            "dispatcher_mcp_hermes_untouched": True,
            "note": "Suite used disposable Hermes one-shot + synthetic LaunchAgents tree only",
        }
        executed += 1

        scenarios["patch_rehearsal_procedure"] = {
            "sha256": PATCH_REHEARSAL_PROCEDURE_SHA256,
            "order": ["backend.patch", "ui-readiness.patch", "ui.patch"],
            "source_and_patch_hashes_verified": True,
            "workflow": "one_shot_apply_hash_repeat_reverse_hash_delete",
        }
        executed += 1

        synthetic_ids = {
            "run_id": stand.run_id,
            "one_shot_contour_root_deleted": rehearse_payload["contour_root"],
            "journal_sha256": sha256_bytes(b"SYNTH-JOURNAL-v1\n"),
            "project_id": stand.synthetic_ids.get("project_id"),
            "task_id": stand.synthetic_ids.get("task_id"),
            "marker": stand.synthetic_ids.get("marker"),
            "api_token_id": stand.synthetic_ids.get("api_token_id"),
            "api_token_handle": stand.synthetic_ids.get("api_token_handle"),
            "vikunja_task_marker": stand.synthetic_ids.get("marker") or stand.fixture_name("task"),
        }
        return {
            "scenarios": scenarios,
            "executed_count": executed,
            "synthetic_ids": synthetic_ids,
            "created_contours": created_contours,
        }
    finally:
        leftovers: list[dict[str, Any]] = []
        for path in created_contours:
            info = life.delete_owned_contour(path)
            if not info["contour_deleted"]:
                leftovers.append(info)
        if leftovers:
            raise StandError(f"owned patch contours remain after suite finally: {leftovers}")


def run_lifecycle_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite lifecycle supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    try:
        with stand:
            session_ids = stand.create_synthetic_session()
            body = run_lifecycle_scenarios(stand, worktree)
            source = _source_digest(worktree)
            synthetic_ids = {
                **dict(session_ids),
                **body["synthetic_ids"],
            }
            _assert(synthetic_ids.get("task_id") is not None, "lifecycle suite missing real Vikunja task_id")
            _assert(synthetic_ids.get("project_id") is not None, "lifecycle suite missing real Vikunja project_id")
            result = {
                "suite": "lifecycle",
                "mode": mode,
                "argv": list(argv),
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "executed_count": body["executed_count"],
                "scenarios": body["scenarios"],
                "scenario_names": sorted(body["scenarios"]),
                "synthetic_ids": synthetic_ids,
                "source": {
                    **(stand.manifest.get("source") or {}),
                    **source,
                },
                "binary": stand.manifest.get("binary"),
                "created_contours": [str(path) for path in body["created_contours"]],
            }
        contours = [Path(path) for path in result.get("created_contours") or []]
        patch_contours_removed = all(not path.exists() for path in contours)
        result["cleanup"] = {
            "confirmed": stand.cleaned,
            "patch_contours_removed": patch_contours_removed,
            "patch_contour_paths_checked": [str(path) for path in contours],
        }
        result.pop("created_contours", None)
        if not stand.cleaned:
            raise StandError("lifecycle suite cleanup was not confirmed")
        if not patch_contours_removed:
            remaining = [str(path) for path in contours if path.exists()]
            raise StandError(f"lifecycle suite patch contours still present: {remaining}")
        if result["executed_count"] <= 0:
            raise StandError("lifecycle suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise
