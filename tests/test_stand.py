"""s01 isolated stand: real binary lifecycle, faults, signals, cleanup."""

from __future__ import annotations

import errno
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.support.stand import (  # noqa: E402
    MANIFEST_SCHEMA,
    IsolatedStand,
    StandError,
    _stop_manifest_process,
    assert_immediate_run_child,
    assert_ports_free,
    assert_suite_result_contract,
    bind_manifest_paths,
    cleanup_manifest,
    cleanup_orphaned_runs,
    listeners,
    load_ownership_claim,
    load_profile,
    ownership_claim_path,
    process_matches_identity,
    process_start_identity,
    run_stand_cycle,
    sha256_file,
    validate_journal_path,
    verify_binary,
    write_secure,
)
from tests.support.suites import SUITE_REGISTRY, get_suite, list_suites, register_suite  # noqa: E402

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")


def _write_profile(path: Path, **overrides):
    base = load_profile()
    raw = dict(base.raw)
    for key, value in overrides.items():
        if key == "binary" and isinstance(value, dict):
            merged = dict(base.raw.get("binary") or {})
            merged.update(value)
            raw["binary"] = merged
        elif key == "ports" and isinstance(value, dict):
            merged = dict(base.raw.get("ports") or {})
            merged.update(value)
            raw["ports"] = merged
        elif key == "limits" and isinstance(value, dict):
            merged = dict(base.raw.get("limits") or {})
            merged.update(value)
            raw["limits"] = merged
        else:
            raw[key] = value
    write_secure(path, json.dumps(raw, indent=2, sort_keys=True) + "\n", 0o600)
    return path


def _load_profile_from(path: Path):
    previous = os.environ.get("EA010_ISOLATED_STAND_PROFILE")
    os.environ["EA010_ISOLATED_STAND_PROFILE"] = str(path)
    try:
        return load_profile()
    finally:
        if previous is None:
            os.environ.pop("EA010_ISOLATED_STAND_PROFILE", None)
        else:
            os.environ["EA010_ISOLATED_STAND_PROFILE"] = previous


class StandUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profile = load_profile()
        cls.worktree = ROOT
        verify_binary(cls.profile)

    def setUp(self):
        # Ensure previous aborted runs do not poison the shared ports.
        try:
            cleanup_orphaned_runs(self.profile)
        except StandError:
            pass
        assert_ports_free(self.profile.all_ports, self.profile.listen_host)

    def tearDown(self):
        try:
            cleanup_orphaned_runs(self.profile)
        except StandError:
            pass

    def test_profile_and_binary_pin(self):
        profile = load_profile()
        info = verify_binary(profile)
        self.assertEqual(info["sha256"], profile.binary_sha256)
        self.assertIn(profile.binary_version, info["version_line"])
        self.assertEqual(sha256_file(profile.path), sha256_file(Path(os.environ.get(
            "EA010_ISOLATED_STAND_PROFILE",
            "/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json",
        ))))

    def test_unsafe_profile_rejected_without_side_effects(self):
        with tempfile.TemporaryDirectory(prefix="ea010-unsafe-profile-") as tmp:
            tmp_path = Path(tmp).resolve()
            marker = tmp_path / "side-effect-marker"
            cases = [
                {"listen_host": "0.0.0.0"},
                {"run_root": str(ROOT.resolve())},
                {"run_directory_mode": "0777"},
                {"sensitive_file_mode": "0644"},
                {"ports": {"vikunja": 0, "fault_proxy": 0, "synthetic_hermes": 0}},
                {"ports": {"vikunja": 19345, "fault_proxy": 19345, "synthetic_hermes": 19347}},
                {"limits": {"wall_seconds": 0, "disk_bytes": 0, "owned_process_tree_rss_bytes": 0}},
            ]
            for overrides in cases:
                profile_path = (tmp_path / f"profile-{uuid.uuid4().hex}.json").resolve()
                _write_profile(profile_path, **overrides)
                with self.assertRaises(StandError):
                    _load_profile_from(profile_path)
                self.assertFalse(marker.exists())
                # No chmod/create under candidate worktree from load-only validation.
                self.assertTrue(ROOT.exists())

    def test_run_dir_containment_rejects_root_and_siblings(self):
        with tempfile.TemporaryDirectory(prefix="ea010-contain-") as tmp:
            base = Path(tmp).resolve()
            allowed = (base / "allowed").resolve()
            sibling = (base / "allowed-sibling").resolve()
            allowed.mkdir(mode=0o700)
            sibling.mkdir(mode=0o700)
            run_id = uuid.uuid4().hex
            with self.assertRaises(StandError):
                assert_immediate_run_child(allowed, run_id, allowed)
            with self.assertRaises(StandError):
                assert_immediate_run_child(allowed, run_id, sibling)
            with self.assertRaises(StandError):
                assert_immediate_run_child(allowed, run_id, allowed / "..")
            from tests.support.stand import validate_run_id

            with self.assertRaises(StandError):
                validate_run_id("/abs")
            with self.assertRaises(StandError):
                validate_run_id("../escape")
            ok = assert_immediate_run_child(allowed, run_id, allowed / run_id)
            self.assertEqual(ok, (allowed / run_id).resolve())
            # cleanup_manifest must refuse sibling even if startswith would pass.
            profile = load_profile()
            fake = {
                "schema": "ea010-stand-resource-manifest/v1",
                "run_id": run_id,
                "run_dir": str(sibling),
                "process": None,
            }
            # Bind against a disposable profile root by constructing a thin stand profile clone.
            from tests.support.stand import StandProfile

            disposable = StandProfile(
                raw=profile.raw,
                path=profile.path,
                binary_path=profile.binary_path,
                binary_version=profile.binary_version,
                binary_sha256=profile.binary_sha256,
                run_root=allowed,
                listen_host=profile.listen_host,
                ports=profile.ports,
                limits=profile.limits,
                fixture_prefix=profile.fixture_prefix,
                run_directory_mode=profile.run_directory_mode,
                sensitive_file_mode=profile.sensitive_file_mode,
            )
            with self.assertRaises(StandError):
                bind_manifest_paths(disposable, fake)
            with self.assertRaises(StandError):
                cleanup_manifest(disposable, fake, manifest_path=sibling / "resource-manifest.json")
            self.assertTrue(sibling.exists())

    def test_normal_cycle_readiness_fixtures_revoke_cleanup(self):
        old_umask = os.umask(0o022)
        try:
            result = run_stand_cycle(self.profile, worktree=self.worktree, revoke_token=True)
        finally:
            os.umask(old_umask)
        self.assertEqual(result["readiness"]["http"], 200)
        self.assertIn("v2.8.0", str(result["readiness"]["info"].get("version")))
        self.assertIn("task_id", result["synthetic_ids"])
        self.assertTrue(result["revoke_readback"]["token_absent_after_revoke"])
        self.assertIn(result["revoke_readback"]["revoked_token_http"], (401, 403))
        self.assertTrue(result["cleanup"]["confirmed"])
        for name in ("config.json", "stand.db"):
            self.assertIn(name, result["file_modes"])
            self.assertEqual(result["file_modes"][name], "0o600")
        run_dir = self.profile.run_root / result["run_id"]
        self.assertFalse(run_dir.exists())
        for port in self.profile.all_ports:
            self.assertEqual(listeners(port), set())

    def test_exception_path_still_cleans_owned_resources(self):
        stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
        stand.prepare()
        stand.start()
        stand.wait_ready()
        run_dir = stand.run_dir
        self.assertTrue(run_dir.exists())

        def boom(_stand):
            raise RuntimeError("synthetic failure after readiness")

        with self.assertRaises(RuntimeError):
            try:
                boom(stand)
            finally:
                stand.cleanup(revoke=False)
        self.assertTrue(stand.cleaned)
        self.assertFalse(run_dir.exists())
        for port in self.profile.all_ports:
            self.assertEqual(listeners(port), set())

        # P1 regression: revoke transport/IO faults must not abort cleanup before
        # owned-process stop / port read-back / cleanup_failed persistence.
        for fault_name, fault in (
            ("URLError", URLError("synthetic revoke transport fault")),
            ("TimeoutError", TimeoutError("synthetic revoke timeout")),
        ):
            with self.subTest(fault=fault_name):
                stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
                stand.__enter__()
                try:
                    stand.create_synthetic_session()
                    child = stand._child
                    self.assertIsNotNone(child)
                    self.assertIsNone(child.poll())
                    run_dir = stand.run_dir
                    self.assertTrue(run_dir.exists())
                    with mock.patch("tests.support.stand._OPENER.open", side_effect=fault):
                        with self.assertRaises(StandError) as ctx:
                            stand.__exit__(None, None, None)
                    self.assertIn("token revoke failed", str(ctx.exception).lower())
                    self.assertIsNotNone(child.poll())
                    self.assertEqual(listeners(self.profile.vikunja_port), set())
                    self.assertEqual(stand.manifest.get("status"), "cleanup_failed")
                    self.assertIn("cleanup_failure", stand.manifest)
                    failure_errors = (stand.manifest.get("cleanup_failure") or {}).get("errors") or []
                    self.assertTrue(
                        any("token revoke failed" in str(item).lower() for item in failure_errors)
                    )
                    self.assertTrue(run_dir.exists())
                    self.assertFalse(stand.cleaned)
                finally:
                    if not stand.cleaned:
                        # Opener restored; process already stopped — finish directory reclaim.
                        stand.cleanup(revoke=False)
                    stand.restore_signal_handlers()
                self.assertTrue(stand.cleaned)
                self.assertFalse(run_dir.exists())
                for port in self.profile.all_ports:
                    self.assertEqual(listeners(port), set())
                claim = ownership_claim_path(self.profile, stand.run_id)
                self.assertFalse(claim.exists())

    def test_busy_port_fails_without_killing_foreign_listener(self):
        host = self.profile.listen_host
        port = self.profile.vikunja_port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen(1)
            stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
            with self.assertRaises(StandError) as ctx:
                stand.prepare()
            self.assertIn(str(port), str(ctx.exception))
            # Foreign listener must still be ours (the test socket), not killed.
            owned = listeners(port)
            self.assertTrue(owned)

    def test_enter_admission_failure_restores_handlers(self):
        host = self.profile.listen_host
        port = self.profile.vikunja_port
        prior = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen(1)
            with self.assertRaises(StandError) as ctx:
                run_stand_cycle(self.profile, worktree=self.worktree, revoke_token=False)
            self.assertIn(str(port), str(ctx.exception))
            foreign = listeners(port)
            self.assertTrue(foreign)
            self.assertIn(os.getpid(), foreign)
            for sig, handler in prior.items():
                self.assertIs(signal.getsignal(sig), handler)

    def test_enter_start_failure_cleans_owned_resources(self):
        prior = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)}
        stand = IsolatedStand(profile=self.profile, worktree=self.worktree)

        def boom(_stand, _child):
            raise StandError("synthetic start failure after popen")

        stand._test_crash_after_popen = boom
        run_dir = stand.run_dir
        with self.assertRaises(StandError) as ctx:
            with stand:
                raise AssertionError("body must not run after start failure")
        self.assertIn("synthetic start failure after popen", str(ctx.exception))
        self.assertTrue(stand.cleaned)
        self.assertFalse(run_dir.exists())
        self.assertTrue(stand._child is None or stand._child.poll() is not None)
        for port in self.profile.all_ports:
            self.assertEqual(listeners(port), set())
        for sig, handler in prior.items():
            self.assertIs(signal.getsignal(sig), handler)

    def test_enter_readiness_failure_cleans_owned_resources(self):
        prior = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)}

        class ImmediateReadinessTimeoutStand(IsolatedStand):
            def wait_ready(self, timeout: float = 20.0):
                return super().wait_ready(timeout=0)

        stand = ImmediateReadinessTimeoutStand(profile=self.profile, worktree=self.worktree)
        run_dir = stand.run_dir
        with self.assertRaises(StandError) as ctx:
            with stand:
                raise AssertionError("body must not run after readiness failure")
        self.assertIn("did not become ready", str(ctx.exception))
        time.sleep(0.2)
        self.assertTrue(stand.cleaned)
        self.assertFalse(run_dir.exists())
        self.assertTrue(stand._child is None or stand._child.poll() is not None)
        for port in self.profile.all_ports:
            self.assertEqual(listeners(port), set())
        for sig, handler in prior.items():
            self.assertIs(signal.getsignal(sig), handler)

    def test_enter_popen_failure_closes_handshake_fds(self):
        """Popen OSError keeps spawn primary, surfaces cleanup EACCES, closes FDs."""
        for mode in ("context", "cycle"):
            with self.subTest(entrypoint=mode):
                prior = {
                    sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)
                }
                stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
                observed: dict = {}
                run_dir = stand.run_dir
                claim_path = ownership_claim_path(self.profile, stand.run_id)

                def boom(*_args, **_kwargs):
                    observed["read_fd"] = stand._handshake_read_fd
                    observed["write_fd"] = stand._handshake_write_fd
                    # Real unlink attempts against a 0500 tree fail with EACCES.
                    os.chmod(stand.run_dir, 0o500)
                    raise OSError(errno.EAGAIN, "synthetic child-spawn failure")

                stand._test_popen = boom
                try:
                    with self.assertRaises(OSError) as ctx:
                        if mode == "context":
                            with stand:
                                raise AssertionError("body must not run after popen failure")
                        else:
                            run_stand_cycle(stand=stand, revoke_token=False)
                    primary = ctx.exception
                    self.assertEqual(primary.errno, errno.EAGAIN)
                    self.assertIn("synthetic child-spawn failure", str(primary))
                    notes = getattr(primary, "__notes__", None) or []
                    self.assertTrue(any("secondary cleanup failure" in note for note in notes))
                    self.assertIsNotNone(stand.secondary_cleanup_error)
                    secondary_text = str(stand.secondary_cleanup_error).upper()
                    self.assertTrue(
                        "EACCES" in secondary_text
                        or "PERMISSION DENIED" in secondary_text
                        or str(errno.EACCES) in secondary_text,
                        secondary_text,
                    )
                    read_fd = observed["read_fd"]
                    write_fd = observed["write_fd"]
                    self.assertIsInstance(read_fd, int)
                    self.assertIsInstance(write_fd, int)
                    self.assertIsNone(stand._handshake_read_fd)
                    self.assertIsNone(stand._handshake_write_fd)
                    for fd in (read_fd, write_fd):
                        with self.assertRaises(OSError) as fd_ctx:
                            os.fstat(fd)
                        self.assertEqual(fd_ctx.exception.errno, errno.EBADF)
                    self.assertFalse(stand.cleaned)
                    self.assertTrue(run_dir.exists())
                    self.assertTrue(claim_path.exists())
                    claim = load_ownership_claim(self.profile, stand.run_id)
                    self.assertIsNotNone(claim)
                    self.assertEqual(claim.get("status"), "cleanup_failed")
                    self.assertTrue((run_dir / "config.json").exists() or claim_path.exists())
                    for sig, handler in prior.items():
                        self.assertIs(signal.getsignal(sig), handler)
                    # Restore permissions, drop same-process lock, reclaim via next launch path.
                    os.chmod(run_dir, 0o700)
                    for root, dirs, files in os.walk(run_dir):
                        for name in dirs:
                            os.chmod(Path(root) / name, 0o700)
                        for name in files:
                            os.chmod(Path(root) / name, 0o600)
                    stand._release_live_lock()
                    cleaned = cleanup_orphaned_runs(self.profile)
                    self.assertTrue(cleaned)
                    self.assertFalse(run_dir.exists())
                    self.assertIsNone(load_ownership_claim(self.profile, stand.run_id))
                    for port in self.profile.all_ports:
                        self.assertEqual(listeners(port), set())
                finally:
                    if run_dir.exists():
                        try:
                            os.chmod(run_dir, 0o700)
                            for root, dirs, files in os.walk(run_dir):
                                for name in dirs:
                                    os.chmod(Path(root) / name, 0o700)
                                for name in files:
                                    os.chmod(Path(root) / name, 0o600)
                        except OSError:
                            pass
                    try:
                        stand._release_live_lock()
                    except Exception:
                        pass
                    try:
                        cleanup_orphaned_runs(self.profile)
                    except StandError:
                        if run_dir.exists():
                            shutil.rmtree(run_dir, ignore_errors=True)
                        claim_path.unlink(missing_ok=True)

    def _assert_readiness_partial_delete_retains_durable_claim(self, stand, entrypoint):
        prior = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGHUP)}
        run_dir = stand.run_dir
        manifest_path = stand.manifest_path
        claim_path = ownership_claim_path(self.profile, stand.run_id)
        real_rmtree = shutil.rmtree

        def partial_rmtree(path, *args, **kwargs):
            # Recursive delete can erase the in-tree manifest before failing later.
            if manifest_path.exists():
                manifest_path.unlink()
            os.chmod(run_dir, 0o500)
            return real_rmtree(path, *args, **kwargs)

        try:
            with mock.patch("tests.support.stand.shutil.rmtree", side_effect=partial_rmtree):
                with self.assertRaises(StandError) as ctx:
                    entrypoint(stand)
            primary = ctx.exception
            self.assertIn("did not become ready", str(primary))
            self.assertIsInstance(primary, StandError)
            self.assertIsNotNone(stand.secondary_cleanup_error)
            self.assertIsInstance(stand.secondary_cleanup_error, StandError)
            notes = getattr(primary, "__notes__", None) or []
            self.assertTrue(any("secondary cleanup failure" in note for note in notes))
            for sig, handler in prior.items():
                self.assertIs(signal.getsignal(sig), handler)
            self.assertFalse(stand.cleaned)
            self.assertTrue(run_dir.exists())
            self.assertTrue(claim_path.exists())
            claim = load_ownership_claim(self.profile, stand.run_id)
            self.assertIsNotNone(claim)
            self.assertEqual(claim.get("status"), "cleanup_failed")
            self.assertIn("cleanup_failure", claim)
            self.assertTrue(stand._child is None or stand._child.poll() is not None)
            for port in self.profile.all_ports:
                self.assertEqual(listeners(port), set())
            # Restore permissions and release lock: next controlled reclaim must find
            # the durable claim even when the in-tree manifest was erased.
            os.chmod(run_dir, 0o700)
            for root, dirs, files in os.walk(run_dir):
                for name in dirs:
                    os.chmod(Path(root) / name, 0o700)
                for name in files:
                    os.chmod(Path(root) / name, 0o600)
            stand._release_live_lock()
            # Without durable claim this returned [] and left the directory behind.
            cleaned = cleanup_orphaned_runs(self.profile)
            self.assertTrue(cleaned)
            self.assertFalse(run_dir.exists())
            self.assertIsNone(load_ownership_claim(self.profile, stand.run_id))
            self.assertEqual(listeners(self.profile.vikunja_port), set())
        finally:
            if run_dir.exists():
                try:
                    os.chmod(run_dir, 0o700)
                    for root, dirs, files in os.walk(run_dir):
                        for name in dirs:
                            os.chmod(Path(root) / name, 0o700)
                        for name in files:
                            os.chmod(Path(root) / name, 0o600)
                except OSError:
                    pass
            try:
                stand._release_live_lock()
            except Exception:
                pass
            try:
                cleanup_orphaned_runs(self.profile)
            except StandError:
                if run_dir.exists():
                    shutil.rmtree(run_dir, ignore_errors=True)
                claim_path.unlink(missing_ok=True)

    def test_enter_readiness_cleanup_eacces_preserves_primary(self):
        class ImmediateReadinessTimeoutStand(IsolatedStand):
            def wait_ready(self, timeout: float = 20.0):
                return super().wait_ready(timeout=0)

        stand = ImmediateReadinessTimeoutStand(profile=self.profile, worktree=self.worktree)

        def entrypoint(s):
            with s:
                raise AssertionError("body must not run after readiness failure")

        self._assert_readiness_partial_delete_retains_durable_claim(stand, entrypoint)

    def test_run_stand_cycle_readiness_cleanup_eacces_preserves_primary(self):
        class ImmediateReadinessTimeoutStand(IsolatedStand):
            def wait_ready(self, timeout: float = 20.0):
                return super().wait_ready(timeout=0)

        stand = ImmediateReadinessTimeoutStand(profile=self.profile, worktree=self.worktree)

        def entrypoint(s):
            run_stand_cycle(stand=s, revoke_token=False)

        self._assert_readiness_partial_delete_retains_durable_claim(stand, entrypoint)

    def test_wrong_hash_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="ea010-profile-") as tmp:
            profile_path = (Path(tmp) / "profile.json").resolve()
            _write_profile(
                profile_path,
                binary={
                    "path": str(self.profile.binary_path),
                    "version": self.profile.binary_version,
                    "sha256": "0" * 64,
                },
            )
            bad = _load_profile_from(profile_path)
            stand = IsolatedStand(profile=bad, worktree=self.worktree)
            with self.assertRaises(StandError) as ctx:
                stand.prepare()
            self.assertIn("SHA256 mismatch", str(ctx.exception))

    def test_wrong_hash_preserves_preexisting_run_dir(self):
        run_id = uuid.uuid4().hex
        run_dir = self.profile.run_root / run_id
        run_dir.mkdir(mode=0o700)
        sentinel = run_dir / "sentinel.txt"
        sentinel.write_text("preexisting-admission-sentinel\n", encoding="utf-8")
        try:
            with tempfile.TemporaryDirectory(prefix="ea010-profile-") as tmp:
                profile_path = (Path(tmp) / "profile.json").resolve()
                _write_profile(
                    profile_path,
                    binary={
                        "path": str(self.profile.binary_path),
                        "version": self.profile.binary_version,
                        "sha256": "0" * 64,
                    },
                )
                bad = _load_profile_from(profile_path)
                stand = IsolatedStand(profile=bad, run_id=run_id, worktree=self.worktree)
                with self.assertRaises(StandError) as ctx:
                    stand.prepare()
                self.assertIn("SHA256 mismatch", str(ctx.exception))
            self.assertTrue(run_dir.exists())
            self.assertTrue(sentinel.exists())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preexisting-admission-sentinel\n")
        finally:
            if run_dir.exists():
                shutil.rmtree(run_dir, ignore_errors=True)

    def test_existing_run_dir_collision_preserves_directory(self):
        run_id = uuid.uuid4().hex
        run_dir = self.profile.run_root / run_id
        run_dir.mkdir(mode=0o700)
        sentinel = run_dir / "sentinel.txt"
        sentinel.write_text("collision-sentinel\n", encoding="utf-8")
        try:
            stand = IsolatedStand(profile=self.profile, run_id=run_id, worktree=self.worktree)
            with self.assertRaises(StandError) as ctx:
                stand.prepare()
            self.assertIn("already exists", str(ctx.exception).lower())
            self.assertTrue(run_dir.exists())
            self.assertTrue(sentinel.exists())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "collision-sentinel\n")
        finally:
            if run_dir.exists():
                shutil.rmtree(run_dir, ignore_errors=True)

    def test_concurrent_live_second_launch_refused_while_a_alive(self):
        stand_a = IsolatedStand(profile=self.profile, worktree=self.worktree)
        stand_a.prepare()
        stand_a.start()
        stand_a.wait_ready()
        a_pid = stand_a._child.pid
        a_dir = stand_a.run_dir
        try:
            # Valid second launch must refuse without harming A.
            stand_b = IsolatedStand(profile=self.profile, worktree=self.worktree)
            with self.assertRaises(StandError) as ctx:
                stand_b.prepare()
            self.assertIn("concurrent live stand", str(ctx.exception).lower())
            self.assertTrue(a_dir.exists())
            self.assertEqual(listeners(self.profile.vikunja_port), {a_pid})

            # Invalid-hash second launch must also refuse harmlessly (lock before hash).
            with tempfile.TemporaryDirectory(prefix="ea010-profile-") as tmp:
                profile_path = (Path(tmp) / "profile.json").resolve()
                _write_profile(
                    profile_path,
                    binary={
                        "path": str(self.profile.binary_path),
                        "version": self.profile.binary_version,
                        "sha256": "0" * 64,
                    },
                )
                bad = _load_profile_from(profile_path)
                stand_c = IsolatedStand(profile=bad, worktree=self.worktree)
                with self.assertRaises(StandError) as ctx2:
                    stand_c.prepare()
                self.assertIn("concurrent live stand", str(ctx2.exception).lower())
            self.assertTrue(a_dir.exists())
            self.assertEqual(listeners(self.profile.vikunja_port), {a_pid})
            self.assertIsNone(stand_a._child.poll())
        finally:
            stand_a.cleanup(revoke=False)

    def test_cleanup_retains_manifest_on_identity_mismatch(self):
        stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
        stand.prepare()
        stand.start()
        stand.wait_ready()
        run_dir = stand.run_dir
        manifest_path = stand.manifest_path
        real_pid = stand._child.pid
        # Synthetic identity mismatch while process still listens.
        stand.manifest["process"]["identity"]["lstart"] = "Mon Jan  1 00:00:00 2000"
        stand._save_manifest()
        with self.assertRaises(StandError) as ctx:
            stand.cleanup(revoke=False)
        self.assertIn("recovery state retained", str(ctx.exception).lower())
        self.assertTrue(run_dir.exists())
        self.assertTrue(manifest_path.exists())
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(saved.get("status"), "cleanup_failed")
        self.assertIn("cleanup_failure", saved)
        self.assertIn(real_pid, listeners(self.profile.vikunja_port))
        # Repair identity and finish cleanup.
        from tests.support.stand import process_start_identity

        stand.manifest["process"]["identity"] = process_start_identity(real_pid)
        stand._save_manifest()
        result = stand.cleanup(revoke=False)
        self.assertTrue(result["directory_removed"])
        self.assertFalse(run_dir.exists())
        self.assertEqual(listeners(self.profile.vikunja_port), set())

    def test_pre_readiness_crash_window_keeps_service_from_listening(self):
        # Run start() in a child manager so SIGKILL does not kill the test process.
        with tempfile.TemporaryDirectory(prefix="ea010-crash-") as tmp:
            ready = Path(tmp) / "state.json"
            script = f"""
import json, os, signal, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
from tests.support.stand import IsolatedStand, load_profile
profile = load_profile()
stand = IsolatedStand(profile=profile, worktree=Path({str(ROOT)!r}))
def crash(_s, child):
    Path({str(ready)!r}).write_text(json.dumps({{
        'run_id': _s.run_id,
        'child_pid': child.pid,
        'manager_pid': os.getpid(),
    }}))
    os.kill(os.getpid(), signal.SIGKILL)
stand._test_crash_after_popen = crash
stand.prepare()
stand.start()
"""
            proc = subprocess.Popen(
                [sys.executable, "-I", "-c", script],
                cwd=str(ROOT),
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            try:
                deadline = time.monotonic() + 30
                state = None
                while time.monotonic() < deadline:
                    if ready.exists() and ready.stat().st_size > 0:
                        state = json.loads(ready.read_text(encoding="utf-8"))
                        break
                    if proc.poll() is not None and ready.exists():
                        state = json.loads(ready.read_text(encoding="utf-8"))
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(state)
                proc.wait(timeout=10)
                self.assertEqual(proc.returncode, -signal.SIGKILL)
                child_pid = int(state["child_pid"])
                run_id = state["run_id"]
                # Handshake must prevent the service from listening.
                time.sleep(0.5)
                self.assertNotIn(child_pid, listeners(self.profile.vikunja_port))
                self.assertEqual(listeners(self.profile.vikunja_port), set())
                run_dir = self.profile.run_root / run_id
                self.assertTrue(run_dir.exists())
                manifest = json.loads((run_dir / "resource-manifest.json").read_text(encoding="utf-8"))
                self.assertIsNone((manifest.get("process") or {}).get("identity"))
                cleaned = cleanup_orphaned_runs(self.profile)
                self.assertTrue(cleaned)
                self.assertFalse(run_dir.exists())
                # Ensure handshake child is gone.
                alive = subprocess.run(
                    ["/bin/ps", "-p", str(child_pid)],
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(alive.returncode, 0)
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                try:
                    cleanup_orphaned_runs(self.profile)
                except StandError:
                    pass

    def test_wall_limit_watchdog_during_callback_stall(self):
        stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
        stand.prepare()
        stand.start()
        stand.wait_ready()
        # Advance in-memory elapsed-time anchor past admitted wall budget.
        stand.profile.limits["wall_seconds"] = 1
        stand._started_at = time.monotonic()
        stand._test_time_anchor = stand._started_at + 5
        # Watchdog / enforce during idle callback path must notice without an API call.
        breached = False
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                if stand._watchdog is not None:
                    stand._watchdog.raise_if_failed()
                stand._enforce_limits()
            except StandError as exc:
                self.assertIn("Wall-clock", str(exc))
                breached = True
                break
            time.sleep(0.2)
        self.assertTrue(breached)
        # Safe cleanup after breach.
        try:
            stand.cleanup(revoke=False)
        except StandError:
            # If identity/stop raced with watchdog SIGKILL, finish via orphan cleanup.
            cleanup_orphaned_runs(self.profile)
        self.assertEqual(listeners(self.profile.vikunja_port), set())

    def test_missing_binary_fails_not_skip(self):
        with tempfile.TemporaryDirectory(prefix="ea010-profile-") as tmp:
            # Use resolve() so macOS /var -> /private/var matches profile pin rules.
            missing = (Path(tmp) / "no-such-vikunja").resolve()
            profile_path = (Path(tmp) / "profile.json").resolve()
            _write_profile(
                profile_path,
                binary={
                    "path": str(missing),
                    "version": self.profile.binary_version,
                    "sha256": self.profile.binary_sha256,
                },
            )
            bad = _load_profile_from(profile_path)
            with self.assertRaises(StandError):
                verify_binary(bad)

    def _spawn_stand_manager(self, ready_file: Path, signal_name: str) -> subprocess.Popen:
        script = f"""
import os, signal, sys, time
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
from tests.support.stand import IsolatedStand, load_profile
profile = load_profile()
stand = IsolatedStand(profile=profile, worktree=Path({str(ROOT)!r}))
stand.install_signal_handlers()
stand.prepare()
stand.start()
stand.wait_ready()
Path({str(ready_file)!r}).write_text(stand.run_id + '\\n' + str(stand._child.pid) + '\\n')
# Wait until parent signals us.
while True:
    time.sleep(0.2)
"""
        return subprocess.Popen(
            [sys.executable, "-I", "-c", script],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    def _wait_ready_file(self, path: Path, proc: subprocess.Popen, timeout: float = 30.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if path.exists() and path.stat().st_size > 0:
                return path.read_text(encoding="utf-8").strip().splitlines()
            if proc.poll() is not None:
                raise AssertionError(f"manager exited early rc={proc.returncode}")
            time.sleep(0.1)
        raise AssertionError("stand manager did not publish ready file")

    def test_sigterm_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="ea010-sig-") as tmp:
            ready = Path(tmp) / "ready"
            proc = self._spawn_stand_manager(ready, "SIGTERM")
            try:
                run_id, child_pid = self._wait_ready_file(ready, proc)
                os.kill(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                    self.fail("manager did not exit after SIGTERM")
                self.assertFalse((self.profile.run_root / run_id).exists())
                for port in self.profile.all_ports:
                    self.assertEqual(listeners(port), set())
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                cleanup_orphaned_runs(self.profile)

    def test_sighup_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="ea010-sig-") as tmp:
            ready = Path(tmp) / "ready"
            proc = self._spawn_stand_manager(ready, "SIGHUP")
            try:
                run_id, child_pid = self._wait_ready_file(ready, proc)
                os.kill(proc.pid, signal.SIGHUP)
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                    self.fail("manager did not exit after SIGHUP")
                self.assertFalse((self.profile.run_root / run_id).exists())
                for port in self.profile.all_ports:
                    self.assertEqual(listeners(port), set())
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                cleanup_orphaned_runs(self.profile)

    def test_sigkill_then_next_run_cleans_manifest_with_pid_identity(self):
        with tempfile.TemporaryDirectory(prefix="ea010-sigkill-") as tmp:
            ready = Path(tmp) / "ready"
            # Manager without signal cleanup handlers; SIGKILL leaves Vikunja orphaned.
            script = f"""
import sys, time
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
from tests.support.stand import IsolatedStand, load_profile
profile = load_profile()
stand = IsolatedStand(profile=profile, worktree=Path({str(ROOT)!r}))
stand.prepare()
stand.start()
stand.wait_ready()
Path({str(ready)!r}).write_text(stand.run_id + '\\n' + str(stand._child.pid) + '\\n')
while True:
    time.sleep(0.5)
"""
            proc = subprocess.Popen(
                [sys.executable, "-I", "-c", script],
                cwd=str(ROOT),
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            try:
                run_id, vikunja_pid = self._wait_ready_file(ready, proc)
                vikunja_pid = int(vikunja_pid)
                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                # Orphaned Vikunja should still own the port.
                self.assertIn(vikunja_pid, listeners(self.profile.vikunja_port))
                self.assertTrue((self.profile.run_root / run_id).exists())
                # Public next-controlled-launch path (prepare under live lock) must
                # recover the owned orphan before refusing remaining foreign ports.
                result = run_stand_cycle(
                    self.profile,
                    worktree=self.worktree,
                    revoke_token=False,
                )
                self.assertEqual(result["readiness"]["http"], 200)
                self.assertTrue(result["cleanup"]["confirmed"])
                self.assertFalse((self.profile.run_root / run_id).exists())
                self.assertNotIn(vikunja_pid, listeners(self.profile.vikunja_port))
                for port in self.profile.all_ports:
                    self.assertEqual(listeners(port), set())
            finally:
                if proc.poll() is None:
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                try:
                    cleanup_orphaned_runs(self.profile)
                except StandError:
                    pass

    def test_stale_receipt_reused_pid_not_killed(self):
        """Identity-less receipt must not SIGKILL a reused unrelated PID."""
        sleeper = subprocess.Popen(
            [sys.executable, "-I", "-c", "import time; time.sleep(120)"],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        run_dir = None
        try:
            sleeper_pid = sleeper.pid
            sleeper_identity = process_start_identity(sleeper_pid)
            run_id = uuid.uuid4().hex
            run_dir = self.profile.run_root / run_id
            run_dir.mkdir(mode=0o700)
            manifest = {
                "schema": MANIFEST_SCHEMA,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "process": {"pid": sleeper_pid, "identity": None},
                "status": "launching",
            }
            write_secure(
                run_dir / "resource-manifest.json",
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                0o600,
            )
            receipt = {
                "schema": "ea010-stand-launch-receipt/v1",
                "run_id": run_id,
                "pid": sleeper_pid,
                "phase": "pre-identity",
                "at": "2000-01-01T00:00:00+00:00",
            }
            write_secure(
                run_dir / "launch-receipt.json",
                json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                0o600,
            )
            with self.assertRaises(StandError) as ctx:
                cleanup_orphaned_runs(self.profile)
            self.assertIn("recovery state retained", str(ctx.exception).lower())
            self.assertIsNone(sleeper.poll())
            self.assertTrue(process_matches_identity(sleeper_identity))
            self.assertTrue(run_dir.exists())
            saved = json.loads((run_dir / "resource-manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(saved.get("status"), "cleanup_failed")
        finally:
            if sleeper.poll() is None:
                os.kill(sleeper.pid, signal.SIGKILL)
                sleeper.wait(timeout=5)
            if run_dir is not None and run_dir.exists():
                shutil.rmtree(run_dir, ignore_errors=True)

    def test_unknown_and_empty_suite_fail(self):
        with self.assertRaises(StandError):
            get_suite("")
        with self.assertRaises(StandError):
            get_suite(None)  # type: ignore[arg-type]
        with self.assertRaises(StandError):
            get_suite("no-such-suite")
        # s02 registers the real api suite alongside stand; retain unknown/empty negatives.
        self.assertEqual(list_suites(), ["api", "stand"])
        verify = ROOT / "scripts" / "ea010_verify.py"
        unknown = subprocess.run(
            [sys.executable, str(verify), "--suite", "nope", "--mode", "isolated"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            check=False,
        )
        self.assertNotEqual(unknown.returncode, 0)
        empty = subprocess.run(
            [sys.executable, str(verify), "--suite", "", "--mode", "isolated"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            check=False,
        )
        self.assertNotEqual(empty.returncode, 0)

    def test_stand_suite_registration_real_cycle(self):
        runner = get_suite("stand")
        result = runner(
            mode="isolated",
            worktree=self.worktree,
            argv=["python3", "scripts/ea010_verify.py", "--suite", "stand", "--mode", "isolated"],
        )
        self.assertEqual(result["suite"], "stand")
        self.assertEqual(result["readiness"]["http"], 200)
        self.assertTrue(result["cleanup"]["confirmed"])
        self.assertIn("task_id", result["synthetic_ids"])
        self.assertEqual(result["executed_count"], 1)
        assert_suite_result_contract(result)

    def test_mismatched_manifest_pid_identity_not_signalled(self):
        """Corrupt pid=B + identity=A must not signal B; A stays alive."""
        children = [
            subprocess.Popen(
                [sys.executable, "-I", "-c", "import time; time.sleep(60)"],
                cwd=str(ROOT),
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            for _ in range(2)
        ]
        try:
            proc_a, proc_b = children
            identity_a = process_start_identity(proc_a.pid)
            stopped, errors = _stop_manifest_process(
                {"process": {"pid": proc_b.pid, "identity": identity_a}}
            )
            self.assertFalse(stopped)
            self.assertTrue(errors)
            self.assertIn("binding mismatch", " ".join(errors).lower())
            self.assertIsNone(proc_a.poll())
            self.assertIsNone(proc_b.poll())
            self.assertTrue(process_matches_identity(identity_a))
        finally:
            for child in children:
                if child.poll() is None:
                    os.kill(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)

    def test_cleanup_refuses_when_other_process_holds_live_lock(self):
        stand = IsolatedStand(profile=self.profile, worktree=self.worktree)
        stand.prepare()
        try:
            self.assertTrue(stand.run_dir.exists())
            child_script = f"""
import json
import sys
sys.path.insert(0, {str(ROOT)!r})
from tests.support.stand import StandError, cleanup_orphaned_runs, load_profile
try:
    print(json.dumps(cleanup_orphaned_runs(load_profile(), require_exclusive_owner=True)))
except StandError as exc:
    print('REFUSED:' + str(exc))
    raise SystemExit(2)
"""
            child = subprocess.run(
                [sys.executable, "-I", "-c", child_script],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                check=False,
            )
            self.assertNotEqual(child.returncode, 0)
            self.assertIn("lock busy", (child.stdout + child.stderr).lower())
            self.assertTrue(stand.run_dir.exists())
        finally:
            if not stand.cleaned:
                stand.cleanup(revoke=False)

    def test_registered_empty_suite_fails_contract(self):
        name = f"review-empty-{uuid.uuid4().hex[:8]}"
        register_suite(name, lambda **kwargs: {})
        try:
            verify = ROOT / "scripts" / "ea010_verify.py"
            # Import-path registration is process-local; exercise contract helper and
            # a subprocess that registers then invokes main the same way review did.
            with self.assertRaises(StandError):
                assert_suite_result_contract({})
            script = f"""
import contextlib, io, json, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
from tests.support.suites import register_suite
import importlib.util
spec = importlib.util.spec_from_file_location('ea010_verify', Path({str(verify)!r}))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
register_suite({name!r}, lambda **kwargs: {{}})
buf = io.StringIO()
err = io.StringIO()
with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
    rc = mod.main(['--suite', {name!r}, '--mode', 'isolated'])
print(json.dumps({{'rc': rc, 'stdout': buf.getvalue(), 'stderr': err.getvalue()}}))
"""
            probe = subprocess.run(
                [sys.executable, "-I", "-c", script],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                check=False,
            )
            self.assertEqual(probe.returncode, 0)
            payload = json.loads(probe.stdout)
            self.assertNotEqual(payload["rc"], 0)
            combined = (payload["stdout"] + payload["stderr"]).lower()
            self.assertTrue(
                "executed_count" in combined
                or "empty result" in combined
                or "no scenarios" in combined
            )
        finally:
            SUITE_REGISTRY.pop(name, None)

    def test_journal_path_must_stay_under_approved_run_root(self):
        with tempfile.TemporaryDirectory(prefix="ea010-journal-outside-") as tmp:
            outside = Path(tmp) / "outside-approved-root.json"
            with self.assertRaises(StandError):
                validate_journal_path(self.profile, outside)
            # Relative escape via .. must also fail after resolve.
            with self.assertRaises(StandError):
                validate_journal_path(self.profile, "../escape-journal.json")
            # Admitted relative journal under run_root/journals is accepted.
            ok = validate_journal_path(self.profile, Path("journals") / "unit-ok.json")
            self.assertTrue(str(ok).startswith(str(self.profile.run_root.resolve())))
            verify = ROOT / "scripts" / "ea010_verify.py"
            script = f"""
import contextlib, io, json, sys
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
from tests.support.suites import register_suite
import importlib.util
spec = importlib.util.spec_from_file_location('ea010_verify', Path({str(verify)!r}))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
name = 'review-journal-{uuid.uuid4().hex[:8]}'
register_suite(name, lambda **kwargs: {{'executed_count': 1, 'ok': True}})
outside = {str(outside)!r}
buf = io.StringIO(); err = io.StringIO()
with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
    rc = mod.main(['--suite', name, '--mode', 'isolated', '--journal', outside])
print(json.dumps({{
    'rc': rc,
    'outside_exists': Path(outside).exists(),
    'stderr': err.getvalue(),
    'stdout': buf.getvalue(),
}}))
"""
            probe = subprocess.run(
                [sys.executable, "-I", "-c", script],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                check=False,
            )
            self.assertEqual(probe.returncode, 0)
            payload = json.loads(probe.stdout)
            self.assertNotEqual(payload["rc"], 0)
            self.assertFalse(payload["outside_exists"])
            self.assertFalse(outside.exists())


if __name__ == "__main__":
    unittest.main()
