"""Unit tests for s09 manual consistent copy and isolated restore gates."""

from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from vikunja_local import restore as restore_mod
from vikunja_local.api import build_marker
from vikunja_local.journal import IntegrationJournal, JournalError, OperationState, new_operation_id
from vikunja_local.restore import (
    COPY_SCHEMA,
    ContourPaths,
    RestoreError,
    attachment_checksums,
    create_manual_copy,
    reconcile_restored_contour,
    restore_into_empty_target,
    verify_consistent_copy,
)
from vikunja_local.storage import authority_path_for

from tests.support.stand import (
    IsolatedStand,
    cleanup_orphaned_runs,
    listeners,
    load_profile,
    process_start_identity,
)

ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, data: bytes | str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = data if isinstance(data, bytes) else data.encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)
    os.chmod(path, mode)


def _seed_contour(base: Path) -> tuple[ContourPaths, dict[str, str]]:
    root = base / "source"
    files = root / "files"
    files.mkdir(parents=True)
    attachment = files / "synth-note.txt"
    _write(attachment, b"EA010-SYNTHETIC-ATTACHMENT-v1\n")
    # Real SQLite file so the consistent-copy path exercises the backup API.
    db_path = root / "stand.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
        conn.execute("INSERT INTO synth(note) VALUES (?)", ("EA010-SYNTHETIC-DB",))
        conn.commit()
    finally:
        conn.close()
    os.chmod(db_path, 0o600)
    _write(
        root / "config.json",
        json.dumps(
            {
                "service": {
                    "interface": "127.0.0.1:19345",
                    "publicurl": "http://127.0.0.1:19345/",
                    "rootpath": str(root),
                    "secret": "synthetic-service-secret",
                },
                "database": {"type": "sqlite", "path": str(root / "stand.db")},
                "files": {"basepath": str(files), "maxsize": "1MB"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    journal_path = root / "journal" / "integration.sqlite3"
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    with IntegrationJournal(journal_path) as journal:
        issuer = journal.ensure_issuer()
        op = new_operation_id()
        prepared = journal.prepare_intent(
            issuer_id=issuer,
            operation_id=op,
            op_type="create",
            project_id=2,
            payload={"title": "EA010-SYNTHETIC-restore-unit"},
            source_kind="chat",
            source_key="ea010-synthetic-restore-unit",
        )
        marker = prepared.marker
        journal.begin_dispatch(issuer, op)
        journal.record_receipt(
            issuer,
            op,
            vikunja_id=42,
            project_id=2,
            marker=marker,
            effect={"id": 42},
        )
    ids = {
        "issuer_id": issuer,
        "operation_id": op,
        "marker": marker,
        "vikunja_id": "42",
        "attachment_sha256": __import__("hashlib").sha256(
            b"EA010-SYNTHETIC-ATTACHMENT-v1\n"
        ).hexdigest(),
    }
    return ContourPaths.from_stand_root(root), ids


class RestoreUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="EA010-SYNTHETIC-restore-ut-")
        self.base = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_manual_copy_excludes_authority_and_scheduler(self) -> None:
        source, ids = _seed_contour(self.base)
        dest = self.base / "copy"
        manifest = create_manual_copy(
            source,
            dest,
            secrets_extra={"username": "synth", "password": "synth-pass"},
        )
        self.assertEqual(manifest["schema"], COPY_SCHEMA)
        self.assertIs(manifest["scheduler"], False)
        self.assertIsNone(manifest["rpo"])
        self.assertIsNone(manifest["rto"])
        self.assertIsNone(manifest["retention"])
        self.assertIs(manifest["authority_included"], False)
        self.assertIs(manifest["mini_local_copy_not_device_loss_protection"], True)
        self.assertFalse((dest / "journal" / "integration.sqlite3.authority").exists())
        self.assertTrue((dest / "secrets" / "service.secret").is_file())
        mode = (dest / "secrets").stat().st_mode & 0o777
        self.assertEqual(mode, 0o700)
        safe = json.loads((dest / "safe_config" / "config.json").read_text(encoding="utf-8"))
        self.assertIsInstance(safe["service"]["secret"], dict)
        self.assertNotEqual(safe["service"]["secret"], "synthetic-service-secret")
        links = json.loads((dest / "links" / "mappings.json").read_text(encoding="utf-8"))
        self.assertEqual(links["receipts"][0]["vikunja_id"], 42)
        self.assertEqual(links["mappings"][0]["operation_id"], ids["operation_id"])
        verified = verify_consistent_copy(dest)
        self.assertTrue(verified["ok"])

    def test_restore_into_empty_target_forces_writes_off(self) -> None:
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        result = restore_into_empty_target(copy_dir, target, listen_host="127.0.0.1", listen_port=19345)
        self.assertTrue(result["ok"])
        self.assertFalse(result["writes_enabled"])
        self.assertTrue(result["restore_drift"])
        self.assertTrue(result["admitted"])
        self.assertFalse(result["working_contour_restore"])
        self.assertTrue((target / "stand.db").is_file())
        self.assertTrue((target / "files" / "synth-note.txt").is_file())
        checksums = attachment_checksums(target / "files")
        self.assertEqual(checksums["synth-note.txt"], ids["attachment_sha256"])
        # Authority exists only after admit, not from the copy pair.
        self.assertTrue(authority_path_for(Path(result["journal_path"])).is_file())
        with IntegrationJournal(result["journal_path"]) as journal:
            self.assertFalse(
                journal.can_dispatch(ids["issuer_id"], ids["operation_id"])
            )
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], new_operation_id()))

    def test_non_empty_target_refused(self) -> None:
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        target.mkdir()
        _write(target / "preexisting.txt", b"nope\n")
        with self.assertRaises(RestoreError) as ctx:
            restore_into_empty_target(copy_dir, target)
        self.assertEqual(ctx.exception.code, "target_not_empty")

    def test_foreign_db_after_empty_preflight_is_preserved(self) -> None:
        """F1: post-preflight stand.db must not be unlinked/replaced."""
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-foreign-preflight"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-foreign-preflight"
        sentinel = b"EA010-SYNTHETIC-FOREIGN-DATABASE-CREATED-AFTER-PREFLIGHT\n"
        original = restore_mod._assert_empty_target

        def concurrent_creation(path: Path) -> None:
            original(path)
            (path / "stand.db").write_bytes(sentinel)

        restore_mod._assert_empty_target = concurrent_creation  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                restore_into_empty_target(copy_dir, target)
        finally:
            restore_mod._assert_empty_target = original  # type: ignore[assignment]
        self.assertEqual(ctx.exception.code, "dest_exists")
        self.assertEqual((target / "stand.db").read_bytes(), sentinel)

    def test_symlink_swap_after_exclusive_create_does_not_overwrite_foreign(self) -> None:
        """F1: destination symlink after O_EXCL must not redirect content writes."""
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-symlink-swap"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-symlink-swap"
        foreign = self.base / "foreign.sqlite3"
        sentinel = b"EA010-SYNTHETIC-FOREIGN-DATABASE\n"
        foreign.write_bytes(sentinel)
        original = restore_mod._prepare_secure_file

        bound_target = target.resolve()

        def replace_after_exclusive_open(dest: Path, mode: int = restore_mod.SAFE_FILE_MODE) -> int:
            fd = original(dest, mode)
            # Compare resolve()-bound paths: restore materializes through canonical roots.
            if dest.resolve() == bound_target / "stand.db":
                # Unlink the exclusive name while the caller still holds the fd,
                # then plant a symlink to an out-of-target sentinel.
                dest.unlink()
                dest.symlink_to(foreign)
            return fd

        restore_mod._prepare_secure_file = replace_after_exclusive_open  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                restore_into_empty_target(copy_dir, target)
        finally:
            restore_mod._prepare_secure_file = original  # type: ignore[assignment]
        self.assertEqual(ctx.exception.code, "symlink_refused")
        self.assertEqual(foreign.read_bytes(), sentinel)
        self.assertTrue((bound_target / "stand.db").is_symlink())

    def test_destination_ancestor_substitution_refuses_foreign_write(self) -> None:
        """P2: swapping target/files to a foreign dir before leaf open must fail closed.

        Leaf O_NOFOLLOW is not enough: _ensure_dir must not follow/chmod a
        substituted parent. Foreign stays empty and keeps mode 0755.
        """
        source, _ids = _seed_contour(self.base)
        backup = (self.base / "backup-ancestor").resolve()
        target = (self.base / "target-ancestor").resolve()
        foreign = (self.base / "foreign-ancestor").resolve()
        foreign.mkdir(mode=0o755)
        os.chmod(foreign, 0o755)
        create_manual_copy(source, backup)
        original = restore_mod._prepare_secure_file
        hits: list[str] = []

        def swap_parent_to_foreign(dest: Path, mode: int = restore_mod.SAFE_FILE_MODE) -> int:
            if dest.resolve() == (target / "files" / "synth-note.txt").resolve():
                hits.append(str(dest))
                parent = dest.parent
                parent.rmdir()
                parent.symlink_to(foreign, target_is_directory=True)
            return original(dest, mode)

        restore_mod._prepare_secure_file = swap_parent_to_foreign  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                restore_into_empty_target(backup, target)
        finally:
            restore_mod._prepare_secure_file = original  # type: ignore[assignment]
        self.assertEqual(len(hits), 1)
        self.assertEqual(ctx.exception.code, "symlink_refused")
        self.assertEqual(list(foreign.iterdir()), [])
        self.assertEqual(foreign.stat().st_mode & 0o777, 0o755)
        self.assertTrue((target / "files").is_symlink())

    def test_verified_manifest_binds_materialized_attachment_bytes(self) -> None:
        """P2: post-verify attachment mutation must not materialize drifted bytes."""
        source, _ids = _seed_contour(self.base)
        backup = (self.base / "backup-verified-bind").resolve()
        target = (self.base / "target-verified-bind").resolve()
        manifest = create_manual_copy(source, backup)
        expected = dict(manifest["components"]["attachments"]["files"])
        original = restore_mod._assert_empty_target
        hits: list[bool] = []

        def corrupt_after_verify(path: Path) -> None:
            original(path)
            hits.append(True)
            (backup / "attachments" / "synth-note.txt").write_bytes(
                b"EA010-SYNTHETIC-CORRUPT-AFTER-VERIFY\n"
            )

        restore_mod._assert_empty_target = corrupt_after_verify  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                restore_into_empty_target(backup, target)
        finally:
            restore_mod._assert_empty_target = original  # type: ignore[assignment]
        self.assertEqual(len(hits), 1)
        self.assertEqual(ctx.exception.code, "copy_drift")
        if (target / "files").exists() and not (target / "files").is_symlink():
            self.assertNotEqual(attachment_checksums(target / "files"), expected)
        with self.assertRaises(RestoreError) as verify_ctx:
            verify_consistent_copy(backup)
        self.assertEqual(verify_ctx.exception.code, "copy_drift")

    def _corrupt_text_component_after_verify(
        self,
        backup: Path,
        target: Path,
        *,
        component: str,
        mutate,
    ) -> RestoreError:
        original = restore_mod._assert_empty_target
        hits: list[str] = []

        def corrupt_after_verify(path: Path) -> None:
            original(path)
            hits.append(component)
            mutate()

        restore_mod._assert_empty_target = corrupt_after_verify  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                restore_into_empty_target(backup, target)
        finally:
            restore_mod._assert_empty_target = original  # type: ignore[assignment]
        self.assertEqual(hits, [component])
        self.assertEqual(ctx.exception.code, "copy_drift")
        with self.assertRaises(RestoreError) as verify_ctx:
            verify_consistent_copy(backup)
        self.assertEqual(verify_ctx.exception.code, "copy_drift")
        return ctx.exception

    def test_verified_manifest_binds_safe_config_bytes(self) -> None:
        """P1: post-verify safe_config mutation must not admit or publish drifted listen."""
        source, ids = _seed_contour(self.base)
        backup = (self.base / "backup-safe-config-bind").resolve()
        target = (self.base / "target-safe-config-bind").resolve()
        create_manual_copy(source, backup)

        def mutate() -> None:
            path = backup / "safe_config" / "config.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["service"]["interface"] = "0.0.0.0:19345"
            path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        self._corrupt_text_component_after_verify(
            backup, target, component="safe_config", mutate=mutate
        )
        self.assertFalse((target / "config.json").exists())
        # Control: intact copy still restores and exact-marker reconcile reopens writes.
        clean_backup = (self.base / "backup-safe-config-clean").resolve()
        clean_target = (self.base / "target-safe-config-clean").resolve()
        create_manual_copy(source, clean_backup)
        restored = restore_into_empty_target(clean_backup, clean_target)
        expected = attachment_checksums(clean_target / "files")
        reconciled = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(reconciled["writes_enabled"])
        published = json.loads((clean_target / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(published["service"]["interface"], "127.0.0.1:19345")

    def test_verified_manifest_binds_service_secret_bytes(self) -> None:
        """P1: post-verify service.secret mutation must not admit drifted secret bytes."""
        source, ids = _seed_contour(self.base)
        backup = (self.base / "backup-secret-bind").resolve()
        target = (self.base / "target-secret-bind").resolve()
        create_manual_copy(source, backup)

        def mutate() -> None:
            path = backup / "secrets" / "service.secret"
            path.write_text("EA010-SYNTHETIC-DRIFTED-SECRET\n", encoding="utf-8")

        self._corrupt_text_component_after_verify(
            backup, target, component="secrets", mutate=mutate
        )
        self.assertFalse((target / "config.json").exists())
        clean_backup = (self.base / "backup-secret-clean").resolve()
        clean_target = (self.base / "target-secret-clean").resolve()
        create_manual_copy(source, clean_backup)
        restored = restore_into_empty_target(clean_backup, clean_target)
        expected = attachment_checksums(clean_target / "files")
        reconciled = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(reconciled["writes_enabled"])
        published = json.loads((clean_target / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(published["service"]["secret"], "synthetic-service-secret")

    def test_verified_manifest_binds_links_bytes(self) -> None:
        """P1: post-verify links mutation must not admit or publish drifted mappings."""
        source, ids = _seed_contour(self.base)
        backup = (self.base / "backup-links-bind").resolve()
        target = (self.base / "target-links-bind").resolve()
        create_manual_copy(source, backup)

        def mutate() -> None:
            path = backup / "links" / "mappings.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["mappings"] = []
            path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        self._corrupt_text_component_after_verify(
            backup, target, component="links", mutate=mutate
        )
        self.assertFalse((target / "links" / "mappings.json").exists())
        clean_backup = (self.base / "backup-links-clean").resolve()
        clean_target = (self.base / "target-links-clean").resolve()
        create_manual_copy(source, clean_backup)
        restored = restore_into_empty_target(clean_backup, clean_target)
        expected = attachment_checksums(clean_target / "files")
        reconciled = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(reconciled["writes_enabled"])
        self.assertEqual(restored["links"]["mappings"][0]["operation_id"], ids["operation_id"])

    def test_text_component_toctou_reopen_cannot_materialize_drift(self) -> None:
        """P1: digest-then-reopen race must not publish bytes unseen by the digest."""
        source, _ids = _seed_contour(self.base)
        backup = (self.base / "backup-text-toctou").resolve()
        target = (self.base / "target-text-toctou").resolve()
        create_manual_copy(source, backup)
        original = restore_mod._read_verified_bytes
        mutated: list[str] = []

        def read_then_rewrite(path: Path, expected_sha256: str, *, component: str) -> bytes:
            payload = original(path, expected_sha256, component=component)
            if component == "safe_config":
                mutated.append(component)
                data = json.loads(payload.decode("utf-8"))
                data["service"]["interface"] = "0.0.0.0:19345"
                path.write_text(
                    json.dumps(data, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            elif component == "secrets":
                mutated.append(component)
                path.write_text("EA010-SYNTHETIC-TOCTOU-SECRET\n", encoding="utf-8")
            elif component == "links":
                mutated.append(component)
                data = json.loads(payload.decode("utf-8"))
                data["mappings"] = []
                path.write_text(
                    json.dumps(data, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            return payload

        restore_mod._read_verified_bytes = read_then_rewrite  # type: ignore[assignment]
        try:
            result = restore_into_empty_target(backup, target)
        finally:
            restore_mod._read_verified_bytes = original  # type: ignore[assignment]
        self.assertEqual(sorted(mutated), ["links", "safe_config", "secrets"])
        self.assertTrue(result["ok"])
        self.assertTrue(result["admitted"])
        published = json.loads((target / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(published["service"]["interface"], "127.0.0.1:19345")
        self.assertEqual(published["service"]["secret"], "synthetic-service-secret")
        links = json.loads((target / "links" / "mappings.json").read_text(encoding="utf-8"))
        self.assertEqual(len(links.get("mappings") or []), 1)
        # Disk generation drifted after the bound read; verify must refuse the copy.
        with self.assertRaises(RestoreError) as verify_ctx:
            verify_consistent_copy(backup)
        self.assertEqual(verify_ctx.exception.code, "copy_drift")

    def test_partial_copy_refuses_restore(self) -> None:
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        (copy_dir / "journal" / "integration.sqlite3").unlink()
        with self.assertRaises(RestoreError) as ctx:
            verify_consistent_copy(copy_dir)
        self.assertEqual(ctx.exception.code, "partial_copy")
        target = self.base / "target"
        with self.assertRaises(RestoreError):
            restore_into_empty_target(copy_dir, target)
        self.assertFalse(target.exists() and any(target.iterdir()))

    def test_corrupt_attachment_hash_refuses(self) -> None:
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        damaged = copy_dir / "attachments" / "synth-note.txt"
        _write(damaged, b"tampered\n")
        with self.assertRaises(RestoreError) as ctx:
            verify_consistent_copy(copy_dir)
        self.assertEqual(ctx.exception.code, "copy_drift")

    def test_old_copy_reconcile_mismatch_keeps_writes_off(self) -> None:
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks={},  # missing Vikunja read-back
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertFalse(ctx.exception.details.get("creates_issued"))
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], ids["operation_id"]))

    def test_unaccounted_observed_marker_keeps_writes_off(self) -> None:
        """Older journal + newer Vikunja marker absent from journal must fence writes.

        Regression for review P1: reconcile must not publish writes when an
        exact EA010 marker is observed but its entire operation/mapping is
        missing from the restored journal; replaying that identity must stay
        create-closed.
        """
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        lost_op = new_operation_id()
        lost_marker = build_marker(ids["issuer_id"], lost_op)
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
            },
            99: {
                "id": 99,
                "project_id": 2,
                "description": f"{lost_marker}\nEA010-SYNTHETIC-lost-op",
            },
        }
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertFalse(ctx.exception.details.get("creates_issued"))
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        mismatches = ctx.exception.details.get("mismatches") or []
        unaccounted = [
            item
            for item in mismatches
            if isinstance(item, dict) and item.get("reason") == "observed_marker_unaccounted"
        ]
        self.assertEqual(len(unaccounted), 1)
        self.assertEqual(unaccounted[0].get("operation_id"), lost_op)
        self.assertEqual(unaccounted[0].get("matches"), [99])
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], ids["operation_id"]))
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], lost_op))
            self.assertIsNone(journal.get_operation(ids["issuer_id"], lost_op))
            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=ids["issuer_id"],
                    operation_id=lost_op,
                    op_type="create",
                    project_id=2,
                    payload={"title": "EA010-SYNTHETIC-lost-replay"},
                    source_kind="chat",
                    source_key="ea010-synthetic-lost-replay",
                )
            self.assertIsNone(journal.get_operation(ids["issuer_id"], lost_op))

    def test_successful_reconcile_after_matching_facts(self) -> None:
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        self.assertFalse(result["restore_drift"])
        self.assertFalse(result["creates_issued"])

    def test_empty_links_fences_writes_after_prior_success(self) -> None:
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
            }
        }
        ok = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(ok["writes_enabled"])
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links={"schema": restored["links"]["schema"], "receipts": [], "mappings": []},
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_empty")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], ids["operation_id"]))

    def test_malformed_receipts_fence_writes_after_prior_success(self) -> None:
        """R1: non-list receipts must fail closed after a successful reconcile."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
            }
        }
        ok = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(ok["writes_enabled"])
        self.assertFalse(ok["restore_drift"])
        fresh_create = new_operation_id()
        with IntegrationJournal(restored["journal_path"]) as journal:
            journal.prepare_intent(
                issuer_id=ids["issuer_id"],
                operation_id=fresh_create,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-malformed-receipts"},
                source_kind="chat",
                source_key="ea010-synthetic-malformed-receipts",
            )
            self.assertTrue(journal.can_dispatch(ids["issuer_id"], fresh_create))
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links={
                    "schema": restored["links"]["schema"],
                    "receipts": 1,
                    "mappings": list(restored["links"]["mappings"]),
                },
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_malformed_links")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        flags = restore_mod._durable_write_flags(Path(restored["journal_path"]))
        self.assertFalse(flags["writes_enabled"])
        self.assertTrue(flags["restore_drift"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], fresh_create))
            peer_create = new_operation_id()
            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=ids["issuer_id"],
                    operation_id=peer_create,
                    op_type="create",
                    project_id=2,
                    payload={"title": "EA010-SYNTHETIC-malformed-receipts-peer"},
                    source_kind="chat",
                    source_key="ea010-synthetic-malformed-receipts-peer",
                )

    def test_malformed_mappings_fence_writes_after_prior_success(self) -> None:
        """R1: non-list mappings must fail closed after a successful reconcile."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nEA010-SYNTHETIC-user-portion",
            }
        }
        ok = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(ok["writes_enabled"])
        self.assertFalse(ok["restore_drift"])
        fresh_create = new_operation_id()
        with IntegrationJournal(restored["journal_path"]) as journal:
            journal.prepare_intent(
                issuer_id=ids["issuer_id"],
                operation_id=fresh_create,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-malformed-mappings"},
                source_kind="chat",
                source_key="ea010-synthetic-malformed-mappings",
            )
            self.assertTrue(journal.can_dispatch(ids["issuer_id"], fresh_create))
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links={
                    "schema": restored["links"]["schema"],
                    "receipts": list(restored["links"]["receipts"]),
                    "mappings": 1,
                },
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_malformed_links")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        flags = restore_mod._durable_write_flags(Path(restored["journal_path"]))
        self.assertFalse(flags["writes_enabled"])
        self.assertTrue(flags["restore_drift"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], fresh_create))

    def test_partial_or_substituted_links_rejected(self) -> None:
        source, ids = _seed_contour(self.base)
        # Second confirmed receipt so partial omission is observable.
        journal_path = source.journal
        with IntegrationJournal(journal_path) as journal:
            issuer = ids["issuer_id"]
            op2 = new_operation_id()
            prepared2 = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=op2,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-restore-unit-2"},
                source_kind="chat",
                source_key="ea010-synthetic-restore-unit-2",
            )
            journal.begin_dispatch(issuer, op2)
            journal.record_receipt(
                issuer,
                op2,
                vikunja_id=99,
                project_id=2,
                marker=prepared2.marker,
                effect={"id": 99},
            )
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        full_links = restored["links"]
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\none",
            },
            99: {
                "id": 99,
                "project_id": 2,
                "description": f"{prepared2.marker}\ntwo",
            },
        }
        # Omit second receipt/mapping.
        partial = {
            "schema": full_links["schema"],
            "receipts": [full_links["receipts"][0]],
            "mappings": [full_links["mappings"][0]],
        }
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=partial,
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertTrue(
            any(m.get("reason") == "receipt_inventory_mismatch" for m in ctx.exception.details["mismatches"])
        )
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        # Substitute durable target 42 → 99 while the journal still binds 42.
        substituted_receipts = []
        for receipt in full_links["receipts"]:
            if receipt.get("vikunja_id") == 42:
                substituted_receipts.append({**receipt, "vikunja_id": 99})
            else:
                substituted_receipts.append(dict(receipt))
        substituted = {
            "schema": full_links["schema"],
            "receipts": substituted_receipts,
            "mappings": list(full_links["mappings"]),
        }
        with self.assertRaises(RestoreError) as ctx2:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=substituted,
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx2.exception.code, "reconcile_failed")
        self.assertTrue(
            any(m.get("reason") == "receipt_inventory_mismatch" for m in ctx2.exception.details["mismatches"])
        )

    def test_multiple_exact_markers_keep_writes_off(self) -> None:
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks={
                    42: {
                        "id": 42,
                        "project_id": 2,
                        "description": f"{ids['marker']}\none",
                    },
                    99: {
                        "id": 99,
                        "project_id": 2,
                        "description": f"{ids['marker']}\ndupe",
                    },
                },
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertTrue(
            any(m.get("reason") == "multiple_exact_matches" for m in ctx.exception.details["mismatches"])
        )
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))

    def test_prepared_pending_recovered_not_rearmed(self) -> None:
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            pending_op = new_operation_id()
            pending = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending"},
                source_kind="chat",
                source_key="ea010-synthetic-pending",
            )
        copy_dir = self.base / "copy"
        create_manual_copy(source, copy_dir)
        target = self.base / "target"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        # Observed inventory already carries the pending marker (prior create).
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "description": f"{ids['marker']}\nconfirmed",
                },
                7: {
                    "id": 7,
                    "project_id": 2,
                    "description": f"{pending.marker}\nrecovered",
                },
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            recovered = journal.get_operation(issuer, pending_op)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(recovered.state.value, "confirmed")
            self.assertIsNotNone(recovered.receipt)
            self.assertEqual(int(recovered.receipt["vikunja_id"]), 7)
            self.assertFalse(recovered.allows_first_http_dispatch())
            self.assertFalse(journal.can_dispatch(issuer, pending_op))

    def test_process_death_mid_recovery_keeps_dispatch_closed(self) -> None:
        """F1: crash before publish_writes must leave writes_enabled=0 and B unarmed."""
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            pending_op = new_operation_id()
            pending = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending-crash"},
                source_kind="chat",
                source_key="ea010-synthetic-pending-crash",
            )
        copy_dir = self.base / "copy-crash"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-crash"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nconfirmed",
            },
            7: {
                "id": 7,
                "project_id": 2,
                "description": f"{pending.marker}\nrecovered",
            },
        }

        def _crash(point: str) -> None:
            if point == "before_publish_writes":
                raise RuntimeError("synthetic-process-death-before-publish")

        restore_mod._test_crash_hook = _crash
        try:
            with self.assertRaises(RestoreError) as ctx:
                reconcile_restored_contour(
                    journal_path=restored["journal_path"],
                    links=restored["links"],
                    observed_tasks=observed,
                    observed_attachment_checksums=expected,
                    expected_attachment_checksums=expected,
                )
        finally:
            restore_mod._test_crash_hook = None
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        with IntegrationJournal(restored["journal_path"]) as journal:
            flags = restore_mod._durable_write_flags(Path(restored["journal_path"]))
            self.assertFalse(flags["writes_enabled"])
            self.assertTrue(flags["restore_drift"])
            pending_row = journal.get_operation(issuer, pending_op)
            self.assertIsNotNone(pending_row)
            assert pending_row is not None
            # May already be confirmed/unknown from recovery commits, but never armed.
            self.assertFalse(pending_row.allows_first_http_dispatch())
            self.assertFalse(journal.can_dispatch(issuer, pending_op))
            self.assertFalse(journal.can_dispatch(issuer, new_operation_id()))

        # Restart with the original verified links must resume, not treat the
        # durable recovery receipt as backup drift (UC-EA010-04 / FR-EA010-24).
        resumed = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(resumed["ok"])
        self.assertTrue(resumed["writes_enabled"])
        self.assertFalse(resumed["restore_drift"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            recovered = journal.get_operation(issuer, pending_op)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(recovered.state, OperationState.CONFIRMED)
            self.assertEqual(int(recovered.receipt["vikunja_id"]), 7)  # type: ignore[index]
            self.assertEqual(
                recovered.receipt["effect"].get("source"),  # type: ignore[index]
                restore_mod.RESTORE_RECEIPT_SOURCE,
            )
            self.assertFalse(journal.can_dispatch(issuer, pending_op))

    def test_repeat_reconcile_after_success_keeps_original_links(self) -> None:
        """Recovery delta stays attributable across success and dry-run repeats."""
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            pending_op = new_operation_id()
            pending = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending-repeat"},
                source_kind="chat",
                source_key="ea010-synthetic-pending-repeat",
            )
        copy_dir = self.base / "copy-repeat"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-repeat"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        original_links = restored["links"]
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nconfirmed",
            },
            7: {
                "id": 7,
                "project_id": 2,
                "description": f"{pending.marker}\nrecovered",
            },
        }
        first = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=original_links,
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(first["ok"])
        self.assertTrue(first["writes_enabled"])
        # Ordinary successful recovery: original links must still reconcile.
        second = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=original_links,
            observed_tasks=observed,
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(second["ok"])
        self.assertTrue(second["writes_enabled"])

        # Dry-run path also commits the recovery receipt but leaves writes off.
        source2, ids2 = _seed_contour(self.base / "dry")
        with IntegrationJournal(source2.journal) as journal:
            issuer2 = ids2["issuer_id"]
            pending_op2 = new_operation_id()
            pending2 = journal.prepare_intent(
                issuer_id=issuer2,
                operation_id=pending_op2,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending-dry"},
                source_kind="chat",
                source_key="ea010-synthetic-pending-dry",
            )
        copy_dir2 = self.base / "copy-dry"
        create_manual_copy(source2, copy_dir2)
        target2 = self.base / "target-dry"
        restored2 = restore_into_empty_target(copy_dir2, target2)
        expected2 = attachment_checksums(target2 / "files")
        observed2 = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids2['marker']}\nconfirmed",
            },
            8: {
                "id": 8,
                "project_id": 2,
                "description": f"{pending2.marker}\nrecovered",
            },
        }
        dry = reconcile_restored_contour(
            journal_path=restored2["journal_path"],
            links=restored2["links"],
            observed_tasks=observed2,
            observed_attachment_checksums=expected2,
            expected_attachment_checksums=expected2,
            allow_writes_on_success=False,
        )
        self.assertTrue(dry["ok"])
        self.assertFalse(dry["writes_enabled"])
        self.assertTrue(dry["restore_drift"])
        after_dry = reconcile_restored_contour(
            journal_path=restored2["journal_path"],
            links=restored2["links"],
            observed_tasks=observed2,
            observed_attachment_checksums=expected2,
            expected_attachment_checksums=expected2,
        )
        self.assertTrue(after_dry["ok"])
        self.assertTrue(after_dry["writes_enabled"])
        with IntegrationJournal(restored2["journal_path"]) as journal:
            recovered = journal.get_operation(issuer2, pending_op2)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(int(recovered.receipt["vikunja_id"]), 8)  # type: ignore[index]

    def test_first_unknown_create_and_unused_journal_reconcile(self) -> None:
        """Zero-receipt copy may recover the first UNKNOWN create or admit unused."""
        # Pending-only journal: mapping present, no confirmed receipt in links.
        root = self.base / "first-source"
        files = root / "files"
        files.mkdir(parents=True)
        _write(files / "synth-note.txt", b"EA010-SYNTHETIC-ATTACHMENT-v1\n")
        db_path = root / "stand.db"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            conn.execute("INSERT INTO synth(note) VALUES (?)", ("EA010-SYNTHETIC-DB",))
            conn.commit()
        finally:
            conn.close()
        os.chmod(db_path, 0o600)
        _write(
            root / "config.json",
            json.dumps(
                {
                    "service": {
                        "interface": "127.0.0.1:19347",
                        "publicurl": "http://127.0.0.1:19347/",
                        "rootpath": str(root),
                        "secret": "synthetic-service-secret",
                    },
                    "database": {"type": "sqlite", "path": str(root / "stand.db")},
                    "files": {"basepath": str(files), "maxsize": "1MB"},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        journal_path = root / "journal" / "integration.sqlite3"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        with IntegrationJournal(journal_path) as journal:
            issuer = journal.ensure_issuer()
            first_op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=first_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-first-op"},
                source_kind="chat",
                source_key="ea010-synthetic-first-op",
            )
            journal.begin_dispatch(issuer, first_op)
            journal.mark_unknown(
                issuer,
                first_op,
                reason="synthetic_timeout",
                details={"first_operation": True},
            )
        source = ContourPaths.from_stand_root(root)
        copy_dir = self.base / "copy-first"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-first"
        restored = restore_into_empty_target(copy_dir, target)
        self.assertEqual(restored["links"]["receipts"], [])
        self.assertEqual(len(restored["links"]["mappings"]), 1)
        expected = attachment_checksums(target / "files")
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                5: {
                    "id": 5,
                    "project_id": 2,
                    "description": f"{prepared.marker}\nfirst",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            recovered = journal.get_operation(issuer, first_op)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(recovered.state, OperationState.CONFIRMED)
            self.assertEqual(int(recovered.receipt["vikunja_id"]), 5)  # type: ignore[index]
        # Repeat with the same zero-receipt links after recovery.
        repeat = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                5: {
                    "id": 5,
                    "project_id": 2,
                    "description": f"{prepared.marker}\nfirst",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(repeat["ok"])
        self.assertTrue(repeat["writes_enabled"])

        # Unused journal (issuer only): empty receipts/mappings must admit writes.
        unused_root = self.base / "unused-source"
        unused_files = unused_root / "files"
        unused_files.mkdir(parents=True)
        _write(unused_files / "synth-note.txt", b"EA010-SYNTHETIC-ATTACHMENT-v1\n")
        unused_db = unused_root / "stand.db"
        conn = sqlite3.connect(str(unused_db))
        try:
            conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            conn.commit()
        finally:
            conn.close()
        os.chmod(unused_db, 0o600)
        _write(
            unused_root / "config.json",
            json.dumps(
                {
                    "service": {
                        "interface": "127.0.0.1:19348",
                        "publicurl": "http://127.0.0.1:19348/",
                        "rootpath": str(unused_root),
                        "secret": "synthetic-service-secret",
                    },
                    "database": {"type": "sqlite", "path": str(unused_root / "stand.db")},
                    "files": {"basepath": str(unused_files), "maxsize": "1MB"},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        unused_journal = unused_root / "journal" / "integration.sqlite3"
        unused_journal.parent.mkdir(parents=True, exist_ok=True)
        with IntegrationJournal(unused_journal) as journal:
            journal.ensure_issuer()
        unused_source = ContourPaths.from_stand_root(unused_root)
        unused_copy = self.base / "copy-unused"
        create_manual_copy(unused_source, unused_copy)
        unused_target = self.base / "target-unused"
        unused_restored = restore_into_empty_target(unused_copy, unused_target)
        self.assertEqual(unused_restored["links"]["receipts"], [])
        self.assertEqual(unused_restored["links"]["mappings"], [])
        unused_expected = attachment_checksums(unused_target / "files")
        unused_result = reconcile_restored_contour(
            journal_path=unused_restored["journal_path"],
            links=unused_restored["links"],
            observed_tasks={},
            observed_attachment_checksums=unused_expected,
            expected_attachment_checksums=unused_expected,
        )
        self.assertTrue(unused_result["ok"])
        self.assertTrue(unused_result["writes_enabled"])
        self.assertFalse(unused_result["restore_drift"])

    def test_concurrent_writer_sees_closed_fence_mid_recovery(self) -> None:
        """F1: durable writes stay off mid-recovery; peer cannot arm dispatch early."""
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            pending_op = new_operation_id()
            pending = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending-concurrent"},
                source_kind="chat",
                source_key="ea010-synthetic-pending-concurrent",
            )
        copy_dir = self.base / "copy-concurrent"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-concurrent"
        restored = restore_into_empty_target(copy_dir, target)
        journal_path = Path(restored["journal_path"])
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nconfirmed",
            },
            7: {
                "id": 7,
                "project_id": 2,
                "description": f"{pending.marker}\nrecovered",
            },
        }
        mid: dict[str, object] = {}
        peer_results: dict[str, object] = {}
        sample_ready = threading.Event()
        release = threading.Event()

        def _sample(point: str) -> None:
            if point != "before_publish_writes":
                return
            # Separate SQLite read bypasses the OS-lock: durable fence must stay closed.
            flags = restore_mod._durable_write_flags(journal_path)
            mid["writes_enabled"] = flags["writes_enabled"]
            mid["restore_drift"] = flags["restore_drift"]
            sample_ready.set()
            if not release.wait(timeout=5.0):
                raise RuntimeError("peer release timeout")

        def _peer() -> None:
            if not sample_ready.wait(timeout=5.0):
                peer_results["error"] = "sample-timeout"
                release.set()
                return
            # Observe durable meta without competing for the recovery OS-lock.
            flags = restore_mod._durable_write_flags(journal_path)
            peer_results["writes_enabled"] = flags["writes_enabled"]
            peer_results["restore_drift"] = flags["restore_drift"]
            release.set()

        restore_mod._test_crash_hook = _sample
        peer_thread = threading.Thread(target=_peer, daemon=True)
        peer_thread.start()
        try:
            result = reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        finally:
            restore_mod._test_crash_hook = None
            release.set()
            peer_thread.join(timeout=5.0)
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        self.assertFalse(mid.get("writes_enabled"))
        self.assertTrue(mid.get("restore_drift"))
        self.assertNotIn("error", peer_results)
        self.assertFalse(peer_results.get("writes_enabled"))
        self.assertTrue(peer_results.get("restore_drift"))
        with IntegrationJournal(journal_path) as journal:
            self.assertFalse(journal.can_dispatch(issuer, pending_op))

    def test_unknown_complete_not_confirmed_by_exact_marker(self) -> None:
        """F2: exact marker on task cannot prove complete; retain durable unknown."""
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            complete_op = new_operation_id()
            complete = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=complete_op,
                op_type="complete",
                project_id=2,
                target_id=42,
                payload={"done": True, "preimage": {"done": False}},
                source_kind="chat",
                source_key="ea010-synthetic-complete-unknown",
            )
            journal.begin_dispatch(issuer, complete_op)
            journal.mark_unknown(
                issuer,
                complete_op,
                reason="synthetic_timeout",
                details={"causal_proof": False},
            )
            self.assertEqual(complete.marker, ids["marker"])
        copy_dir = self.base / "copy-complete"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-complete"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "done": False,
                    "description": f"{ids['marker']}\nstill-open",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            recovered = journal.get_operation(issuer, complete_op)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(recovered.state, OperationState.UNKNOWN)
            self.assertIsNone(recovered.receipt)
            self.assertFalse(recovered.allows_first_http_dispatch())

    def test_unknown_update_retained_without_causal_receipt(self) -> None:
        """F2: unknown update stays unknown even when the marked task is observed."""
        source, ids = _seed_contour(self.base)
        with IntegrationJournal(source.journal) as journal:
            issuer = ids["issuer_id"]
            update_op = new_operation_id()
            journal.prepare_intent(
                issuer_id=issuer,
                operation_id=update_op,
                op_type="update",
                project_id=2,
                target_id=42,
                payload={
                    "title": "EA010-SYNTHETIC-updated",
                    "preimage": {"title": "EA010-SYNTHETIC-restore-unit"},
                },
                source_kind="chat",
                source_key="ea010-synthetic-update-unknown",
            )
            journal.begin_dispatch(issuer, update_op)
            journal.mark_unknown(
                issuer,
                update_op,
                reason="synthetic_timeout",
                details={"causal_proof": False},
            )
        copy_dir = self.base / "copy-update"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-update"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                42: {
                    "id": 42,
                    "project_id": 2,
                    "title": "EA010-SYNTHETIC-restore-unit",
                    "description": f"{ids['marker']}\npreimage",
                }
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            recovered = journal.get_operation(issuer, update_op)
            self.assertIsNotNone(recovered)
            assert recovered is not None
            self.assertEqual(recovered.state, OperationState.UNKNOWN)
            self.assertIsNone(recovered.receipt)

    def test_unmarked_manual_update_complete_backup_reconciles(self) -> None:
        """F3: empty-marker update/complete receipts must not refuse restore."""
        root = self.base / "manual-source"
        files = root / "files"
        files.mkdir(parents=True)
        attachment = files / "synth-note.txt"
        _write(attachment, b"EA010-SYNTHETIC-ATTACHMENT-v1\n")
        db_path = root / "stand.db"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            conn.execute("INSERT INTO synth(note) VALUES (?)", ("EA010-SYNTHETIC-DB",))
            conn.commit()
        finally:
            conn.close()
        os.chmod(db_path, 0o600)
        _write(
            root / "config.json",
            json.dumps(
                {
                    "service": {
                        "interface": "127.0.0.1:19346",
                        "publicurl": "http://127.0.0.1:19346/",
                        "rootpath": str(root),
                        "secret": "synthetic-service-secret",
                    },
                    "database": {"type": "sqlite", "path": str(root / "stand.db")},
                    "files": {"basepath": str(files), "maxsize": "1MB"},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        journal_path = root / "journal" / "integration.sqlite3"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        with IntegrationJournal(journal_path) as journal:
            issuer = journal.ensure_issuer()
            create_op = new_operation_id()
            created = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=create_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-marked-create"},
                source_kind="chat",
                source_key="ea010-synthetic-marked-create",
            )
            journal.begin_dispatch(issuer, create_op)
            journal.record_receipt(
                issuer,
                create_op,
                vikunja_id=7,
                project_id=2,
                marker=created.marker,
                effect={"id": 7},
            )
            update_op = new_operation_id()
            updated = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=update_op,
                op_type="update",
                project_id=2,
                target_id=99,
                payload={"title": "EA010-SYNTHETIC-manual-updated"},
                source_kind="chat",
                source_key="ea010-synthetic-manual-update",
            )
            self.assertEqual(updated.marker, "")
            journal.begin_dispatch(issuer, update_op)
            journal.record_receipt(
                issuer,
                update_op,
                vikunja_id=99,
                project_id=2,
                marker="",
                effect={"id": 99, "title": "EA010-SYNTHETIC-manual-updated"},
            )
            complete_op = new_operation_id()
            completed = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=complete_op,
                op_type="complete",
                project_id=2,
                target_id=99,
                payload={"done": True},
                source_kind="chat",
                source_key="ea010-synthetic-manual-complete",
            )
            self.assertEqual(completed.marker, "")
            journal.begin_dispatch(issuer, complete_op)
            journal.record_receipt(
                issuer,
                complete_op,
                vikunja_id=99,
                project_id=2,
                marker=None,
                effect={"id": 99, "done": True},
            )
        source = ContourPaths.from_stand_root(root)
        copy_dir = self.base / "copy-manual"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-manual"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        result = reconcile_restored_contour(
            journal_path=restored["journal_path"],
            links=restored["links"],
            observed_tasks={
                7: {
                    "id": 7,
                    "project_id": 2,
                    "description": f"{created.marker}\nmarked",
                },
                99: {
                    "id": 99,
                    "project_id": 2,
                    "title": "EA010-SYNTHETIC-manual-updated",
                    "done": True,
                    "description": "manual unmarked task",
                },
            },
            observed_attachment_checksums=expected,
            expected_attachment_checksums=expected,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["writes_enabled"])
        self.assertFalse(result["restore_drift"])
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertEqual(
                journal.get_operation(issuer, update_op).state,  # type: ignore[union-attr]
                OperationState.CONFIRMED,
            )
            self.assertEqual(
                journal.get_operation(issuer, complete_op).state,  # type: ignore[union-attr]
                OperationState.CONFIRMED,
            )

    def test_observed_task_id_mismatch_fences_writes(self) -> None:
        """P1: inventory key must equal positive observed task.id before writes reopen."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-id-mismatch"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-id-mismatch"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks={
                    42: {
                        "id": 999,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-id-mismatch",
                    }
                },
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        mismatches = ctx.exception.details.get("mismatches") or []
        self.assertTrue(
            any(
                isinstance(item, dict) and item.get("reason") == "observed_identity_malformed"
                for item in mismatches
            )
        )
        with IntegrationJournal(restored["journal_path"]) as journal:
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], ids["operation_id"]))
            fresh = new_operation_id()
            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=ids["issuer_id"],
                    operation_id=fresh,
                    op_type="create",
                    project_id=2,
                    payload={"title": "EA010-SYNTHETIC-id-mismatch-create"},
                    source_kind="chat",
                    source_key="ea010-synthetic-id-mismatch-create",
                )
            self.assertFalse(journal.can_dispatch(ids["issuer_id"], fresh))

    def test_malformed_extra_identity_fences_alongside_confirmed(self) -> None:
        """P1: malformed extras must fence even when a valid confirmed task matches.

        Boundary probe: inventory contains confirmed 42 plus key 43 with id 999
        sharing the same exact marker. Dropping the malformed row must not reopen
        writes or rearm create for a lost identity carried on that row.
        """
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-malformed-extra"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-malformed-extra"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        lost_op = new_operation_id()
        lost_marker = build_marker(ids["issuer_id"], lost_op)

        cases = (
            {
                "label": "key_id_conflict_same_marker",
                "observed": {
                    42: {
                        "id": 42,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-exact-A",
                    },
                    43: {
                        "id": 999,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-exact-A",
                    },
                },
            },
            {
                "label": "key_id_conflict_lost_marker",
                "observed": {
                    42: {
                        "id": 42,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-exact-A",
                    },
                    43: {
                        "id": 999,
                        "project_id": 2,
                        "description": f"{lost_marker}\nEA010-SYNTHETIC-lost",
                    },
                },
            },
            {
                "label": "bool_inventory_key",
                "observed": {
                    42: {
                        "id": 42,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-exact-A",
                    },
                    True: {
                        "id": 1,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-bool-key",
                    },
                },
            },
            {
                "label": "nonpositive_observed_id",
                "observed": {
                    42: {
                        "id": 42,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-exact-A",
                    },
                    7: {
                        "id": 0,
                        "project_id": 2,
                        "description": f"{ids['marker']}\nEA010-SYNTHETIC-zero-id",
                    },
                },
            },
        )
        for case in cases:
            with self.subTest(case=case["label"]):
                with self.assertRaises(RestoreError) as ctx:
                    reconcile_restored_contour(
                        journal_path=restored["journal_path"],
                        links=restored["links"],
                        observed_tasks=case["observed"],
                        observed_attachment_checksums=expected,
                        expected_attachment_checksums=expected,
                    )
                self.assertEqual(ctx.exception.code, "reconcile_failed")
                self.assertFalse(ctx.exception.details.get("writes_enabled"))
                self.assertTrue(ctx.exception.details.get("restore_drift"))
                mismatches = ctx.exception.details.get("mismatches") or []
                self.assertTrue(
                    any(
                        isinstance(item, dict)
                        and item.get("reason") == "observed_identity_malformed"
                        for item in mismatches
                    ),
                    mismatches,
                )
                with IntegrationJournal(restored["journal_path"]) as journal:
                    self.assertFalse(journal.can_dispatch(ids["issuer_id"], ids["operation_id"]))
                    with self.assertRaises(JournalError):
                        journal.prepare_intent(
                            issuer_id=ids["issuer_id"],
                            operation_id=lost_op,
                            op_type="create",
                            project_id=2,
                            payload={"title": "EA010-SYNTHETIC-malformed-extra-create"},
                            source_kind="chat",
                            source_key=f"ea010-synthetic-malformed-extra-{case['label']}",
                        )
                    self.assertFalse(journal.can_dispatch(ids["issuer_id"], lost_op))

    def test_observed_project_missing_or_null_fences_writes(self) -> None:
        """P1: missing/null observed project_id must not reopen writes."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-project-null"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-project-null"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        for observed in (
            {
                42: {
                    "id": 42,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-project-missing",
                }
            },
            {
                42: {
                    "id": 42,
                    "project_id": None,
                    "description": f"{ids['marker']}\nEA010-SYNTHETIC-project-null",
                }
            },
        ):
            with self.subTest(observed=observed[42]):
                with self.assertRaises(RestoreError) as ctx:
                    reconcile_restored_contour(
                        journal_path=restored["journal_path"],
                        links=restored["links"],
                        observed_tasks=observed,
                        observed_attachment_checksums=expected,
                        expected_attachment_checksums=expected,
                    )
                self.assertEqual(ctx.exception.code, "reconcile_failed")
                self.assertFalse(ctx.exception.details.get("writes_enabled"))
                self.assertTrue(ctx.exception.details.get("restore_drift"))
                mismatches = ctx.exception.details.get("mismatches") or []
                self.assertTrue(
                    any(
                        isinstance(item, dict) and item.get("reason") == "project_mismatch"
                        for item in mismatches
                    )
                )
        with IntegrationJournal(restored["journal_path"]) as journal:
            fresh = new_operation_id()
            with self.assertRaises(JournalError):
                journal.prepare_intent(
                    issuer_id=ids["issuer_id"],
                    operation_id=fresh,
                    op_type="create",
                    project_id=2,
                    payload={"title": "EA010-SYNTHETIC-project-null-create"},
                    source_kind="chat",
                    source_key="ea010-synthetic-project-null-create",
                )

    def test_pending_create_requires_strict_observed_project(self) -> None:
        """P1: pending recovery rejects missing project even when marker matches once."""
        root = self.base / "pending-project"
        files = root / "files"
        files.mkdir(parents=True)
        _write(files / "synth-note.txt", b"EA010-SYNTHETIC-ATTACHMENT-v1\n")
        db_path = root / "stand.db"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            conn.execute("INSERT INTO synth(note) VALUES (?)", ("EA010-SYNTHETIC-DB",))
            conn.commit()
        finally:
            conn.close()
        os.chmod(db_path, 0o600)
        _write(
            root / "config.json",
            json.dumps(
                {
                    "service": {
                        "interface": "127.0.0.1:19345",
                        "publicurl": "http://127.0.0.1:19345/",
                        "rootpath": str(root),
                        "secret": "synthetic-service-secret",
                    },
                    "database": {"type": "sqlite", "path": str(root / "stand.db")},
                    "files": {"basepath": str(files), "maxsize": "1MB"},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        journal_path = root / "journal" / "integration.sqlite3"
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        with IntegrationJournal(journal_path) as journal:
            issuer = journal.ensure_issuer()
            pending_op = new_operation_id()
            prepared = journal.prepare_intent(
                issuer_id=issuer,
                operation_id=pending_op,
                op_type="create",
                project_id=2,
                payload={"title": "EA010-SYNTHETIC-pending-project"},
                source_kind="chat",
                source_key="ea010-synthetic-pending-project",
            )
            marker = prepared.marker
        source = ContourPaths.from_stand_root(root)
        copy_dir = self.base / "copy-pending-project"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-pending-project"
        restored = restore_into_empty_target(copy_dir, target)
        expected = attachment_checksums(target / "files")
        with self.assertRaises(RestoreError) as ctx:
            reconcile_restored_contour(
                journal_path=restored["journal_path"],
                links=restored["links"],
                observed_tasks={
                    77: {
                        "id": 77,
                        "project_id": None,
                        "description": f"{marker}\nEA010-SYNTHETIC-pending",
                    }
                },
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        mismatches = ctx.exception.details.get("mismatches") or []
        self.assertTrue(
            any(
                isinstance(item, dict) and item.get("reason") == "pending_project_mismatch"
                for item in mismatches
            )
        )
        self.assertFalse(ctx.exception.details.get("writes_enabled"))

    def test_manual_copy_holds_journal_lock_across_generation(self) -> None:
        """P2: concurrent prepare_intent cannot split DB/.install/links generation."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-locked-generation"
        barrier = threading.Event()
        started = threading.Event()
        errors: list[BaseException] = []

        def _contending_prepare() -> None:
            try:
                started.set()
                barrier.wait(timeout=5)
                with IntegrationJournal(source.journal) as journal:
                    journal.prepare_intent(
                        issuer_id=ids["issuer_id"],
                        operation_id=new_operation_id(),
                        op_type="create",
                        project_id=2,
                        payload={"title": "EA010-SYNTHETIC-race"},
                        source_kind="chat",
                        source_key=f"ea010-synthetic-race-{new_operation_id()}",
                    )
            except BaseException as exc:  # noqa: BLE001 — collect for main thread
                errors.append(exc)

        original_copy = restore_mod._copy_sqlite_consistent

        def _copy_and_release(src: Path, dest: Path, mode: int = 0o600) -> str:
            digest = original_copy(src, dest, mode)
            if src == source.journal:
                barrier.set()
                # Contender blocks on the held journal lock until copy exits.
                time.sleep(0.05)
            return digest

        peer = threading.Thread(target=_contending_prepare, daemon=True)
        peer.start()
        self.assertTrue(started.wait(timeout=2))
        previous = restore_mod._copy_sqlite_consistent
        restore_mod._copy_sqlite_consistent = _copy_and_release  # type: ignore[assignment]
        try:
            manifest = create_manual_copy(source, copy_dir)
        finally:
            restore_mod._copy_sqlite_consistent = previous  # type: ignore[assignment]
            barrier.set()
            peer.join(timeout=5)
        self.assertTrue(manifest["copy_id"])
        verified = verify_consistent_copy(copy_dir)
        self.assertTrue(verified["ok"])
        self.assertIn("journal_db_install_pair", verified["checked"])
        links = json.loads((copy_dir / "links" / "mappings.json").read_text(encoding="utf-8"))
        self.assertEqual(links["receipts"][0]["vikunja_id"], 42)
        target = self.base / "target-locked-generation"
        restored = restore_into_empty_target(copy_dir, target)
        self.assertTrue(restored["ok"])
        self.assertFalse(restored["writes_enabled"])
        self.assertTrue(peer.is_alive() is False)
        self.assertEqual(errors, [])

    def test_source_db_symlink_refused_before_checkpoint_preserves_foreign(self) -> None:
        """P1/NFR-13/15: refuse source symlink before mode=rw checkpoint mutates foreign WAL."""
        source, _ids = _seed_contour(self.base)
        foreign_dir = self.base / "foreign-wal-db"
        foreign_dir.mkdir()
        foreign_db = foreign_dir / "foreign.sqlite3"
        # Keep the writer open so the WAL is not auto-checkpointed/removed on last close.
        foreign_conn = sqlite3.connect(str(foreign_db))
        try:
            foreign_conn.execute("PRAGMA journal_mode=WAL")
            foreign_conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            foreign_conn.execute(
                "INSERT INTO synth(note) VALUES (?)", ("FOREIGN-COMMITTED-ROW",)
            )
            foreign_conn.commit()
            mode = foreign_conn.execute("PRAGMA journal_mode").fetchone()[0]
            self.assertEqual(str(mode).lower(), "wal")
            foreign_wal = Path(str(foreign_db) + "-wal")
            self.assertTrue(foreign_wal.is_file())
            self.assertGreater(foreign_wal.stat().st_size, 0)
            pre_sha = restore_mod.sha256_file(foreign_db)
            pre_wal_size = foreign_wal.stat().st_size
            pre_wal_sha = restore_mod.sha256_file(foreign_wal)
            # Replace the synthetic source DB with a symlink to the foreign WAL DB.
            source.vikunja_db.unlink()
            source.vikunja_db.symlink_to(foreign_db)
            copy_dir = self.base / "copy-symlink-source-refused"
            with self.assertRaises(RestoreError) as ctx:
                create_manual_copy(source, copy_dir)
            self.assertEqual(ctx.exception.code, "symlink_refused")
            self.assertEqual(restore_mod.sha256_file(foreign_db), pre_sha)
            self.assertEqual(foreign_wal.stat().st_size, pre_wal_size)
            self.assertEqual(restore_mod.sha256_file(foreign_wal), pre_wal_sha)
            note = foreign_conn.execute("SELECT note FROM synth").fetchone()[0]
            self.assertEqual(note, "FOREIGN-COMMITTED-ROW")
            self.assertFalse((copy_dir / "manifest.json").is_file())
        finally:
            foreign_conn.close()

    def test_create_manual_copy_never_opens_source_writable_preserves_foreign(self) -> None:
        """P1/NFR-13/15/18: capture must not mode=rw the supplied source; foreign WAL stays intact.

        Full create_manual_copy entry point with an unrelated live WAL DB present.
        Any sqlite3.connect of the source path with mode=rw would be a product
        defect; checkpoint belongs only on the tool-owned temp copy. Source DB and
        any source WAL companions must stay byte-identical after success.
        """
        source, _ids = _seed_contour(self.base)
        # Keep a source writer open so the source WAL stays nonempty across capture.
        source_writer = sqlite3.connect(str(source.vikunja_db))
        source_writer.execute("PRAGMA journal_mode=WAL")
        source_writer.execute("UPDATE synth SET note = ?", ("EA010-SYNTHETIC-SRC-WAL",))
        source_writer.commit()
        source_wal = Path(str(source.vikunja_db) + "-wal")
        self.assertTrue(source_wal.is_file())
        self.assertGreater(source_wal.stat().st_size, 0)
        source_before = source.vikunja_db.read_bytes()
        source_wal_before = source_wal.read_bytes()
        foreign_db = self.base / "foreign-uri-race.sqlite3"
        foreign_conn = sqlite3.connect(str(foreign_db))
        try:
            foreign_conn.execute("PRAGMA journal_mode=WAL")
            foreign_conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            foreign_conn.execute(
                "INSERT INTO synth(note) VALUES (?)", ("EA010-SYNTHETIC-FOREIGN",)
            )
            foreign_conn.commit()
            foreign_wal = Path(str(foreign_db) + "-wal")
            self.assertGreater(foreign_wal.stat().st_size, 0)
            pre_wal = foreign_wal.read_bytes()
            pre_db = foreign_db.read_bytes()
            real_connect = sqlite3.connect
            source_rw_opens: list[str] = []

            def watch_connect(database: object, *args: object, **kwargs: object):  # noqa: ANN001
                text = str(database)
                if f"file:{source.vikunja_db}?mode=rw" == text or (
                    str(source.vikunja_db) in text and "mode=rw" in text
                ):
                    source_rw_opens.append(text)
                return real_connect(database, *args, **kwargs)  # type: ignore[arg-type]

            copy_dir = self.base / "copy-no-source-rw"
            sqlite3.connect = watch_connect  # type: ignore[assignment]
            try:
                manifest = create_manual_copy(source, copy_dir)
            finally:
                sqlite3.connect = real_connect  # type: ignore[assignment]
            self.assertEqual(source_rw_opens, [])
            self.assertTrue(manifest["copy_id"])
            self.assertEqual(foreign_wal.read_bytes(), pre_wal)
            self.assertEqual(foreign_db.read_bytes(), pre_db)
            self.assertEqual(source.vikunja_db.read_bytes(), source_before)
            self.assertEqual(source_wal.read_bytes(), source_wal_before)
            self.assertTrue(verify_consistent_copy(copy_dir)["ok"])
        finally:
            foreign_conn.close()
            source_writer.close()

    def test_symlink_refusal_does_not_mutate_foreign_or_open_source_rw(self) -> None:
        """P1: symlink refusal leaves foreign DB/WAL byte-identical and never mode=rw source."""
        source, _ids = _seed_contour(self.base)
        foreign_db = self.base / "foreign-symlink-preserve.sqlite3"
        foreign_conn = sqlite3.connect(str(foreign_db))
        try:
            foreign_conn.execute("PRAGMA journal_mode=WAL")
            foreign_conn.execute("CREATE TABLE synth (id INTEGER PRIMARY KEY, note TEXT)")
            foreign_conn.execute(
                "INSERT INTO synth(note) VALUES (?)", ("FOREIGN-PRESERVE",)
            )
            foreign_conn.commit()
            foreign_wal = Path(str(foreign_db) + "-wal")
            pre_db = foreign_db.read_bytes()
            pre_wal = foreign_wal.read_bytes()
            source.vikunja_db.unlink()
            source.vikunja_db.symlink_to(foreign_db)
            real_connect = sqlite3.connect
            source_rw_opens: list[str] = []

            def watch_connect(database: object, *args: object, **kwargs: object):  # noqa: ANN001
                text = str(database)
                if str(source.vikunja_db) in text and "mode=rw" in text:
                    source_rw_opens.append(text)
                return real_connect(database, *args, **kwargs)  # type: ignore[arg-type]

            sqlite3.connect = watch_connect  # type: ignore[assignment]
            try:
                with self.assertRaises(RestoreError) as ctx:
                    create_manual_copy(source, self.base / "copy-symlink-preserve")
            finally:
                sqlite3.connect = real_connect  # type: ignore[assignment]
            self.assertEqual(ctx.exception.code, "symlink_refused")
            self.assertEqual(source_rw_opens, [])
            self.assertEqual(foreign_db.read_bytes(), pre_db)
            self.assertEqual(foreign_wal.read_bytes(), pre_wal)
        finally:
            foreign_conn.close()

    def test_writer_mutation_between_db_and_attachments_refused(self) -> None:
        """P2: generation change during Vikunja capture must fail closed.

        Synthetic boundary: DB and attachment start at GENERATION-1. Immediately
        after the DB backup returns, a writer advances the attachment (and DB)
        to GENERATION-2. Stock quiescence + stable-generation checks must refuse
        the inconsistent copy instead of advertising it as ok.
        """
        source, _ids = _seed_contour(self.base)
        conn = sqlite3.connect(str(source.vikunja_db))
        try:
            conn.execute("UPDATE synth SET note = ?", ("GENERATION-1",))
            conn.commit()
        finally:
            conn.close()
        attachment = source.attachments / "synth-note.txt"
        _write(attachment, b"GENERATION-1\n")
        copy_dir = self.base / "copy-writer-boundary"
        original = restore_mod._copy_sqlite_consistent

        def _copy_then_advance_attachment(src: Path, dest: Path, mode: int = 0o600) -> str:
            digest = original(src, dest, mode)
            if src == source.vikunja_db:
                _write(attachment, b"GENERATION-2\n")
                writer = sqlite3.connect(
                    f"file:{source.vikunja_db}?mode=rw",
                    uri=True,
                    timeout=1.0,
                )
                try:
                    writer.execute("BEGIN IMMEDIATE")
                    writer.execute("UPDATE synth SET note = ?", ("GENERATION-2",))
                    writer.execute("COMMIT")
                finally:
                    writer.close()
            return digest

        restore_mod._copy_sqlite_consistent = _copy_then_advance_attachment  # type: ignore[assignment]
        try:
            with self.assertRaises(RestoreError) as ctx:
                create_manual_copy(source, copy_dir)
        finally:
            restore_mod._copy_sqlite_consistent = original  # type: ignore[assignment]
        self.assertEqual(ctx.exception.code, "source_not_quiescent")
        self.assertIn("generation changed", str(ctx.exception).lower())
        self.assertFalse((copy_dir / "manifest.json").is_file())

    def test_quiescent_generation_copy_stays_consistent(self) -> None:
        """P2 positive control: stable GENERATION-1 DB+attachment copy verifies."""
        source, _ids = _seed_contour(self.base)
        conn = sqlite3.connect(str(source.vikunja_db))
        try:
            conn.execute("UPDATE synth SET note = ?", ("GENERATION-1",))
            conn.commit()
        finally:
            conn.close()
        _write(source.attachments / "synth-note.txt", b"GENERATION-1\n")
        copy_dir = self.base / "copy-generation-stable"
        manifest = create_manual_copy(source, copy_dir)
        self.assertTrue(manifest["source_quiescence"]["vikunja_db"]["ok"])
        self.assertTrue(manifest["source_quiescence"]["vikunja_db"].get("generation_stable"))
        self.assertIn(
            "owned_temp",
            str(manifest["source_quiescence"]["vikunja_db"].get("method") or ""),
        )
        verified = verify_consistent_copy(copy_dir)
        self.assertTrue(verified["ok"])
        copied_db = copy_dir / "vikunja" / "stand.db"
        check = sqlite3.connect(f"file:{copied_db}?mode=ro", uri=True)
        try:
            note = check.execute("SELECT note FROM synth").fetchone()[0]
        finally:
            check.close()
        self.assertEqual(note, "GENERATION-1")
        self.assertEqual(
            (copy_dir / "attachments" / "synth-note.txt").read_bytes(),
            b"GENERATION-1\n",
        )

    def test_copy_root_0755_tightened_and_db_pre_chmod_is_0600(self) -> None:
        """P2: existing 0755 dest becomes 0700; DB files are 0600 before final chmod."""
        source, _ids = _seed_contour(self.base)
        dest = self.base / "copy-modes"
        dest.mkdir(mode=0o755)
        os.chmod(dest, 0o755)
        seen: list[tuple[str, int]] = []

        def _watch(path: Path, observed_mode: int) -> None:
            if path.name in {"stand.db", "integration.sqlite3"}:
                seen.append((path.name, observed_mode))

        previous = restore_mod._test_pre_chmod_hook
        restore_mod._test_pre_chmod_hook = _watch
        try:
            create_manual_copy(source, dest)
        finally:
            restore_mod._test_pre_chmod_hook = previous
        self.assertEqual(dest.stat().st_mode & 0o777, 0o700)
        self.assertGreaterEqual(len(seen), 2)
        self.assertTrue(all(mode == 0o600 for _name, mode in seen))
        self.assertEqual((dest / "vikunja" / "stand.db").stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            (dest / "journal" / "integration.sqlite3").stat().st_mode & 0o777,
            0o600,
        )

    def test_cli_module_is_executable_helper(self) -> None:
        script = ROOT / "scripts" / "ea010_backup.py"
        self.assertTrue(script.is_file())
        text = script.read_text(encoding="utf-8")
        self.assertIn("No scheduler", text)
        self.assertNotIn("cron", text.lower())
        self.assertIn("never printed", text)

    def test_restore_nested_in_copy_attachments_refused_preserves_backup(self) -> None:
        """F2: restore target under copy attachments must not mutate the backup."""
        source, _ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-nested-target"
        create_manual_copy(source, copy_dir)
        before = restore_mod._tree_file_hashes(copy_dir)
        nested_target = copy_dir / "attachments" / "restored"
        with self.assertRaises(RestoreError) as ctx:
            restore_into_empty_target(copy_dir, nested_target)
        self.assertEqual(ctx.exception.code, "path_overlap")
        self.assertEqual(restore_mod._tree_file_hashes(copy_dir), before)
        self.assertTrue(verify_consistent_copy(copy_dir)["ok"])
        self.assertFalse(nested_target.exists())

    def test_copy_into_source_attachments_refused_preserves_source(self) -> None:
        """F2: copy dest under source attachments must not inject files first."""
        source, _ids = _seed_contour(self.base)
        before_source = restore_mod._tree_file_hashes(source.root)
        before_attachments = restore_mod._tree_file_hashes(source.attachments)
        nested_dest = source.attachments / "backup"
        with self.assertRaises(RestoreError) as ctx:
            create_manual_copy(source, nested_dest)
        self.assertEqual(ctx.exception.code, "path_overlap")
        self.assertEqual(restore_mod._tree_file_hashes(source.root), before_source)
        self.assertEqual(restore_mod._tree_file_hashes(source.attachments), before_attachments)
        self.assertFalse(nested_dest.exists())
        self.assertFalse((source.attachments / "backup" / "vikunja" / "stand.db").exists())

    def test_copy_dest_alias_under_source_refused_preserves_both(self) -> None:
        """F2: resolve() alias of source must still reject nested copy dest."""
        source, _ids = _seed_contour(self.base)
        alias_parent = self.base / "alias-parent"
        alias_parent.mkdir()
        alias = alias_parent / "source-link"
        alias.symlink_to(source.root)
        before_source = restore_mod._tree_file_hashes(source.root)
        # Dest resolves through the alias into source/files/backup.
        nested_via_alias = alias / "files" / "backup"
        with self.assertRaises(RestoreError) as ctx:
            create_manual_copy(source, nested_via_alias)
        self.assertEqual(ctx.exception.code, "path_overlap")
        self.assertEqual(restore_mod._tree_file_hashes(source.root), before_source)
        self.assertFalse(nested_via_alias.exists())
        self.assertFalse((source.attachments / "backup").exists())

    def test_concurrent_reconcile_holds_lock_across_inventory(self) -> None:
        """R1: peer reconcile cannot interleave after durable inventory load."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-lock-inventory"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-lock-inventory"
        restored = restore_into_empty_target(copy_dir, target)
        journal_path = Path(restored["journal_path"])
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nconfirmed",
            }
        }
        hold = threading.Event()
        inventory_held = threading.Event()
        peer_entered = threading.Event()
        peer_finished = threading.Event()
        peer_result: dict[str, object] = {}
        saw_peer_blocked = threading.Event()

        def _hold(point: str) -> None:
            if point != "after_load_durable_inventory":
                return
            inventory_held.set()
            # Keep the OS-lock while the peer tries to enter its own generation.
            if not hold.wait(timeout=5.0):
                raise RuntimeError("hold release timeout")

        def _peer() -> None:
            if not inventory_held.wait(timeout=5.0):
                peer_result["error"] = "inventory-hold-timeout"
                peer_finished.set()
                return
            peer_entered.set()
            try:
                peer_result["result"] = reconcile_restored_contour(
                    journal_path=journal_path,
                    links=restored["links"],
                    observed_tasks=observed,
                    observed_attachment_checksums=expected,
                    expected_attachment_checksums=expected,
                )
            except Exception as exc:  # noqa: BLE001
                peer_result["error"] = repr(exc)
            finally:
                peer_finished.set()

        def _release_after_peer_blocks() -> None:
            if not peer_entered.wait(timeout=5.0):
                hold.set()
                return
            # Peer has entered reconcile and should be blocked on the OS-lock.
            time.sleep(0.25)
            if not peer_finished.is_set():
                saw_peer_blocked.set()
            hold.set()

        restore_mod._test_crash_hook = _hold
        peer = threading.Thread(target=_peer, daemon=True)
        releaser = threading.Thread(target=_release_after_peer_blocks, daemon=True)
        peer.start()
        releaser.start()
        try:
            first = reconcile_restored_contour(
                journal_path=journal_path,
                links=restored["links"],
                observed_tasks=observed,
                observed_attachment_checksums=expected,
                expected_attachment_checksums=expected,
            )
        finally:
            restore_mod._test_crash_hook = None
            hold.set()
            peer.join(timeout=10.0)
            releaser.join(timeout=5.0)

        self.assertTrue(inventory_held.is_set())
        self.assertTrue(saw_peer_blocked.is_set())
        self.assertTrue(first["ok"])
        self.assertTrue(first["writes_enabled"])
        self.assertTrue(peer_finished.wait(timeout=5.0))
        self.assertNotIn("error", peer_result)
        second = peer_result.get("result")
        self.assertIsInstance(second, dict)
        assert isinstance(second, dict)
        self.assertTrue(second["ok"])
        self.assertTrue(second["writes_enabled"])

    def test_stale_reconcile_generation_cannot_publish_after_newer_failure(self) -> None:
        """R1: publish_writes refuses when durable generation advanced mid-flight."""
        source, ids = _seed_contour(self.base)
        copy_dir = self.base / "copy-stale-gen"
        create_manual_copy(source, copy_dir)
        target = self.base / "target-stale-gen"
        restored = restore_into_empty_target(copy_dir, target)
        journal_path = Path(restored["journal_path"])
        expected = attachment_checksums(target / "files")
        observed = {
            42: {
                "id": 42,
                "project_id": 2,
                "description": f"{ids['marker']}\nconfirmed",
            }
        }
        bumped: dict[str, str] = {}

        def _bump(point: str) -> None:
            if point != "before_publish_writes":
                return
            # Simulate a newer failure fence bumping generation under a separate
            # connection while this section still holds its claimed generation.
            conn = sqlite3.connect(str(journal_path))
            try:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key='restore_reconcile_generation'"
                ).fetchone()
                current = int(row[0]) if row and row[0] is not None else 0
                nxt = str(current + 1)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("restore_reconcile_generation", nxt),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("restore_drift", "1"),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("writes_enabled", "0"),
                )
                conn.commit()
                bumped["generation"] = nxt
            finally:
                conn.close()

        restore_mod._test_crash_hook = _bump
        try:
            with self.assertRaises(RestoreError) as ctx:
                reconcile_restored_contour(
                    journal_path=journal_path,
                    links=restored["links"],
                    observed_tasks=observed,
                    observed_attachment_checksums=expected,
                    expected_attachment_checksums=expected,
                )
        finally:
            restore_mod._test_crash_hook = None
        self.assertEqual(ctx.exception.code, "reconcile_failed")
        self.assertFalse(ctx.exception.details.get("writes_enabled"))
        self.assertTrue(ctx.exception.details.get("restore_drift"))
        self.assertIn("stale restore reconcile generation", str(ctx.exception))
        flags = restore_mod._durable_write_flags(journal_path)
        self.assertFalse(flags["writes_enabled"])
        self.assertTrue(flags["restore_drift"])
        self.assertTrue(bumped.get("generation"))


class RestoreOwnedResourceRecoveryTests(unittest.TestCase):
    """R2: durable owned restore child/root recovery after supervisor SIGKILL."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = load_profile()
        cls.worktree = ROOT

    def tearDown(self) -> None:
        try:
            cleanup_orphaned_runs(self.profile)
        except Exception:
            pass

    def test_sigkill_after_owned_restore_ready_reclaimed_from_claim(self) -> None:
        ready = Path(tempfile.mkdtemp(prefix="EA010-SYNTHETIC-restore-sigkill-")) / "state.json"
        script = f"""
import json, os, signal, shutil, sys, time
from pathlib import Path
sys.path.insert(0, {str(ROOT)!r})
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
from tests.support.stand import IsolatedStand, load_profile

profile = load_profile()
stand = IsolatedStand(profile=profile, worktree=Path({str(ROOT)!r}))
stand.prepare()
stand.start()
stand.wait_ready()
stand.stop_process()
stand._child = None
owned = stand.claim_owned_root(prefix='EA010-SYNTHETIC-owned-target-')
# Materialize a second contour from the prepared stand files.
for name in ('config.json', 'stand.db', 'files'):
    src = stand.run_dir / name
    dst = owned / name
    if src.is_dir():
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)
config = json.loads((owned / 'config.json').read_text(encoding='utf-8'))
config['service']['rootpath'] = str(owned)
config['database']['path'] = str(owned / 'stand.db')
config['files']['basepath'] = str(owned / 'files')
(owned / 'config.json').write_text(json.dumps(config, indent=2, sort_keys=True) + '\\n', encoding='utf-8')
os.chmod(owned / 'config.json', 0o600)
child = stand.start_owned_vikunja(target=owned, role='restore-older')
Path({str(ready)!r}).write_text(json.dumps({{
    'run_id': stand.run_id,
    'child_pid': child.pid,
    'owned_root': str(owned),
    'run_dir': str(stand.run_dir),
}}), encoding='utf-8')
os.kill(os.getpid(), signal.SIGKILL)
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
            deadline = time.monotonic() + 60
            state = None
            while time.monotonic() < deadline:
                if ready.exists() and ready.stat().st_size > 0:
                    state = json.loads(ready.read_text(encoding="utf-8"))
                    break
                if proc.poll() is not None and ready.exists():
                    state = json.loads(ready.read_text(encoding="utf-8"))
                    break
                time.sleep(0.1)
            self.assertIsNotNone(state)
            assert state is not None
            proc.wait(timeout=15)
            self.assertEqual(proc.returncode, -signal.SIGKILL)
            child_pid = int(state["child_pid"])
            run_id = state["run_id"]
            owned_root = Path(state["owned_root"])
            run_dir = Path(state["run_dir"])
            # Child must still be alive with a durable identity before recovery.
            identity = process_start_identity(child_pid)
            self.assertEqual(int(identity["pid"]), child_pid)
            self.assertIn(child_pid, listeners(self.profile.vikunja_port))
            # Recovery uses only durable claim/manifest — no private supervisor state.
            cleaned = cleanup_orphaned_runs(self.profile)
            self.assertTrue(cleaned)
            self.assertFalse(run_dir.exists())
            self.assertFalse(owned_root.exists())
            self.assertEqual(listeners(self.profile.vikunja_port), set())
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
            except Exception:
                pass
            shutil.rmtree(ready.parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
