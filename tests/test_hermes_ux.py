"""Unit tests for Hermes UX helpers and patch integrity (s07 / C5 0.2+0.3)."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

ROOT = Path(__file__).resolve().parents[1]
FROM_JS = "bb0144e8bcb343880a334b342f74cdcbdf08a5d8996e96d5442b503a77b822e8"
BASELINE_JS = Path(
    "/Users/efimovivan/Projects/cto-agent-environment/plugins/personal-tasks/dashboard/dist/index.js"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class HermesUxManifestTests(unittest.TestCase):
    def test_manifest_and_patches_present(self) -> None:
        manifest_path = ROOT / "integration" / "hermes" / "ui-manifest.json"
        self.assertTrue(manifest_path.is_file())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema"], "ea010-hermes-ui-manifest/v1")
        self.assertFalse(manifest["production_apply"])
        ui_patch = ROOT / manifest["patches"]["ui"]["path"]
        ready_patch = ROOT / manifest["patches"]["ui_readiness"]["path"]
        self.assertTrue(ui_patch.is_file())
        self.assertTrue(ready_patch.is_file())
        self.assertEqual(_sha256(ui_patch), manifest["patches"]["ui"]["sha256"])
        self.assertEqual(_sha256(ready_patch), manifest["patches"]["ui_readiness"]["sha256"])
        self.assertEqual(manifest["c5_scope"]["sha256"], "6a975adb4c842b6c6cbb40a52b157eba577b038c5594503deab463e88a580eaf")
        self.assertEqual(
            manifest["c5_scope"]["addendum_0_3"]["sha256"],
            "c9662c397c88aba6608800bcd44ff4c1de12a4d4b3596b41bd8cee4a814006f4",
        )
        self.assertEqual(manifest["browser"]["playwright"], "1.62.1")
        self.assertEqual(manifest["browser"]["chromium_revision"], "1243")
        self.assertFalse(manifest["browser"]["network_install"])
        readiness_text = (ROOT / "integration/hermes/ui-readiness.patch").read_text(encoding="utf-8")
        self.assertIn("tracker/recover", readiness_text)
        self.assertIn("VikunjaClient", readiness_text)
        self.assertIn("_normalize_api_origin", readiness_text)
        self.assertIn("token_ref.load", readiness_text)
        self.assertIn("_DeadlineSocket", readiness_text)
        self.assertIn("http.client", readiness_text)
        self.assertIn("time.monotonic()", readiness_text)
        self.assertIn("read1(65536)", readiness_text)
        self.assertIn("readiness budget exhausted", readiness_text)
        bridge_text = (ROOT / "vikunja_local/hermes_bridge.py").read_text(encoding="utf-8")
        self.assertIn("allow_first_http", bridge_text)
        self.assertIn("last_check_outcome", bridge_text)
        self.assertIn("no_source_mapping", bridge_text)
        self.assertIn("stamp_verified_outbox_details", bridge_text)
        self.assertIn("stamp_failed_check_outbox_details", bridge_text)

    def test_baseline_js_hash(self) -> None:
        self.assertEqual(_sha256(BASELINE_JS), FROM_JS)

    def test_ui_patch_apply_and_reverse(self) -> None:
        manifest = json.loads((ROOT / "integration/hermes/ui-manifest.json").read_text(encoding="utf-8"))
        ui_patch = ROOT / "integration/hermes/ui.patch"
        with tempfile.TemporaryDirectory(prefix="ea010-ux-ut-") as tmp:
            dest = Path(tmp)
            target = dest / "plugins/personal-tasks/dashboard/dist"
            target.mkdir(parents=True)
            js = target / "index.js"
            js.write_bytes(BASELINE_JS.read_bytes())
            check = subprocess.run(
                ["git", "apply", "--check", "--unidiff-zero", str(ui_patch)],
                cwd=dest,
                capture_output=True,
                text=True,
            )
            self.assertEqual(check.returncode, 0, check.stderr)
            apply = subprocess.run(
                ["git", "apply", "--unidiff-zero", str(ui_patch)],
                cwd=dest,
                capture_output=True,
                text=True,
            )
            self.assertEqual(apply.returncode, 0, apply.stderr)
            self.assertEqual(_sha256(js), manifest["patches"]["ui"]["to_sha256"])
            reverse = subprocess.run(
                ["git", "apply", "--reverse", "--unidiff-zero", str(ui_patch)],
                cwd=dest,
                capture_output=True,
                text=True,
            )
            self.assertEqual(reverse.returncode, 0, reverse.stderr)
            self.assertEqual(_sha256(js), FROM_JS)

    def test_decision_payload_and_delivery_helpers(self) -> None:
        manifest = json.loads((ROOT / "integration/hermes/ui-manifest.json").read_text(encoding="utf-8"))
        ui_patch = ROOT / "integration/hermes/ui.patch"
        with tempfile.TemporaryDirectory(prefix="ea010-ux-helpers-") as tmp:
            dest = Path(tmp)
            target = dest / "plugins/personal-tasks/dashboard/dist"
            target.mkdir(parents=True)
            js = target / "index.js"
            js.write_bytes(BASELINE_JS.read_bytes())
            apply = subprocess.run(
                ["git", "apply", "--unidiff-zero", str(ui_patch)],
                cwd=dest,
                capture_output=True,
                text=True,
            )
            self.assertEqual(apply.returncode, 0, apply.stderr)
            self.assertEqual(_sha256(js), manifest["patches"]["ui"]["to_sha256"])
            script = f"""
const ui = require({json.dumps(str(js))});
const proposal = {{id:'p', meeting_id:'m', version:{json.dumps('b'*64)}, revision:2, text:'task'}};
let threw = false;
try {{ ui.decisionPayload(proposal, 'accept', '', {{title:'x', due:null, due_confirmed:true}}, {{ready:false}}); }} catch (_) {{ threw = true; }}
if (!threw) throw new Error('ready false must throw');
const ok = ui.decisionPayload(proposal, 'accept', '', {{title:' x ', due:null, due_confirmed:true}}, {{ready:true}});
if (ok.tracker_target !== 'vikunja' || ok.source_version !== proposal.version || ok.due_confirmed !== true) throw new Error(JSON.stringify(ok));
const pending = ui.deliveryPresentation({{tracker:{{state:'pending'}}}});
if (pending.kind !== 'accepted_pending') throw new Error('pending');
const lost = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:9, url:null}}}});
if (lost.kind !== 'lost_backlink') throw new Error('lost');
const denied = ui.deliveryPresentation({{tracker:{{state:'linked', availability:{{status:'denied', outcome:'auth_denied'}}}}}});
if (denied.kind !== 'denied') throw new Error('denied');
const deniedDone = ui.deliveryPresentation({{tracker:{{state:'linked', availability:{{status:'denied', outcome:'auth_denied'}}, done_observation:{{done:true, checked_at:'t'}}}}}});
if (deniedDone.kind !== 'denied' || deniedDone.action !== 'check_connection' || !deniedDone.label.includes('не актуальный статус')) throw new Error('denied+done');
const conflict = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:3, url:'http://127.0.0.1/tasks/3', availability:{{status:'conflict', outcome:'conflict', reason:'incompatible_existing_link'}}}}}});
if (conflict.kind !== 'conflict' || conflict.action !== 'reconcile') throw new Error('conflict '+JSON.stringify(conflict));
const deleted = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:3, url:'http://127.0.0.1/tasks/3', availability:{{status:'unavailable', outcome:'unknown', reason:'target_deleted_or_gone'}}}}}});
if (deleted.kind !== 'deleted' || deleted.action !== null) throw new Error('deleted '+JSON.stringify(deleted));
const nomap = ui.deliveryPresentation({{tracker:{{state:'linked', task_id:3, url:'http://127.0.0.1/tasks/3', availability:{{status:'unavailable', outcome:'unknown', reason:'no_source_mapping'}}}}}});
if (nomap.kind !== 'unknown' || nomap.action !== 'reconcile') throw new Error('nomap '+JSON.stringify(nomap));
const src = ui.proposalSourceUrl({{id:'prop-1'}}, {{origin:'http://127.0.0.1:9'}});
if (src !== 'http://127.0.0.1:9/#pt-proposal-prop-1') throw new Error('source '+src);
console.log('ok');
"""
            proc = subprocess.run(["node", "-e", script], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertIn("ok", proc.stdout)

    def test_suite_registered(self) -> None:
        from tests.support.suites import list_suites

        self.assertIn("hermes-ux", list_suites())

    def test_availability_stamps_clear_and_deny(self) -> None:
        from vikunja_local.api import Outcome
        from vikunja_local.hermes_bridge import (
            stamp_failed_check_outbox_details,
            stamp_verified_outbox_details,
            tracker_availability_projection,
        )

        stale = {
            "reason": "prepared_without_dispatch",
            "last_check_outcome": "unknown",
            "cannot_first_deliver": True,
            "ui_reconcile": True,
            "forbid_create": True,
        }
        verified = stamp_verified_outbox_details(stale, path="deliver_link", checked_at="t0")
        self.assertEqual(verified["last_check_outcome"], "confirmed")
        self.assertNotIn("reason", verified)
        self.assertNotIn("cannot_first_deliver", verified)
        self.assertIsNone(tracker_availability_projection(details=verified))

        failed = stamp_failed_check_outbox_details(
            {},
            outcome=Outcome.AUTH_DENIED,
            path="recover_unresolved",
            recovered_details={"reason": "token"},
            checked_at="t1",
        )
        self.assertEqual(failed["last_check_outcome"], "auth_denied")
        self.assertEqual(failed["last_explicit_check"], "t1")
        self.assertEqual(failed["reason"], "token")
        proj = tracker_availability_projection(details=failed)
        assert proj is not None
        self.assertEqual(proj["status"], "denied")
        self.assertEqual(proj["outcome"], "auth_denied")


if __name__ == "__main__":
    unittest.main()
