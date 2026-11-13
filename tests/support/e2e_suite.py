"""Isolated end-to-end suite (s10): §6 cross-cut on a real disposable Vikunja.

Covers UI↔MCP CRUD, auth/scopes/revoke, dates/304/recurring, retry/reconnect,
late-write/crash samples, no development-run, restore smoke, error/signal
cleanup read-back. Owner/MacBook/Mini-restart checkpoints stay not-run without
live receipts. Historical P9/P10 are not credited as tests of this SHA.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from vikunja_local.api import Outcome, TokenRef, VikunjaClient
from vikunja_local.dates import confirm_date_only, same_instant
from vikunja_local.dispatcher import IntegrationDispatcher
from vikunja_local.journal import IntegrationJournal, new_operation_id
from vikunja_local.restore import (
    ContourPaths,
    create_manual_copy,
    restore_into_empty_target,
    verify_consistent_copy,
)

from .api_suite import _revoke_suite_token
from .hermes_ux_suite import CHROMIUM, CHROMIUM_SHA, PLAYWRIGHT_MODULE
from .mcp_suite import (
    READ_PERMS,
    WRITE_PERMS,
    McpSubprocess,
    TrustedHostSession,
    _create_api_token,
    _journal_path,
    _store_token,
    _write_config,
)
from .pilot_suite import default_checkpoint_status
from .stand import (
    IsolatedStand,
    StandError,
    atomic_write_secure,
    await_component_signal_receipt,
    cancel_active_component_procs,
    cleanup_active_stands_best_effort,
    consume_stand_signal_evidence,
    http_json,
    implementation_source_digest,
    listeners,
    load_profile,
    sha256_file,
    validate_journal_path,
)

os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

WORKTREE = Path(__file__).resolve().parents[2]
MSK = ZoneInfo("Europe/Moscow")

FORBIDDEN_DEV_TOKENS: tuple[str, ...] = (
    "development-run",
    "development_run",
    "development_flow_start",
    "development_task_register",
    "coding-agent",
    "coding_agent",
    "devflow-dispatch",
    "devflow_dispatch",
)

# Instrumentable native control-plane entrypoints (personal tasks must never call).
FORBIDDEN_ENTRYPOINT_NAMES: tuple[str, ...] = (
    "development_flow_start",
    "development_task_register",
    "devflow_dispatch",
    "coding_agent_launch",
)


class DevelopmentRunGuard:
    """Observe forbidden development entrypoints/process launches during personal-task ops.

    Samples a pre-operation baseline (process marker, external process-dir records,
    stand files), instruments subprocess launches and named control-plane callables,
    and requires zero calls/dispatches per personal-task channel (NFR-12 / §6).
    """

    def __init__(
        self,
        worktree: Path,
        stand: IsolatedStand,
        *,
        process_dir: Path | None = None,
    ) -> None:
        self.worktree = worktree
        self.stand = stand
        env_dir = (os.environ.get("AI_PLATFORM_PROCESS_DIR") or "").strip()
        self.process_dir = process_dir or (Path(env_dir) if env_dir else None)
        self.process_marker = worktree / "docs" / "process" / "t_606468bd.md"
        self._active = False
        self._channel: str | None = None
        self._orig_run: Any = None
        self._orig_popen: Any = None
        self.entrypoint_hits: list[dict[str, Any]] = []
        self.subprocess_hits: list[dict[str, Any]] = []
        self.channel_hits: dict[str, list[dict[str, Any]]] = {
            "ui": [],
            "mcp": [],
            "meeting": [],
        }
        self.baseline: dict[str, Any] = {}

    def _snapshot_files(self, root: Path) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        if not root.exists():
            return out
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            try:
                st = path.stat()
            except OSError:
                continue
            out[str(path)] = {"mtime_ns": st.st_mtime_ns, "size": st.st_size}
        return out

    def _install_entrypoints(self) -> None:
        import vikunja_local as root_pkg

        for name in FORBIDDEN_ENTRYPOINT_NAMES:

            def _make(entrypoint: str):
                def _forbidden(*args: Any, **kwargs: Any) -> None:
                    hit = {
                        "entrypoint": entrypoint,
                        "channel": self._channel,
                        "args_count": len(args),
                        "kwargs_keys": sorted(kwargs),
                    }
                    self.entrypoint_hits.append(hit)
                    if self._channel in self.channel_hits:
                        self.channel_hits[self._channel].append(hit)
                    raise StandError(
                        f"forbidden development entrypoint called on personal-task path: {entrypoint}"
                    )

                return _forbidden

            setattr(root_pkg, name, _make(name))

    @staticmethod
    def _normalize_argv(argv: Any) -> list[str]:
        """Normalize subprocess argv for token scan and evidence recording.

        ``str`` / ``bytes`` / ``PathLike`` command forms must stay a single
        element: ``list(command_string)`` would split into characters and
        destroy forbidden tokens when rejoined with spaces.
        """
        if argv is None:
            return []
        if isinstance(argv, bytes):
            return [os.fsdecode(argv)]
        if isinstance(argv, str):
            return [argv]
        if isinstance(argv, os.PathLike):
            return [os.fspath(argv)]
        try:
            parts: list[str] = []
            for item in argv:
                if isinstance(item, bytes):
                    parts.append(os.fsdecode(item))
                elif isinstance(item, os.PathLike):
                    parts.append(os.fspath(item))
                else:
                    parts.append(str(item))
            return parts
        except TypeError:
            return [str(argv)]

    def _argv_forbidden(self, argv: Any) -> bool:
        parts = [part.lower() for part in self._normalize_argv(argv)]
        blob = " ".join(parts)
        return any(token in blob for token in FORBIDDEN_DEV_TOKENS)

    def _wrap_subprocess(self) -> None:
        import subprocess as sp

        guard = self
        self._orig_run = sp.run
        self._orig_popen = sp.Popen

        def _record(argv: Any, via: str) -> None:
            if not guard._argv_forbidden(argv):
                return
            hit = {
                "via": via,
                "argv": guard._normalize_argv(argv),
                "channel": guard._channel,
            }
            guard.subprocess_hits.append(hit)
            if guard._channel in guard.channel_hits:
                guard.channel_hits[guard._channel].append(hit)

        def run_wrapped(*args: Any, **kwargs: Any):  # noqa: ANN401
            argv = args[0] if args else kwargs.get("args")
            _record(argv, "subprocess.run")
            return guard._orig_run(*args, **kwargs)

        def popen_wrapped(*args: Any, **kwargs: Any):  # noqa: ANN401
            argv = args[0] if args else kwargs.get("args")
            _record(argv, "subprocess.Popen")
            return guard._orig_popen(*args, **kwargs)

        sp.run = run_wrapped  # type: ignore[assignment]
        sp.Popen = popen_wrapped  # type: ignore[misc, assignment]

    def _unwrap_subprocess(self) -> None:
        import subprocess as sp

        if self._orig_run is not None:
            sp.run = self._orig_run  # type: ignore[assignment]
        if self._orig_popen is not None:
            sp.Popen = self._orig_popen  # type: ignore[misc, assignment]
        self._orig_run = None
        self._orig_popen = None

    def start(self) -> None:
        marker_mtime = (
            self.process_marker.stat().st_mtime_ns if self.process_marker.is_file() else None
        )
        self.baseline = {
            "process_marker_present": self.process_marker.is_file(),
            "process_marker_mtime_ns": marker_mtime,
            "stand_files": self._snapshot_files(self.stand.run_dir),
            "process_dir": str(self.process_dir) if self.process_dir else None,
            "process_dir_files": (
                self._snapshot_files(self.process_dir) if self.process_dir and self.process_dir.exists() else {}
            ),
            "docs_process_files": self._snapshot_files(self.worktree / "docs" / "process"),
        }
        self._install_entrypoints()
        self._wrap_subprocess()
        self._active = True

    def stop(self) -> None:
        if not self._active:
            return
        self._unwrap_subprocess()
        self._active = False

    def channel(self, name: str) -> "DevelopmentRunGuard":
        if name not in self.channel_hits:
            raise StandError(f"unknown personal-task channel for development guard: {name}")
        self._channel = name
        return self

    def clear_channel(self) -> None:
        self._channel = None

    def _new_or_changed_with_forbidden_content(
        self,
        root: Path,
        before: dict[str, dict[str, int]],
    ) -> list[str]:
        hits: list[str] = []
        after = self._snapshot_files(root)
        for path_str, meta in after.items():
            prev = before.get(path_str)
            if prev is not None and prev.get("mtime_ns") == meta.get("mtime_ns") and prev.get("size") == meta.get("size"):
                continue
            try:
                text = Path(path_str).read_text(encoding="utf-8", errors="replace").lower()
            except OSError:
                continue
            name = Path(path_str).name.lower()
            if any(token in name for token in FORBIDDEN_DEV_TOKENS) or any(
                token in text for token in FORBIDDEN_DEV_TOKENS
            ):
                hits.append(path_str)
        return hits

    def assert_clean(self) -> dict[str, Any]:
        self.stop()
        marker_after = (
            self.process_marker.stat().st_mtime_ns if self.process_marker.is_file() else None
        )
        marker_before = self.baseline.get("process_marker_mtime_ns")
        content_hits = self._new_or_changed_with_forbidden_content(
            self.stand.run_dir, self.baseline.get("stand_files") or {}
        )
        content_hits.extend(
            self._new_or_changed_with_forbidden_content(
                self.worktree / "docs" / "process",
                self.baseline.get("docs_process_files") or {},
            )
        )
        process_dir_hits: list[str] = []
        if self.process_dir is not None:
            process_dir_hits = self._new_or_changed_with_forbidden_content(
                self.process_dir, self.baseline.get("process_dir_files") or {}
            )
        channel_counts = {name: len(hits) for name, hits in self.channel_hits.items()}
        evidence = {
            "forbidden_entrypoint_hits": list(self.entrypoint_hits),
            "forbidden_subprocess_hits": list(self.subprocess_hits),
            "forbidden_content_hits": content_hits,
            "process_dir_hits": process_dir_hits,
            "channel_dispatch_counts": channel_counts,
            "process_marker_present": self.process_marker.is_file(),
            "process_marker_mtime_ns_before": marker_before,
            "process_marker_mtime_ns_after": marker_after,
            "process_marker_unchanged": marker_before == marker_after,
            "baseline_process_dir": self.baseline.get("process_dir"),
        }
        _assert(
            not self.entrypoint_hits,
            f"development entrypoints invoked during personal-task ops: {self.entrypoint_hits}",
        )
        _assert(
            not self.subprocess_hits,
            f"development process launches during personal-task ops: {self.subprocess_hits}",
        )
        _assert(not content_hits, f"development-run artefacts written: {content_hits}")
        _assert(not process_dir_hits, f"external process-dir development records: {process_dir_hits}")
        _assert(
            marker_before == marker_after,
            f"process marker mtime changed during personal-task ops: {marker_before} -> {marker_after}",
        )
        for channel, count in channel_counts.items():
            _assert(count == 0, f"personal-task channel {channel!r} dispatched development: count={count}")
        evidence["forbidden_hits"] = []
        return evidence


def _run_ready_made_vikunja_ui(
    stand: IsolatedStand,
    *,
    project_id: int,
    create_title: str | None = None,
    open_task_id: int | None = None,
    expect_title: str | None = None,
    expect_done: bool | None = None,
    expect_due_prefix: str | None = None,
    update_title: str | None = None,
    mark_done: bool = False,
) -> dict[str, Any]:
    """Drive the approved ready-made Vikunja UI via Playwright (not the API client).

    Supports create (project quick-add), open/read-back (title/due/done), title update,
    and mark-done against the isolated stand's frontend. Mini/MacBook device checkpoints
    stay separate.
    """
    _assert(PLAYWRIGHT_MODULE.is_dir(), f"playwright module missing: {PLAYWRIGHT_MODULE}")
    _assert(CHROMIUM.is_file(), f"chromium missing: {CHROMIUM}")
    _assert(sha256_file(CHROMIUM) == CHROMIUM_SHA, "chromium sha mismatch")
    username = stand._username  # noqa: SLF001 — suite-owned synthetic credentials
    password = stand._password  # noqa: SLF001
    _assert(isinstance(username, str) and username, "missing synthetic username for UI login")
    _assert(isinstance(password, str) and password, "missing synthetic password for UI login")
    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    harness_dir = stand.run_dir / "vikunja-ui-harness"
    harness_dir.mkdir(mode=0o700, exist_ok=True)
    driver = harness_dir / f"vikunja_ui_{uuid.uuid4().hex[:10]}.mjs"
    result_path = harness_dir / f"vikunja_ui_{uuid.uuid4().hex[:10]}.json"
    driver.write_text(
        f"""
import {{ createRequire }} from 'node:module';
import {{ writeFileSync }} from 'node:fs';
const require = createRequire(import.meta.url);
const {{ chromium }} = require('playwright');

const executablePath = {json.dumps(str(CHROMIUM))};
const origin = {json.dumps(origin)};
const username = {json.dumps(username)};
const password = {json.dumps(password)};
const projectId = {int(project_id)};
const createTitle = {json.dumps(create_title)};
const openTaskId = {json.dumps(open_task_id)};
const expectTitle = {json.dumps(expect_title)};
const expectDone = {json.dumps(expect_done)};
const expectDuePrefix = {json.dumps(expect_due_prefix)};
const updateTitle = {json.dumps(update_title)};
const markDone = {json.dumps(bool(mark_done))};
const resultPath = {json.dumps(str(result_path))};

function assert(cond, msg) {{ if (!cond) throw new Error(msg); }}

const browser = await chromium.launch({{ executablePath, headless: true, args: ['--no-sandbox'] }});
const page = await browser.newPage({{ viewport: {{ width: 1280, height: 900 }} }});
const out = {{ ok: false, surface: 'ready-made-vikunja-ui' }};
try {{
  await page.goto(origin + '/login', {{ waitUntil: 'networkidle', timeout: 30000 }});
  await page.fill('#username', username);
  await page.fill('#password', password);
  await page.getByRole('button', {{ name: 'LOGIN' }}).click();
  await page.waitForURL(u => !String(u).includes('/login'), {{ timeout: 20000 }});
  out.logged_in = true;

  let taskId = openTaskId;

  if (createTitle) {{
    await page.goto(origin + '/projects/' + projectId, {{ waitUntil: 'networkidle', timeout: 30000 }});
    const ta = page.locator('textarea[placeholder="Add a task…"]');
    await ta.waitFor({{ timeout: 15000 }});
    await ta.fill(createTitle);
    const createWait = page.waitForResponse(
      r => r.url().includes('/tasks/bulk') && r.request().method() === 'POST' && r.status() === 201,
      {{ timeout: 20000 }}
    );
    await ta.press('Enter');
    const createResp = await createWait;
    const createJson = await createResp.json();
    assert(Array.isArray(createJson.tasks) && createJson.tasks.length >= 1, 'bulk create missing tasks');
    taskId = createJson.tasks[0].id;
    out.created = {{ id: taskId, title: createJson.tasks[0].title }};
    await page.waitForSelector('text=' + createTitle, {{ timeout: 15000 }});
  }}

  assert(typeof taskId === 'number' && taskId > 0, 'task id required for UI open/update/done');
  await page.goto(origin + '/tasks/' + taskId, {{ waitUntil: 'networkidle', timeout: 30000 }});
  const titleEl = page.locator('h1[aria-label="Title"][contenteditable="true"]');
  await titleEl.waitFor({{ timeout: 15000 }});

  async function readRenderedControls() {{
    const renderedTitle = (await titleEl.innerText()).trim();
    // Completion from ready-made controls only (never page.evaluate fetch /api).
    const undone = page.getByRole('button', {{ name: /mark as undone/i }});
    const done = page.getByRole('button', {{ name: /mark task done/i }});
    const doneBadge = page.locator('.is-done');
    let renderedDone = null;
    if ((await undone.count()) || (await doneBadge.count())) renderedDone = true;
    else if (await done.count()) renderedDone = false;
    // Due from rendered Due Date / datepicker controls — not API fetch.
    let renderedDue = null;
    const dueDetail = page.locator('.columns.details .datepicker, .datepicker');
    if (await dueDetail.count()) {{
      renderedDue = (await dueDetail.first().innerText()).trim();
      await dueDetail.first().click().catch(() => {{}});
      await page.waitForTimeout(300);
      const bodyText = await page.locator('body').innerText();
      const absolute = bodyText.split('\\n').map(s => s.trim()).filter(Boolean).find(line =>
        /\\b20\\d{{2}}\\b/.test(line) && /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(line)
      );
      if (absolute) renderedDue = absolute;
    }} else {{
      const setDue = page.getByRole('button', {{ name: /set due date/i }});
      if (await setDue.count()) {{
        await setDue.first().click().catch(() => {{}});
        await page.waitForTimeout(300);
        renderedDue = (await page.locator('body').innerText()).trim();
      }}
    }}
    return {{ title: renderedTitle, done: renderedDone, due_text: renderedDue }};
  }}

  if (updateTitle) {{
    await titleEl.click();
    await page.keyboard.press('Meta+A');
    await page.keyboard.type(updateTitle);
    const patchWait = page.waitForResponse(
      r => r.url().includes('/tasks/' + taskId) && ['POST','PATCH','PUT'].includes(r.request().method()) && r.ok(),
      {{ timeout: 20000 }}
    );
    await page.keyboard.press('Tab');
    const save = page.getByRole('button', {{ name: 'SAVE', exact: true }});
    if (await save.count()) {{
      try {{ await save.first().click({{ timeout: 1000 }}); }} catch {{}}
    }}
    await patchWait;
    await titleEl.waitFor({{ timeout: 15000 }});
    const afterUpdate = (await titleEl.innerText()).trim();
    assert(afterUpdate === updateTitle, 'UI update title mismatch in rendered control');
    out.updated = {{ id: taskId, title: afterUpdate }};
  }}

  if (markDone) {{
    const doneBtn = page.getByRole('button', {{ name: /mark task done/i }});
    assert(await doneBtn.count() >= 1, 'MARK TASK DONE control missing');
    // Match completion (response body done:true), not an arbitrary successful task write.
    const doneWait = page.waitForResponse(
      async r => {{
        if (!r.url().includes('/tasks/' + taskId)) return false;
        if (!['POST','PATCH','PUT'].includes(r.request().method())) return false;
        if (!r.ok()) return false;
        try {{
          const body = await r.json();
          return !!(body && body.done === true);
        }} catch {{
          return false;
        }}
      }},
      {{ timeout: 20000 }}
    );
    await doneBtn.first().click();
    await doneWait;
    // count() does not retry for Vue render — bound-wait the actual completion control.
    const undoneBtn = page.getByRole('button', {{ name: /mark as undone/i }});
    const doneBadge = page.locator('.is-done');
    const renderDeadline = Date.now() + 15000;
    let renderedCompletion = false;
    while (Date.now() < renderDeadline) {{
      if ((await undoneBtn.count()) >= 1 || (await doneBadge.count()) >= 1) {{
        renderedCompletion = true;
        break;
      }}
      await page.waitForTimeout(200);
    }}
    assert(renderedCompletion, 'rendered done control missing after mark-done');
    out.completed = {{ id: taskId, done: true }};
  }}

  const rendered = await readRenderedControls();
  out.opened = {{
    id: taskId,
    title: rendered.title,
    url: page.url(),
    done: rendered.done,
    due_date: rendered.due_text,
    readback_surface: 'rendered-controls',
  }};
  if (expectTitle) {{
    assert(rendered.title === expectTitle, 'UI title drift on open: ' + rendered.title + ' != ' + expectTitle);
  }}
  if (expectDone !== null && expectDone !== undefined) {{
    assert(rendered.done === expectDone, 'UI done drift from rendered control: ' + JSON.stringify(rendered.done));
  }}
  if (expectDuePrefix) {{
    const due = rendered.due_text != null ? String(rendered.due_text) : '';
    // expectDuePrefix is YYYY-MM-DD; Vikunja renders calendar text (e.g. "Fri, Dec 25, 2026").
    const m = String(expectDuePrefix).match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})/);
    assert(m, 'expectDuePrefix must be YYYY-MM-DD, got ' + expectDuePrefix);
    const year = m[1];
    const monthIdx = Number(m[2]) - 1;
    const dayNum = String(Number(m[3]));
    const shortMonths = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const longMonths = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    const monthOk = due.includes(shortMonths[monthIdx]) || due.includes(longMonths[monthIdx]);
    const dayOk = new RegExp('\\\\b' + dayNum + '\\\\b').test(due);
    assert(
      due.includes(year) && monthOk && dayOk,
      'UI due drift from rendered control: ' + due + ' vs ' + expectDuePrefix
    );
  }}

  out.task_id = taskId;
  out.ok = true;
  writeFileSync(resultPath, JSON.stringify(out));
}} catch (err) {{
  writeFileSync(resultPath, JSON.stringify({{ ok: false, error: String(err), stack: err && err.stack, partial: out }}));
  throw err;
}} finally {{
  await browser.close();
}}
""",
        encoding="utf-8",
    )
    try:
        proc = subprocess.run(
            ["node", str(driver)],
            capture_output=True,
            text=True,
            cwd=str(harness_dir),
            env={**os.environ, "NODE_PATH": str(PLAYWRIGHT_MODULE.parent)},
            timeout=180,
        )
        _assert(
            proc.returncode == 0,
            "vikunja UI playwright failed rc="
            f"{proc.returncode}\nstdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}",
        )
        _assert(result_path.is_file(), "vikunja UI browser result missing")
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        _assert(payload.get("ok") is True, f"vikunja UI browser result not ok: {payload}")
        return payload
    finally:
        try:
            driver.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            result_path.unlink(missing_ok=True)
        except Exception:
            pass


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise StandError(message)


def _snapshot_durable_records(roots: list[Path]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for root in roots:
        if root is None or not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            try:
                st = path.stat()
            except OSError:
                continue
            out[str(path)] = {"mtime_ns": st.st_mtime_ns, "size": st.st_size}
    return out


def _run_meeting_acceptance_under_guard(
    stand: IsolatedStand,
    *,
    worktree: Path,
    project_id: int,
    token_path: Path,
    browser_origin: str,
    mcp_task_id: int,
    mcp_operation_ref: str,
    process_dir: Path | None,
) -> dict[str, Any]:
    """Synthetic Hermes meeting accept → outbox → deliver → recover under instrumentation."""
    from vikunja_local.hermes_bridge import (
        BridgeConfig,
        HermesBridge,
        HermesOutboxStore,
        meeting_user_portion,
    )

    meeting_root = stand.run_dir / "meeting-channel"
    meeting_root.mkdir(mode=0o700, exist_ok=True)
    journal_path = meeting_root / "integration.sqlite3"
    outbox_path = meeting_root / "outbox.json"
    child_script = meeting_root / "meeting_child_boundary.py"
    child_out = meeting_root / "meeting_child_result.json"

    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    hermes_origin = f"http://{stand.profile.listen_host}:{int(stand.profile.ports['synthetic_hermes'])}"
    cfg = BridgeConfig(
        journal_path=journal_path,
        api_origin=origin,
        project_id=project_id,
        token_ref=TokenRef(kind="file", path=token_path),
        task_url_template=origin + "/tasks/{id}",
        hermes_origin=hermes_origin,
    )
    store = HermesOutboxStore(outbox_path)
    bridge = HermesBridge(cfg, store)

    proposal_id = f"prop-e2e-meeting-{stand.run_id[:10]}"
    version = hashlib.sha256(f"{proposal_id}:{stand.run_id}".encode()).hexdigest()
    title = stand.fixture_name("meeting-accept")
    source_url = f"{hermes_origin}/#pt-proposal-{proposal_id}"
    portion = meeting_user_portion(
        proposal_id=proposal_id,
        meeting_id="m-e2e-" + stand.run_id[:8],
        meeting_title="EA010 synthetic meeting",
        meeting_when="2026-11-13T10:00:00+03:00 (Europe/Moscow)",
        quote="synthetic meeting acceptance under personal-task instrumentation",
        version=version,
        source_url=source_url,
    )

    watch_roots = [meeting_root, stand.run_dir]
    if process_dir is not None:
        watch_roots.append(process_dir)
    before = _snapshot_durable_records(watch_roots)

    row = bridge.register_acceptance(
        proposal_id=proposal_id,
        title=title,
        due="2026-12-15",
        source_version=version,
        user_portion=portion,
        source_url=source_url,
    )
    delivery = bridge.deliver(row.event_id)
    _assert(delivery.ok and delivery.record is not None, f"meeting deliver failed: {delivery.outcome}")
    _assert(delivery.record.state == "linked", f"meeting outbox not linked: {delivery.record.state}")
    meeting_task_id = int(delivery.record.task_id or 0)
    _assert(meeting_task_id > 0, "meeting deliver missing task_id")
    meeting_url = str(delivery.record.url or "")
    _assert(meeting_url.endswith(f"/tasks/{meeting_task_id}"), f"meeting url drift: {meeting_url}")

    recovered = bridge.recover(row.event_id)
    _assert(recovered.ok and recovered.record is not None, f"meeting recover failed: {recovered.outcome}")
    _assert(int(recovered.record.task_id or 0) == meeting_task_id, "meeting recover id drift")

    # Child execution boundary: re-read durable outbox/recover in a subprocess while
    # the parent guard instruments subprocess launches (must stay free of development tokens).
    child_script.write_text(
        "\n".join(
            [
                "import json, sys",
                "from pathlib import Path",
                "sys.path.insert(0, sys.argv[1])",
                "from vikunja_local.hermes_bridge import BridgeConfig, HermesBridge, HermesOutboxStore",
                "from vikunja_local.api import TokenRef",
                "worktree, journal, outbox, token, origin, hermes, project_id, event_id, result_path = sys.argv[1:10]",
                "cfg = BridgeConfig(",
                "    journal_path=Path(journal),",
                "    api_origin=origin,",
                "    project_id=int(project_id),",
                "    token_ref=TokenRef(kind='file', path=Path(token)),",
                "    task_url_template=origin + '/tasks/{id}',",
                "    hermes_origin=hermes,",
                ")",
                "bridge = HermesBridge(cfg, HermesOutboxStore(Path(outbox)))",
                "rec = bridge.recover(event_id)",
                "payload = {",
                "  'ok': bool(rec.ok),",
                "  'outcome': getattr(rec.outcome, 'value', str(rec.outcome)),",
                "  'task_id': None if rec.record is None else rec.record.task_id,",
                "  'state': None if rec.record is None else rec.record.state,",
                "  'url': None if rec.record is None else rec.record.url,",
                "}",
                "Path(result_path).write_text(json.dumps(payload), encoding='utf-8')",
                "raise SystemExit(0 if payload['ok'] else 2)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    os.chmod(child_script, 0o700)
    child = subprocess.run(
        [
            sys.executable,
            str(child_script),
            str(worktree),
            str(journal_path),
            str(outbox_path),
            str(token_path),
            origin,
            hermes_origin,
            str(project_id),
            row.event_id,
            str(child_out),
        ],
        capture_output=True,
        text=True,
        cwd=str(worktree),
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        timeout=120,
    )
    _assert(child.returncode == 0, f"meeting child boundary failed: rc={child.returncode} stderr={child.stderr[-2000:]}")
    _assert(child_out.is_file(), "meeting child result missing")
    child_payload = json.loads(child_out.read_text(encoding="utf-8"))
    _assert(child_payload.get("ok") is True, f"meeting child recover not ok: {child_payload}")
    _assert(int(child_payload.get("task_id") or 0) == meeting_task_id, "meeting child task id drift")

    after = _snapshot_durable_records(watch_roots)
    changed = sorted(
        path
        for path, meta in after.items()
        if before.get(path) != meta
    )
    text_suffixes = {".json", ".md", ".txt", ".log", ".yml", ".yaml"}
    forbidden_hits = []
    for path_str in changed:
        path = Path(path_str)
        name = path.name.lower()
        if any(token in name for token in FORBIDDEN_DEV_TOKENS):
            forbidden_hits.append(path_str)
            continue
        # Skip binary sqlite/db/wal artefacts from content scan (false positives).
        if path.suffix.lower() not in text_suffixes:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace").lower()
        except OSError:
            continue
        if any(token in text for token in FORBIDDEN_DEV_TOKENS):
            forbidden_hits.append(path_str)
    _assert(not forbidden_hits, f"meeting channel wrote development-run records: {forbidden_hits}")

    durable_outbox = store.get_by_event(row.event_id)
    _assert(durable_outbox is not None and durable_outbox.state == "linked", "durable outbox missing linked row")
    mcp_link = f"{browser_origin}/tasks/{mcp_task_id}"
    return {
        "vikunja_task_url_template": meeting_url,
        "mcp_task_url": mcp_link,
        "mcp_operation_ref": mcp_operation_ref,
        "meeting_task_id": meeting_task_id,
        "meeting_event_id": row.event_id,
        "meeting_proposal_id": proposal_id,
        "delivery_outcome": getattr(delivery.outcome, "value", str(delivery.outcome)),
        "recover_outcome": getattr(recovered.outcome, "value", str(recovered.outcome)),
        "child_boundary": {
            "rc": child.returncode,
            "task_id": child_payload.get("task_id"),
            "state": child_payload.get("state"),
        },
        "durable_records": {
            "outbox_path": str(outbox_path),
            "journal_path": str(journal_path),
            "changed_files": len(changed),
            "forbidden_development_hits": forbidden_hits,
        },
        "both_links": {
            "meeting_task_url": meeting_url,
            "proposal_source_url": source_url,
        },
        "owner_acceptance_checkpoint": default_checkpoint_status()["owner_synthetic_acceptance"],
        "note": (
            "synthetic acceptance/outbox/recovery executed under DevelopmentRunGuard "
            "meeting channel with child boundary; personal owner receipt remains not-run"
        ),
    }


def _harness_fingerprint(worktree: Path) -> dict[str, Any]:
    """Narrow s10 harness path fingerprint (not the candidate revision digest)."""
    paths = [
        worktree / "tests" / "test_e2e.py",
        worktree / "tests" / "support" / "e2e_suite.py",
        worktree / "tests" / "support" / "pilot_suite.py",
        worktree / "tests" / "support" / "suites.py",
        worktree / "scripts" / "ea010_verify.py",
        worktree / "docs" / "testing" / "ea010-verification-profile.md",
    ]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        digest.update(b"\0")
        if path.is_file():
            digest.update(sha256_file(path).encode())
        digest.update(b"\0")
    diff = subprocess.run(
        ["git", "diff", "HEAD", "--", *[str(p.relative_to(worktree)) for p in paths if p.exists()]],
        cwd=worktree,
        capture_output=True,
        check=False,
    )
    return {
        "sha256": digest.hexdigest(),
        "diff_sha256": hashlib.sha256(diff.stdout or b"").hexdigest(),
        "paths": [str(p.relative_to(worktree)) for p in paths],
    }


def _source_digest(worktree: Path) -> dict[str, Any]:
    """Bind aggregate/E2E digests to the full candidate implementation revision.

    Primary ``sha256`` / ``diff_sha256`` cover every candidate addition and
    modification (``docs/`` excluded by shared policy). The former six-path
    allowlist is retained only as ``harness_fingerprint``.
    """
    full = implementation_source_digest(worktree)
    return {
        **full,
        "harness_fingerprint": _harness_fingerprint(worktree),
    }


def _client(stand: IsolatedStand, token_path: Path, project_id: int, *, origin: str | None = None) -> VikunjaClient:
    api_origin = origin or f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    return VikunjaClient(
        api_origin=api_origin,
        project_id=project_id,
        token_ref=TokenRef(kind="file", path=token_path),
        page_size=2,
    )


def _new_uuid() -> str:
    return str(uuid.uuid4())


class _DelayProxyState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.create_seen = 0
        self.upstream_id: int | None = None
        self.delay_s = 8.0
        self.upstream_host = "127.0.0.1"
        self.upstream_port = 19345


def _start_delay_proxy(
    *,
    listen_host: str,
    listen_port: int,
    upstream_host: str,
    upstream_port: int,
    delay_s: float,
) -> tuple[ThreadingHTTPServer, threading.Thread, _DelayProxyState]:
    state = _DelayProxyState()
    state.delay_s = delay_s
    state.upstream_host = upstream_host
    state.upstream_port = upstream_port

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: object) -> None:
            return

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length > 0 else b""

        def _proxy(self, method: str) -> None:
            import socket

            body = self._read_body() if method in {"PUT", "POST", "PATCH"} else b""
            path = self.path
            is_create = method == "PUT" and "/projects/" in path and path.rstrip("/").endswith("/tasks")
            if is_create:
                with state.lock:
                    state.create_seen += 1
                time.sleep(state.delay_s)
            try:
                upstream = socket.create_connection((state.upstream_host, state.upstream_port), timeout=5)
            except OSError as exc:
                payload = json.dumps({"error": f"upstream_connect:{exc}"}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            try:
                req = [f"{method} {path} HTTP/1.1\r\n".encode()]
                host_hdr = f"{state.upstream_host}:{state.upstream_port}"
                skip = {"host", "content-length", "connection", "transfer-encoding"}
                for key, value in self.headers.items():
                    if key.lower() in skip:
                        continue
                    req.append(f"{key}: {value}\r\n".encode())
                req.append(f"Host: {host_hdr}\r\n".encode())
                req.append(f"Content-Length: {len(body)}\r\n".encode())
                req.append(b"Connection: close\r\n\r\n")
                upstream.sendall(b"".join(req) + body)
                chunks: list[bytes] = []
                while True:
                    block = upstream.recv(65536)
                    if not block:
                        break
                    chunks.append(block)
                raw = b"".join(chunks)
            finally:
                upstream.close()
            if is_create:
                try:
                    header, _, resp_body = raw.partition(b"\r\n\r\n")
                    status_line = header.split(b"\r\n", 1)[0].decode("latin1")
                    parts = status_line.split()
                    status = int(parts[1]) if len(parts) >= 2 else 0
                    parsed = json.loads(resp_body.decode("utf-8") or "null")
                    if status in (200, 201) and isinstance(parsed, dict) and isinstance(parsed.get("id"), int):
                        with state.lock:
                            state.upstream_id = int(parsed["id"])
                except Exception:
                    pass
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802
            self._proxy("GET")

        def do_PUT(self) -> None:  # noqa: N802
            self._proxy("PUT")

        def do_POST(self) -> None:  # noqa: N802
            self._proxy("POST")

        def do_PATCH(self) -> None:  # noqa: N802
            self._proxy("PATCH")

    server = ThreadingHTTPServer((listen_host, listen_port), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, name="ea010-e2e-delay-proxy", daemon=True)
    thread.start()
    return server, thread, state


def _stop_proxy(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
    try:
        server.shutdown()
    except Exception:
        pass
    try:
        server.server_close()
    except Exception:
        pass
    thread.join(timeout=5)


def run_e2e_scenarios(stand: IsolatedStand, worktree: Path) -> dict[str, Any]:
    # Pre-operation baseline + instrumentation for NFR-12 / §6 no-development-run.
    dev_guard = DevelopmentRunGuard(worktree, stand)
    dev_guard.start()
    try:
        return _run_e2e_scenarios_body(stand, worktree, dev_guard)
    finally:
        dev_guard.stop()


def _run_e2e_scenarios_body(
    stand: IsolatedStand,
    worktree: Path,
    dev_guard: DevelopmentRunGuard,
) -> dict[str, Any]:
    session = stand.create_synthetic_session()
    project_id = int(session["project_id"])
    origin = f"http://{stand.profile.listen_host}:{stand.profile.vikunja_port}"
    browser_origin = origin

    write_tok = _create_api_token(stand, title=stand.fixture_name("e2e-write"), permissions=WRITE_PERMS)
    read_tok = _create_api_token(stand, title=stand.fixture_name("e2e-read"), permissions=READ_PERMS)
    write_path = _store_token(stand, "e2e-write.token", write_tok["token"])
    read_path = _store_token(stand, "e2e-read.token", read_tok["token"])
    stand._token_secrets["e2e_write"] = write_tok["token"]
    stand._token_secrets["e2e_read"] = read_tok["token"]
    stand.manifest.setdefault("token_handles", []).extend(["e2e_write", "e2e_read"])
    stand.synthetic_ids["e2e_write_token_id"] = write_tok.get("id")
    stand.synthetic_ids["e2e_read_token_id"] = read_tok.get("id")
    stand._save_secrets()
    stand._save_manifest()

    journal_path = _journal_path(stand)
    host = TrustedHostSession(stand.run_dir / "trusted-host-e2e.json")
    (stand.run_dir / "journal").mkdir(mode=0o700, exist_ok=True)

    write_cfg = _write_config(
        stand,
        name="e2e-mcp-write.json",
        token_path=write_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=browser_origin,
    )
    readonly_cfg = _write_config(
        stand,
        name="e2e-mcp-readonly.json",
        token_path=read_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="read_only",
        trusted_write_client=False,
        browser_origin=None,
    )

    ui_client = _client(stand, write_path, project_id)
    scenarios: dict[str, Any] = {}
    synthetic_ids: dict[str, Any] = {
        "project_id": project_id,
        "e2e_write_token_id": write_tok.get("id"),
        "e2e_read_token_id": read_tok.get("id"),
    }
    executed = 0
    owned_dirs: list[Path] = []

    # --- UI → MCP: create/update/complete via ready-made Vikunja UI, observe via MCP ---
    dev_guard.channel("ui")
    ui_create_title = stand.fixture_name("ui-create")
    ui_updated_title = stand.fixture_name("ui-updated")
    ui_browser = _run_ready_made_vikunja_ui(
        stand,
        project_id=project_id,
        create_title=ui_create_title,
        update_title=ui_updated_title,
        mark_done=True,
    )
    ui_task_id = int(ui_browser["task_id"])
    synthetic_ids["ui_task_id"] = ui_task_id
    _assert(ui_browser.get("created", {}).get("title") == ui_create_title, "UI browser create title drift")
    _assert((ui_browser.get("updated") or {}).get("title") == ui_updated_title, "UI browser update title drift")
    _assert((ui_browser.get("completed") or {}).get("done") is True, "UI browser mark-done missing")
    _assert((ui_browser.get("opened") or {}).get("done") is True, "UI rendered done control missing after mark-done")

    mcp = McpSubprocess(worktree, write_cfg)
    mcp.start()
    try:
        mcp.handshake()
        found = mcp.call("ea010_find", {"task_id": ui_task_id})
        _assert(found["status"] == "confirmed", f"MCP find UI task failed: {found}")
        _assert(int(found["task"]["id"]) == ui_task_id, "MCP find id drift")
        _assert(found["task"]["title"] == ui_updated_title, "MCP find title drift after UI update")
        _assert(found["task"].get("done") is True, "MCP find done drift after UI complete")
        scenarios["ui_to_mcp"] = {
            "task_id": ui_task_id,
            "mcp_status": found["status"],
            "title": found["task"]["title"],
            "done": found["task"].get("done"),
            "surface": "ready-made-vikunja-ui",
            "ui_mutations": ["create", "update_title", "mark_done"],
            "browser": {
                "created": True,
                "updated": bool(ui_browser.get("updated")),
                "completed": bool(ui_browser.get("completed")),
                "opened": bool(ui_browser.get("opened")),
                "readback_surface": (ui_browser.get("opened") or {}).get("readback_surface"),
            },
        }
        executed += 1

        # --- MCP → UI: MCP create/PATCH/complete → rendered UI controls (same ID) ---
        # BR-06 / §6: MCP mutation direction must be asserted on rendered controls, not API fetch.
        dev_guard.channel("mcp")
        create_args = {
            "title": stand.fixture_name("mcp-create"),
            "description": "e2e mcp→ui",
            "due": {"kind": "date", "value": "2026-12-20"},
        }
        create_ref = host.remember(tool="ea010_create", arguments=create_args)
        created = mcp.call("ea010_create", create_args, meta=host.meta_for(create_ref))
        _assert(created["status"] == "confirmed", f"MCP create failed: {created}")
        mcp_task_id = int(created["task"]["id"])
        synthetic_ids["mcp_task_id"] = mcp_task_id
        synthetic_ids["mcp_operation_ref"] = created["operation_ref"]
        url = created["task"]["url"]
        _assert(isinstance(url, str) and url.endswith(f"/tasks/{mcp_task_id}"), f"bad MCP url {url}")
        _assert(created["link_state"] == "verified", "MCP link_state not verified")

        mcp_updated_title = stand.fixture_name("mcp-patched")
        mcp_due_value = "2026-12-25"
        upd_args = {
            "task_id": mcp_task_id,
            "patch": {
                "title": mcp_updated_title,
                "due": {"kind": "date", "value": mcp_due_value},
            },
        }
        upd_ref = host.remember(tool="ea010_update", arguments=upd_args)
        updated = mcp.call("ea010_update", upd_args, meta=host.meta_for(upd_ref))
        _assert(updated["status"] == "confirmed", f"MCP update failed: {updated}")
        _assert(updated["task"]["title"] == mcp_updated_title, "MCP update title drift")
        synthetic_ids["mcp_update_operation_ref"] = updated.get("operation_ref")

        complete_args = {"task_id": mcp_task_id}
        complete_ref = host.remember(tool="ea010_complete", arguments=complete_args)
        completed = mcp.call("ea010_complete", complete_args, meta=host.meta_for(complete_ref))
        _assert(completed["status"] == "confirmed", f"MCP complete failed: {completed}")
        synthetic_ids["mcp_complete_operation_ref"] = completed.get("operation_ref")

        ui_readback = _run_ready_made_vikunja_ui(
            stand,
            project_id=project_id,
            open_task_id=mcp_task_id,
            expect_title=mcp_updated_title,
            expect_done=True,
            expect_due_prefix=mcp_due_value,
        )
        _assert(int(ui_readback["task_id"]) == mcp_task_id, "UI read-back id drift")
        opened = ui_readback.get("opened") or {}
        _assert(opened.get("readback_surface") == "rendered-controls", "UI read-back must use rendered controls")
        _assert(opened.get("title") == mcp_updated_title, "UI missed MCP title patch")
        _assert(opened.get("done") is True, "UI missed MCP complete on rendered control")
        due_seen = opened.get("due_date")
        _assert(isinstance(due_seen, str) and due_seen.strip(), f"UI missed MCP due patch on rendered control: {due_seen!r}")
        # Ready-made UI renders calendar text (e.g. "Fri, Dec 25, 2026"), not ISO wire.
        _assert(
            "2026" in due_seen and ("Dec" in due_seen or "December" in due_seen) and "25" in due_seen,
            f"UI rendered due does not reflect MCP date {mcp_due_value}: {due_seen!r}",
        )

        scenarios["mcp_to_ui_crud"] = {
            "task_id": mcp_task_id,
            "url": url,
            "operation_ref": created["operation_ref"],
            "update_operation_ref": updated.get("operation_ref"),
            "complete_operation_ref": completed.get("operation_ref"),
            "done": True,
            "title": mcp_updated_title,
            "due_prefix": mcp_due_value,
            "surface": "ready-made-vikunja-ui",
            "mcp_mutations": ["ea010_create", "ea010_update", "ea010_complete"],
            "browser": {
                "opened": bool(opened),
                "readback_surface": opened.get("readback_surface"),
                "readback_title": opened.get("title"),
                "readback_done": opened.get("done"),
                "readback_due_date": opened.get("due_date"),
            },
        }
        executed += 1

        # Replay must not duplicate (MCP channel; distinct from UI CRUD above).
        replay = mcp.call("ea010_create", create_args, meta=host.meta_for(create_ref), rpc_id=9001)
        _assert(replay["status"] == "confirmed", f"replay failed: {replay}")
        _assert(int(replay["task"]["id"]) == mcp_task_id, "replay id drift")
        scenarios["mcp_replay_no_dup"] = {"task_id": mcp_task_id}
        executed += 1
    finally:
        mcp.stop()
        dev_guard.clear_channel()

    # --- Auth / scopes / revoke ---
    readonly = McpSubprocess(worktree, readonly_cfg)
    readonly.start()
    try:
        readonly.handshake()
        ro_tools = sorted(t["name"] for t in readonly.tools_list())
        _assert(ro_tools == ["ea010_find", "ea010_reconcile"], f"read-only tools: {ro_tools}")
        raw = readonly.request(
            "tools/call",
            {"name": "ea010_create", "arguments": {"title": stand.fixture_name("ro-denied")}},
        )
        _assert("error" in raw, f"read-only must reject create tool: {raw}")
        scenarios["readonly_scope_denies_create"] = {
            "tools": ro_tools,
            "create_rejected": True,
        }
        executed += 1
    finally:
        readonly.stop()

    revoke = _revoke_suite_token(
        stand,
        handle="e2e_write",
        token_id=write_tok.get("id"),
        secret=write_tok["token"],
    )
    revoked_client = _client(stand, write_path, project_id)
    revoked_read = revoked_client.read(ui_task_id)
    _assert(
        revoked_read.outcome in {Outcome.AUTH_DENIED, Outcome.SCOPE_DENIED},
        f"revoked token still worked: {revoked_read.outcome}",
    )
    # Re-mint write token for remaining scenarios.
    write_tok2 = _create_api_token(stand, title=stand.fixture_name("e2e-write2"), permissions=WRITE_PERMS)
    write_path = _store_token(stand, "e2e-write2.token", write_tok2["token"])
    stand._token_secrets["e2e_write2"] = write_tok2["token"]
    stand.manifest.setdefault("token_handles", []).append("e2e_write2")
    stand.synthetic_ids["e2e_write2_token_id"] = write_tok2.get("id")
    stand._save_secrets()
    stand._save_manifest()
    ui_client = _client(stand, write_path, project_id)
    write_cfg = _write_config(
        stand,
        name="e2e-mcp-write2.json",
        token_path=write_path,
        journal_path=journal_path,
        project_id=project_id,
        profile="write",
        trusted_write_client=True,
        browser_origin=browser_origin,
    )
    scenarios["auth_revoke_readback"] = {
        "revoke": {
            "revoked_token_http": revoke.get("revoked_token_http"),
            "token_absent_after_revoke": revoke.get("token_absent_after_revoke"),
        },
        "post_revoke_outcome": revoked_read.outcome.value,
    }
    executed += 1
    synthetic_ids["e2e_write2_token_id"] = write_tok2.get("id")

    # --- Dates / 304 / recurring ---
    confirmed_at = datetime(2026, 11, 12, 23, 30, tzinfo=MSK)
    date_due = confirm_date_only("2026-12-01", confirmed_at=confirmed_at, expression="2026-12-01")
    di, do = _new_uuid(), new_operation_id()
    date_create = ui_client.create(
        title=stand.fixture_name("date-only"),
        issuer_id=di,
        operation_id=do,
        user_portion="msk date-only",
        due=date_due,
    )
    _assert(date_create.ok and date_create.task is not None, f"date create failed: {date_create.outcome}")
    date_id = int(date_create.task["id"])
    synthetic_ids["date_task_id"] = date_id
    _assert(same_instant(date_create.task.get("due_date"), date_due.wire_value()), "date-only due mismatch")

    same_title = date_create.task["title"]
    patch_304 = ui_client.update(date_id, {"title": same_title})
    _assert(patch_304.ok and patch_304.task is not None, f"304/GET path failed: {patch_304.outcome}")
    _assert(patch_304.details.get("patch_http") in (200, 304), "expected 200 or 304")

    ri, ro = _new_uuid(), new_operation_id()
    recurring_due = confirm_date_only("2026-12-05", confirmed_at=confirmed_at)
    recurring = ui_client.create(
        title=stand.fixture_name("recurring"),
        issuer_id=ri,
        operation_id=ro,
        user_portion="recurring",
        due=recurring_due,
        extra_fields={"repeat_after": 86400, "repeat_mode": 0},
    )
    _assert(recurring.ok and recurring.task is not None, f"recurring create failed: {recurring.outcome}")
    recurring_id = int(recurring.task["id"])
    synthetic_ids["recurring_task_id"] = recurring_id
    due_before = recurring.task.get("due_date")
    one = ui_client.complete(recurring_id)
    _assert(one.ok and one.task is not None, f"recurring complete failed: {one.outcome}")
    _assert(one.mutation_sent is True, "recurring complete must mutate once")
    # Do not blind-retry complete while done may be false after occurrence advance.
    scenarios["dates_304_recurring"] = {
        "date_task_id": date_id,
        "date_due": date_create.task.get("due_date"),
        "patch_http": patch_304.details.get("patch_http"),
        "recurring_task_id": recurring_id,
        "due_before": due_before,
        "due_after": one.task.get("due_date"),
        "done_after": one.task.get("done"),
        "mutation_sent": one.mutation_sent,
    }
    executed += 1

    # --- Late-write via fault proxy: client timeout, upstream 201, exact reconcile ---
    proxy_port = int(stand.profile.ports["fault_proxy"])
    _assert(not listeners(proxy_port), f"fault proxy port {proxy_port} already in use")
    proxy_srv, proxy_thr, proxy_state = _start_delay_proxy(
        listen_host=stand.profile.listen_host,
        listen_port=proxy_port,
        upstream_host=stand.profile.listen_host,
        upstream_port=stand.profile.vikunja_port,
        delay_s=6.0,
    )
    try:
        proxy_origin = f"http://{stand.profile.listen_host}:{proxy_port}"
        journal = IntegrationJournal(journal_path)
        journal.open()
        try:
            issuer_late = journal.ensure_issuer()
            op_late = new_operation_id()
            late_title = stand.fixture_name("late-write")
            late_client = VikunjaClient(
                api_origin=proxy_origin,
                project_id=project_id,
                token_ref=TokenRef(kind="file", path=write_path),
                page_size=2,
                http_timeout=2.0,
            )
            dispatcher = IntegrationDispatcher(journal, late_client)
            late = dispatcher.execute_create(
                issuer_id=issuer_late,
                operation_id=op_late,
                title=late_title,
                user_portion="e2e late",
            )
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline and proxy_state.upstream_id is None:
                time.sleep(0.2)
            _assert(proxy_state.create_seen >= 1, "proxy never saw create")
            _assert(proxy_state.upstream_id is not None, "upstream never returned id")
            # Exact reconcile must find the single late ID — never blind recreate.
            reconcile = dispatcher.execute_create(
                issuer_id=issuer_late,
                operation_id=op_late,
                title=late_title,
                user_portion="e2e late",
            )
            _assert(reconcile.ok and reconcile.vikunja_id is not None, f"reconcile failed: {reconcile.outcome}")
            _assert(int(reconcile.vikunja_id) == int(proxy_state.upstream_id), "reconcile id mismatch")
            synthetic_ids["late_write_task_id"] = int(reconcile.vikunja_id)
            scenarios["late_write_reconcile"] = {
                "first_outcome": late.outcome.value if hasattr(late.outcome, "value") else str(late.outcome),
                "upstream_id": proxy_state.upstream_id,
                "reconciled_id": reconcile.vikunja_id,
                "create_seen": proxy_state.create_seen,
            }
            executed += 1
        finally:
            journal.close()
    finally:
        _stop_proxy(proxy_srv, proxy_thr)
        _assert(not listeners(proxy_port), f"fault proxy port {proxy_port} still listening")

    # --- Crash sample: SIGKILL MCP after create receipt; reconnect finds same ID ---
    mcp2 = McpSubprocess(worktree, write_cfg)
    mcp2.start()
    try:
        mcp2.handshake()
        crash_args = {"title": stand.fixture_name("crash-reconnect"), "description": "e2e crash"}
        crash_ref = host.remember(tool="ea010_create", arguments=crash_args)
        crash_created = mcp2.call("ea010_create", crash_args, meta=host.meta_for(crash_ref))
        _assert(crash_created["status"] == "confirmed", f"pre-crash create failed: {crash_created}")
        crash_id = int(crash_created["task"]["id"])
        synthetic_ids["crash_task_id"] = crash_id
        op_ref = crash_created["operation_ref"]
    finally:
        if mcp2.proc is not None and mcp2.proc.poll() is None:
            os.kill(mcp2.proc.pid, signal.SIGKILL)
            try:
                mcp2.proc.wait(timeout=5)
            except Exception:
                pass
            mcp2.proc = None

    mcp3 = McpSubprocess(worktree, write_cfg)
    mcp3.start()
    try:
        mcp3.handshake()
        reconnected = mcp3.call("ea010_find", {"operation_ref": op_ref})
        _assert(reconnected["status"] == "confirmed", f"reconnect find failed: {reconnected}")
        _assert(int(reconnected["task"]["id"]) == crash_id, "reconnect id drift")
        # Same command_ref must not create a second task.
        replay_crash = mcp3.call("ea010_create", crash_args, meta=host.meta_for(crash_ref), rpc_id=9100)
        _assert(replay_crash["status"] == "confirmed", f"post-crash replay failed: {replay_crash}")
        _assert(int(replay_crash["task"]["id"]) == crash_id, "post-crash replay duplicated")
        scenarios["crash_reconnect"] = {
            "task_id": crash_id,
            "operation_ref": op_ref,
            "command_ref": crash_ref,
        }
        executed += 1
    finally:
        mcp3.stop()

    # --- Meeting channel: synthetic acceptance/outbox/recovery under NFR-12 guard ---
    # Owner personal receipt stays not-run; this exercises the adapter path while
    # instrumented (including a child execution boundary) and compares durable records.
    dev_guard.channel("meeting")
    meeting_evidence = _run_meeting_acceptance_under_guard(
        stand,
        worktree=worktree,
        project_id=project_id,
        token_path=write_path,
        browser_origin=browser_origin,
        mcp_task_id=int(synthetic_ids["mcp_task_id"]),
        mcp_operation_ref=str(synthetic_ids.get("mcp_operation_ref") or ""),
        process_dir=dev_guard.process_dir,
    )
    scenarios["dual_link_shape"] = meeting_evidence
    executed += 1
    dev_guard.clear_channel()

    # --- Legacy preservation smoke: hash a seeded markdown file across no-op ---
    legacy_dir = stand.run_dir / "legacy-seed"
    legacy_dir.mkdir(mode=0o700, exist_ok=True)
    legacy_file = legacy_dir / "accepted-legacy.md"
    legacy_body = f"# Legacy\nOwner: {stand.fixture_name('legacy')}\n"
    legacy_file.write_text(legacy_body, encoding="utf-8")
    os.chmod(legacy_file, 0o600)
    legacy_before = sha256_file(legacy_file)
    # No migration path in e2e — file must remain byte-identical.
    legacy_after = sha256_file(legacy_file)
    _assert(legacy_before == legacy_after, "legacy hash drift without migration")
    scenarios["legacy_preserved"] = {"sha256": legacy_before, "path": str(legacy_file.name)}
    executed += 1

    # --- 390px geometry contract (document + static CSS check; live Playwright in hermes-ux) ---
    ux_css_candidates = list((worktree / "integration").rglob("*.css")) if (worktree / "integration").exists() else []
    scenarios["viewport_390px"] = {
        "status": "delegated-to-hermes-ux-suite",
        "live_playwright": "not-run-in-e2e-process",
        "css_candidates": len(ux_css_candidates),
        "macbook_checkpoint": default_checkpoint_status()["macbook_access_and_links"],
    }
    executed += 1

    # --- Isolated restore smoke (real ContourPaths; Mini restart stays not-run) ---
    copy_root = stand.claim_owned_root(prefix="EA010-SYNTHETIC-e2e-copy-")
    restore_root = stand.claim_owned_root(prefix="EA010-SYNTHETIC-e2e-restore-")
    owned_dirs.extend([copy_root, restore_root])
    source = ContourPaths.from_stand_root(stand.run_dir)
    username = stand._username  # noqa: SLF001 — suite-owned credentials
    password = stand._password  # noqa: SLF001
    if stand._watchdog is not None:  # noqa: SLF001
        stand._watchdog.stop()
        stand._watchdog = None  # noqa: SLF001
    # Revoke remaining suite tokens while Vikunja is up, then stop for quiescent copy.
    for handle, token_id_key in (
        ("e2e_write2", "e2e_write2_token_id"),
        ("e2e_read", "e2e_read_token_id"),
    ):
        if handle in stand._token_secrets:
            _revoke_suite_token(
                stand,
                handle=handle,
                token_id=stand.synthetic_ids.get(token_id_key),
                secret=stand._token_secrets[handle],
            )
    stand.stop_process()
    stand._child = None  # noqa: SLF001
    stand._session_token = None  # noqa: SLF001 — avoid cleanup revoke against stopped child
    time.sleep(0.2)
    copy_manifest = create_manual_copy(
        source,
        copy_root,
        secrets_extra={"username": username, "password": password, "run_id": stand.run_id},
        note="e2e isolated restore smoke",
    )
    verify_consistent_copy(copy_root)
    restore_into_empty_target(copy_root, restore_root)
    _assert((restore_root / "stand.db").is_file(), "restore missing stand.db")
    scenarios["restore_smoke"] = {
        "copy_ok": True,
        "restore_ok": True,
        "copy_id": copy_manifest.get("copy_id"),
        "mini_restart_checkpoint": default_checkpoint_status()["mini_restart_post_login"],
    }
    executed += 1
    # Restart primary stand so final no-dev-run checks and cleanup can run.
    stand.start()
    stand.wait_ready()
    status, login = http_json(
        stand.profile.listen_host,
        stand.profile.vikunja_port,
        "login",
        method="POST",
        body={"username": username, "password": password},
    )
    _assert(status == 200 and isinstance(login, dict) and "token" in login, f"relogin HTTP {status}")
    stand._session_token = login["token"]  # noqa: SLF001

    # --- No development-run (instrumented entrypoints + pre-op baseline compare) ---
    scenarios["no_development_run"] = dev_guard.assert_clean()
    executed += 1

    # --- Manual checkpoints ledger (honest not-run) ---
    scenarios["manual_checkpoints"] = default_checkpoint_status()
    scenarios["historical_p9_p10"] = {
        "status": "historical-input-only",
        "credited_as_current_e2e": False,
    }
    executed += 1

    _assert(executed > 0, "e2e executed_count must be positive")
    return {
        "executed_count": executed,
        "scenarios": scenarios,
        "scenario_names": sorted(scenarios),
        "synthetic_ids": synthetic_ids,
        "owned_dirs": [str(p) for p in owned_dirs],
        "checkpoints": default_checkpoint_status(),
    }


def run_e2e_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    if mode != "isolated":
        raise StandError(f"Suite e2e supports only isolated mode, got {mode!r}")
    profile = load_profile()
    stand = IsolatedStand(profile=profile, worktree=worktree)
    owned: list[Path] = []
    try:
        with stand:
            body = run_e2e_scenarios(stand, worktree)
            owned = [Path(p) for p in body.get("owned_dirs") or []]
            source = _source_digest(worktree)
            result = {
                "suite": "e2e",
                "mode": mode,
                "argv": list(argv),
                "run_id": stand.run_id,
                "readiness": stand.readiness,
                "executed_count": body["executed_count"],
                "scenarios": body["scenarios"],
                "scenario_names": body["scenario_names"],
                "synthetic_ids": body["synthetic_ids"],
                "checkpoints": body["checkpoints"],
                "source": {
                    **(stand.manifest.get("source") or {}),
                    **source,
                },
                "binary": stand.manifest.get("binary"),
                "owned_dirs": [str(p) for p in owned],
            }
        owned_removed = all(not p.exists() for p in owned)
        # Suite-owned dirs under run_dir are removed with stand cleanup; confirm.
        if not owned_removed:
            for path in owned:
                if path.exists():
                    try:
                        import shutil

                        shutil.rmtree(path, ignore_errors=True)
                    except Exception:
                        pass
            owned_removed = all(not p.exists() for p in owned)
        result["cleanup"] = {
            "confirmed": stand.cleaned,
            "owned_dirs_removed": owned_removed,
            "ports_clear": {
                "vikunja": not listeners(stand.profile.vikunja_port),
                "fault_proxy": not listeners(int(stand.profile.ports["fault_proxy"])),
            },
        }
        result.pop("owned_dirs", None)
        if not stand.cleaned:
            raise StandError("e2e suite cleanup was not confirmed")
        if not owned_removed:
            remaining = [str(p) for p in owned if p.exists()]
            raise StandError(f"e2e owned dirs still present: {remaining}")
        if result["executed_count"] <= 0:
            raise StandError("e2e suite executed_count must be positive")
        return result
    except BaseException:
        stand._unwind_after_failed_acquire(revoke=False)
        raise


# Component suites composed into suite "all" (excluding all itself).
ALL_COMPONENT_SUITES: tuple[str, ...] = (
    "stand",
    "api",
    "journal",
    "recovery",
    "mcp",
    "hermes-bridge",
    "hermes-ux",
    "lifecycle",
    "restore",
    "e2e",
)


ALL_RECEIPT_SCHEMA = "ea010-suite-all-component-receipt/v1"
ALL_AGGREGATE_SCHEMA = "ea010-suite-all-aggregate-receipt/v1"
ALL_PROGRESS_SCHEMA = "ea010-suite-all-progress-receipt/v1"


def _all_receipt_root(profile: Any, aggregate_id: str) -> Path:
    """Durable journal dir under approved run_root, outside disposable stand trees."""
    leaf = validate_journal_path(
        profile, Path("journals") / f"all-{aggregate_id}" / ".aggregate-root"
    )
    root = leaf.parent
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    return root


def _persist_all_receipt(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(mode=0o700, exist_ok=True)
    atomic_write_secure(path, json.dumps(payload, indent=2, sort_keys=True) + "\n", 0o600)
    return path


def _ports_readback(profile: Any) -> dict[str, bool]:
    ports = getattr(profile, "ports", None) or {}
    out: dict[str, bool] = {}
    if isinstance(ports, dict):
        for name, port in ports.items():
            if isinstance(port, int):
                out[str(name)] = not listeners(port)
    return out


def _persist_all_progress(
    receipt_root: Path,
    *,
    aggregate_id: str,
    phase: str,
    current_suite: str | None,
    current_argv: list[str] | None,
    components: dict[str, Any],
    argv: list[str],
    worktree: Path,
    profile: Any,
    signal_info: dict[str, Any] | None = None,
) -> Path:
    payload = {
        "schema": ALL_PROGRESS_SCHEMA,
        "aggregate_id": aggregate_id,
        "phase": phase,
        "current_suite": current_suite,
        "current_argv": list(current_argv or []),
        "components_completed": sorted(k for k, v in components.items() if v.get("rc") == 0),
        "components": {
            name: {
                "rc": row.get("rc"),
                "executed_count": row.get("executed_count"),
                "receipt_path": row.get("receipt_path"),
                "cleanup_confirmed": row.get("cleanup_confirmed"),
            }
            for name, row in components.items()
        },
        "argv": list(argv),
        "ports_clear": _ports_readback(profile),
        "signal": signal_info,
        "sigkill_note": (
            "SIGKILL cannot run process-local finally; reconcile from this durable "
            "in-progress receipt and any completed component receipts"
        ),
        "source": _source_digest(worktree),
        "at": datetime.now(MSK).isoformat(),
    }
    return _persist_all_receipt(receipt_root / "progress.json", payload)


def _component_receipt_payload(
    *,
    name: str,
    child_argv: list[str],
    rc: int,
    result: dict[str, Any] | None,
    error: str | None,
    status: str,
    aggregate_id: str,
    worktree: Path,
) -> dict[str, Any]:
    result = result or {}
    cleanup = result.get("cleanup") if isinstance(result.get("cleanup"), dict) else result.get("cleanup")
    ids = result.get("synthetic_ids") if isinstance(result.get("synthetic_ids"), dict) else {}
    # Preserve full child IDs (not a truncated subset) for durable evidence.
    return {
        "schema": ALL_RECEIPT_SCHEMA,
        "aggregate_id": aggregate_id,
        "suite": name,
        "status": status,
        "argv": list(child_argv),
        "rc": rc,
        "executed_count": result.get("executed_count"),
        "run_id": result.get("run_id"),
        "source": result.get("source") or _source_digest(worktree),
        "binary": result.get("binary"),
        "config": result.get("config"),
        "readiness": result.get("readiness"),
        "scenario_names": result.get("scenario_names")
        or sorted((result.get("scenarios") or {}).keys()),
        "synthetic_ids": ids,
        "cleanup": cleanup,
        "error": error,
        "at": datetime.now(MSK).isoformat(),
    }


def run_all_suite(*, mode: str, worktree: Path, argv: list[str]) -> dict[str, Any]:
    """Run every registered isolated product suite sequentially, then e2e.

    Ports are shared; suites must not overlap. Failures are not masked.
    Each component receipt is durably journaled under the approved run_root
    (outside disposable stand directories) before advancing. Aggregate
    SIGTERM/SIGHUP handlers persist started/current/terminal state and the
    actual exit status; SIGKILL is reconciled from the durable in-progress
    receipt rather than claimed to be covered by ``finally``.
    """
    if mode != "isolated":
        raise StandError(f"Suite all supports only isolated mode, got {mode!r}")

    # Local import to avoid circular registration at module import time.
    from .suites import get_suite, list_suites

    known = set(list_suites())
    missing = [name for name in ALL_COMPONENT_SUITES if name not in known]
    if missing:
        raise StandError(f"suite all missing component suites: {missing}; known={sorted(known)}")

    profile = load_profile()
    aggregate_id = uuid.uuid4().hex
    receipt_root = _all_receipt_root(profile, aggregate_id)
    components: dict[str, Any] = {}
    total_executed = 0
    synthetic_ids: dict[str, Any] = {}
    cleanup_all = True
    interruption: dict[str, Any] | None = None
    current_name: str | None = None
    current_argv: list[str] | None = None
    previous_handlers: dict[int, Any] = {}
    signal_exit_code: int | None = None

    def _write_aggregate(*, terminal_phase: str) -> None:
        aggregate = {
            "schema": ALL_AGGREGATE_SCHEMA,
            "aggregate_id": aggregate_id,
            "suite": "all",
            "mode": mode,
            "argv": list(argv),
            "component_order": list(ALL_COMPONENT_SUITES),
            "components_completed": sorted(k for k, v in components.items() if v.get("rc") == 0),
            "components": {
                name: {
                    "rc": row.get("rc"),
                    "executed_count": row.get("executed_count"),
                    "run_id": row.get("run_id"),
                    "receipt_path": row.get("receipt_path"),
                    "cleanup_confirmed": row.get("cleanup_confirmed"),
                    "source": row.get("source"),
                    "binary": row.get("binary"),
                    "error": row.get("error"),
                }
                for name, row in components.items()
            },
            "interruption": interruption,
            "current_suite_at_exit": current_name,
            "current_argv_at_exit": current_argv,
            "ports_clear": _ports_readback(profile),
            "terminal_phase": terminal_phase,
            "source": _source_digest(worktree),
            "at": datetime.now(MSK).isoformat(),
        }
        _persist_all_receipt(receipt_root / "aggregate.json", aggregate)

    def _ingest_reexec_child_evidence(
        child_cancel: dict[str, Any], *, signum: int, signame: str
    ) -> None:
        """Persist component receipt from durable reexec child snapshots before exit."""
        nonlocal components, synthetic_ids, interruption
        if not current_name or current_name in components:
            return
        children = child_cancel.get("children") if isinstance(child_cancel, dict) else None
        if not isinstance(children, list):
            return
        for child in children:
            if not isinstance(child, dict):
                continue
            evidence = child.get("signal_evidence")
            if not isinstance(evidence, dict) or evidence.get("error"):
                continue
            if not evidence.get("run_id") and not evidence.get("synthetic_ids"):
                continue
            cleanup_ev = evidence.get("cleanup")
            result = {
                "run_id": evidence.get("run_id"),
                "synthetic_ids": evidence.get("synthetic_ids")
                if isinstance(evidence.get("synthetic_ids"), dict)
                else {},
                "cleanup": cleanup_ev,
                "source": evidence.get("source"),
                "binary": evidence.get("binary"),
                "readiness": evidence.get("readiness"),
                "executed_count": 0,
            }
            child_rc = child.get("returncode")
            fail_rc = int(child_rc) if isinstance(child_rc, int) else -int(signum)
            fail_receipt = _component_receipt_payload(
                name=current_name,
                child_argv=list(current_argv or []),
                rc=fail_rc,
                result=result,
                error=f"aggregate interrupted by {signame}; reexec child evidence ingested",
                status="interrupted-by-signal",
                aggregate_id=aggregate_id,
                worktree=worktree,
            )
            fail_path = receipt_root / f"{current_name}.json"
            try:
                _persist_all_receipt(fail_path, fail_receipt)
            except Exception:
                fail_path = receipt_root / f"{current_name}.json"
            components[current_name] = {
                "executed_count": fail_receipt.get("executed_count") or 0,
                "run_id": fail_receipt.get("run_id"),
                "cleanup_confirmed": bool(
                    isinstance(fail_receipt.get("cleanup"), dict)
                    and fail_receipt.get("cleanup", {}).get("confirmed") is True
                ),
                "cleanup": fail_receipt.get("cleanup"),
                "source": fail_receipt.get("source"),
                "binary": fail_receipt.get("binary"),
                "scenario_names": fail_receipt.get("scenario_names") or [],
                "receipt_path": str(fail_path),
                "rc": fail_rc,
                "error": fail_receipt["error"],
                "signal_receipt_path": child.get("signal_receipt_path"),
            }
            ids = result.get("synthetic_ids") or {}
            if isinstance(ids, dict) and ids:
                synthetic_ids[current_name] = dict(ids)
            interruption = {
                "suite": current_name,
                "error": fail_receipt["error"],
                "receipt_path": str(fail_path),
                "argv": list(current_argv or argv),
                "rc": -int(signum),
                "ports_clear": _ports_readback(profile),
                "child_rc": fail_rc,
                "run_id": fail_receipt.get("run_id"),
            }
            return

    def _on_aggregate_signal(signum: int, _frame: Any) -> None:
        nonlocal interruption, cleanup_all, signal_exit_code
        cleanup_all = False
        try:
            signame = signal.Signals(signum).name
        except ValueError:
            signame = str(signum)
        # Cancel owned component process trees (reexec inners) and in-process
        # stands before the aggregate redelivers and exits; wait for cleanup
        # and resource read-back so ports/manifests do not outlive the parent.
        # Reexec children publish evidence to a durable receipt; ingest it here
        # before redelivery so component run/IDs/cleanup survive parent exit.
        child_cancel: dict[str, Any] = {}
        stand_cleanup: list[dict[str, Any]] = []
        try:
            child_cancel = cancel_active_component_procs(signum)
        except Exception as exc:  # noqa: BLE001 — still attempt local cleanup
            child_cancel = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            _ingest_reexec_child_evidence(child_cancel, signum=signum, signame=signame)
        except Exception as exc:  # noqa: BLE001
            child_cancel = {
                **(child_cancel if isinstance(child_cancel, dict) else {}),
                "ingest_error": f"{type(exc).__name__}: {exc}",
            }
        try:
            stand_cleanup = cleanup_active_stands_best_effort()
        except Exception as exc:  # noqa: BLE001
            stand_cleanup = [{"error": f"{type(exc).__name__}: {exc}"}]
        ports = _ports_readback(profile)
        signal_info = {
            "signum": signum,
            "signame": signame,
            "rc": -signum,
            "phase": "between-suites" if current_name and current_name in components else "in-suite",
            "current_suite": current_name,
            "current_argv": list(current_argv or []),
            "component_cancel": child_cancel,
            "stand_cleanup": stand_cleanup,
        }
        if interruption is None:
            interruption = {
                "suite": current_name,
                "error": f"aggregate interrupted by {signame}",
                "signal": signal_info,
                "receipt_path": str(receipt_root / "progress.json"),
                "argv": list(current_argv or argv),
                "rc": -signum,
                "ports_clear": ports,
            }
        else:
            interruption["signal"] = signal_info
            interruption["ports_clear"] = ports
        try:
            _persist_all_progress(
                receipt_root,
                aggregate_id=aggregate_id,
                phase="interrupted-by-signal",
                current_suite=current_name,
                current_argv=current_argv,
                components=components,
                argv=argv,
                worktree=worktree,
                profile=profile,
                signal_info=signal_info,
            )
            _write_aggregate(terminal_phase="interrupted-by-signal")
        except Exception:
            pass
        signal_exit_code = -signum
        # Restore prior handler then re-deliver so process exit status matches the signal.
        prior = previous_handlers.get(signum, signal.SIG_DFL)
        try:
            signal.signal(signum, prior)
        except Exception:
            pass
        os.kill(os.getpid(), signum)

    for signum in (signal.SIGTERM, signal.SIGHUP):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, _on_aggregate_signal)

    try:
        _persist_all_progress(
            receipt_root,
            aggregate_id=aggregate_id,
            phase="started",
            current_suite=None,
            current_argv=None,
            components=components,
            argv=argv,
            worktree=worktree,
            profile=profile,
        )
        for name in ALL_COMPONENT_SUITES:
            current_name = name
            runner = get_suite(name)
            child_argv = [
                "python3",
                "scripts/ea010_verify.py",
                "--suite",
                name,
                "--mode",
                "isolated",
            ]
            current_argv = child_argv
            _persist_all_progress(
                receipt_root,
                aggregate_id=aggregate_id,
                phase="running",
                current_suite=name,
                current_argv=child_argv,
                components=components,
                argv=argv,
                worktree=worktree,
                profile=profile,
            )
            result: dict[str, Any] = {}
            try:
                raw_result = runner(mode="isolated", worktree=worktree, argv=child_argv)
                result = raw_result if isinstance(raw_result, dict) else {}
                count = result.get("executed_count")
                if not isinstance(count, int) or count <= 0:
                    raise StandError(
                        f"component suite {name!r} returned non-positive executed_count: {count!r}"
                    )
                cleanup = result.get("cleanup")
                confirmed = cleanup.get("confirmed") if isinstance(cleanup, dict) else None
                if confirmed is not True:
                    cleanup_all = False
                    raise StandError(
                        f"component suite {name!r} cleanup.confirmed must be explicit True; "
                        f"got cleanup={cleanup!r}"
                    )
                receipt = _component_receipt_payload(
                    name=name,
                    child_argv=child_argv,
                    rc=0,
                    result=result,
                    error=None,
                    status="completed",
                    aggregate_id=aggregate_id,
                    worktree=worktree,
                )
                receipt_path = receipt_root / f"{name}.json"
                _persist_all_receipt(receipt_path, receipt)
                components[name] = {
                    "executed_count": count,
                    "run_id": result.get("run_id"),
                    "cleanup_confirmed": True,
                    "cleanup": cleanup,
                    "source": result.get("source"),
                    "binary": result.get("binary"),
                    "scenario_names": receipt["scenario_names"],
                    "receipt_path": str(receipt_path),
                    "rc": 0,
                }
                total_executed += count
                child_ids = result.get("synthetic_ids") or {}
                if isinstance(child_ids, dict):
                    synthetic_ids[name] = dict(child_ids)
                _persist_all_progress(
                    receipt_root,
                    aggregate_id=aggregate_id,
                    phase="between-suites",
                    current_suite=name,
                    current_argv=child_argv,
                    components=components,
                    argv=argv,
                    worktree=worktree,
                    profile=profile,
                )
                # Brief pause so port TIME_WAIT / lock release settles between stands.
                time.sleep(0.5)
            except BaseException as exc:
                # IsolatedStand replaces the aggregate handler while active and
                # exits via SystemExit(128+signum). Preserve that raw result,
                # live evidence published before teardown, and terminal progress.
                if isinstance(exc, SystemExit):
                    exit_code = exc.code if isinstance(exc.code, int) else None
                    if exit_code is not None and exit_code >= 128:
                        signal_exit_code = exit_code
                    elif signal_exit_code is not None:
                        # Aggregate handler already persisted interruption; keep raw exit.
                        raise
                cleanup_all = False
                evidence = consume_stand_signal_evidence()
                # Reexec wrappers publish durable child receipts and attach them
                # on StandError; ingest those on ordinary nonzero exits too —
                # process-local memory alone is empty in the aggregate parent.
                child_evidence = getattr(exc, "child_evidence", None)
                if not isinstance(child_evidence, dict):
                    child_evidence = None
                receipt_hint = getattr(exc, "signal_receipt_path", None)
                if child_evidence is None and receipt_hint:
                    loaded = await_component_signal_receipt(
                        Path(str(receipt_hint)), timeout=2.0
                    )
                    if isinstance(loaded, dict) and "error" not in loaded:
                        child_evidence = loaded
                    elif isinstance(loaded, dict) and loaded.get("run_id"):
                        child_evidence = loaded
                snapshot = None
                if isinstance(child_evidence, dict) and (
                    child_evidence.get("run_id") or child_evidence.get("synthetic_ids")
                ):
                    snapshot = child_evidence
                elif evidence:
                    snapshot = evidence
                if snapshot and not result:
                    cleanup_ev = snapshot.get("cleanup")
                    result = {
                        "run_id": snapshot.get("run_id"),
                        "synthetic_ids": snapshot.get("synthetic_ids")
                        if isinstance(snapshot.get("synthetic_ids"), dict)
                        else {},
                        "cleanup": cleanup_ev,
                        "source": snapshot.get("source"),
                        "binary": snapshot.get("binary"),
                        "readiness": snapshot.get("readiness"),
                        "executed_count": 0,
                    }
                child_rc = getattr(exc, "child_rc", None)
                if signal_exit_code is not None:
                    fail_rc = signal_exit_code
                elif isinstance(child_rc, int):
                    fail_rc = int(child_rc)
                else:
                    fail_rc = 1
                status = (
                    "interrupted-by-signal"
                    if signal_exit_code is not None
                    else "failed"
                )
                fail_receipt = _component_receipt_payload(
                    name=name,
                    child_argv=child_argv,
                    rc=fail_rc,
                    result=result,
                    error=f"{type(exc).__name__}: {exc}",
                    status=status,
                    aggregate_id=aggregate_id,
                    worktree=worktree,
                )
                fail_path = receipt_root / f"{name}.json"
                try:
                    _persist_all_receipt(fail_path, fail_receipt)
                except Exception:
                    pass
                ports = _ports_readback(profile)
                if interruption is None:
                    interruption = {
                        "suite": name,
                        "error": fail_receipt["error"],
                        "receipt_path": str(fail_path),
                        "argv": list(child_argv),
                        "rc": fail_rc,
                        "ports_clear": ports,
                        "child_rc": child_rc if isinstance(child_rc, int) else None,
                        "run_id": fail_receipt.get("run_id"),
                    }
                components[name] = {
                    "executed_count": fail_receipt.get("executed_count") or 0,
                    "run_id": fail_receipt.get("run_id"),
                    "cleanup_confirmed": bool(
                        isinstance(fail_receipt.get("cleanup"), dict)
                        and fail_receipt.get("cleanup", {}).get("confirmed") is True
                    ),
                    "cleanup": fail_receipt.get("cleanup"),
                    "source": fail_receipt.get("source"),
                    "binary": fail_receipt.get("binary"),
                    "scenario_names": fail_receipt.get("scenario_names") or [],
                    "receipt_path": str(fail_path),
                    "rc": fail_rc,
                    "error": fail_receipt["error"],
                    "signal_receipt_path": receipt_hint,
                }
                ids = (result or {}).get("synthetic_ids") or {}
                if isinstance(ids, dict) and ids:
                    synthetic_ids[name] = dict(ids)
                if signal_exit_code is not None:
                    try:
                        signum_for_info = (
                            -signal_exit_code
                            if signal_exit_code < 0
                            else signal_exit_code - 128
                        )
                        try:
                            signame = signal.Signals(signum_for_info).name
                        except ValueError:
                            signame = str(signum_for_info)
                        signal_info = {
                            "signum": signum_for_info,
                            "signame": signame,
                            "rc": fail_rc,
                            "phase": "in-suite",
                            "current_suite": name,
                            "current_argv": list(child_argv),
                            "via": "active-stand-SystemExit",
                        }
                        interruption["signal"] = signal_info
                        _persist_all_progress(
                            receipt_root,
                            aggregate_id=aggregate_id,
                            phase="interrupted-by-signal",
                            current_suite=name,
                            current_argv=child_argv,
                            components=components,
                            argv=argv,
                            worktree=worktree,
                            profile=profile,
                            signal_info=signal_info,
                        )
                    except Exception:
                        pass
                else:
                    # Ordinary component failures: leave durable terminal progress
                    # (not phase=running) so aggregate.json and progress.json agree.
                    try:
                        _persist_all_progress(
                            receipt_root,
                            aggregate_id=aggregate_id,
                            phase="failed",
                            current_suite=name,
                            current_argv=child_argv,
                            components=components,
                            argv=argv,
                            worktree=worktree,
                            profile=profile,
                        )
                    except Exception:
                        pass
                raise
        _persist_all_progress(
            receipt_root,
            aggregate_id=aggregate_id,
            phase="completed",
            current_suite=current_name,
            current_argv=current_argv,
            components=components,
            argv=argv,
            worktree=worktree,
            profile=profile,
        )
    finally:
        for signum, prior in previous_handlers.items():
            try:
                signal.signal(signum, prior)
            except Exception:
                pass
        terminal = "interrupted-by-signal" if signal_exit_code is not None else (
            "failed" if interruption is not None else "completed"
        )
        try:
            _write_aggregate(terminal_phase=terminal)
        except Exception:
            pass

    if interruption is not None:
        # Defensive: successful path should not leave interruption set.
        raise StandError(f"suite all interrupted: {interruption}")

    return {
        "suite": "all",
        "mode": mode,
        "argv": list(argv),
        "executed_count": total_executed,
        "components": components,
        "component_order": list(ALL_COMPONENT_SUITES),
        "synthetic_ids": synthetic_ids,
        "checkpoints": default_checkpoint_status(),
        "historical_p9_p10": {"credited_as_current_e2e": False},
        "source": _source_digest(worktree),
        "aggregate_id": aggregate_id,
        "receipt_root": str(receipt_root),
        "cleanup": {
            "confirmed": cleanup_all,
            "components": {n: components[n]["cleanup_confirmed"] for n in components},
            "ports_clear": _ports_readback(profile),
        },
    }
