# EA-010 verification profile (s10)

Repository-local description of exact argv, dependencies, selection, limits, and
cleanup for synthetic verification. **Registration of an external execution
profile in the platform `repositories.json` remains the environment owner's
responsibility**; this document does not perform or claim that registration.

## Modes

| Mode | Purpose | Gate |
| --- | --- | --- |
| `isolated` | Default-safe disposable Vikunja under the C7 stand profile | Profile at `EA010_ISOLATED_STAND_PROFILE` (default `/Users/efimovivan/.ai-platform/work/ea010-stand/profile-v1.json`) |
| `approved-pilot` | Guarded operational pilot | Exact local permit JSON via `EA010_APPROVED_PILOT_PERMIT`; **fail-closed / not-run** without it — no environment or service writes |

Coding workers execute **isolated** suites only. A live pilot is separately
authorised by the coordinator; absence of a permit is a non-zero result, not a
simulated pass. After `allow_live_pilot=true` and verified owner receipts, the
gate still validates the **current timezone-aware window**, suite coverage in
`authorized_actions`, typed non-null `service_bindings`, and effective
profile/resource binding **before any runner call**. Matching
`EA010_APPROVED_PILOT_EXECUTOR` to `permit.executor` does **not** relabel an
isolated C7 suite as an operational run: receipts stay
`receipt-present-not-executed` until a real operational contour executor uses
and reads back permit-bound services/paths/device actions. Missing or
mismatched executor identity keeps the coding-worker refusal
(`authorised-not-executed-by-coding-worker`).

## Exact argv

Working directory: the assigned worktree.

```text
python3 -m unittest discover -s tests -p test_e2e.py
python3 scripts/ea010_verify.py --suite all --mode isolated
python3 scripts/ea010_verify.py --suite all --mode approved-pilot
git diff --check
```

Optional journal under the approved run-root:

```text
python3 scripts/ea010_verify.py --suite <name> --mode isolated --journal journals/<leaf>.json
```

## Suite selection

Suite `all` runs these **real** registered component suites sequentially
(shared loopback ports — never in parallel), then aggregates coverage:

1. `stand`
2. `api`
3. `journal`
4. `recovery`
5. `mcp`
6. `hermes-bridge`
7. `hermes-ux`
8. `lifecycle`
9. `restore`
10. `e2e` (§6 cross-cut on a fresh disposable stand)

Unknown or empty suite names fail closed. Empty/skip success is forbidden.
Each component must report `executed_count > 0` and confirmed cleanup.

### E2E §6 groups (suite `e2e`)

| Group | Isolated evidence on this SHA | Manual / device |
| --- | --- | --- |
| UI ↔ CHAT | Ready-made Vikunja UI create→update→complete then MCP observes same ID; MCP create/PATCH/complete then UI **rendered** title/due/done controls on the same ID; replay no-dup | Mini/MacBook transitions: **not-run** |
| MEETING | Synthetic acceptance/outbox/delivery/recovery under `DevelopmentRunGuard` meeting channel (child execution boundary + durable record compare); dual links | Owner synthetic acceptance receipt: **not-run** |
| RELIABILITY | Late-write proxy + exact reconcile; SIGKILL MCP + reconnect | — |
| UPDATE/DATES/ACCESS | date-only MSK, 304/GET, one recurring complete, readonly tools, revoke read-back | — |
| PRESERVATION/UX | Legacy markdown hash; 390px live Playwright in `hermes-ux` | — |
| RESTART/RESTORE | Isolated manual copy + empty-target restore smoke | Mini restart post-login: **not-run** |
| CLEANUP/DELIVERY | Port/resource read-back; owned roots removed; instrumented no-development-run (entrypoint/subprocess run+Popen for str/bytes/sequence argv + process-dir + process-marker baseline) across UI/MCP/meeting channels | — |
| suite `all` receipts | Per-component durable journal under `<run_root>/journals/all-<id>/` with argv/rc/source/count/IDs/cleanup; `progress.json` (started/current/terminal) for SIGKILL reconciliation; aggregate SIGTERM/SIGHUP cancel owned reexec/component trees and in-process stands, wait for cleanup + ports read-back, then redeliver; Hermes reexec spawn+register is atomic vs those signals (blocked across Popen/registration) so a cancel cannot orphan a newly acquired child; ordinary reexec nonzero exits ingest the durable child receipt (run/IDs/cleanup + raw child rc) into the component failure receipt; active-stand `SystemExit(128+signum)` preserves raw rc, live synthetic IDs and cleanup evidence with terminal progress | — |

Historical P9/P10 evidence is **input only** and is **not** credited as
end-to-end acceptance of the current candidate SHA.

## Dependencies

- Python 3 standard library + project package `vikunja_local`
- C7 isolated stand profile (binary v2.8.0 SHA pinned in profile)
- Hermes python / FastAPI / uvicorn / Playwright only where `hermes-ux` re-execs
- No network package installs during verification
- `PYTHONDONTWRITEBYTECODE=1` for all Python imports/tests

## Limits (from C7 stand profile)

- Loopback only: Vikunja `19345`, fault proxy `19346`, synthetic Hermes `19347`
- Run-root: `/Users/efimovivan/.ai-platform/work/ea010-r2/<run_id>/` (0700 / 0600)
- Wall / disk / RSS caps as recorded in the stand profile (research limits, not product SLO)
- Synthetic prefix `EA010-SYNTHETIC-<run_id>`
- Occupied profile port → stop; do not kill foreign listeners

## Cleanup

Every suite (including fault/error/signal paths) must:

1. Revoke suite/session tokens with deny read-back where a live session exists
2. Stop only owned processes (aggregate SIGTERM/SIGHUP must reach registered reexec/component children and identity-checked owned stand processes before the aggregate exits)
3. Remove only owned synthetic roots / claim_owned_root trees
4. Confirm profile ports clear
5. Persist argv/rc/synthetic IDs/source digest in the run journal under run-root (including active-stand signal paths that exit via `SystemExit(128+signum)`)

Cleanup failure fails the run even when scenarios were green.

## Approved-pilot permit (exact)

Environment: `EA010_APPROVED_PILOT_PERMIT=/absolute/path/permit.json`

Required schemas:

- Permit: `ea010-approved-pilot-permit/v1`
- Embedded resources: `ea010-approved-pilot-resources/v1` with
  `service_bindings`, `owned_paths`, `run_root`
- Matching `resources_manifest_sha256`
- Fields: timezone-aware ISO-8601 `window.starts_at`/`window.ends_at` (reject
  malformed / inverted / expired / not-yet-open), non-empty
  `authorized_actions` that **cover the requested suite** (`<suite>`,
  `suite:<suite>`, `suite:*`, or `*`), non-empty `executor`
- Resources: non-empty typed `service_bindings` (non-null objects with
  non-empty `label`; optional host/port/origin), absolute non-empty
  `owned_paths`, absolute non-empty `run_root`
- Effective C7 profile `run_root` / listen target must bind inside the permit
  roots before any runner call
- Live authorisation additionally requires `allow_live_pilot=true` and
  verified `owner_action_receipts` for each of
  `owner_synthetic_acceptance`, `macbook_access_and_links`,
  `mini_restart_post_login` (`verified=true` plus `receipt_id` or `attested_at`)
- Matching `EA010_APPROVED_PILOT_EXECUTOR=<permit.executor>` still yields
  `authorised-not-executed-operational-contour-unverified` in this package:
  isolated runners are never invoked or relabeled as approved-pilot. Coding-worker
  / mismatched identity yields `authorised-not-executed-by-coding-worker`.
  Unknown suite names fail before permit success

Without the permit file, with an invalid/placeholder/expired/out-of-scope
permit, without verified checkpoint receipts, without a matching operational
executor identity, on early suite/profile/journal validation failures (including
unknown suite names), or when the operational contour is not executed: non-zero
rc, zero mutations, zero runner calls, **and no runtime-root journal /
environment writes**. Refusal evidence is emitted on stderr (JSON); optional
`EA010_REVIEW_EVIDENCE=<absolute path>` may capture the same payload only when
containment can be established and the resolved destination is absolute, free
of symlink aliases, and strictly outside every protected operational root:
effective profile `run_root`, permit `resources_manifest.run_root`, and each
`resources_manifest.owned_paths` entry (including owned service files outside
the C7 run root). Destinations under those roots (or symlink aliases into them)
are refused before any mkdir/write/chmod so existing runtime/owned files are
preserved. Existing multiply-linked destinations (`st_nlink > 1`) are also
refused, and successful writes always install a fresh inode via exclusive temp
+ `os.replace` so an outside hard-link alias cannot mutate permit-owned bytes
or mode through `write_text`/`chmod` on a shared inode. Parent create/open walks every directory component with `O_NOFOLLOW` descriptors
(create via mkdirat), and all temp create/chmod/replace/unlink steps use the
bound leaf descriptor (openat/renameat); a concurrent rename+symlink of a
validated review parent or an intermediate ancestor therefore cannot redirect
the write into a permit-owned root. Intermediate system aliases such as macOS
`/var -> /private/var` remain usable when their target stays outside protected
roots; user-controlled intermediate aliases into owned/runtime paths are refused. When the profile is missing or unusable, or a
configured permit path cannot establish its resource roots (including a refused
symlink permit whose owned_paths must still be protected via resolved read-only
discovery), containment cannot be established and all review-evidence file
writes are refused. The CLI must not call `append_journal` under the effective
profile `run_root` before exact authorization. Never credit `executed_count>0` /
`ok=true` for an unexecuted pilot. Active-exception / active-signal / reexec
helpers use `TemporaryDirectory` (or equivalent registered cleanup) so their
`EA010-SYNTHETIC-*` fixture roots are absent after every normal and error path.

`DevelopmentRunGuard` (no-development-run attestation) normalizes subprocess
`str` / `bytes` / `PathLike` argv separately from sequence forms so a shell
command string containing a forbidden token is recorded and fails the guard;
character-splitting a command string must not erase tokens.

## Manual checkpoints (this dispatch)

| Checkpoint | Status |
| --- | --- |
| `owner_synthetic_acceptance` | not-run |
| `macbook_access_and_links` | not-run |
| `mini_restart_post_login` | not-run |

Full working readiness is **not** claimed by s10 harness alone. Independent
QA/review and later owner/device runs remain separate.
