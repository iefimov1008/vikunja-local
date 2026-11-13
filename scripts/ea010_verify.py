#!/usr/bin/env python3
"""EA-010 verification runner. Suites fail closed; no skip-as-success."""

from __future__ import annotations

import argparse
import errno
import json
import os
import stat as stat_mod
import sys
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.support.stand import (  # noqa: E402
    StandError,
    StandProfile,
    append_journal,
    assert_suite_result_contract,
    load_profile,
    validate_journal_path,
)
from tests.support.suites import get_suite, list_suites  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EA-010 isolated verification runner")
    parser.add_argument("--suite", required=True, help="Suite name (unknown/empty => failure)")
    parser.add_argument(
        "--mode",
        default="isolated",
        choices=("isolated", "approved-pilot"),
        help="isolated (default-safe) or approved-pilot (fail-closed without exact permit)",
    )
    parser.add_argument(
        "--journal",
        default="",
        help="Optional argv/rc journal path under approved run-root",
    )
    return parser.parse_args(argv)


def _resolve_journal(profile, args: argparse.Namespace, *, suite_name: str, failure: bool) -> Path:
    if args.journal:
        return validate_journal_path(profile, args.journal)
    leaf = f"failure-{os.getpid()}.json" if failure else f"{suite_name}-{os.getpid()}.json"
    return validate_journal_path(profile, Path("journals") / leaf)


def _append_unique_root(roots: list[Path], seen: set[str], raw: Path | str) -> None:
    try:
        resolved = Path(raw).expanduser().resolve()
    except Exception:
        return
    key = str(resolved)
    if key in seen:
        return
    seen.add(key)
    roots.append(resolved)


def _best_effort_permit_resource_roots() -> tuple[list[Path], bool]:
    """Collect permit owned_paths / run_root without requiring full authorization.

    Returns ``(roots, configured_permit_roots_ok)``.

    When ``EA010_APPROVED_PILOT_PERMIT`` is unset, returns ``([], True)``.
    When set, reads the permit solely to discover protected resource roots —
    including resolving a leaf symlink for read-only discovery. This never
    authorizes the permit (authorization still refuses symlinks). If the
    configured path cannot establish a root set (relative, missing, dangling
    symlink, unreadable, or unparseable), returns ``([], False)`` so callers
    refuse optional evidence writes rather than protecting only the C7 profile.
    """
    raw = (os.environ.get("EA010_APPROVED_PILOT_PERMIT") or "").strip()
    if not raw:
        return [], True
    path = Path(raw).expanduser()
    try:
        if not path.is_absolute():
            return [], False
        # Resolve for read-only root discovery. Authorization still refuses
        # symlink permits; here we only need owned_paths / run_root material.
        try:
            reader = path.resolve(strict=True)
        except OSError:
            return [], False
        if not reader.is_file():
            return [], False
        payload = json.loads(reader.read_text(encoding="utf-8"))
    except Exception:
        return [], False
    if not isinstance(payload, dict):
        return [], False
    resources = payload.get("resources_manifest")
    if not isinstance(resources, dict):
        # Configured permit readable but declares no resource roots to protect.
        return [], True
    roots: list[Path] = []
    seen: set[str] = set()
    run_root = resources.get("run_root")
    if isinstance(run_root, str) and run_root.strip() and Path(run_root).is_absolute():
        _append_unique_root(roots, seen, run_root)
    owned = resources.get("owned_paths")
    if isinstance(owned, list):
        for item in owned:
            if isinstance(item, str) and item.strip() and Path(item).is_absolute():
                _append_unique_root(roots, seen, item)
    return roots, True


def _forbidden_review_roots(profile: StandProfile | None) -> tuple[list[Path], bool]:
    """Return (roots, containment_established).

    Protects the effective profile run_root and every absolute permit
    resources_manifest.run_root / owned_paths entry. When the profile is missing
    or its run_root cannot be resolved, or a configured permit cannot establish
    its resource roots, containment is not established and callers must refuse
    all review-evidence writes.
    """
    roots: list[Path] = []
    seen: set[str] = set()
    profile_ok = False
    if profile is not None:
        try:
            _append_unique_root(roots, seen, Path(profile.run_root))
            profile_ok = True
        except Exception:
            profile_ok = False
    permit_roots, permit_ok = _best_effort_permit_resource_roots()
    for root in permit_roots:
        _append_unique_root(roots, seen, root)
    return roots, profile_ok and permit_ok


def _path_is_under(child: Path, root: Path) -> bool:
    try:
        child.relative_to(root)
        return True
    except ValueError:
        return False


def _symlink_aliases_into_roots(path: Path, roots: list[Path]) -> bool:
    """True when *path* or an ancestor symlink resolves into a forbidden root.

    System path aliases such as macOS ``/var -> /private/var`` are ignored unless
    they cause the destination to land under a runtime/resource root.
    """
    probe = path
    while True:
        try:
            if probe.is_symlink():
                try:
                    target = probe.resolve()
                except OSError:
                    return True
                for root in roots:
                    if _path_is_under(target, root) or target == root:
                        return True
                # Leaf symlink destinations are always treated as aliases: the
                # caller must pass a concrete non-symlink path outside runtime.
                if probe == path:
                    return True
        except OSError:
            return True
        parent = probe.parent
        if parent == probe:
            break
        probe = parent
    return False


def validate_review_evidence_destination(
    raw: str, profile: StandProfile | None
) -> Path | None:
    """Return a safe absolute evidence path, or None when write must be refused.

    Destination must be absolute, free of symlink aliases into protected roots,
    and strictly outside every effective runtime/resource root (profile run_root
    and permit owned_paths/run_root) before any mkdir/write/chmod. Existing
    multiply-linked regular files are refused so shared inodes under owned roots
    cannot be mutated through an outside hard-link alias. When containment
    cannot be established (missing/unusable profile, or configured permit roots
    cannot be established), refuse.
    """
    text = (raw or "").strip()
    if not text:
        return None
    dest = Path(text).expanduser()
    if not dest.is_absolute():
        return None
    abs_dest = Path(os.path.abspath(str(dest)))
    roots, containment_ok = _forbidden_review_roots(profile)
    if not containment_ok:
        return None
    if _symlink_aliases_into_roots(abs_dest, roots):
        return None
    if _symlink_aliases_into_roots(abs_dest.parent, roots):
        return None
    try:
        resolved = abs_dest.resolve(strict=False)
        parent_resolved = abs_dest.parent.resolve(strict=False)
    except OSError:
        return None
    for root in roots:
        if (
            _path_is_under(abs_dest, root)
            or _path_is_under(resolved, root)
            or _path_is_under(parent_resolved, root)
            or abs_dest == root
            or resolved == root
            or parent_resolved == root
        ):
            return None
    try:
        if abs_dest.exists():
            if abs_dest.is_symlink() or not abs_dest.is_file():
                return None
            if abs_dest.stat().st_nlink > 1:
                return None
    except OSError:
        return None
    return abs_dest


def _open_dir_component(dir_fd: int | None, name: str) -> int:
    """Open a single directory component without following a leaf symlink."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        if dir_fd is None:
            return os.open(name, flags)
        return os.open(name, flags, dir_fd=dir_fd)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise OSError("review evidence parent path follows a symlink") from exc
        raise


def _component_is_symlink(dir_fd: int, name: str) -> bool:
    try:
        st = os.lstat(name, dir_fd=dir_fd)
    except OSError:
        return False
    return stat_mod.S_ISLNK(st.st_mode)


def _safe_system_alias_target(
    current: Path, name: str, *, dir_fd: int, forbidden_roots: list[Path]
) -> Path | None:
    """Return the resolved target of a component symlink when it is a safe alias.

    User-controlled intermediate symlink substitution into a forbidden
    runtime/owned root is refused. Stable system aliases such as macOS
    ``/var -> /private/var`` that do not land under a protected root are
    expanded so the subsequent O_NOFOLLOW component walk can continue.
    """
    try:
        link = os.readlink(name, dir_fd=dir_fd)
    except OSError:
        return None
    raw = Path(link)
    candidate = raw if raw.is_absolute() else (current / raw)
    try:
        abs_candidate = Path(os.path.abspath(str(candidate)))
        resolved = abs_candidate.resolve(strict=False)
    except OSError:
        return None
    # A symlink step is only treated as a system alias when neither the
    # absolute nor resolved target lands under a protected operational root.
    for probe in (abs_candidate, resolved):
        for root in forbidden_roots:
            if _path_is_under(probe, root) or probe == root:
                return None
        if _symlink_aliases_into_roots(probe, forbidden_roots):
            return None
    if not resolved.is_dir():
        return None
    return resolved


def _ensure_parent_dirfd(
    path: Path,
    mode: int = 0o700,
    *,
    forbidden_roots: list[Path] | None = None,
) -> int:
    """Create/open the parent directory via an O_NOFOLLOW component walk.

    Every path component is opened (and created) with directory descriptors so
    an intermediate-ancestor rename+symlink into a permit-owned root cannot be
    followed between validation and leaf open. Stable system aliases such as
    macOS ``/var -> /private/var`` are expanded only when their target stays
    outside every protected root. Later openat/renameat calls stay bound to the
    returned descriptor even if a path name is replaced.
    """
    absolute = Path(os.path.abspath(str(path)))
    roots = list(forbidden_roots or [])
    # Track the logical prefix for relative symlink resolution while walking.
    current = Path(absolute.anchor)
    fd = _open_dir_component(None, absolute.anchor)
    parts = list(absolute.parts[1:])
    index = 0
    try:
        while index < len(parts):
            part = parts[index]
            created = False
            next_fd = -1
            open_error: OSError | None = None
            try:
                next_fd = _open_dir_component(fd, part)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode, dir_fd=fd)
                    created = True
                except FileExistsError:
                    created = False
                except OSError as exc:
                    if exc.errno == errno.EEXIST:
                        created = False
                    elif exc.errno in {errno.ELOOP, errno.ENOTDIR} or _component_is_symlink(
                        fd, part
                    ):
                        open_error = OSError(
                            "review evidence parent path follows a symlink"
                        )
                        open_error.__cause__ = exc
                    else:
                        raise
                if open_error is None:
                    try:
                        next_fd = _open_dir_component(fd, part)
                    except FileNotFoundError as exc:
                        raise OSError(
                            "review evidence parent disappeared during create"
                        ) from exc
                    except OSError as exc:
                        open_error = exc
                        next_fd = -1
            except OSError as exc:
                open_error = exc
                next_fd = -1

            if open_error is not None or next_fd < 0:
                exc = open_error or OSError("review evidence parent path follows a symlink")
                if not (
                    getattr(exc, "errno", None) in {errno.ELOOP, errno.ENOTDIR}
                    or "follows a symlink" in str(exc)
                    or _component_is_symlink(fd, part)
                ):
                    raise exc
                alias = _safe_system_alias_target(
                    current, part, dir_fd=fd, forbidden_roots=roots
                )
                if alias is None:
                    raise OSError(
                        "review evidence parent path follows a symlink"
                    ) from exc
                os.close(fd)
                fd = _open_dir_component(None, alias.anchor)
                current = Path(alias.anchor)
                parts = list(alias.parts[1:]) + parts[index + 1 :]
                index = 0
                continue

            if created:
                try:
                    os.fchmod(next_fd, mode)
                except OSError:
                    # Pre-existing dirs may refuse chmod; writes still use the fd.
                    pass
            os.close(fd)
            fd = next_fd
            current = current / part
            index += 1
        owned = fd
        fd = -1
        return owned
    finally:
        if fd >= 0:
            os.close(fd)


def _write_review_evidence_atomically(
    dest: Path,
    body: str,
    *,
    forbidden_roots: list[Path] | None = None,
) -> None:
    """Install evidence on a fresh inode via a no-follow parent directory fd.

    Parent create/open binds every traversed directory with descriptors
    (O_NOFOLLOW per component, safe system aliases only). Temp create, chmod,
    replace, and cleanup all go through that bound descriptor (openat/renameat),
    so a concurrent intermediate-ancestor rename+symlink cannot redirect the
    write into a permit-owned root. Multiply-linked destinations are refused
    before any write.
    """
    parent_fd = _ensure_parent_dirfd(
        dest.parent, 0o700, forbidden_roots=forbidden_roots
    )
    tmp_name: str | None = None
    fd = -1
    try:
        try:
            existing_fd = os.open(
                dest.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd
            )
        except FileNotFoundError:
            existing_fd = -1
        except OSError as exc:
            if exc.errno == errno.ELOOP or dest.is_symlink():
                raise OSError("review evidence destination is not a regular file") from exc
            raise
        if existing_fd >= 0:
            try:
                st = os.fstat(existing_fd)
                if not stat_mod.S_ISREG(st.st_mode):
                    raise OSError("review evidence destination is not a regular file")
                if st.st_nlink > 1:
                    raise OSError("review evidence destination is multiply linked")
            finally:
                os.close(existing_fd)
        tmp_name = f".ea010-review-{os.getpid()}-{os.urandom(4).hex()}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        fd = os.open(tmp_name, flags, 0o600, dir_fd=parent_fd)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        os.replace(tmp_name, dest.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        tmp_name = None
    except Exception:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        if tmp_name is not None:
            try:
                os.unlink(tmp_name, dir_fd=parent_fd)
            except OSError:
                pass
        raise
    finally:
        os.close(parent_fd)


def _emit_approved_pilot_refusal(
    *,
    command: list[str],
    suite_name: str,
    mode: str,
    payload: dict[str, Any],
    profile: StandProfile | None,
    rc: int = 2,
) -> int:
    """Stderr-only refusal for unexecuted/unpermitted approved-pilot errors.

    Never appends a runtime journal. Optional EA010_REVIEW_EVIDENCE writes only
    after containment is established and the destination is outside every
    profile/permit runtime and owned resource root.
    """
    print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
    review_dest = (os.environ.get("EA010_REVIEW_EVIDENCE") or "").strip()
    if review_dest:
        dest = validate_review_evidence_destination(review_dest, profile)
        if dest is not None:
            try:
                evidence = {
                    "schema": "ea010-approved-pilot-refusal/v1",
                    "argv": ["python3", "scripts/ea010_verify.py", *command],
                    "rc": rc,
                    "suite": suite_name,
                    "mode": mode,
                    "payload": payload,
                }
                body = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
                # Fresh inode via exclusive temp + replace; never write_text/chmod
                # an existing inode that may be hard-linked into owned roots.
                # Re-bind forbidden roots at write time so the component walk can
                # refuse intermediate-ancestor aliases into owned/runtime paths.
                roots, _ok = _forbidden_review_roots(profile)
                _write_review_evidence_atomically(
                    dest, body, forbidden_roots=roots
                )
            except Exception:
                pass
    return rc


def _approved_pilot_error_payload(
    *,
    error: str,
    status: str = "not-run",
    details: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": error,
        "status": status,
        "details": details or {},
        "known_suites": list_suites(),
        "mutations": False,
        "executed_count": (details or {}).get("executed_count", 0),
        "runner_calls": (details or {}).get("runner_calls", 0),
        "runtime_root_writes": False,
    }
    if extra:
        payload.update(extra)
    return payload


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv if argv is None else argv)
    # Preserve real process argv for the journal even when main() is called with a list.
    command = argv[1:] if argv and argv[0].endswith("ea010_verify.py") else argv
    try:
        args = parse_args(command)
    except SystemExit as exc:
        code = int(exc.code or 1)
        return code if code != 0 else 1

    suite_name = args.suite
    mode = args.mode
    rc = 1
    payload: dict = {"ok": False}
    journal_path: Path | None = None
    profile = None
    try:
        if not suite_name or not str(suite_name).strip():
            raise StandError("Suite must be specified; empty suite is a failure")
        profile = load_profile()

        if mode == "approved-pilot":
            # Fail closed before any environment/service mutation when permit is
            # absent/invalid/expired/out-of-scope, coding-worker policy refuses,
            # early suite/profile/journal validation fails, or no real operational
            # contour is executed/read back. Never append_journal under the
            # runtime run-root for unexecuted/unpermitted refusals.
            from tests.support.pilot_suite import PilotRefused, run_approved_pilot

            try:
                # Unknown/empty suite fails closed before permit success; keep the
                # lookup inside the approved-pilot refusal path so the common
                # error footer cannot create a runtime journal.
                get_suite(suite_name)
                result = run_approved_pilot(
                    mode=mode,
                    worktree=ROOT,
                    argv=["python3", "scripts/ea010_verify.py", *command],
                    probe_roots=[profile.run_root],
                    suite=str(suite_name),
                    profile=profile,
                )
            except PilotRefused as refused:
                payload = _approved_pilot_error_payload(
                    error=str(refused),
                    status=refused.status,
                    details=refused.details,
                )
                return _emit_approved_pilot_refusal(
                    command=command,
                    suite_name=str(suite_name),
                    mode=mode,
                    payload=payload,
                    profile=profile,
                    rc=2,
                )
            except StandError as exc:
                payload = _approved_pilot_error_payload(
                    error=str(exc),
                    status="not-run",
                    details={"reason": "approved-pilot early validation failure"},
                )
                return _emit_approved_pilot_refusal(
                    command=command,
                    suite_name=str(suite_name),
                    mode=mode,
                    payload=payload,
                    profile=profile,
                    rc=2,
                )
            except Exception as exc:  # noqa: BLE001 - surface as refusal, no runtime write
                payload = _approved_pilot_error_payload(
                    error=f"unexpected: {exc}",
                    status="not-run",
                    details={"reason": "approved-pilot unexpected failure before execution"},
                    extra={"traceback": traceback.format_exc()},
                )
                return _emit_approved_pilot_refusal(
                    command=command,
                    suite_name=str(suite_name),
                    mode=mode,
                    payload=payload,
                    profile=profile,
                    rc=1,
                )
            # A future operational-contour executor may return a real suite
            # payload here; this package currently refuses before that path.
            journal_path = _resolve_journal(profile, args, suite_name=str(suite_name), failure=False)
        else:
            # Containment/Git/symlink checks happen before any suite execution or write.
            journal_path = _resolve_journal(profile, args, suite_name=str(suite_name), failure=False)
            runner = get_suite(suite_name)
            result = runner(mode=mode, worktree=ROOT, argv=["python3", "scripts/ea010_verify.py", *command])

        result = assert_suite_result_contract(result)
        payload = {"ok": True, "result": result, "known_suites": list_suites()}
        print(json.dumps(payload, indent=2, sort_keys=True))
        rc = 0
        append_journal(
            journal_path,
            argv=["python3", "scripts/ea010_verify.py", *command],
            rc=rc,
            suite=suite_name,
            mode=mode,
            profile=profile,
            extra={
                "run_id": (result or {}).get("run_id"),
                "synthetic_ids": (result or {}).get("synthetic_ids"),
                "source": (result or {}).get("source"),
                "executed_count": (result or {}).get("executed_count"),
            },
        )
        return rc
    except StandError as exc:
        # Isolated-mode failures (and approved-pilot only if somehow escaped the
        # dedicated branch) still journal. Approved-pilot must never reach here
        # for unexecuted refusals — that branch returns above.
        if mode == "approved-pilot":
            payload = _approved_pilot_error_payload(
                error=str(exc),
                status="not-run",
                details={"reason": "approved-pilot refused without runtime journal"},
            )
            return _emit_approved_pilot_refusal(
                command=command,
                suite_name=str(getattr(args, "suite", "") or ""),
                mode=mode,
                payload=payload,
                profile=profile,
                rc=2,
            )
        payload = {"ok": False, "error": str(exc), "known_suites": list_suites()}
        print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        rc = 2
    except Exception as exc:  # noqa: BLE001 - surface unexpected failures as runner errors
        if mode == "approved-pilot":
            payload = _approved_pilot_error_payload(
                error=f"unexpected: {exc}",
                status="not-run",
                details={"reason": "approved-pilot unexpected failure"},
                extra={"traceback": traceback.format_exc()},
            )
            return _emit_approved_pilot_refusal(
                command=command,
                suite_name=str(getattr(args, "suite", "") or ""),
                mode=mode,
                payload=payload,
                profile=profile,
                rc=1,
            )
        payload = {
            "ok": False,
            "error": f"unexpected: {exc}",
            "traceback": traceback.format_exc(),
            "known_suites": list_suites(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True), file=sys.stderr)
        rc = 1
    # Isolated error footer only — approved-pilot returns above without writes.
    try:
        if profile is None:
            profile = load_profile()
        if journal_path is None:
            journal_path = _resolve_journal(
                profile,
                args,
                suite_name=str(getattr(args, "suite", "failure") or "failure"),
                failure=True,
            )
        else:
            journal_path = validate_journal_path(profile, journal_path)
        append_journal(
            journal_path,
            argv=["python3", "scripts/ea010_verify.py", *command],
            rc=rc,
            suite=str(getattr(args, "suite", "")),
            mode=str(getattr(args, "mode", "")),
            profile=profile,
            extra={"error": payload.get("error")},
        )
    except Exception:
        pass
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
