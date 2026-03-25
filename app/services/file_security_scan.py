"""
Optional security checks on supplier files before FTP staging or local ingest.

Controlled by SUPPLIER_FETCH_SECURITY_SCAN (default: basic):
  none | off | 0  — skip
  basic            — max size, optional extension allowlist, ZIP magic for office-like names
  clamav           — run `clamscan` (must be on PATH)

Related env:
  SUPPLIER_FETCH_SCAN_MAX_BYTES       default 524288000 (500 MiB)
  SUPPLIER_FETCH_SCAN_ALLOW_EXTENSIONS  comma list e.g. txt,xlsx,xls,csv (empty = no extension filter)
"""

from __future__ import annotations

import fnmatch
import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional


def fnmatch_ci(name: str, pattern: str) -> bool:
    return fnmatch.fnmatch(name.lower(), pattern.lower())


def _scan_basic(path: Path, max_bytes: int, allow_extensions: Optional[List[str]]) -> None:
    st = path.stat()
    if st.st_size > max_bytes:
        raise SystemExit(f"basic scan ({path.name}): file too large ({st.st_size} > {max_bytes})")
    if allow_extensions:
        suf = path.suffix.lower().lstrip(".")
        allowed = {x.lower().lstrip(".") for x in allow_extensions}
        if suf not in allowed:
            raise SystemExit(
                f"basic scan ({path.name}): extension .{suf} not in allowlist {sorted(allowed)}"
            )
    with open(path, "rb") as f:
        head = f.read(8)
    if path.suffix.lower() in {".xlsx", ".xlsm", ".docx", ".pptx", ".zip"}:
        if not head.startswith(b"PK\x03\x04"):
            raise SystemExit(f"basic scan ({path.name}): expected ZIP magic for office-like extension")


def _scan_clamav(path: Path, label: str) -> None:
    exe = shutil.which("clamscan")
    if not exe:
        raise SystemExit(f"clamav scan ({label}): `clamscan` not on PATH")
    r = subprocess.run(
        [exe, "--no-summary", str(path)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if r.returncode == 1:
        raise SystemExit(
            f"clamav ({label}): infected or suspicious — {r.stdout.strip() or r.stderr.strip()}"
        )
    if r.returncode != 0:
        raise SystemExit(
            f"clamav ({label}): error exit {r.returncode} — {r.stderr.strip() or r.stdout.strip()}"
        )


def run_security_scan(
    path: str | Path,
    *,
    mode: str,
    label: str,
    max_bytes: Optional[int] = None,
    allow_extensions: Optional[List[str]] = None,
) -> None:
    """Run a single scan mode (basic | clamav | none). Raises SystemExit on failure."""
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"security scan ({label}): not a file: {p}")
    m = (mode or "none").strip().lower()
    if m in {"", "none", "off", "0", "false"}:
        return
    if m == "basic":
        mb = max_bytes if max_bytes is not None else int(
            os.getenv("SUPPLIER_FETCH_SCAN_MAX_BYTES", str(500 * 1024 * 1024))
        )
        _scan_basic(p, mb, allow_extensions)
        print(f"[security] basic OK: {label} ({p.name})")
    elif m == "clamav":
        _scan_clamav(p, label)
        print(f"[security] clamav OK: {label} ({p.name})")
    else:
        raise SystemExit(f"security scan: unknown mode {mode!r}")


def scan_after_supplier_fetch(local_path: str, label: str) -> None:
    """
    Scan a file after FTP download (or before import), using SUPPLIER_FETCH_SECURITY_SCAN.
    Default mode is 'basic' unless env sets none.
    """
    raw = os.getenv("SUPPLIER_FETCH_SECURITY_SCAN", "basic").strip().lower()
    if raw in {"", "none", "off", "0", "false"}:
        return
    allow_raw = os.getenv("SUPPLIER_FETCH_SCAN_ALLOW_EXTENSIONS", "").strip()
    allow = [x.strip() for x in allow_raw.split(",") if x.strip()] if allow_raw else None
    run_security_scan(local_path, mode=raw, label=label, allow_extensions=allow)
