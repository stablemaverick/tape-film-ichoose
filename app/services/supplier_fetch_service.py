"""
Fetch latest Moovies and Lasgo supplier files from FTP (pipeline step 00).

Strict catalog fetch: a supplier folder with no matching files yields skipped_no_files for that
supplier only; the other supplier is still fetched and the pipeline can complete.

Lasgo SFTP → FTP mirror (optional):
  When LASGO_SFTP_MIRROR_ENABLED=1, before any Lasgo FTP download we pull the latest
  matching file from the vendor SFTP tree, run the security scan, and STOR it onto our
  Lasgo FTP inbox (stock or catalog path, matching --mode). Moovies is always FTP-only.
  Stock and catalog use the same SFTP host/port/user/auth; only LASGO_SFTP_*_REMOTE_DIR /
  *_GLOB and the FTP staging path differ. Stock fetch uses swallow_errors on mirror failure
  so FTP→local still runs; catalog strict fetch still fails the step if the mirror fails.
  See app.services.lasgo_sftp_mirror for timeouts (LASGO_SFTP_AUTH_TIMEOUT_SEC, etc.).

Security scan before downstream import:
  After each file is written to disk (Moovies + Lasgo, stock and catalog paths),
  scan_after_supplier_fetch() runs using SUPPLIER_FETCH_SECURITY_SCAN (default: basic).
  Failed scans abort step 00 so imports never see unchecked files.

Set MOOVIES_FTP_* (and optionally LASGO_FTP_* for a second server). Lasgo falls back to
MOOVIES_FTP_* after shared FTP_* / SFTP_* so both suppliers can use the same host with
only Moovies vars set.

CLI shim: scripts/fetch_supplier_files.py (optional)
Pipeline: pipeline/00_fetch_supplier_files.py -> run_from_argv()
"""

from __future__ import annotations

import ftplib
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Dict, Literal, Optional, Tuple

CatalogSourceFetchStatus = Literal["success", "skipped_no_files"]

from dotenv import load_dotenv

from app.services.file_security_scan import fnmatch_ci as _fnmatch_ci

FetchMode = Literal["stock", "catalog"]
FtpSupplier = Literal["moovies", "lasgo"]

# Production catalog FTP layout (override only via MOOVIES_CATALOG_REMOTE_DIR / LASGO_CATALOG_REMOTE_DIR if needed).
CATALOG_FTP_MOOVIES_DIR = "/TAPE_Film/Moovies/Catalog"
CATALOG_FTP_LASGO_DIR = "/TAPE_Film/Lasgo/Catalog"
CATALOG_FTP_ARCHIVE_SUBDIR = "Archive"


def choose_latest_file(ftp: ftplib.FTP, remote_dir: str, pattern: str) -> Optional[Tuple[str, int]]:
    latest_name: Optional[str] = None
    latest_mtime = -1

    try:
        ftp.cwd(remote_dir)
    except Exception as e:
        print(f"Failed to change directory to '{remote_dir}': {e}", file=sys.stderr)
        raise SystemExit(1)

    used_mlsd = False
    try:
        for name, facts in ftp.mlsd():
            if not _fnmatch_ci(name, pattern):
                continue
            if facts.get("type") and facts.get("type") != "file":
                continue
            modify = facts.get("modify", "")
            mtime = int(modify) if modify.isdigit() else -1
            if mtime > latest_mtime:
                latest_name = name
                latest_mtime = mtime
        used_mlsd = True
    except Exception:
        pass

    if not used_mlsd:
        candidates = []
        try:
            candidates = [n for n in ftp.nlst() if _fnmatch_ci(n, pattern)]
        except Exception:
            listing: list[str] = []
            ftp.retrlines("LIST", listing.append)
            for line in listing:
                name = line.split(maxsplit=8)[-1].strip() if line.strip() else ""
                if name and _fnmatch_ci(name, pattern):
                    candidates.append(name)

        for name in candidates:
            mtime = -1
            try:
                resp = ftp.sendcmd(f"MDTM {name}")
                parts = resp.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    mtime = int(parts[1])
            except Exception:
                pass
            if mtime > latest_mtime:
                latest_name = name
                latest_mtime = mtime

    if latest_name is None:
        return None
    return latest_name, latest_mtime


def fetch_latest(
    ftp: ftplib.FTP,
    remote_dir: str,
    pattern: str,
    local_dir: str,
    supplier_name: str,
) -> Optional[str]:
    last_err: Optional[Exception] = None
    chosen = None
    for _ in range(3):
        try:
            chosen = choose_latest_file(ftp, remote_dir, pattern)
            break
        except Exception as e:
            last_err = e
            time.sleep(1)
    if chosen is None and last_err is not None:
        print(
            f"[{supplier_name}] Failed listing '{remote_dir}' with pattern '{pattern}': "
            f"{type(last_err).__name__}: {last_err}"
        )
        return None
    if not chosen:
        print(f"[{supplier_name}] No files matched pattern '{pattern}' in {remote_dir}")
        return None

    filename, _mtime = chosen
    Path(local_dir).mkdir(parents=True, exist_ok=True)
    local_path = str(Path(local_dir) / filename)

    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {filename}", f.write)
    print(f"[{supplier_name}] Downloaded: {remote_dir.rstrip('/')}/{filename} -> {local_path}")
    from app.services.file_security_scan import scan_after_supplier_fetch

    scan_after_supplier_fetch(local_path, supplier_name)
    return local_path


def _supplier_ftp_prefix(supplier: Optional[FtpSupplier]) -> str:
    if supplier == "moovies":
        return "MOOVIES_"
    if supplier == "lasgo":
        return "LASGO_"
    return ""


def _ftp_setting(prefix: str, key: str) -> Optional[str]:
    """
    Read PREFIXFTP_KEY (e.g. MOOVIES_FTP_HOST), then shared FTP_* / legacy SFTP_*.

    Lasgo then falls back to MOOVIES_FTP_* (same server as Moovies); Moovies falls back to
    LASGO_FTP_* for symmetry. Omit per-supplier vars to use one host for both.
    """
    if prefix:
        v = os.getenv(f"{prefix}FTP_{key}")
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    v = os.getenv(f"FTP_{key}")
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    if key == "HOST":
        h = os.getenv("SFTP_HOST")
        if h and str(h).strip():
            return h.strip()
    elif key == "USER":
        u = os.getenv("SFTP_USER")
        if u and str(u).strip():
            return u.strip()
    elif key == "PASSWORD":
        p = os.getenv("SFTP_PASSWORD")
        if p is not None:
            return p
    if prefix == "LASGO_":
        v = os.getenv(f"MOOVIES_FTP_{key}")
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    elif prefix == "MOOVIES_":
        v = os.getenv(f"LASGO_FTP_{key}")
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def _ftp_port(prefix: str) -> int:
    raw = _ftp_setting(prefix, "PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return 21


def _parse_host_inline_port(host_raw: str) -> Tuple[str, Optional[int]]:
    """
    Allow MOOVIES_FTP_HOST=1.2.3.4:21 or [::1]:21 — return (host_only, port or None).
    If no :port suffix, returns (host_raw stripped, None).
    """
    s = (host_raw or "").strip()
    if not s:
        return "", None
    if s.startswith("["):
        close = s.find("]:")
        if close != -1:
            try:
                return s[: close + 1], int(s[close + 2 :])
            except ValueError:
                return s, None
        return s, None
    if s.count(":") == 1:
        h, _, ps = s.partition(":")
        try:
            p = int(ps)
            if 1 <= p <= 65535:
                return h, p
        except ValueError:
            pass
    return s, None


def _ftp_port_explicitly_set(prefix: str) -> bool:
    if prefix:
        return f"{prefix}FTP_PORT" in os.environ
    return "FTP_PORT" in os.environ


def _ftp_host_and_connect_port(prefix: str) -> Tuple[str, int]:
    """Resolve host string and TCP port (inline :port in HOST unless FTP_PORT env overrides)."""
    raw = _ftp_setting(prefix, "HOST") or ""
    host_only, inline_port = _parse_host_inline_port(raw)
    port = _ftp_port(prefix)
    if not _ftp_port_explicitly_set(prefix) and inline_port is not None:
        port = inline_port
    return host_only, port


def _connect_ftp(host: str, user: str, password: str, port: int, tls_mode: str) -> ftplib.FTP:
    def connect_plain() -> ftplib.FTP:
        ftp = ftplib.FTP()
        ftp.connect(host=host, port=port, timeout=30)
        ftp.login(user=user, passwd=password)
        return ftp

    def connect_tls() -> ftplib.FTP:
        ftp = ftplib.FTP_TLS()
        ftp.connect(host=host, port=port, timeout=30)
        ftp.login(user=user, passwd=password)
        ftp.prot_p()
        return ftp

    if tls_mode in {"1", "true", "yes", "tls"}:
        return connect_tls()
    if tls_mode in {"0", "false", "no", "plain"}:
        return connect_plain()

    try:
        return connect_plain()
    except Exception:
        return connect_tls()


def build_ftp_client(*, supplier: Optional[FtpSupplier] = None) -> ftplib.FTP:
    """
    Connect to FTP. Per-supplier MOOVIES_FTP_* / LASGO_FTP_* first, then shared FTP_* / SFTP_*.
    Lasgo then inherits MOOVIES_FTP_* (and Moovies can inherit LASGO_FTP_*) so one server works for both.
    If supplier is None, only FTP_* / SFTP_* are used (legacy single-server).
    """
    pre = _supplier_ftp_prefix(supplier)
    host, port = _ftp_host_and_connect_port(pre)
    user = _ftp_setting(pre, "USER")
    password = _ftp_setting(pre, "PASSWORD")
    tls_raw = _ftp_setting(pre, "USE_TLS")
    tls_mode = (tls_raw or "auto").strip().lower()

    if not host:
        if supplier == "moovies":
            raise SystemExit("Missing Moovies FTP host (MOOVIES_FTP_HOST or FTP_HOST)")
        if supplier == "lasgo":
            raise SystemExit("Missing Lasgo FTP host (LASGO_FTP_HOST or FTP_HOST)")
        raise SystemExit("Missing FTP_HOST in .env")
    if not user:
        if supplier == "moovies":
            raise SystemExit("Missing Moovies FTP user (MOOVIES_FTP_USER or FTP_USER)")
        if supplier == "lasgo":
            raise SystemExit("Missing Lasgo FTP user (LASGO_FTP_USER or FTP_USER)")
        raise SystemExit("Missing FTP_USER in .env")
    if not password:
        if supplier == "moovies":
            raise SystemExit("Missing Moovies FTP password (MOOVIES_FTP_PASSWORD or FTP_PASSWORD)")
        if supplier == "lasgo":
            raise SystemExit("Missing Lasgo FTP password (LASGO_FTP_PASSWORD or FTP_PASSWORD)")
        raise SystemExit("Missing FTP_PASSWORD in .env")

    return _connect_ftp(host, user, password, port, tls_mode)


def _default_moovies_stock_remote() -> str:
    return os.getenv("MOOVIES_REMOTE_DIR", "/TAPE_Film/Moovies/Inventory")


def _default_lasgo_stock_remote() -> str:
    return os.getenv("LASGO_REMOTE_DIR", "/TAPE_Film/Lasgo/Incoming")


def _default_moovies_catalog_remote() -> str:
    return os.getenv("MOOVIES_CATALOG_REMOTE_DIR", CATALOG_FTP_MOOVIES_DIR).strip() or CATALOG_FTP_MOOVIES_DIR


def _default_lasgo_catalog_remote() -> str:
    return os.getenv("LASGO_CATALOG_REMOTE_DIR", CATALOG_FTP_LASGO_DIR).strip() or CATALOG_FTP_LASGO_DIR


def _local_under_cwd(*parts: str) -> str:
    return str(Path.cwd().joinpath(*parts))


def _move_remote_into_archive(
    ftp: ftplib.FTP,
    parent_remote_dir: str,
    filename: str,
    *,
    archive_subdir: str = CATALOG_FTP_ARCHIVE_SUBDIR,
) -> None:
    """Move a file under parent_remote_dir into parent_remote_dir/Archive/."""
    ftp.cwd(parent_remote_dir)
    try:
        ftp.mkd(archive_subdir)
    except ftplib.error_perm:
        pass
    dest = f"{archive_subdir}/{filename}"
    try:
        if hasattr(ftp, "rename"):
            ftp.rename(filename, dest)
        else:
            raise AttributeError
    except (AttributeError, ftplib.error_perm):
        ftp.sendcmd(f"RNFR {filename}")
        ftp.sendcmd(f"RNTO {dest}")
    print(f"[FTP] Archived on server: {parent_remote_dir.rstrip('/')}/{filename} -> {dest}")


def _read_kv_file(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    p = Path(path)
    if not p.is_file():
        return out
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip("'\"")
    return out


def run_catalog_archive_from_env(*, fetch_env_path: str, env_file: str = ".env") -> None:
    """
    Move catalog files recorded in fetch env into .../Catalog/Archive on FTP.
    Archives each supplier only when CATALOG_FTP_* remote + file are present (skipped sources omitted).
    """
    load_dotenv(env_file)
    data = _read_kv_file(fetch_env_path)

    moovies_remote = (data.get("CATALOG_FTP_MOOVIES_REMOTE") or "").strip()
    moovies_file = (data.get("CATALOG_FTP_MOOVIES_FILE") or "").strip().strip("'\"")
    lasgo_remote = (data.get("CATALOG_FTP_LASGO_REMOTE") or "").strip()
    lasgo_file = (data.get("CATALOG_FTP_LASGO_FILE") or "").strip().strip("'\"")

    if not moovies_remote and not lasgo_remote:
        print(
            f"catalog archive: no CATALOG_FTP_* remotes in {fetch_env_path}; nothing to archive (ok)"
        )
        return

    did_any = False

    if moovies_remote and moovies_file:
        ftp_m = build_ftp_client(supplier="moovies")
        try:
            _move_remote_into_archive(ftp_m, moovies_remote, moovies_file)
            did_any = True
        finally:
            try:
                ftp_m.quit()
            except Exception:
                ftp_m.close()
    else:
        print("catalog archive: skipping Moovies (no file fetched this run)")

    if lasgo_remote and lasgo_file:
        ftp_l = build_ftp_client(supplier="lasgo")
        try:
            _move_remote_into_archive(ftp_l, lasgo_remote, lasgo_file)
            did_any = True
        finally:
            try:
                ftp_l.quit()
            except Exception:
                ftp_l.close()
    else:
        print("catalog archive: skipping Lasgo (no file fetched this run)")

    if not did_any:
        print("catalog archive: no files to move (all sources had no fetch this run; ok)")
        return


def _norm_source_status(raw: str) -> CatalogSourceFetchStatus:
    s = (raw or "").strip()
    if s == "success":
        return "success"
    return "skipped_no_files"


def catalog_sync_source_summary_message(
    *,
    lasgo: str,
    moovies: str,
) -> str:
    """Single-line human summary for logs, history, and Slack (Lasgo then Moovies)."""
    ls = _norm_source_status(lasgo)
    ms = _norm_source_status(moovies)
    parts = ["Catalog sync completed."]
    if ls == "success":
        parts.append("Lasgo catalog processed successfully.")
    else:
        parts.append("No Lasgo catalog files were available to process.")
    if ms == "success":
        parts.append("Moovies catalog processed successfully.")
    else:
        parts.append("No Moovies catalog files were available to process.")
    return " ".join(parts)


def run_catalog_fetch_strict(*, env_file: str = ".env", write_fetch_env: str) -> None:
    """
    Catalog sync: download latest Moovies + Lasgo files from fixed TAPE_Film catalog paths only.
    A source with no matching file is recorded as skipped_no_files (not a fatal error).
    Writes a shell-sourceable env file with local paths, per-source status, and archive keys
    only for downloaded files.

    Runs Lasgo SFTP→FTP mirror first when LASGO_SFTP_MIRROR_ENABLED; scans each local
    file immediately after download (before writing fetch manifest).
    """
    load_dotenv(env_file)

    from app.services.file_security_scan import scan_after_supplier_fetch
    from app.services.lasgo_sftp_mirror import mirror_lasgo_sftp_to_ftp_if_enabled

    # Catalog strict: mirror failure must abort (no silent stale catalog).
    mirror_lasgo_sftp_to_ftp_if_enabled("catalog", env_file=env_file)

    moovies_remote_dir = _default_moovies_catalog_remote()
    lasgo_remote_dir = _default_lasgo_catalog_remote()
    moovies_pattern = os.getenv("MOOVIES_CATALOG_GLOB") or os.getenv("MOOVIES_GLOB", "Feed-*")
    lasgo_pattern = os.getenv("LASGO_CATALOG_GLOB") or os.getenv("LASGO_GLOB", "LASGO_*")
    moovies_local_dir = os.getenv("MOOVIES_CATALOG_DIR") or _local_under_cwd(
        "supplier_exports", "moovies", "catalog"
    )
    lasgo_local_dir = os.getenv("LASGO_CATALOG_DIR") or _local_under_cwd(
        "supplier_exports", "lasgo", "catalog"
    )

    lasgo_status: CatalogSourceFetchStatus = "skipped_no_files"
    moovies_status: CatalogSourceFetchStatus = "skipped_no_files"
    lasgo_local = ""
    lasgo_name = ""
    moovies_local = ""
    moovies_name = ""

    ftp_l = build_ftp_client(supplier="lasgo")
    try:
        chosen_l = choose_latest_file(ftp_l, lasgo_remote_dir, lasgo_pattern)
        if chosen_l is None:
            print(
                f"[LASGO(catalog)] No catalog files to process (skipped) — remote path {lasgo_remote_dir} "
                f"(pattern {lasgo_pattern!r})"
            )
        else:
            lasgo_name, _ = chosen_l
            Path(lasgo_local_dir).mkdir(parents=True, exist_ok=True)
            lasgo_local = str(Path(lasgo_local_dir) / lasgo_name)
            with open(lasgo_local, "wb") as f:
                ftp_l.retrbinary(f"RETR {lasgo_name}", f.write)
            print(
                f"[LASGO(catalog)] Downloaded: {lasgo_remote_dir.rstrip('/')}/{lasgo_name} -> {lasgo_local}"
            )
            scan_after_supplier_fetch(lasgo_local, "LASGO(catalog)")
            lasgo_status = "success"
    finally:
        try:
            ftp_l.quit()
        except Exception:
            ftp_l.close()

    ftp_m = build_ftp_client(supplier="moovies")
    try:
        chosen_m = choose_latest_file(ftp_m, moovies_remote_dir, moovies_pattern)
        if chosen_m is None:
            print(
                f"[MOOVIES(catalog)] No catalog files to process (skipped) — remote path {moovies_remote_dir} "
                f"(pattern {moovies_pattern!r})"
            )
        else:
            moovies_name, _ = chosen_m
            Path(moovies_local_dir).mkdir(parents=True, exist_ok=True)
            moovies_local = str(Path(moovies_local_dir) / moovies_name)
            with open(moovies_local, "wb") as f:
                ftp_m.retrbinary(f"RETR {moovies_name}", f.write)
            print(
                f"[MOOVIES(catalog)] Downloaded: {moovies_remote_dir.rstrip('/')}/{moovies_name} -> {moovies_local}"
            )
            scan_after_supplier_fetch(moovies_local, "MOOVIES(catalog)")
            moovies_status = "success"
    finally:
        try:
            ftp_m.quit()
        except Exception:
            ftp_m.close()

    lines = [
        f"CATALOG_SOURCE_LASGO_STATUS={lasgo_status}",
        f"CATALOG_SOURCE_MOOVIES_STATUS={moovies_status}",
    ]
    if lasgo_status == "success" and lasgo_local:
        lines.append(f"LASGO_FILE={shlex.quote(lasgo_local)}")
        lines.append(f"CATALOG_FTP_LASGO_REMOTE={lasgo_remote_dir}")
        lines.append(f"CATALOG_FTP_LASGO_FILE={shlex.quote(lasgo_name)}")
    else:
        lines.append("LASGO_FILE=")

    if moovies_status == "success" and moovies_local:
        lines.append(f"MOOVIES_FILE={shlex.quote(moovies_local)}")
        lines.append(f"CATALOG_FTP_MOOVIES_REMOTE={moovies_remote_dir}")
        lines.append(f"CATALOG_FTP_MOOVIES_FILE={shlex.quote(moovies_name)}")
    else:
        lines.append("MOOVIES_FILE=")

    out_p = Path(write_fetch_env)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote fetch manifest for archive step: {write_fetch_env}")

    print(f"CATALOG_SYNC_SOURCE_STATUS lasgo={lasgo_status} moovies={moovies_status}")


def run_fetch(*, env_file: str = ".env", mode: FetchMode = "stock") -> None:
    """
    Download latest Moovies + Lasgo files.

    Order: (1) Lasgo SFTP→FTP mirror if enabled for this mode; (2) Lasgo FTP→local;
    (3) Moovies FTP→local. Each local file is scanned before returning.

    mode=stock  — operational / inventory feeds (default). Uses *_{STOCK,REMOTE}_* envs.
    mode=catalog — full catalog drops: remotes default to .../Moovies/Catalog and .../Lasgo/Catalog
                   (never inventory paths). Local dirs default to supplier_exports/*/catalog only;
                   never falls back to MOOVIES_STOCK_DIR / LASGO_STOCK_DIR.
    """
    load_dotenv(env_file)

    from app.services.lasgo_sftp_mirror import mirror_lasgo_sftp_to_ftp

    # Stock: tolerate SFTP mirror errors so Lasgo FTP → local still runs (true E2E when FTP is fresh).
    # Catalog (this same function, mode=catalog): mirror errors abort the step.
    mirror_lasgo_sftp_to_ftp(
        mode,
        env_file=env_file,
        require_mirror_enabled=True,
        swallow_errors=(mode == "stock"),
    )

    if mode == "stock":
        moovies_remote_dir = os.getenv("MOOVIES_STOCK_REMOTE_DIR") or _default_moovies_stock_remote()
        lasgo_remote_dir = os.getenv("LASGO_STOCK_REMOTE_DIR") or _default_lasgo_stock_remote()
        moovies_pattern = os.getenv("MOOVIES_STOCK_GLOB") or os.getenv("MOOVIES_GLOB", "Feed-*")
        lasgo_pattern = os.getenv("LASGO_STOCK_GLOB") or os.getenv("LASGO_GLOB", "LASGO_*")
        moovies_local_dir = os.getenv("MOOVIES_STOCK_DIR", "/opt/tape-film/sftp/moovies")
        lasgo_local_dir = os.getenv("LASGO_STOCK_DIR", "/opt/tape-film/sftp/lasgo")
        tag_m = "MOOVIES"
        tag_l = "LASGO"
    else:
        # Catalog mode: never fall back to inventory remotes or stock local dirs (prevents wrong file family).
        moovies_remote_dir = _default_moovies_catalog_remote()
        lasgo_remote_dir = _default_lasgo_catalog_remote()
        moovies_pattern = os.getenv("MOOVIES_CATALOG_GLOB") or os.getenv("MOOVIES_GLOB", "Feed-*")
        lasgo_pattern = os.getenv("LASGO_CATALOG_GLOB") or os.getenv("LASGO_GLOB", "LASGO_*")
        moovies_local_dir = os.getenv("MOOVIES_CATALOG_DIR") or _local_under_cwd(
            "supplier_exports", "moovies", "catalog"
        )
        lasgo_local_dir = os.getenv("LASGO_CATALOG_DIR") or _local_under_cwd(
            "supplier_exports", "lasgo", "catalog"
        )
        tag_m = "MOOVIES(catalog)"
        tag_l = "LASGO(catalog)"

    ftp_l = build_ftp_client(supplier="lasgo")
    try:
        fetch_latest(ftp_l, lasgo_remote_dir, lasgo_pattern, lasgo_local_dir, tag_l)
    finally:
        try:
            ftp_l.quit()
        except Exception:
            ftp_l.close()

    ftp_m = build_ftp_client(supplier="moovies")
    try:
        fetch_latest(ftp_m, moovies_remote_dir, moovies_pattern, moovies_local_dir, tag_m)
    finally:
        try:
            ftp_m.quit()
        except Exception:
            ftp_m.close()


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Fetch latest Moovies and Lasgo files from FTP.")
    parser.add_argument(
        "--mode",
        choices=("stock", "catalog"),
        default="stock",
        help="stock=inventory/operational FTP folders (default); catalog=full catalog FTP folders",
    )
    parser.add_argument("--env", default=".env", help="Env file path")
    parser.add_argument(
        "--strict-catalog",
        action="store_true",
        help="Catalog mode only: FTP under TAPE_Film .../Catalog (no local fallback). "
        "Missing files for a source are skipped_no_files (non-fatal); manifest still written.",
    )
    parser.add_argument(
        "--write-fetch-env",
        default=None,
        metavar="PATH",
        help="With --strict-catalog: write MOOVIES_FILE/LASGO_FILE + remote names for --archive-from-env.",
    )
    parser.add_argument(
        "--archive-from-env",
        default=None,
        metavar="PATH",
        help="Move Moovies+Lasgo catalog files from fetch manifest into .../Catalog/Archive on FTP.",
    )
    args = parser.parse_args(argv if argv is not None else None)

    try:
        if args.archive_from_env:
            run_catalog_archive_from_env(fetch_env_path=args.archive_from_env, env_file=args.env)
            return 0
        if args.mode == "catalog" and args.strict_catalog:
            if not args.write_fetch_env:
                print("--strict-catalog requires --write-fetch-env PATH", file=sys.stderr)
                raise SystemExit(2)
            run_catalog_fetch_strict(env_file=args.env, write_fetch_env=args.write_fetch_env)
            return 0
        run_fetch(env_file=args.env, mode=args.mode)
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
