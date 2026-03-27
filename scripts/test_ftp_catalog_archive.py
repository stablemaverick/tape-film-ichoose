#!/usr/bin/env python3
"""
Isolated FTP catalog archive test (same paths / rename logic as pipeline step after strict fetch).

Reads a fetch manifest (like .locks/catalog_ftp_fetch.env) with:
  CATALOG_FTP_MOOVIES_REMOTE, CATALOG_FTP_MOOVIES_FILE,
  CATALOG_FTP_LASGO_REMOTE, CATALOG_FTP_LASGO_FILE

Default mode is --dry-run: connect, verify dirs and files, show what would be moved.
Use --apply only when you intend to move files into .../<Archive>/ on the server.

Examples:
  ./venv/bin/python scripts/test_ftp_catalog_archive.py
  ./venv/bin/python scripts/test_ftp_catalog_archive.py --fetch-env .locks/catalog_ftp_fetch.env
  ./venv/bin/python scripts/test_ftp_catalog_archive.py --only lasgo
  ./venv/bin/python scripts/test_ftp_catalog_archive.py --apply
"""
from __future__ import annotations

import argparse
import ftplib
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from app.services.supplier_fetch_service import (  # noqa: E402
    CATALOG_FTP_ARCHIVE_SUBDIR,
    _read_kv_file,
    build_ftp_client,
    run_catalog_archive_from_env,
)


_MANIFEST_KEYS = (
    "CATALOG_FTP_MOOVIES_REMOTE",
    "CATALOG_FTP_MOOVIES_FILE",
    "CATALOG_FTP_LASGO_REMOTE",
    "CATALOG_FTP_LASGO_FILE",
)


def _parse_manifest(path: str) -> dict[str, str]:
    data = _read_kv_file(path)
    out = {k: (data.get(k) or "").strip().strip("'\"") for k in _MANIFEST_KEYS}
    moovies_ok = bool(out["CATALOG_FTP_MOOVIES_REMOTE"] and out["CATALOG_FTP_MOOVIES_FILE"])
    lasgo_ok = bool(out["CATALOG_FTP_LASGO_REMOTE"] and out["CATALOG_FTP_LASGO_FILE"])
    if not moovies_ok and not lasgo_ok:
        print(
            f"No archiveable supplier in {path} (need at least one of Moovies or Lasgo remote+file).",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return out


def _manifest_from_cli(args: argparse.Namespace) -> dict[str, str] | None:
    """If all four paths are set, return a manifest from CLI only (no fetch-env file)."""
    mr, mf, lr, lf = (
        args.moovies_remote,
        args.moovies_file,
        args.lasgo_remote,
        args.lasgo_file,
    )
    if all((mr, mf, lr, lf)):
        return {
            "CATALOG_FTP_MOOVIES_REMOTE": mr,
            "CATALOG_FTP_MOOVIES_FILE": mf,
            "CATALOG_FTP_LASGO_REMOTE": lr,
            "CATALOG_FTP_LASGO_FILE": lf,
        }
    if any((mr, mf, lr, lf)):
        return None  # merge onto fetch-env after load
    return None


def _apply_partial_cli_overrides(manifest: dict[str, str], args: argparse.Namespace) -> None:
    if args.moovies_remote:
        manifest["CATALOG_FTP_MOOVIES_REMOTE"] = args.moovies_remote
    if args.moovies_file:
        manifest["CATALOG_FTP_MOOVIES_FILE"] = args.moovies_file
    if args.lasgo_remote:
        manifest["CATALOG_FTP_LASGO_REMOTE"] = args.lasgo_remote
    if args.lasgo_file:
        manifest["CATALOG_FTP_LASGO_FILE"] = args.lasgo_file


def _write_manifest_temp(m: dict[str, str]) -> str:
    import tempfile

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix="_catalog_ftp_archive.env",
        delete=False,
        encoding="utf-8",
    ) as tf:
        for k in _MANIFEST_KEYS:
            tf.write(f"{k}={m[k]}\n")
        return tf.name


def _file_in_cwd(ftp: ftplib.FTP, filename: str) -> bool:
    try:
        ftp.size(filename)
        return True
    except ftplib.all_errors:
        pass
    try:
        return filename in ftp.nlst()
    except ftplib.all_errors:
        return False


def _archive_dir_status(ftp: ftplib.FTP, parent: str) -> str:
    try:
        ftp.cwd(parent)
    except ftplib.all_errors as e:
        return f"cannot cwd to {parent!r}: {e}"
    sub = CATALOG_FTP_ARCHIVE_SUBDIR
    try:
        ftp.cwd(sub)
        try:
            n = len(ftp.nlst())
        except ftplib.all_errors:
            n = -1
        ftp.cwd(parent)
        return f"{sub}/ exists (~{n} entries in listing)"
    except ftplib.all_errors:
        return f"{sub}/ missing (archive step would try mkd {sub!r})"


def dry_run_pair(
    ftp: ftplib.FTP,
    label: str,
    parent_remote: str,
    filename: str,
) -> bool:
    print(f"\n=== {label} ===")
    print(f"  remote_dir: {parent_remote}")
    print(f"  filename:   {filename}")
    ok = True
    try:
        ftp.cwd(parent_remote)
    except ftplib.all_errors as e:
        print(f"  FAIL: cwd: {e}")
        return False
    st = _archive_dir_status(ftp, parent_remote)
    print(f"  archive:    {st}")
    if not _file_in_cwd(ftp, filename):
        print(f"  FAIL: file not found under {parent_remote!r} (SIZE/nlst)")
        ok = False
    else:
        dest = f"{CATALOG_FTP_ARCHIVE_SUBDIR}/{filename}"
        print(f"  OK:   would move -> {parent_remote.rstrip('/')}/{dest}")
    return ok


def main() -> int:
    p = argparse.ArgumentParser(description="Test FTP catalog archive (dry-run or --apply).")
    p.add_argument("--env", default=".env", help="Dotenv path for FTP_* credentials")
    p.add_argument(
        "--fetch-env",
        default=None,
        metavar="PATH",
        help="Manifest from strict fetch (default: .locks/catalog_ftp_fetch.env if it exists)",
    )
    p.add_argument("--moovies-remote", default=None, help="Override manifest Moovies remote dir")
    p.add_argument("--moovies-file", default=None, help="Override manifest Moovies filename")
    p.add_argument("--lasgo-remote", default=None, help="Override manifest Lasgo remote dir")
    p.add_argument("--lasgo-file", default=None, help="Override manifest Lasgo filename")
    p.add_argument(
        "--only",
        choices=("moovies", "lasgo", "both"),
        default="both",
        help="Dry-run / apply only one supplier (default: both)",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform RNFR/RNTO into Archive/ (default is dry-run only)",
    )
    args = p.parse_args()

    cli_m = _manifest_from_cli(args)
    if cli_m is not None:
        manifest = cli_m
        fetch_env_label = "(cli)"
    else:
        fetch_env_label = args.fetch_env
        if not fetch_env_label:
            default_lock = os.path.join(ROOT, ".locks", "catalog_ftp_fetch.env")
            fetch_env_label = default_lock if os.path.isfile(default_lock) else None
        if not fetch_env_label or not os.path.isfile(fetch_env_label):
            if any(
                (
                    args.moovies_remote,
                    args.moovies_file,
                    args.lasgo_remote,
                    args.lasgo_file,
                )
            ):
                print(
                    "Partial --moovies-* / --lasgo-* flags require a fetch-env file "
                    "(--fetch-env or .locks/catalog_ftp_fetch.env).",
                    file=sys.stderr,
                )
                raise SystemExit(2)
            print(
                "No fetch manifest. Pass --fetch-env PATH or create .locks/catalog_ftp_fetch.env\n"
                "Or pass all four: --moovies-remote --moovies-file --lasgo-remote --lasgo-file",
                file=sys.stderr,
            )
            raise SystemExit(2)
        manifest = _parse_manifest(fetch_env_label)
        _apply_partial_cli_overrides(manifest, args)
    fetch_env = fetch_env_label

    if args.apply:
        if args.only != "both":
            print(
                "--apply with --only is not supported; archive uses the full manifest (partial ok).",
                file=sys.stderr,
            )
            raise SystemExit(2)
        tmp = _write_manifest_temp(manifest)
        try:
            run_catalog_archive_from_env(fetch_env_path=tmp, env_file=args.env)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return 0

    # Dry-run
    from dotenv import load_dotenv

    load_dotenv(args.env)
    print(f"Dry-run: manifest source {fetch_env}")
    print(f"Archive subdir: {CATALOG_FTP_ARCHIVE_SUBDIR!r}")
    all_ok = True
    if args.only in ("lasgo", "both") and manifest.get("CATALOG_FTP_LASGO_REMOTE") and manifest.get(
        "CATALOG_FTP_LASGO_FILE"
    ):
        ftp_l = build_ftp_client(supplier="lasgo")
        try:
            all_ok = dry_run_pair(
                ftp_l,
                "Lasgo",
                manifest["CATALOG_FTP_LASGO_REMOTE"],
                manifest["CATALOG_FTP_LASGO_FILE"],
            ) and all_ok
        finally:
            try:
                ftp_l.quit()
            except Exception:
                ftp_l.close()
    elif args.only == "lasgo":
        print("Dry-run: Lasgo skipped (no Lasgo remote/file in manifest).")
    if args.only in ("moovies", "both") and manifest.get("CATALOG_FTP_MOOVIES_REMOTE") and manifest.get(
        "CATALOG_FTP_MOOVIES_FILE"
    ):
        ftp_m = build_ftp_client(supplier="moovies")
        try:
            all_ok = dry_run_pair(
                ftp_m,
                "Moovies",
                manifest["CATALOG_FTP_MOOVIES_REMOTE"],
                manifest["CATALOG_FTP_MOOVIES_FILE"],
            ) and all_ok
        finally:
            try:
                ftp_m.quit()
            except Exception:
                ftp_m.close()
    elif args.only == "moovies":
        print("Dry-run: Moovies skipped (no Moovies remote/file in manifest).")

    print()
    if all_ok:
        print("Dry-run: all checks passed. Re-run with --apply to archive on the server.")
        return 0
    print("Dry-run: one or more checks failed.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
