#!/usr/bin/env python3
"""
Smoke-test FTP using the same env resolution as supplier_fetch_service.

Checks login (Moovies + Lasgo credential chains) and CWD into stock/catalog paths.
Does not print passwords.

Usage (from repo root):
  ./venv/bin/python scripts/test_ftp_connection.py
  ./venv/bin/python scripts/test_ftp_connection.py --env .env.prod
"""

from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from dotenv import load_dotenv

from app.services.supplier_fetch_service import (  # noqa: E402
    FtpSupplier,
    _default_lasgo_catalog_remote,
    _default_lasgo_stock_remote,
    _default_moovies_catalog_remote,
    _default_moovies_stock_remote,
    build_ftp_client,
)


def _mask(s: str | None) -> str:
    if not s:
        return "(empty)"
    if len(s) <= 2:
        return "*" * len(s)
    return s[0] + "***" + s[-1]


def _sample_listing(ftp, limit: int = 12) -> list[str]:
    names: list[str] = []
    try:
        for name, _facts in ftp.mlsd():
            if name in (".", ".."):
                continue
            names.append(name)
    except Exception:
        try:
            names = list(ftp.nlst())[: limit * 2]
        except Exception:
            return []
    names.sort()
    return names[:limit]


def _try_paths(label: str, supplier: FtpSupplier, paths: list[tuple[str, str]]) -> bool:
    """paths: (description, remote_dir)."""
    from ftplib import error_perm

    pre = build_ftp_client(supplier=supplier)
    try:
        pwd0 = pre.pwd()
        print(f"  [{label}] connected; PWD after login: {pwd0!r}")
    except Exception as e:
        print(f"  [{label}] FAIL login: {e}")
        return False
    ok = True
    for desc, remote in paths:
        if not remote:
            print(f"  [{label}] skip {desc}: (empty path)")
            continue
        try:
            pre.cwd(remote)
            pwd = pre.pwd()
            sample = _sample_listing(pre)
            print(f"  [{label}] OK {desc} -> cwd {remote!r} pwd={pwd!r} sample={sample}")
        except error_perm as e:
            print(f"  [{label}] FAIL {desc} cwd {remote!r}: {e}")
            ok = False
        except Exception as e:
            print(f"  [{label}] FAIL {desc} cwd {remote!r}: {type(e).__name__}: {e}")
            ok = False
        try:
            pre.cwd("/")
        except Exception:
            try:
                pre.cwd(".")
            except Exception:
                pass
    try:
        pre.quit()
    except Exception:
        pre.close()
    return ok


def main() -> int:
    p = argparse.ArgumentParser(description="Test FTP login and remote paths from .env")
    p.add_argument("--env", default=".env", help="Dotenv file path")
    args = p.parse_args()
    load_dotenv(args.env)

    print("FTP connection test (repo root=%s, env=%s)" % (ROOT, args.env))
    print()

    moovies_paths = [
        ("stock/inventory", _default_moovies_stock_remote()),
        ("catalog", _default_moovies_catalog_remote()),
    ]
    lasgo_paths = [
        ("stock/incoming", _default_lasgo_stock_remote()),
        ("catalog", _default_lasgo_catalog_remote()),
    ]

    # Show resolved host:port (no secrets)
    os.environ.setdefault("_ECHO", "0")
    from app.services import supplier_fetch_service as sfs

    for sup in ("moovies", "lasgo"):
        pre = sfs._supplier_ftp_prefix(sup)  # type: ignore[arg-type]
        host, port = sfs._ftp_host_and_connect_port(pre)
        user = sfs._ftp_setting(pre, "USER")
        print(
            f"Resolved {sup}: host={host!r} port={port} user={_mask(user)} "
            f"(from MOOVIES_FTP_* / LASGO_FTP_* / FTP_* chain)"
        )
    print()

    a = _try_paths("moovies", "moovies", moovies_paths)
    print()
    b = _try_paths("lasgo", "lasgo", lasgo_paths)

    print()
    if a and b:
        print("All checks passed.")
        return 0
    print("One or more checks failed — fix remote paths or credentials in .env.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
