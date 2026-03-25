#!/usr/bin/env python3
"""
Copy or move a single file between remote hosts (FTP ↔ SFTP, any combination).

There is no standard way to "rename across servers" on the wire; this script
downloads to a temporary file locally, optionally runs a security check, then
uploads to the destination. With --move, the source file is deleted only after
a successful upload.

Examples:

  # SFTP → FTP (password auth both sides)
  ./venv/bin/python scripts/cross_server_file_transfer.py \\
    --src-protocol sftp --src-host sftp.example.com --src-user u --src-password p \\
    --src-remote /inbox/report.xlsx \\
    --dst-protocol ftp --dst-host ftp.other.com --dst-user u2 --dst-password p2 \\
    --dst-remote /drop/report.xlsx

  # FTP → SFTP, delete source after success
  ./venv/bin/python scripts/cross_server_file_transfer.py \\
    --src-protocol ftp --src-host 10.0.0.1 --src-port 21 --src-user u --src-password p \\
    --src-remote /moovies/Inbound/feed.txt \\
    --dst-protocol sftp --dst-host backup.internal --dst-user u --dst-identity ~/.ssh/id_ed25519 \\
    --dst-remote /archive/feed.txt --move

  # With ClamAV (must be installed: brew install clamav && freshclam)
  ./venv/bin/python scripts/cross_server_file_transfer.py ... --scan clamav

Security scans (--scan):
  none     — no scan (default)
  basic    — max size, optional extension allowlist, obvious ZIP/office magic for common types
  clamav   — run `clamscan` on the temp file (fails transfer if infected / error)

SFTP → FTP: what to pass (no env vars required; optional secrets via env below)
──────────────────────────────────────────────────────────────────────────────
  Source (SFTP)              Enter / notes
  --src-host                 SFTP server hostname or IP (e.g. files.partner.com)
  --src-port                 Optional; default 22
  --src-user                 SSH username
  --src-password             Account password, OR key passphrase if using --src-identity
  --src-identity             Path to private key (e.g. ~/.ssh/id_ed25519); omit if password-only
  --src-remote               Full path to file on SFTP side (e.g. /outbound/FEED.xlsx)

  Destination (FTP)        Enter / notes
  --dst-host                 FTP server hostname or IP; use host:21 if you prefer one string
  --dst-port                 Optional; default 21 (overrides :port in host if both set)
  --dst-user                 FTP login
  --dst-password             FTP password
  --dst-tls                  Add flag if server requires explicit TLS (FTPS)
  --dst-remote               Target path including filename (e.g. /moovies/Inbound/FEED.xlsx)

  Optional env (avoid passwords in shell history):
    SRC_PASSWORD   used as default for --src-password if flag omitted
    DST_PASSWORD   used as default for --dst-password if flag omitted

  On a server (cron/systemd): same CLI; temp file uses OS temp dir (often /tmp).
  Ensure disk space ≥ largest file; for production consider a dedicated temp path
  (wrap script and set TMPDIR=...).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _parse_host_port(s: str, default_port: int) -> Tuple[str, int]:
    s = (s or "").strip()
    if not s:
        return "", default_port
    if s.startswith("["):
        close = s.find("]:")
        if close != -1:
            return s[: close + 1], int(s[close + 2 :])
        return s, default_port
    if s.count(":") == 1:
        h, _, p = s.partition(":")
        try:
            return h, int(p)
        except ValueError:
            pass
    return s, default_port


def _ftp_connect(host: str, port: int, user: str, password: str, tls: bool):
    import ftplib

    if tls:
        ftp = ftplib.FTP_TLS()
        ftp.connect(host=host, port=port, timeout=60)
        ftp.login(user=user, passwd=password)
        ftp.prot_p()
    else:
        ftp = ftplib.FTP()
        ftp.connect(host=host, port=port, timeout=60)
        ftp.login(user=user, passwd=password)
    return ftp


def _ftp_download(ftp, remote_path: str, local_path: str) -> None:
    import ftplib

    remote_path = remote_path.replace("\\", "/")
    parent, name = remote_path.rsplit("/", 1) if "/" in remote_path else ("", remote_path)
    if parent:
        ftp.cwd(parent)
    with open(local_path, "wb") as out:
        ftp.retrbinary(f"RETR {name}", out.write)


def _ftp_ensure_cwd(ftp, remote_dir: str) -> None:
    """Create each path segment if needed, then cwd (from current working dir)."""
    remote_dir = remote_dir.strip("/").replace("\\", "/")
    if not remote_dir:
        return
    for part in remote_dir.split("/"):
        if not part:
            continue
        try:
            ftp.cwd(part)
        except Exception:
            try:
                ftp.mkd(part)
            except Exception:
                pass
            ftp.cwd(part)


def _ftp_upload(ftp, local_path: str, remote_path: str) -> None:
    remote_path = remote_path.replace("\\", "/")
    parent, name = remote_path.rsplit("/", 1) if "/" in remote_path else ("", remote_path)
    if parent:
        _ftp_ensure_cwd(ftp, parent)
    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {name}", f)


def _ftp_delete(ftp, remote_path: str) -> None:
    remote_path = remote_path.replace("\\", "/")
    parent, name = remote_path.rsplit("/", 1) if "/" in remote_path else ("", remote_path)
    if parent:
        ftp.cwd(parent)
    ftp.delete(name)


def _sftp_connect(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
    identity: Optional[str],
):
    import paramiko

    t = paramiko.Transport((host, port))
    pkey = None
    if identity:
        path = Path(identity).expanduser()
        for loader in (
            paramiko.Ed25519Key.from_private_key_file,
            paramiko.RSAKey.from_private_key_file,
            paramiko.ECDSAKey.from_private_key_file,
        ):
            try:
                pkey = loader(str(path), password=password)
                break
            except Exception:
                continue
        if pkey is None:
            raise SystemExit(f"Could not load private key: {path}")
        t.connect(username=user, pkey=pkey)
    else:
        if not password:
            raise SystemExit("SFTP: provide --dst-password / --src-password or --identity")
        t.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(t)
    if sftp is None:
        t.close()
        raise SystemExit("SFTP handshake failed")
    return t, sftp


def _sftp_download(sftp, remote_path: str, local_path: str) -> None:
    sftp.get(remote_path, local_path)


def _sftp_mkdir_p(sftp, remote_dir: str) -> None:
    remote_dir = remote_dir.replace("\\", "/").rstrip("/")
    if not remote_dir or remote_dir == "/":
        return
    if remote_dir.startswith("/"):
        cur = ""
        for c in [x for x in remote_dir.split("/") if x]:
            cur = cur + "/" + c
            try:
                sftp.stat(cur)
            except OSError:
                sftp.mkdir(cur)
    else:
        cur = ""
        for c in remote_dir.split("/"):
            if not c:
                continue
            cur = f"{cur}/{c}" if cur else c
            try:
                sftp.stat(cur)
            except OSError:
                sftp.mkdir(cur)


def _sftp_upload(sftp, local_path: str, remote_path: str) -> None:
    remote_path = remote_path.replace("\\", "/")
    parent = str(Path(remote_path).parent).replace("\\", "/")
    if parent and parent not in (".", "/"):
        _sftp_mkdir_p(sftp, parent)
    sftp.put(local_path, remote_path)


def _sftp_delete(sftp, remote_path: str) -> None:
    sftp.remove(remote_path)


def _run_transfer(args: argparse.Namespace) -> None:
    scan = args.scan
    tmp: Optional[tempfile.NamedTemporaryFile] = None
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(args.src_remote).suffix)
        tmp.close()
        local = Path(tmp.name)

        # --- download ---
        if args.src_protocol == "ftp":
            h, p = _parse_host_port(args.src_host, args.src_port or 21)
            ftp = _ftp_connect(h, p, args.src_user, args.src_password or "", args.src_tls)
            try:
                _ftp_download(ftp, args.src_remote, str(local))
            finally:
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
        else:
            h, p = _parse_host_port(args.src_host, args.src_port or 22)
            t, sftp = _sftp_connect(
                h, p, args.src_user, args.src_password, args.src_identity
            )
            try:
                _sftp_download(sftp, args.src_remote, str(local))
            finally:
                sftp.close()
                t.close()

        if scan != "none":
            from app.services.file_security_scan import run_security_scan

            allow = (
                [x.strip() for x in args.allow_extensions.split(",") if x.strip()]
                if args.allow_extensions
                else None
            )
            run_security_scan(
                local,
                mode=scan,
                label="cross-server-transfer",
                max_bytes=args.max_bytes,
                allow_extensions=allow,
            )

        # --- upload ---
        if args.dst_protocol == "ftp":
            h, p = _parse_host_port(args.dst_host, args.dst_port or 21)
            ftp = _ftp_connect(h, p, args.dst_user, args.dst_password or "", args.dst_tls)
            try:
                _ftp_upload(ftp, str(local), args.dst_remote)
            finally:
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
        else:
            h, p = _parse_host_port(args.dst_host, args.dst_port or 22)
            t, sftp = _sftp_connect(
                h, p, args.dst_user, args.dst_password, args.dst_identity
            )
            try:
                _sftp_upload(sftp, str(local), args.dst_remote)
            finally:
                sftp.close()
                t.close()

        if args.move:
            if args.src_protocol == "ftp":
                h, p = _parse_host_port(args.src_host, args.src_port or 21)
                ftp = _ftp_connect(h, p, args.src_user, args.src_password or "", args.src_tls)
                try:
                    _ftp_delete(ftp, args.src_remote)
                finally:
                    try:
                        ftp.quit()
                    except Exception:
                        ftp.close()
            else:
                h, p = _parse_host_port(args.src_host, args.src_port or 22)
                t, sftp = _sftp_connect(
                    h, p, args.src_user, args.src_password, args.src_identity
                )
                try:
                    _sftp_delete(sftp, args.src_remote)
                finally:
                    sftp.close()
                    t.close()

        print(f"OK: {args.src_remote} -> {args.dst_remote}" + (" (source removed)" if args.move else ""))
    finally:
        if tmp is not None:
            try:
                Path(tmp.name).unlink(missing_ok=True)
            except Exception:
                pass


def main() -> int:
    p = argparse.ArgumentParser(description="Transfer one file between FTP/SFTP servers (via local temp).")
    p.add_argument("--src-protocol", choices=("ftp", "sftp"), required=True)
    p.add_argument("--src-host", required=True)
    p.add_argument("--src-port", type=int, default=None)
    p.add_argument("--src-user", required=True)
    p.add_argument("--src-password", default=os.getenv("SRC_PASSWORD", ""))
    p.add_argument("--src-identity", help="SFTP private key path (source); password may be key passphrase")
    p.add_argument("--src-tls", action="store_true", help="FTP over TLS (source)")
    p.add_argument("--src-remote", required=True, help="Absolute or chroot-relative remote path")

    p.add_argument("--dst-protocol", choices=("ftp", "sftp"), required=True)
    p.add_argument("--dst-host", required=True)
    p.add_argument("--dst-port", type=int, default=None)
    p.add_argument("--dst-user", required=True)
    p.add_argument("--dst-password", default=os.getenv("DST_PASSWORD", ""))
    p.add_argument("--dst-identity", help="SFTP private key path (destination)")
    p.add_argument("--dst-tls", action="store_true", help="FTP over TLS (destination)")
    p.add_argument("--dst-remote", required=True)

    p.add_argument("--move", action="store_true", help="Delete source file after successful upload")
    p.add_argument(
        "--scan",
        choices=("none", "basic", "clamav"),
        default="none",
        help="Security check on temp file before upload",
    )
    p.add_argument("--max-bytes", type=int, default=500 * 1024 * 1024, help="basic scan: max file size")
    p.add_argument(
        "--allow-extensions",
        default="",
        help="basic scan: comma list e.g. xlsx,csv,txt (empty = skip extension check)",
    )

    args = p.parse_args()
    if args.src_protocol == "ftp" and not args.src_password:
        p.error("--src-password required for FTP (or set SRC_PASSWORD)")
    if args.dst_protocol == "ftp" and not args.dst_password:
        p.error("--dst-password required for FTP (or set DST_PASSWORD)")
    if args.src_protocol == "sftp" and not args.src_identity and not args.src_password:
        p.error("SFTP source: set --src-password or --src-identity")
    if args.dst_protocol == "sftp" and not args.dst_identity and not args.dst_password:
        p.error("SFTP destination: set --dst-password or --dst-identity")

    os.chdir(ROOT)
    _run_transfer(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
