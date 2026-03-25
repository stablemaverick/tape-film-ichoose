import fnmatch
import ftplib
import os
import time
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv


def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or str(value).strip() == "":
        raise SystemExit(f"Missing required env var: {name}")
    return str(value).strip()


def choose_latest_file(ftp: ftplib.FTP, remote_dir: str, pattern: str) -> Optional[Tuple[str, int]]:
    latest_name: Optional[str] = None
    latest_mtime = -1

    try:
        ftp.cwd(remote_dir)
    except Exception as e:
        raise SystemExit(f"Failed to change directory to '{remote_dir}': {e}")

    # Prefer MLSD (includes file metadata). Fall back to NLST + MDTM.
    used_mlsd = False
    try:
        for name, facts in ftp.mlsd():
            if not fnmatch.fnmatch(name, pattern):
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
        used_mlsd = False

    if not used_mlsd:
        candidates = []
        try:
            candidates = [n for n in ftp.nlst() if fnmatch.fnmatch(n, pattern)]
        except Exception:
            # Some FTP servers reject NLST but allow LIST.
            listing: list[str] = []
            ftp.retrlines("LIST", listing.append)
            for line in listing:
                # Best-effort parse: filename is usually the last token.
                name = line.split(maxsplit=8)[-1].strip() if line.strip() else ""
                if name and fnmatch.fnmatch(name, pattern):
                    candidates.append(name)

        for name in candidates:
            mtime = -1
            try:
                resp = ftp.sendcmd(f"MDTM {name}")
                # Typical response: "213 20260320003001"
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
        print(f"[{supplier_name}] Failed listing '{remote_dir}' with pattern '{pattern}': {type(last_err).__name__}: {last_err}")
        return None
    if not chosen:
        print(f"[{supplier_name}] No files matched pattern '{pattern}' in {remote_dir}")
        return None

    filename, _mtime = chosen
    remote_path = f"{remote_dir.rstrip('/')}/{filename}"
    Path(local_dir).mkdir(parents=True, exist_ok=True)
    local_path = str(Path(local_dir) / filename)

    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {filename}", f.write)
    print(f"[{supplier_name}] Downloaded: {remote_path} -> {local_path}")
    return local_path


def build_ftp_client() -> ftplib.FTP:
    host = os.getenv("FTP_HOST") or os.getenv("SFTP_HOST")
    user = os.getenv("FTP_USER") or os.getenv("SFTP_USER")
    password = os.getenv("FTP_PASSWORD") or os.getenv("SFTP_PASSWORD")
    port = int(os.getenv("FTP_PORT", "21"))
    tls_mode = str(os.getenv("FTP_USE_TLS", "auto")).strip().lower()

    if not host:
        raise SystemExit("Missing FTP_HOST in .env")
    if not user:
        raise SystemExit("Missing FTP_USER in .env")
    if not password:
        raise SystemExit("Missing FTP_PASSWORD in .env")

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

    # auto mode: try plain first, then explicit TLS.
    try:
        return connect_plain()
    except Exception:
        return connect_tls()


def main() -> None:
    load_dotenv(".env")

    lasgo_remote_dir = os.getenv("LASGO_REMOTE_DIR", "/TAPE_Film/Lasgo/Incoming")
    moovies_remote_dir = os.getenv("MOOVIES_REMOTE_DIR", "/TAPE_Film/Moovies/Inbound/Inventory")
    lasgo_pattern = os.getenv("LASGO_GLOB", "LASGO_*")
    moovies_pattern = os.getenv("MOOVIES_GLOB", "Feed-*")
    lasgo_local_dir = os.getenv("LASGO_STOCK_DIR", "/opt/tape-film/sftp/lasgo")
    moovies_local_dir = os.getenv("MOOVIES_STOCK_DIR", "/opt/tape-film/sftp/moovies")

    ftp = build_ftp_client()
    try:
        fetch_latest(ftp, lasgo_remote_dir, lasgo_pattern, lasgo_local_dir, "LASGO")
        fetch_latest(ftp, moovies_remote_dir, moovies_pattern, moovies_local_dir, "MOOVIES")
    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()


if __name__ == "__main__":
    main()

