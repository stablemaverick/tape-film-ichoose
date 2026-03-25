"""
FTP/FTPS client for supplier file downloads.

Supports:
  - Plain FTP and explicit TLS (FTPS)
  - Auto-detection of TLS support
  - MLSD and NLST fallback for directory listing
  - Latest-file selection by modification time
  - Retry on listing failures
"""

import fnmatch
import ftplib
import os
import time
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv


class FtpClient:
    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 21,
        tls_mode: str = "auto",
    ):
        self.host = host or os.getenv("FTP_HOST") or os.getenv("SFTP_HOST")
        self.user = user or os.getenv("FTP_USER") or os.getenv("SFTP_USER")
        self.password = password or os.getenv("FTP_PASSWORD") or os.getenv("SFTP_PASSWORD")
        self.port = port or int(os.getenv("FTP_PORT", "21"))
        self.tls_mode = tls_mode or str(os.getenv("FTP_USE_TLS", "auto")).strip().lower()

        if not self.host:
            raise SystemExit("Missing FTP_HOST in .env")
        if not self.user:
            raise SystemExit("Missing FTP_USER in .env")
        if not self.password:
            raise SystemExit("Missing FTP_PASSWORD in .env")

        self._ftp: Optional[ftplib.FTP] = None

    def connect(self) -> ftplib.FTP:
        """Establish FTP connection based on TLS mode."""
        if self._ftp is not None:
            return self._ftp

        def _plain() -> ftplib.FTP:
            ftp = ftplib.FTP()
            ftp.connect(host=self.host, port=self.port, timeout=30)
            ftp.login(user=self.user, passwd=self.password)
            return ftp

        def _tls() -> ftplib.FTP:
            ftp = ftplib.FTP_TLS()
            ftp.connect(host=self.host, port=self.port, timeout=30)
            ftp.login(user=self.user, passwd=self.password)
            ftp.prot_p()
            return ftp

        if self.tls_mode in {"1", "true", "yes", "tls"}:
            self._ftp = _tls()
        elif self.tls_mode in {"0", "false", "no", "plain"}:
            self._ftp = _plain()
        else:
            try:
                self._ftp = _plain()
            except Exception:
                self._ftp = _tls()

        return self._ftp

    def close(self) -> None:
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                self._ftp.close()
            self._ftp = None

    def choose_latest_file(
        self, remote_dir: str, pattern: str
    ) -> Optional[Tuple[str, int]]:
        """Find the most recently modified file matching a glob pattern."""
        ftp = self.connect()
        latest_name: Optional[str] = None
        latest_mtime = -1

        try:
            ftp.cwd(remote_dir)
        except Exception as e:
            raise SystemExit(f"Failed to change directory to '{remote_dir}': {e}")

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
            pass

        if not used_mlsd:
            candidates = []
            try:
                candidates = [n for n in ftp.nlst() if fnmatch.fnmatch(n, pattern)]
            except Exception:
                listing: list[str] = []
                ftp.retrlines("LIST", listing.append)
                for line in listing:
                    name = line.split(maxsplit=8)[-1].strip() if line.strip() else ""
                    if name and fnmatch.fnmatch(name, pattern):
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
        self,
        remote_dir: str,
        pattern: str,
        local_dir: str,
        supplier_name: str,
    ) -> Optional[str]:
        """Download the latest matching file from an FTP directory."""
        last_err: Optional[Exception] = None
        chosen = None
        for _ in range(3):
            try:
                chosen = self.choose_latest_file(remote_dir, pattern)
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
        ftp = self.connect()
        Path(local_dir).mkdir(parents=True, exist_ok=True)
        local_path = str(Path(local_dir) / filename)

        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        print(f"[{supplier_name}] Downloaded: {remote_dir.rstrip('/')}/{filename} -> {local_path}")
        return local_path


def get_ftp_client(env_file: str = ".env") -> FtpClient:
    """Factory that loads env and returns a configured FtpClient."""
    load_dotenv(env_file)
    return FtpClient()
