"""
Mirror latest Lasgo file from vendor SFTP into our Lasgo FTP inbox (stock or catalog).

Enabled when LASGO_SFTP_MIRROR_ENABLED is 1/true/yes. Moovies is unchanged.

Flow: list SFTP dir → pick latest matching glob → download to temp → security scan
(SUPPLIER_FETCH_SECURITY_SCAN) → upload to same Lasgo FTP path used by run_fetch /
run_catalog_fetch_strict.

Stock vs catalog (same server credentials; paths differ):
  • LASGO_SFTP_HOST, LASGO_SFTP_PORT, LASGO_SFTP_USER, LASGO_SFTP_PASSWORD / LASGO_SFTP_IDENTITY
    are shared for both modes — only remote directory + glob + FTP destination change.
  • Stock: LASGO_SFTP_STOCK_REMOTE_DIR, LASGO_SFTP_STOCK_GLOB → stages to LASGO_STOCK_REMOTE_DIR (FTP).
  • Catalog: LASGO_SFTP_CATALOG_REMOTE_DIR, LASGO_SFTP_CATALOG_GLOB → stages to default catalog FTP path.

Env (SFTP source):
  LASGO_SFTP_HOST, LASGO_SFTP_PORT (default 22), LASGO_SFTP_USER
  LASGO_SFTP_PASSWORD  and/or  LASGO_SFTP_IDENTITY (private key path; password = key passphrase)
  LASGO_SFTP_CONNECT_TIMEOUT_SEC   — TCP connect (default 30)
  LASGO_SFTP_BANNER_TIMEOUT_SEC    — SSH banner (default 30)
  LASGO_SFTP_AUTH_TIMEOUT_SEC      — auth handshake (default 120; raise if you see Authentication timeout)
  LASGO_SFTP_STOCK_REMOTE_DIR      — directory on SFTP for stock feeds
  LASGO_SFTP_STOCK_GLOB            — default LASGO_*
  LASGO_SFTP_CATALOG_REMOTE_DIR    — directory on SFTP for catalog exports
  LASGO_SFTP_CATALOG_GLOB          — default LASGO_*

FTP destination dirs use the same values as normal fetch:
  LASGO_STOCK_REMOTE_DIR / defaults, or LASGO_CATALOG_REMOTE_DIR / defaults.
"""

from __future__ import annotations

import os
import socket
import stat
import tempfile
from pathlib import Path
from typing import Any, Literal, Optional, Tuple

from dotenv import load_dotenv

from app.services.file_security_scan import fnmatch_ci, run_security_scan

MirrorMode = Literal["stock", "catalog"]


def lasgo_sftp_mirror_enabled() -> bool:
    v = os.getenv("LASGO_SFTP_MIRROR_ENABLED", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _mask_user(u: str) -> str:
    u = (u or "").strip()
    if len(u) <= 2:
        return "***"
    return f"{u[0]}***{u[-1]}"


def _mask_password_hint(password: Optional[str], identity: Optional[str]) -> str:
    if identity:
        p = Path(identity).expanduser()
        return f"key_file={p.name} (passphrase {'set' if password else 'not set'})"
    if password:
        return "password=**** (set)"
    return "password=not set"


def _log_resolved_sftp_config(
    mode: MirrorMode,
    *,
    host: str,
    port: int,
    user: str,
    auth_mode: str,
    password_hint: str,
    connect_timeout: float,
    banner_timeout: float,
    auth_timeout: float,
    sftp_dir: str,
    pattern: str,
    ftp_dest: str,
) -> None:
    print(
        "[LASGO mirror] SFTP connect plan: "
        f"mode={mode} host={host!r} port={port} user={_mask_user(user)} "
        f"auth={auth_mode} {password_hint} "
        f"timeouts_sec tcp={connect_timeout} banner={banner_timeout} auth={auth_timeout} "
        f"remote_dir={sftp_dir!r} glob={pattern!r} ftp_stage_dir={ftp_dest!r}"
    )


def classify_lasgo_sftp_error(exc: BaseException) -> tuple[str, str]:
    """
    Return (category, short guidance). Categories: auth_timeout, auth_rejected, socket_timeout,
    host_key, network, permission_path, protocol, unknown.
    """
    import paramiko
    from paramiko.ssh_exception import (
        AuthenticationException,
        BadHostKeyException,
        BadAuthenticationType,
        PasswordRequiredException,
        SSHException,
    )

    msg = str(exc).strip()
    low = msg.lower()

    if isinstance(exc, socket.timeout):
        return (
            "socket_timeout",
            "TCP connect or socket read timed out — check host, port, firewall, and LASGO_SFTP_CONNECT_TIMEOUT_SEC.",
        )
    if isinstance(exc, TimeoutError):
        return ("socket_timeout", str(exc) or "Operation timed out.")

    if isinstance(exc, OSError) and exc.errno is not None:
        return (
            "network",
            f"OS network error ({type(exc).__name__}): {exc} — check DNS, routing, and firewall.",
        )

    if isinstance(exc, BadHostKeyException):
        return (
            "host_key",
            "Remote host key does not match known_hosts — update known_hosts or verify you are not being MITM'd.",
        )

    if isinstance(exc, PasswordRequiredException):
        return ("auth_rejected", "Private key requires a passphrase (set LASGO_SFTP_PASSWORD as key passphrase).")

    if isinstance(exc, BadAuthenticationType):
        return ("auth_rejected", f"Server rejected auth mechanism: {msg}")

    if isinstance(exc, AuthenticationException):
        if "authentication timeout" in low or "auth timeout" in low:
            return (
                "auth_timeout",
                "Server did not finish authentication in time — try LASGO_SFTP_AUTH_TIMEOUT_SEC (e.g. 180), "
                "or check server load and network latency.",
            )
        return ("auth_rejected", f"SSH authentication failed: {msg or type(exc).__name__}")

    if isinstance(exc, SSHException):
        if "could not negotiate" in low or "no matching" in low:
            return ("protocol", f"SSH algorithm / protocol mismatch: {msg}")
        return ("protocol", msg or type(exc).__name__)

    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return ("permission_path", f"Local path or permissions: {exc}")

    if isinstance(exc, OSError) and "sftp" in low:
        return ("permission_path", f"SFTP path or permission issue: {exc}")

    return ("unknown", f"{type(exc).__name__}: {msg}")


def _sftp_connect(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
    identity: Optional[str],
    *,
    connect_timeout: float,
    banner_timeout: float,
    auth_timeout: float,
):
    import paramiko

    try:
        raw_sock = socket.create_connection((host, port), timeout=connect_timeout)
    except socket.timeout as e:
        raise socket.timeout(f"TCP connect to {host!r}:{port} timed out after {connect_timeout}s") from e

    t = paramiko.Transport(raw_sock)
    t.banner_timeout = int(banner_timeout)
    t.auth_timeout = int(auth_timeout)

    pkey = None
    try:
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
                t.close()
                raise SystemExit(f"LASGO SFTP mirror: could not load private key {path}")
            t.connect(username=user, pkey=pkey)
        else:
            if not password:
                t.close()
                raise SystemExit("LASGO SFTP mirror: set LASGO_SFTP_PASSWORD or LASGO_SFTP_IDENTITY")
            t.connect(username=user, password=password)
    except BaseException:
        try:
            t.close()
        except Exception:
            pass
        raise

    try:
        raw_sock.settimeout(None)
    except OSError:
        pass

    sftp = paramiko.SFTPClient.from_transport(t)
    if sftp is None:
        t.close()
        raise SystemExit("LASGO SFTP mirror: handshake failed")
    return t, sftp


def _sftp_pick_latest_name(sftp, remote_dir: str, pattern: str) -> Optional[Tuple[str, float]]:
    remote_dir = remote_dir.rstrip("/") or "."
    try:
        attrs = sftp.listdir_attr(remote_dir)
    except OSError as e:
        raise SystemExit(f"LASGO SFTP mirror: cannot list {remote_dir!r}: {e}") from e

    best_name: Optional[str] = None
    best_mtime = -1.0
    for a in attrs:
        name = a.filename
        if name in (".", ".."):
            continue
        mode = getattr(a, "st_mode", None) or 0
        if stat.S_ISDIR(mode):
            continue
        if not fnmatch_ci(name, pattern):
            continue
        mtime = float(getattr(a, "st_mtime", 0) or 0)
        if mtime >= best_mtime:
            best_mtime = mtime
            best_name = name
    if best_name is None:
        return None
    return best_name, best_mtime


def _ftp_ensure_cwd(ftp, remote_dir: str) -> None:
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


def _ftp_stor(ftp, local_path: str, remote_parent: str, filename: str) -> None:
    remote_parent = remote_parent.replace("\\", "/").strip("/")
    try:
        ftp.cwd("/")
    except Exception:
        pass
    if remote_parent:
        _ftp_ensure_cwd(ftp, remote_parent)
    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {filename}", f)


def _resolve_mirror_paths(mode: MirrorMode) -> Tuple[str, str, str]:
    """sftp_dir, glob pattern, ftp_dest (for logging and mirror)."""
    from app.services.supplier_fetch_service import (
        _default_lasgo_catalog_remote,
        _default_lasgo_stock_remote,
    )

    if mode == "stock":
        sftp_dir = (os.getenv("LASGO_SFTP_STOCK_REMOTE_DIR") or "").strip()
        pattern = (os.getenv("LASGO_SFTP_STOCK_GLOB") or os.getenv("LASGO_STOCK_GLOB") or "LASGO_*").strip()
        ftp_dest = (os.getenv("LASGO_STOCK_REMOTE_DIR") or "").strip() or _default_lasgo_stock_remote()
    else:
        sftp_dir = (os.getenv("LASGO_SFTP_CATALOG_REMOTE_DIR") or "").strip()
        pattern = (
            os.getenv("LASGO_SFTP_CATALOG_GLOB") or os.getenv("LASGO_CATALOG_GLOB") or "LASGO_*"
        ).strip()
        ftp_dest = _default_lasgo_catalog_remote()
    return sftp_dir, pattern, ftp_dest


def probe_lasgo_sftp(
    mode: MirrorMode,
    *,
    env_file: str = ".env",
    require_mirror_enabled: bool = True,
) -> None:
    """
    Connect, list the configured remote directory once, disconnect. For manual auth checks.
    Raises SystemExit on failure.
    """
    load_dotenv(env_file)
    if require_mirror_enabled and not lasgo_sftp_mirror_enabled():
        raise SystemExit("LASGO_SFTP_MIRROR_ENABLED is not on (use .env or test script --force).")

    host = (os.getenv("LASGO_SFTP_HOST") or "").strip()
    user = (os.getenv("LASGO_SFTP_USER") or "").strip()
    if not host or not user:
        raise SystemExit("LASGO SFTP probe: LASGO_SFTP_HOST / LASGO_SFTP_USER missing")

    port = int(os.getenv("LASGO_SFTP_PORT") or "22")
    password = os.getenv("LASGO_SFTP_PASSWORD")
    identity = (os.getenv("LASGO_SFTP_IDENTITY") or "").strip() or None
    connect_timeout = _env_float("LASGO_SFTP_CONNECT_TIMEOUT_SEC", 30.0)
    banner_timeout = _env_float("LASGO_SFTP_BANNER_TIMEOUT_SEC", 30.0)
    auth_timeout = _env_float("LASGO_SFTP_AUTH_TIMEOUT_SEC", 120.0)

    sftp_dir, pattern, ftp_dest = _resolve_mirror_paths(mode)
    if not sftp_dir:
        raise SystemExit(
            f"LASGO SFTP probe: set LASGO_SFTP_{'STOCK' if mode == 'stock' else 'CATALOG'}_REMOTE_DIR"
        )

    auth_mode = "publickey" if identity else "password"
    _log_resolved_sftp_config(
        mode,
        host=host,
        port=port,
        user=user,
        auth_mode=auth_mode,
        password_hint=_mask_password_hint(password, identity),
        connect_timeout=connect_timeout,
        banner_timeout=banner_timeout,
        auth_timeout=auth_timeout,
        sftp_dir=sftp_dir,
        pattern=pattern,
        ftp_dest=ftp_dest,
    )

    t, sftp = _sftp_connect(
        host,
        port,
        user,
        password,
        identity,
        connect_timeout=connect_timeout,
        banner_timeout=banner_timeout,
        auth_timeout=auth_timeout,
    )
    try:
        names = sftp.listdir(sftp_dir)
        print(f"[LASGO mirror] SFTP probe OK: listed {len(names)} entries under {sftp_dir!r} (mode={mode})")
    finally:
        sftp.close()
        t.close()


def mirror_lasgo_sftp_to_ftp(
    mode: MirrorMode,
    *,
    env_file: str = ".env",
    require_mirror_enabled: bool = True,
    swallow_errors: bool = False,
) -> bool:
    """
    Copy latest Lasgo file from vendor SFTP → Lasgo FTP drop dir (stock or catalog).

    If require_mirror_enabled=True (default for pipelines), no-op when
    LASGO_SFTP_MIRROR_ENABLED is off (returns False).

    If swallow_errors=True (stock fetch only), logs classified errors and returns False instead
    of aborting — FTP fetch can still run; data may be from FTP/local only, not vendor SFTP.

    Returns True if mirror ran and completed successfully, False if disabled or (when
    swallow_errors) on failure.
    """
    load_dotenv(env_file)
    if require_mirror_enabled and not lasgo_sftp_mirror_enabled():
        return False

    host = (os.getenv("LASGO_SFTP_HOST") or "").strip()
    user = (os.getenv("LASGO_SFTP_USER") or "").strip()
    if not host or not user:
        raise SystemExit(
            "LASGO_SFTP_MIRROR_ENABLED but LASGO_SFTP_HOST / LASGO_SFTP_USER missing"
        )
    port = int(os.getenv("LASGO_SFTP_PORT") or "22")
    password = os.getenv("LASGO_SFTP_PASSWORD")
    identity = (os.getenv("LASGO_SFTP_IDENTITY") or "").strip() or None
    connect_timeout = _env_float("LASGO_SFTP_CONNECT_TIMEOUT_SEC", 30.0)
    banner_timeout = _env_float("LASGO_SFTP_BANNER_TIMEOUT_SEC", 30.0)
    auth_timeout = _env_float("LASGO_SFTP_AUTH_TIMEOUT_SEC", 120.0)

    sftp_dir, pattern, ftp_dest = _resolve_mirror_paths(mode)
    if not sftp_dir:
        raise SystemExit(
            f"LASGO_SFTP_MIRROR_ENABLED: set LASGO_SFTP_{'STOCK' if mode == 'stock' else 'CATALOG'}_REMOTE_DIR"
        )

    from app.services.supplier_fetch_service import build_ftp_client

    scan_mode = (os.getenv("SUPPLIER_FETCH_SECURITY_SCAN") or "basic").strip().lower()
    allow_raw = os.getenv("SUPPLIER_FETCH_SCAN_ALLOW_EXTENSIONS", "").strip()
    allow = [x.strip() for x in allow_raw.split(",") if x.strip()] if allow_raw else None
    max_bytes = int(os.getenv("SUPPLIER_FETCH_SCAN_MAX_BYTES", str(500 * 1024 * 1024)))

    auth_mode = "publickey" if identity else "password"
    _log_resolved_sftp_config(
        mode,
        host=host,
        port=port,
        user=user,
        auth_mode=auth_mode,
        password_hint=_mask_password_hint(password, identity),
        connect_timeout=connect_timeout,
        banner_timeout=banner_timeout,
        auth_timeout=auth_timeout,
        sftp_dir=sftp_dir,
        pattern=pattern,
        ftp_dest=ftp_dest,
    )

    def _run() -> None:
        t, sftp = _sftp_connect(
            host,
            port,
            user,
            password,
            identity,
            connect_timeout=connect_timeout,
            banner_timeout=banner_timeout,
            auth_timeout=auth_timeout,
        )
        tmp_path: Optional[str] = None
        try:
            picked = _sftp_pick_latest_name(sftp, sftp_dir, pattern)
            if picked is None:
                raise SystemExit(
                    f"LASGO SFTP mirror: no file matching {pattern!r} under {sftp_dir!r}"
                )
            filename, _ = picked
            src_full = f"{sftp_dir.rstrip('/')}/{filename}"

            fd, tmp_path = tempfile.mkstemp(prefix="lasgo_mirror_", suffix="_" + filename)
            os.close(fd)
            sftp.get(src_full, tmp_path)
            print(f"[LASGO mirror] SFTP pulled: {src_full} -> temp ({filename})")

            if scan_mode not in {"", "none", "off", "0", "false"}:
                run_security_scan(
                    tmp_path,
                    mode=scan_mode,
                    label=f"lasgo-sftp-mirror-{mode}",
                    max_bytes=max_bytes,
                    allow_extensions=allow,
                )

            ftp = build_ftp_client(supplier="lasgo")
            try:
                try:
                    ftp.cwd("/")
                except Exception:
                    pass
                _ftp_stor(ftp, tmp_path, ftp_dest.strip("/"), filename)
                print(
                    f"[LASGO mirror] FTP staged: {filename} -> {ftp_dest.rstrip('/')}/{filename} "
                    f"(for {mode} fetch)"
                )
            finally:
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
        finally:
            sftp.close()
            t.close()
            if tmp_path:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except OSError:
                    pass

    try:
        _run()
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise
        if swallow_errors:
            if isinstance(exc, SystemExit):
                detail = exc.code if isinstance(exc.code, str) else f"exit_code={exc.code!r}"
                print(
                    f"WARN: Lasgo SFTP mirror failed [mirror_abort] {detail}\n"
                    "WARN: Continuing without vendor SFTP mirror — next step is Lasgo FTP → local. "
                    "Stock data may come from the FTP inbox only or from files already on disk, "
                    "not from a fresh vendor SFTP pull."
                )
                return False
            cat, hint = classify_lasgo_sftp_error(exc)
            detail = str(exc).strip() or type(exc).__name__
            print(
                f"WARN: Lasgo SFTP mirror failed [{cat}] {detail}\n"
                f"      {hint}\n"
                "WARN: Continuing without vendor SFTP mirror — next step is Lasgo FTP → local. "
                "Stock data may come from the FTP inbox only or from files already on disk, "
                "not from a fresh vendor SFTP pull."
            )
            return False
        raise

    return True


def mirror_lasgo_sftp_to_ftp_if_enabled(mode: MirrorMode, *, env_file: str = ".env") -> None:
    """Pipeline entry (strict): no-op when mirror off; raises or exits on failure."""
    mirror_lasgo_sftp_to_ftp(mode, env_file=env_file, require_mirror_enabled=True, swallow_errors=False)
    # return value ignored — legacy void API
