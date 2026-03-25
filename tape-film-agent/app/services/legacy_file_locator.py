import fnmatch
import os
from pathlib import Path


def find_latest_matching_file(directory: str, pattern: str) -> str:
    path = Path(directory)

    if not path.exists():
        raise SystemExit(f"Directory does not exist: {directory}")

    matches = [p for p in path.iterdir() if p.is_file() and fnmatch.fnmatch(p.name, pattern)]
    if not matches:
        raise SystemExit(f"No files matched pattern '{pattern}' in {directory}")

    latest = max(matches, key=lambda p: p.stat().st_mtime)
    return str(latest)


def get_supplier_file_locations() -> dict:
    return {
        "lasgo_dir": os.getenv("LASGO_STOCK_DIR", "/opt/tape-film/sftp/lasgo"),
        "moovies_dir": os.getenv("MOOVIES_STOCK_DIR", "/opt/tape-film/sftp/moovies"),
        "lasgo_pattern": os.getenv("LASGO_GLOB", "LASGO_*"),
        "moovies_pattern": os.getenv("MOOVIES_GLOB", "Feed-*"),
    }
