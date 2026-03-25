import os
import subprocess
import time
import traceback
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LEGACY_ROOT = Path(
    os.getenv(
        "TAPE_FILM_LEGACY_ROOT",
        str(REPO_ROOT / "legacy"),
    )
)

def run_command(command: list[str], working_dir: Path | None = None) -> None:
    cwd = str(working_dir or LEGACY_ROOT)
    print(f"[job] running in {cwd}: {' '.join(command)}")
    subprocess.run(command, cwd=cwd, check=True)


def require_file(path: Path, hint: str = "") -> Path:
    if not path.exists():
        extra = f" {hint}" if hint else ""
        raise SystemExit(f"Required file not found: {path}.{extra}")
    return path


# ✅ NEW: standard job runner
def run_job(job_name: str, fn):
    print(f"\n=== START JOB: {job_name} ===")
    start_time = time.time()

    try:
        result = fn()

        duration = round(time.time() - start_time, 2)
        print(f"=== SUCCESS: {job_name} ({duration}s) ===\n")

        return result

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        print(f"=== FAILED: {job_name} ({duration}s) ===")
        print(str(e))
        traceback.print_exc()

        # important for Render cron: non-zero exit
        raise
