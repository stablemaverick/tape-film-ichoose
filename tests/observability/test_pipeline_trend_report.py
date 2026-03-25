"""pipeline_trend_report.py behaviour when history file is absent."""

import os
import subprocess
import sys


def test_missing_history_file_exits_zero():
    root = os.path.join(os.path.dirname(__file__), "..", "..")
    script = os.path.join(root, "scripts", "observability", "pipeline_trend_report.py")
    bogus = os.path.join(root, "logs", "nonexistent_pipeline_history_xyz.json")
    proc = subprocess.run(
        [sys.executable, script, "--history-file", bogus],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0
    assert "No pipeline history file" in proc.stdout

    proc2 = subprocess.run(
        [sys.executable, script, "--history-file", bogus, "--format", "json"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc2.returncode == 0
    assert '"status": "no_history"' in proc2.stdout
