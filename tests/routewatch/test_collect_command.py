import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "collect_routewatch.py"


def run_command(
    working_directory: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        cwd=working_directory,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_help_works_without_an_env_file(tmp_path: Path) -> None:
    result = run_command(tmp_path, "--help")

    assert result.returncode == 0
    assert "--max-messages" in result.stdout
    assert "--collector" in result.stdout
    assert "--log-file" in result.stdout


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("-1", "--max-messages must be zero or positive"),
        ("bad", "invalid int value"),
    ],
)
def test_invalid_limits_fail_before_connecting(
    tmp_path: Path,
    value: str,
    message: str,
) -> None:
    result = run_command(tmp_path, "--max-messages", value)

    assert result.returncode == 2
    assert message in result.stderr
    assert "Traceback" not in result.stderr
