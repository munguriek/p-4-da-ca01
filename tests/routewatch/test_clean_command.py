import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "clean_routewatch.py"


def run_command(
    working_directory: Path, *arguments: str
) -> subprocess.CompletedProcess[str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("ROUTEWATCH_")
    }
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        cwd=working_directory,
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_cleaner_help_works_without_configuration(tmp_path: Path) -> None:
    result = run_command(tmp_path, "--help")

    assert result.returncode == 0
    assert "--max-messages" in result.stdout
    assert "--batch-size" in result.stdout
    assert "--transform-version" in result.stdout
    assert "--follow" in result.stdout


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--max-messages", "-1", "--max-messages must be zero or positive"),
        ("--batch-size", "0", "--batch-size must be positive"),
        ("--transform-version", "0", "--transform-version must be positive"),
    ],
)
def test_bad_cleaner_limits_fail_before_configuration(
    tmp_path: Path, option: str, value: str, message: str
) -> None:
    result = run_command(tmp_path, option, value)

    assert result.returncode == 2
    assert message in result.stderr
    assert "Traceback" not in result.stderr
