## Step 1 · Repository and tooling

**Work done**
- Repository laid out as in the guide; project, package and command named `airdeall`.
- `pyproject.toml`: setuptools 84.2.1; exact pins for confluent-kafka 2.16.1, psycopg[binary] 3.3.8, pytest 9.2.0 and ruff 0.17.4; strict pytest, importlib import mode and the `integration` marker; ruff held to 0.17.4, with the DTZ and S608 rules added.
- `.gitignore`: `data/`, `playground.py` and Claude Code's local files. README brought in line with the current scope.

**Test results** (commit ..., Python ...)
- `ruff check .`: ...
- `ruff format --check .`: ...
- `pytest`: ... (exit code 5 is expected while there are no tests)

**Decisions**
- Build backend: setuptools, pinned, because ...
- Python ... on the laptop, in CI and on the VM (Ubuntu 26.04); `requires-python` stays `>=3.12`.
- Repository rename to `airdeall`: ...

**Next step**
- Step 2: the pipeline stack.