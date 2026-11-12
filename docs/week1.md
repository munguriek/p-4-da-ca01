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


## Step 2 · The pipeline stack

**Work done**
- `compose.yaml`: apache/kafka 4.4.0 (one KRaft broker) and postgres 18.6 on the `pipeline` network; project name `airdeall`; ports published on 127.0.0.1 only.
- `.env.example` and `scripts/create_topics.sh` (safe to rerun).

**Test results** (commit ..., date ...)
- `docker compose up -d --wait`: ...
- `./scripts/create_topics.sh`: ...

**Decisions**
- postgres 18.6 rather than the demo's 18.4, for the newest fixes.
- Share-coordinator settings taken from Kafka's official 4.4.0 single-node example.
- Published ports bound to 127.0.0.1.
- Project name fixed as `airdeall`, so volume names don't depend on the folder name.

## Step 3 · Schema and migrations

**Work done**
- `sql/migrations/001_schemas.sql`, `002_control.sql` and `003_raw.sql`: five schemas; source register, run log and quarantine; `raw.otel` (daily partitions on `kafka_ts`, with `payload_bytes`) and `raw.events`.
- `airdeall.migrations` (`load_migrations`, `migrate`), `airdeall.settings` (`.env` reader and connection string) and the `airdeall migrate` command.
- Tests: 5 unit and 3 integration; each integration test runs in a throwaway database.

**Test results** (commit ..., date ...)
- `pytest -m "not integration"`: ...
- `pytest -m integration`: ...
- `airdeall migrate`: first run ...; second run ...

**Decisions**
- Quarantine payload stored as `bytea`, so records that aren't valid UTF-8 or contain NULL bytes can still be kept.
- Kafka columns named `kpartition` and `koffset` throughout, because `offset` is reserved in SQL.
- `run_id` defaults to `uuidv7()`, new in PostgreSQL 18.
- Settings read from `.env` by `airdeall.settings`, with the environment taking precedence; no new dependency.

