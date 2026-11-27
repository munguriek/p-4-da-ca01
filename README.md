# RouteWatch

Programming for Data Analysis, continuous assessment 1, Dublin Business School.

RouteWatch streams live BGP update messages from [RIPE RIS Live](https://ris-live.ripe.net/) through Kafka into PostgreSQL. Every message is kept unchanged in a raw table, then cleaned into typed rows: one for each prefix a message announces or withdraws.

> **Change of project, 21 November 2026.** This repository first held airdeall, a larger pipeline for OpenTelemetry Demo telemetry and the GitLab.com production tracker. It could not be finished by the deadline, so the work moved to RouteWatch. airdeall's code and history are on the [`airdeall` branch](https://github.com/munguriek/p-4-da-ca01/tree/airdeall).

## How data flows

```text
RIS Live HTTP stream (collector rrc21, UPDATE messages)
  → scripts/collect_routewatch.py     → Kafka topic routewatch.ris.raw
  → scripts/consume_routewatch_raw.py → routewatch_raw.messages, as received
  → scripts/clean_routewatch.py       → routewatch_clean.prefix_events, one row per prefix action
                                        routewatch_clean.processing, one status row per raw message
```

The raw consumer commits each batch to PostgreSQL before it commits the Kafka offsets, and every insert skips rows already stored, so any step can stop and rerun without losing or duplicating data.

## Run it

You need Docker with Compose, and Python 3.12 or later.

```sh
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env    # then fill in the three values it leaves blank
docker compose -f compose.routewatch.yaml up -d --wait
bash scripts/create_routewatch_topics.sh
python scripts/init_routewatch_db.py
python scripts/collect_routewatch.py           # RIS Live into Kafka, until stopped
python scripts/consume_routewatch_raw.py       # Kafka into the raw table, until stopped
python scripts/clean_routewatch.py --follow    # raw into the clean tables, until stopped
```

To compare the clean tables with raw:

```sh
docker compose -f compose.routewatch.yaml exec -T routewatch-db \
  psql -U routewatch -d routewatch < scripts/audit_routewatch_clean.sql
```

## Web UI

A small read-only web UI has two pages. **How it works** explains the pipeline in nine short animations that can be paused, rewound and replayed; its examples use addresses and AS numbers reserved for documentation. **The data** draws the pipeline as a line, shows the raw and clean tables side by side, traces any clean row back to the raw message and Kafka offset it came from, and describes the Kafka topic and the raw consumer's progress. It uses Python's standard-library HTTP server and plain JavaScript, with no web framework.

```sh
python scripts/init_routewatch_ui_role.py   # once, and again after changing the UI password
python scripts/serve_routewatch_ui.py       # then open http://127.0.0.1:8765/
```

The UI connects to PostgreSQL as its own role, which can only read, and it listens on 127.0.0.1. On a server, reach it through an SSH tunnel rather than opening the port.

## Containers and deployment

One image runs every service; `compose.app.yaml` adds the collector, raw consumer, cleaner and UI, and two one-off jobs that create the topic, the raw table and the UI's role, to the Kafka and PostgreSQL in `compose.routewatch.yaml`. To run it all in containers instead of the scripts above (not both at once):

```sh
docker build -t routewatch:local .
docker compose -f compose.routewatch.yaml -f compose.app.yaml up -d --wait
```

Every push runs CI, including the image checked end to end with synthetic messages. A merge into `main` publishes the image and, after approval, deploys it to a Google Cloud VM, where it is checked against live data and rolled back if the checks fail. [docs/deploy.md](docs/deploy.md) covers the setup and day-to-day operation.

## Tests

```sh
pytest -m "not integration"                          # unit tests; nothing needs to run
ROUTEWATCH_RUN_INTEGRATION=1 pytest -m integration   # needs the stack, the topic and the raw table
```

## Data

RIS Live is a public service of the RIPE NCC: the routing data comes from the [RIPE NCC Routing Information Service (RIS)](https://www.ripe.net/ris), which the UI credits on every page. The collector names itself in the stream URL (`client=routewatch`). BGP updates describe networks and routes, not people. Collected data stays out of Git: the repository holds code, documentation and small synthetic test fixtures.