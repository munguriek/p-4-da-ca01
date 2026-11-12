-- 002: the control tables: the source register, the run log and the quarantine.

-- one row per source, written before the source is first ingested.
CREATE TABLE control.source_register (
    source_id        text PRIMARY KEY, -- e.g gitlab.production
    name             text NOT NULL, 
    url              text NOT NULL,
    license          text NOT NULL,
    access           text NOT NULL CHECK(access IN ('stream', 'poll', 'file')),
    cadence          text NOT NULL, -- once, every N minutes, continuous ...etc
    personal_data    text NOT NULL, -- 'none' or pseudonymised
    datasheet_path   text NOT NULL, -- documentation e.g docs/datasheets/gitlab-production.md
    registered_at    timestamptz NOT NULL default now()
);