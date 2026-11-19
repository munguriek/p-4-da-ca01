-- 005: register every source in the week 1 guide before any of it is ingested.
-- A correction goes in a new migration: migrate refuses an edited file.
INSERT INTO control.source_register
    (source_id, name, url, license, access, cadence, personal_data, datasheet_path)
VALUES
    ('otel.demo', 'OpenTelemetry Demo 3.1.0 telemetry',
     'https://github.com/open-telemetry/opentelemetry-demo',
     'Apache-2.0', 'stream', 'continuous',
     'none known: synthetic users from the load generator',
     'docs/datasheets/otel-demo.md'),
    ('gitlab.production', 'GitLab.com production tracker issues',
     'https://gitlab.com/gitlab-com/gl-infra/production',
     'GitLab.com terms; public issues, no licence stated', 'poll', 'every 10 minutes',
     'usernames and @-mentions: HMAC-SHA256 pseudonyms in the poller',
     'docs/datasheets/gitlab-production.md'),
    ('cisa.kev', 'CISA Known Exploited Vulnerabilities catalogue',
     'https://github.com/cisagov/kev-data',
     'CC0-1.0', 'file', 'once', 'none',
     'docs/datasheets/cisa-kev.md'),
    ('gcp.incidents', 'Google Cloud Service Health incident history',
     'https://status.cloud.google.com/incidents.json',
     'Google terms; no licence stated', 'file', 'once', 'none',
     'docs/datasheets/gcp-incidents.md'),
    ('otel.semconv', 'OpenTelemetry semantic conventions v1.45.0',
     'https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.45.0',
     'Apache-2.0', 'file', 'once', 'none',
     'docs/datasheets/otel-semconv.md'),
    ('alibaba.v2018', 'Alibaba cluster trace v2018: machine_meta and container_meta',
     'https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/trace_2018.md',
     'research or study use only, per the publisher', 'file', 'once',
     'none: machines and containers only',
     'docs/datasheets/alibaba-v2018.md')
ON CONFLICT (source_id) DO NOTHING;