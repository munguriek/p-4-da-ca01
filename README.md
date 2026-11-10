# Airdeall 
Programming for Data Analysis - Continuous Assessment I

For this module project I will build a prototype data acquisition pipeline that streams operational data from publicly available live sources into PostgreSQL and cleans it into analysis-ready tables. Each record lands unchanged in a raw table, then moves to typed tables: deduplicated, timestamped, names against a small ontology and usernames pseudonymised. I will demonstrate continuous ingestion, report data quality per source, and answer analytical questions in SQL and Cypher, such as how service error rates respond after a change.

## The Streaming sources

### OpenTelemetry Demo, for metrics, logs and traces
https://github.com/open-telemetry/opentelemetry-demo

### GitLab.com production change requests and incidents
https://gitlab.com/gitlab-com/gl-infra/production/-/issues

## The Data to load

### Alibaba cluster traces for microservice call graphs and container placement
https://github.com/alibaba/clusterdata

### Google Cloud incident history
https://status.cloud.google.com/incidents.json

### CISA Known Exploited Vulnerabilities
https://www.cisa.gov/known-exploited-vulnerabilities-catalog

### The OpenTelemetry semantic conventions as the vocabulary
https://github.com/open-telemetry/semantic-conventions.


## The plan
Leverage public vocabularies for infrastructure and the loaded datasets to define a small ontology of services, hosts, containers, changes, incidents and vulnerabilities. The cleaned streams are to be mapped to the ontology and linked as a context graph. I intend to use this module’s work as a starting point of delivery for the data foundation for my MSc research question which is - “How can AI-based agents reason over enterprise infrastructure state and translate natural-language operational intent into safe, governed and auditable infrastructure actions?”
