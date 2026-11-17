# Airdeall 
Programming for Data Analysis - Continuous Assessment I

For this module project I will build a prototype data acquisition pipeline that streams operational data from publicly available live sources into PostgreSQL and cleans it into analysis-ready tables. Each record lands unchanged in a raw table, then moves to typed tables: deduplicated, timestamped, standardised names against a small ontology and usernames pseudonymised. I will demonstrate continuous ingestion, report data quality per source, and answer analytical questions in SQL and Cypher or NetworkX, such as how service error rates respond after a change.

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

## Declaration of Assistance
Some of this work was delivered with assistance of external resources (youtube videos, blogs, help forums) and AI i.e Claude and will be attributed accordingly. Especially for Kafka, Postgres and Docker however since it is a learning project, all the code has been hand typed (NO copy-paste) line by line AI generated or not 
## DISCLAIMER: IT SHOULD BE NOTED THAT ALL PYTHON CODE HAS BEEN HAND WRITTEN WITHOUT AI ASSISTANCE. FOR PYTHON + TESTS, THE SCOPE OF AI HELP HAS BEEN DELIBERATELY LIMITED TO ONLY PSUEDO CODE GENERATION IN THE PLANNING PHASE.
The folder assistance contains documentation of all external assistance and parts of code that is not my original work including the prelimary discussions/brainstorm sessions done with claude. 
From this point (commit) going forward, all assistance will be attributed with comments and clear commit messages. 
# The structure
In the code, AI generated code will be surrounded with a comment for attribution
\# Start of AI generated code
... code block
... code block
\# End of AI generated code
For in-line code block, it will have a line comment # AI generated
Code obtained from other sources will have their URLs added to the project as a comment
# Assistance Folder Structure
assistance
|
-- prompts
|
-- psuedo-code
|
-- ai-generated-documentation

## Ethics and Data Privacy

Only one source in this project contains personal data. Its handling follows the EDPB's [Guidelines 01/2025 on Pseudonymisation](https://www.edpb.europa.eu/our-work-tools/documents/public-consultations/2025/guidelines-012025-pseudonymisation_en).

* **Sources.** The OpenTelemetry Demo's telemetry is synthetic: its load generator creates the shoppers, and the demo masks card numbers and hashes email addresses in its traces. CISA's Known Exploited Vulnerabilities catalogue, Google Cloud's incident history, the Alibaba cluster traces and the OpenTelemetry semantic conventions describe systems, not people. Issues from GitLab.com's public production tracker do name real people: authors, assignees, people who change labels, and anyone @-mentioned in titles and descriptions.
* **Pseudonymisation at source.** Before any record reaches Kafka, the GitLab poller will replace usernames, display names and @-mentions with HMAC-SHA256 pseudonyms, and will drop profile and avatar URLs. The key is kept only in a local `.env` file, which is excluded from version control; without it, no pseudonym can be linked back to a username. Pseudonymised data is still personal data under the GDPR, so it remains protected as such.
* **Minimisation.** The poller will keep only the issue fields the analysis needs.
* **Where data lives.** This repository publishes no data: it holds code, documentation and small synthetic test fixtures only. Collected data stays in a local PostgreSQL database and a git-ignored `data/` folder, and the database and Kafka accept connections from the local machine only.
* **Access and terms.** GitLab is read with a read-only (`read_api`) token, within its rate limits. Each source's licence and terms are recorded in its datasheet. The Alibaba traces are used for research and study only and are not redistributed.
