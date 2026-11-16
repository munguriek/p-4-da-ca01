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

This data processing methodology follows the framework outlined in the [EDPB Guidelines 01/2025 on Pseudonymisation](https://www.edpb.europa.eu/documents/guideline/guidelines-012025-on-pseudonymisation_en).

* **Purpose & Data Minimisation:** This dataset contains only the minimum commit metadata required for analysis (timestamps, commit statistics, and file paths). **No raw source code is collected or stored.**
* **Pseudonymisation Transformation:** Direct human identifiers extracted from GitLab (usernames, display names, and email addresses) have been replaced with non-attributable pseudonyms (e.g., `User_001`).
* **Pseudonymisation Domain & Safeguards:** Anyone accessing this repository operates strictly within the pseudonymised domain. Any additional information required to reverse the mapping (such as lookup tables or cryptographic salts) is stored in a separate, access-controlled environment and is excluded from version control.
