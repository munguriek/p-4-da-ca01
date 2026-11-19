# OpenTelemetry Demo 3.1.0 telemetry
Publisher and URL: OpenTelemetry project, https://github.com/open-telemetry/opentelemetry-demo
Licence or terms: Apache-2.0
How it arrives (stream, poll or file) and how often: stream, continuous: the demo's Collector exports to the Kafka topics otel.spans, otel.metrics and otel.logs
Format and version: OTLP JSON from demo release 3.1.0, pinned in the demo's .env.override; traces sampled by the Collector extras at the percentage recorded in docs/week1.md
Personal data, and how it is handled: none known: the users are synthetic, made by the demo's load generator
Known issues: the demo's own .env asks for the latest images, so start it only with make start-minimal from the demo folder; payload_bytes counts uncompressed JSON
Checksum and download date (files): not a file; raw.otel keeps each message's SHA-256
Registered on: TODO
