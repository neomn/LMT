# Banking Microservices Observability Lab

A Docker Compose environment that simulates a banking microservice application and produces logs, metrics, and distributed traces for later anomaly detection, bottleneck analysis, and observability experiments.

## Architecture

```text
traffic-generator
        |
        v
payment-service -------------> account-service
        |                              |
        | OTLP traces and metrics     | OTLP traces and metrics
        v                              v
              otel-collector
                 |
       +---------+----------+
       |                    |
       v                    v
    Jaeger             Prometheus endpoint
   traces                 metrics

banking services -- JSON/TCP --> Logstash --> Elasticsearch --> Kibana
```

## Start the environment

From the project root:

```bash
docker compose up -d --build
```

Check the running services:

```bash
docker compose ps
```

Follow application traffic:

```bash
docker compose logs -f traffic-generator payment-service account-service
```

Stop the environment:

```bash
docker compose down
```

Stop the environment and remove Elasticsearch data:

```bash
docker compose down -v
```

## Service endpoints

The host ports below are used by the current Compose configuration. Some ports are remapped from their standard values because they were already in use on the host machine.

| Service | Endpoint | Purpose |
|---|---|---|
| Kibana | http://localhost:5601 | Explore and visualize indexed banking logs |
| Elasticsearch | http://localhost:9200 | Search and store logs |
| Jaeger UI | http://localhost:16686 | Explore distributed traces |
| Account service | http://localhost:18081 | Banking account API |
| Payment service | http://localhost:18082 | Banking transfer API |
| OpenTelemetry Collector metrics | http://localhost:8889/metrics | Prometheus-format application metrics |
| Logstash TCP input | `localhost:5044` | JSON-lines log ingestion |
| Jaeger OTLP gRPC | `localhost:14317` | Host OTLP gRPC ingestion endpoint |
| Jaeger OTLP HTTP | `localhost:14318` | Host OTLP HTTP ingestion endpoint |

The application containers use internal Docker network addresses. For example:

- Collector OTLP gRPC: `http://otel-collector:4317`
- Account service: `http://account-service:8081`
- Payment service: `http://payment-service:8082`
- Logstash: `logstash:5044`

## Account service API

The account service is available externally at `http://localhost:18081`.

### Health check

```bash
curl http://localhost:18081/health
```

Example response:

```json
{"service":"account-service","status":"ok"}
```

### Get account balance

```bash
curl http://localhost:18081/accounts/alice/balance
```

Supported sample accounts:

- `alice`
- `bob`
- `carol`

Example response:

```json
{
  "account_id": "alice",
  "balance": 12000.0,
  "currency": "USD"
}
```

### Debit an account

```bash
curl -X POST http://localhost:18081/accounts/alice/debit \
  -H 'Content-Type: application/json' \
  -d '{"amount":100}'
```

Successful response:

```json
{
  "status": "approved",
  "account_id": "alice",
  "amount": 100.0
}
```

A debit can be rejected with HTTP `409` when the account does not exist, the amount is invalid, or the balance is insufficient.

## Payment service API

The payment service is available externally at `http://localhost:18082`.

### Health check

```bash
curl http://localhost:18082/health
```

### Transfer money

```bash
curl -X POST http://localhost:18082/transfers \
  -H 'Content-Type: application/json' \
  -d '{"from":"alice","to":"bob","amount":100}'
```

Example successful response:

```json
{
  "status": "approved",
  "from": "alice",
  "to": "bob",
  "amount": 100.0
}
```

A transfer may return:

- `200` — transfer approved
- `403` — rejected by the simulated risk rule
- `502` — debit failed in the account service

The payment service calls the account service to debit the source account. This creates a distributed trace spanning the traffic generator, payment service, and account service.

## Telemetry services

### OpenTelemetry Collector

Configuration: `otel-collector/config.yaml`

The Collector accepts OTLP data through:

- Internal gRPC receiver: `otel-collector:4317`
- Internal HTTP receiver: `otel-collector:4318`
- Host gRPC port: `localhost:4319`
- Host HTTP port: `localhost:4320`

The configured pipelines are:

- Traces: OTLP receiver -> memory limiter -> resource enrichment -> batch -> Jaeger and debug exporter
- Metrics: OTLP receiver -> memory limiter -> resource enrichment -> batch -> Prometheus exporter and debug exporter

The Collector exposes application metrics on:

```text
http://localhost:8889/metrics
```

Useful metric names include:

- `banking_requests_total`
- `banking_request_duration_ms_milliseconds`
- `banking_transactions_total`

### Jaeger

Open the Jaeger UI at:

```text
http://localhost:16686
```

The expected services in Jaeger are:

- `traffic-generator`
- `payment-service`
- `account-service`

Useful trace operations include:

- `traffic.scenario`
- `payment.transfer`
- `account.debit`
- `account.lookup_balance`

### Logstash and Elasticsearch

Application logs are emitted as JSON lines over TCP to Logstash on port `5044`.

Pipeline configuration: `logstash/pipeline/logstash.conf`

Logs are written to daily indices using this pattern:

```text
banking-logs-YYYY.MM.dd
```

For example:

```text
banking-logs-2026.09.20
```

Check available banking log indices:

```bash
curl 'http://localhost:9200/_cat/indices/banking-logs-*?format=json'
```

Search the most recent logs:

```bash
curl -X POST 'http://localhost:9200/banking-logs-*/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 20,
    "sort": [{"@timestamp": "desc"}],
    "query": {"match_all": {}}
  }'
```

In Kibana, create a data view with:

```text
banking-logs-*
```

Use `@timestamp` as the time field.

## Generated data for analysis

The traffic generator continuously sends transfers approximately every 1.5 seconds. The application intentionally creates both normal and abnormal behavior:

- Normal approved transfers
- Risk-rule rejections
- Insufficient-funds or invalid-account failures
- HTTP `502` responses when the debit operation fails
- Slow requests caused by simulated tail latency
- Inter-service calls that can be analyzed for bottlenecks

The simulated slow path is enabled for approximately 8% of operations and sleeps for roughly 0.8 to 2.5 seconds. This produces useful latency outliers in traces, logs, and request-duration histograms.

## Structured log fields

Application log records include fields such as:

- `timestamp`
- `level`
- `service`
- `message`
- `trace_id`
- `operation`
- `account_id`
- `source`
- `destination`
- `amount`
- `status_code`
- `duration_ms`
- `reason`

These fields can be used in Kibana queries and anomaly detection experiments.

Example Elasticsearch query for errors:

```bash
curl -X POST 'http://localhost:9200/banking-logs-*/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "should": [
          {"term": {"level.keyword": "ERROR"}},
          {"term": {"level.keyword": "WARN"}},
          {"range": {"duration_ms": {"gte": 1000}}}
        ],
        "minimum_should_match": 1
      }
    }
  }'
```

## Useful operational commands

View all logs:

```bash
docker compose logs -f
```

View only OpenTelemetry Collector logs:

```bash
docker compose logs -f otel-collector
```

Restart the application services:

```bash
docker compose restart account-service payment-service traffic-generator
```

Rebuild the application image after code changes:

```bash
docker compose up -d --build account-service payment-service traffic-generator
```

Inspect the rendered Compose configuration:

```bash
docker compose config
```

## Data persistence

Elasticsearch data is stored in the Docker volume:

```text
lmt_elasticsearch-data
```

Removing the volume with `docker compose down -v` deletes the indexed logs.

## Scope and limitations

This is a local observability simulation, not a production banking system. The application uses in-memory account data, has no authentication, does not implement durable transactions, and does not provide production-grade security or high availability. It is intended for telemetry generation and analysis experiments.
