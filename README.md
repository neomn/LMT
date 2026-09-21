# Banking Microservices Observability Lab

A Docker Compose observability lab that simulates a large banking platform with **30 application services**. The application generates structured logs, OpenTelemetry metrics, and distributed traces for anomaly detection, bottleneck analysis, dependency mapping, and team-level observability experiments.

The 30 application services are:

- 29 banking microservices
- 1 continuous `traffic-generator` workload service

The observability infrastructure is additional and includes Elasticsearch, Logstash, Kibana, Jaeger, and the OpenTelemetry Collector.

## Start the platform

From the project root:

```bash
docker compose up -d --build
```

Check all containers:

```bash
docker compose ps
```

The first build creates one reusable instrumented Python image and runs it with different service metadata and dependency configuration.

Follow all banking traffic:

```bash
docker compose logs -f traffic-generator api-gateway ipg switch esb core-banking
```

Stop the platform:

```bash
docker compose down
```

Stop the platform and delete Elasticsearch data:

```bash
docker compose down -v
```

## Team and service inventory

### Channel & Edge team

| Service | Role | Downstream services |
|---|---|---|
| `api-gateway` | Gateway | `ipg`, `mobile-banking`, `internet-banking`, `branch-channel`, `atm-channel` |
| `ipg` | Internet payment gateway / entry point | `switch`, `fraud-service` |
| `mobile-banking` | Digital channel | `identity-service`, `payment-service` |
| `internet-banking` | Digital channel | `identity-service`, `account-service` |
| `branch-channel` | Branch channel | `identity-service`, `account-service` |
| `atm-channel` | ATM channel | `identity-service`, `atm-service` |

### Integration team

| Service | Role | Downstream services |
|---|---|---|
| `switch` | Payment switch / entry point | `esb`, `fraud-service` |
| `esb` | Enterprise service bus / entry point | `core-banking`, `customer-service` |

### Core Banking team

| Service | Role | Downstream services |
|---|---|---|
| `core-banking` | Core banking orchestrator / entry point | `ledger-service`, `transaction-service` |
| `customer-service` | Customer domain service | `account-service`, `identity-service` |
| `account-service` | Account domain service | None |
| `ledger-service` | Ledger domain service | `transaction-service`, `audit-service` |
| `balance-service` | Balance domain service | `account-service`, `ledger-service` |
| `transaction-service` | Transaction domain service | `account-service`, `ledger-service` |

### Payments & Cards team

| Service | Role | Downstream services |
|---|---|---|
| `payment-service` | Payment orchestrator | `risk-engine`, `transfer-service` |
| `transfer-service` | Transfer domain service | `balance-service`, `ledger-service`, `notification-service` |
| `card-service` | Card domain service | `risk-engine`, `account-service`, `notification-service` |
| `atm-service` | ATM transaction service | `card-service`, `balance-service` |
| `bill-payment-service` | Bill payment domain service | `payment-service`, `customer-service` |

### Risk & Compliance team

| Service | Role | Downstream services |
|---|---|---|
| `fraud-service` | Fraud decision service | `risk-engine`, `aml-service` |
| `aml-service` | Anti-money-laundering decision service | `audit-service`, `customer-service` |
| `risk-engine` | Risk decision service | `customer-service` |

### Operations & Data team

| Service | Role | Downstream services |
|---|---|---|
| `notification-service` | Notification integration service | `customer-service`, `audit-service` |
| `reconciliation-service` | Reconciliation batch service | `ledger-service`, `settlement-service` |
| `settlement-service` | Settlement batch service | `ledger-service`, `transaction-service` |
| `reporting-service` | Reporting service | `reconciliation-service`, `audit-service` |
| `statement-service` | Statement service | `reporting-service`, `account-service` |
| `identity-service` | Identity/platform service | `audit-service` |
| `audit-service` | Audit/platform service | None |

### Workload team

| Service | Role | Behavior |
|---|---|---|
| `traffic-generator` | Workload generator | Calls all 29 banking services in rotation |

## Service endpoints

All internal application services listen on port `8080` inside the Compose network. The main entry points are exposed on the host for manual testing:

| Service | Host endpoint | Purpose |
|---|---|---|
| API gateway | http://localhost:18080 | Main banking entry point |
| IPG | http://localhost:18083 | Internet payment gateway |
| Switch | http://localhost:18084 | Payment switching entry point |
| ESB | http://localhost:18085 | Enterprise integration bus |
| Core banking | http://localhost:18086 | Core banking orchestration |

Each application service exposes:

```text
GET  /health
GET  /info
POST /process
```

Example health check:

```bash
curl http://localhost:18080/health
```

Example transaction submitted to the API gateway:

```bash
curl -X POST http://localhost:18080/process \
  -H 'Content-Type: application/json' \
  -d '{"operation":"fund.transfer","amount":250,"correlation_id":"manual-001"}'
```

Example direct IPG request:

```bash
curl -X POST http://localhost:18083/process \
  -H 'Content-Type: application/json' \
  -d '{"operation":"card.purchase","amount":100,"correlation_id":"ipg-001"}'
```

The response includes the selected downstream calls and any degraded dependencies.

## Observability endpoints

| Component | Endpoint | Purpose |
|---|---|---|
| Jaeger UI | http://localhost:16686 | Search distributed traces and dependency graph |
| Jaeger OTLP gRPC | `localhost:14317` | Host OTLP gRPC endpoint |
| Jaeger OTLP HTTP | `localhost:14318` | Host OTLP HTTP endpoint |
| Kibana | http://localhost:5601 | Explore and visualize logs |
| Elasticsearch | http://localhost:9200 | Log search and storage |
| Collector metrics | http://localhost:8889/metrics | Prometheus-format metrics |
| Logstash TCP | `localhost:5044` | JSON-lines log ingestion |

Internal application exporters use:

- OTLP Collector: `otel-collector:4317`
- Logstash: `logstash:5044`

## How the topology generates traces

The traffic generator calls every banking service in rotation. Each service:

1. Creates an instrumented server span for `/process`.
2. Adds `bank.team`, `bank.service_role`, operation, and dependency attributes.
3. Selects one or two configured downstream services.
4. Calls each downstream service over HTTP.
5. Propagates the active trace context through the OpenTelemetry Requests instrumentation.
6. Emits structured logs containing `trace_id`, team, service, operation, and dependency status.

A typical trace can look like:

```text
traffic-generator
  -> api-gateway
      -> ipg
          -> switch
              -> esb
                  -> core-banking
                      -> ledger-service
                          -> audit-service
```

The exact path varies because services randomly select one or two configured dependencies per request.

## View all services in Jaeger

Open:

```text
http://localhost:16686
```

In Jaeger:

1. Wait at least 15–30 seconds after startup for traces to be exported.
2. Open **System Architecture** or `/dependencies` to see the service graph.
3. In **Search**, select `traffic-generator`, `api-gateway`, or another service.
4. Set the lookback window to `Last Hour`.
5. Click **Find Traces**.
6. Open a trace to inspect cross-team calls and latency.

The service dropdown should eventually contain all 30 application services. The reliable check is the Jaeger API:

```bash
curl -s http://localhost:16686/api/services | jq
```

To count services reported by Jaeger:

```bash
curl -s http://localhost:16686/api/services | jq '.data | length'
```

The count increases as each service emits its first trace. Because the traffic generator rotates through all targets, all 30 application services should appear after the workload has run for a short period.

Query recent traces for a specific service:

```bash
curl -s 'http://localhost:16686/api/traces?service=api-gateway&lookback=1h&limit=10' | jq
```

Continuously watch the reported service count:

```bash
watch -n 5 'curl -s http://localhost:16686/api/services | jq ".data | length"'
```

## Logs in Kibana and Elasticsearch

Open Kibana:

```text
http://localhost:5601
```

Create a data view using:

```text
banking-logs-*
```

Use `@timestamp` as the time field and set the refresh interval to 5 seconds.

Watch logs in the terminal:

```bash
docker compose logs -f traffic-generator api-gateway payment-service core-banking
```

Check indexed log documents:

```bash
curl 'http://localhost:9200/_cat/indices/banking-logs-*?format=json'
```

Search recent errors and slow requests:

```bash
curl -X POST 'http://localhost:9200/banking-logs-*/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 20,
    "sort": [{"@timestamp": "desc"}],
    "query": {
      "bool": {
        "should": [
          {"term": {"level.keyword": "ERROR"}},
          {"term": {"level.keyword": "WARN"}},
          {"range": {"delay_ms": {"gte": 1000}}}
        ],
        "minimum_should_match": 1
      }
    }
  }'
```

Useful Kibana filters:

```text
team: "channel-edge"
```

```text
service: "ipg" AND level: "ERROR"
```

```text
service_role: "entry-point"
```

```text
message: "simulated tail latency"
```

## Metrics

The Collector exposes Prometheus-format metrics:

```text
http://localhost:8889/metrics
```

Useful metrics include:

- `banking_requests_total`
- `banking_request_duration_ms_milliseconds`
- `banking_transactions_total`
- `banking_dependency_calls_total`

Watch service activity:

```bash
watch -n 5 'curl -s http://localhost:8889/metrics | grep banking_requests_total'
```

Watch cross-service calls:

```bash
watch -n 5 'curl -s http://localhost:8889/metrics | grep banking_dependency_calls_total'
```

## Simulated anomaly and bottleneck signals

The simulator intentionally produces useful analysis data:

- Approximately 8% tail-latency requests with 0.8–2.5 second delays
- Approximately 2.5% controlled service failures with HTTP `503`
- Degraded responses when downstream dependencies fail
- Variable fan-out at gateways and orchestrators
- Cross-team dependency calls
- Correlation IDs and trace IDs in logs
- Entry-point, integration, core-banking, payment, risk, and operations paths

These signals support experiments such as:

- Finding the slowest team or service
- Comparing entry-point latency with core-service latency
- Detecting elevated failure rates by team
- Identifying high-fanout services
- Finding dependency edges with repeated failures
- Correlating Kibana log errors with Jaeger trace IDs

## Configuration model

The service topology is defined in `compose.yaml`. Each application service has:

- `SERVICE_NAME`
- `TEAM`
- `SERVICE_ROLE`
- `SERVICE_DEPENDENCIES`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`

The reusable simulator is in `banking-app/app.py`.

The OpenTelemetry Collector configuration is in `otel-collector/config.yaml`.

The Logstash pipeline is in `logstash/pipeline/logstash.conf`.

## Troubleshooting

If Jaeger shows fewer than 30 services:

```bash
docker compose ps
```

Confirm that all application containers are running. Then check the traffic generator:

```bash
docker compose logs --tail=100 traffic-generator
```

Check the service list directly:

```bash
curl -s http://localhost:16686/api/services | jq
```

Restart and rebuild the application services:

```bash
docker compose up -d --build
```

If old trace data makes the graph confusing, restart Jaeger:

```bash
docker compose restart jaeger otel-collector
```

## Scope and limitations

This is a local telemetry simulation, not a production banking system. Services use in-memory behavior, there is no authentication or authorization, transactions are not durable, and the platform does not model production security, persistence, or high availability. It is intended for observability, anomaly detection, and bottleneck-analysis experiments.
