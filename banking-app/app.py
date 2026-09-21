import json
import os
import random
import socket
import sys

import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", os.getenv("SERVICE_NAME", "banking-service"))
TEAM = os.getenv("TEAM", "unknown")
SERVICE_ROLE = os.getenv("SERVICE_ROLE", "microservice")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8080"))
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "localhost")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", "5044"))
DEPENDENCIES = [item.strip() for item in os.getenv("SERVICE_DEPENDENCIES", "").split(",") if item.strip()]
TARGETS = [item.strip() for item in os.getenv("TARGETS", "").split(",") if item.strip()]

resource = Resource.create(
    {
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "observability-lab",
        "bank.team": TEAM,
        "bank.service_role": SERVICE_ROLE,
    }
)
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True))
)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(SERVICE_NAME)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTLP_ENDPOINT, insecure=True), export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(SERVICE_NAME)
request_counter = meter.create_counter("banking_requests_total", description="Banking API requests")
request_duration = meter.create_histogram(
    "banking_request_duration_ms", unit="ms", description="Banking API duration"
)
transaction_counter = meter.create_counter(
    "banking_transactions_total", description="Simulated banking transactions"
)
dependency_counter = meter.create_counter(
    "banking_dependency_calls_total", description="Downstream dependency calls"
)

RequestsInstrumentor().instrument()


def log_event(level, message, **fields):
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": SERVICE_NAME,
        "team": TEAM,
        "service_role": SERVICE_ROLE,
        "message": message,
        "trace_id": format(trace.get_current_span().get_span_context().trace_id, "032x"),
        **fields,
    }
    payload = (json.dumps(event) + "\n").encode()
    try:
        with socket.create_connection((LOGSTASH_HOST, LOGSTASH_PORT), timeout=1) as connection:
            connection.sendall(payload)
    except OSError:
        pass
    print(json.dumps(event), flush=True)


def simulated_latency(operation):
    if random.random() < 0.08:
        delay = random.uniform(0.8, 2.5)
        log_event("WARN", "simulated tail latency", operation=operation, delay_ms=round(delay * 1000))
        time.sleep(delay)
    else:
        time.sleep(random.uniform(0.01, 0.08))



def process_dependencies(payload):
    visited = set(payload.get("visited", []))
    candidates = [dependency for dependency in DEPENDENCIES if dependency not in visited]
    if not candidates or len(visited) >= 10:
        return [], []

    # Keep fanout bounded so a single scenario creates a useful but manageable trace.
    fanout = min(len(candidates), 2 if SERVICE_ROLE in {"entry-point", "orchestrator", "gateway"} else 1)
    selected = random.sample(candidates, fanout)
    responses = []
    failures = []
    for dependency in selected:
        target = f"http://{dependency}:8080"
        dependency_counter.add(1, {"dependency": dependency, "team": TEAM})
        try:
            response = requests.post(f"{target}/process", json=payload, timeout=4)
            responses.append({"service": dependency, "status_code": response.status_code})
            if response.status_code >= 500:
                failures.append(dependency)
        except requests.RequestException as exc:
            failures.append(dependency)
            log_event("WARN", "dependency call failed", dependency=dependency, error=str(exc))
    return responses, failures


def create_app():
    app = Flask(SERVICE_NAME)
    FlaskInstrumentor().instrument_app(app)

    @app.before_request
    def start_request_timer():
        request._started_at = time.perf_counter()

    @app.after_request
    def record_request(response):
        duration_ms = (time.perf_counter() - request._started_at) * 1000
        attributes = {
            "http.method": request.method,
            "http.route": request.path,
            "http.status_code": response.status_code,
            "bank.team": TEAM,
        }
        request_counter.add(1, attributes)
        request_duration.record(duration_ms, attributes)
        return response

    @app.get("/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "service": SERVICE_NAME,
                "team": TEAM,
                "role": SERVICE_ROLE,
                "dependencies": DEPENDENCIES,
            }
        )

    @app.get("/info")
    def info():
        return jsonify({"service": SERVICE_NAME, "team": TEAM, "role": SERVICE_ROLE, "dependencies": DEPENDENCIES})

    @app.post("/process")
    def process():
        body = request.get_json(silent=True) or {}
        operation = body.get("operation", "banking.transaction")
        with tracer.start_as_current_span(f"{SERVICE_NAME}.process") as span:
            span.set_attributes(
                {
                    "bank.service": SERVICE_NAME,
                    "bank.team": TEAM,
                    "bank.service_role": SERVICE_ROLE,
                    "bank.operation": operation,
                    "bank.dependency_count": len(DEPENDENCIES),
                }
            )
            simulated_latency(operation)
            if random.random() < 0.025:
                transaction_counter.add(1, {"service": SERVICE_NAME, "team": TEAM, "result": "error"})
                log_event("ERROR", "simulated service failure", operation=operation, reason="fault_injection")
                return jsonify({"status": "failed", "service": SERVICE_NAME, "reason": "fault_injection"}), 503

            downstream_payload = {
                "operation": operation,
                "correlation_id": body.get("correlation_id", f"txn-{random.randint(100000, 999999)}"),
                "amount": body.get("amount", random.choice([25, 50, 100, 250, 500, 2500])),
                "origin": body.get("origin", SERVICE_NAME),
                "visited": [*body.get("visited", []), SERVICE_NAME],
            }
            responses, failures = process_dependencies(downstream_payload)
            result = "degraded" if failures else "approved"
            transaction_counter.add(1, {"service": SERVICE_NAME, "team": TEAM, "result": result})
            log_event(
                "WARN" if failures else "INFO",
                "service operation completed",
                operation=operation,
                downstream_count=len(responses),
                downstream_failures=failures,
                result=result,
            )
            return jsonify(
                {
                    "status": result,
                    "service": SERVICE_NAME,
                    "team": TEAM,
                    "downstream": responses,
                    "downstream_failures": failures,
                }
            ), (207 if failures else 200)

    return app


def run_traffic():
    if not TARGETS:
        log_event("ERROR", "no traffic targets configured")
        while True:
            time.sleep(60)

    session = requests.Session()
    index = 0
    while True:
        target = TARGETS[index % len(TARGETS)]
        index += 1
        started = time.perf_counter()
        payload = {
            "operation": random.choice(
                ["card.purchase", "cash.withdrawal", "account.inquiry", "fund.transfer", "bill.payment"]
            ),
            "amount": random.choice([25, 50, 100, 250, 500, 2500]),
            "origin": "traffic-generator",
            "correlation_id": f"txn-{random.randint(100000, 999999)}",
        }
        try:
            with tracer.start_as_current_span("traffic.scenario") as span:
                span.set_attributes({"bank.target": target, "bank.operation": payload["operation"]})
                response = session.post(f"http://{target}:8080/process", json=payload, timeout=12)
                log_event(
                    "INFO" if response.ok else "WARN",
                    "traffic scenario completed",
                    target=target,
                    operation=payload["operation"],
                    status_code=response.status_code,
                    duration_ms=round((time.perf_counter() - started) * 1000),
                )
        except requests.RequestException as exc:
            log_event("ERROR", "traffic scenario failed", target=target, error=str(exc))
        time.sleep(1.0)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "service"
    if mode == "traffic":
        run_traffic()
    else:
        create_app().run(host="0.0.0.0", port=SERVICE_PORT)
