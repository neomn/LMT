import json
import logging
import os
import random
import socket
import sys
import threading
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
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "localhost")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", "5044"))

# The exporters are configured in the application so every service emits OTLP
# without needing a sidecar. The Collector remains the central routing point.
resource = Resource.create(
    {
        "service.name": SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": "observability-lab",
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
    "banking_transactions_total", description="Banking transactions by result"
)

RequestsInstrumentor().instrument()


def log_event(level, message, **fields):
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": SERVICE_NAME,
        "message": message,
        "trace_id": format(trace.get_current_span().get_span_context().trace_id, "032x"),
        **fields,
    }
    payload = (json.dumps(event) + "\n").encode()
    try:
        with socket.create_connection((LOGSTASH_HOST, LOGSTASH_PORT), timeout=1) as connection:
            connection.sendall(payload)
    except OSError:
        # Logging must not take down a banking request while Logstash starts/restarts.
        pass
    print(json.dumps(event), flush=True)


accounts = {
    "alice": {"balance": 12000.0, "currency": "USD"},
    "bob": {"balance": 7500.0, "currency": "USD"},
    "carol": {"balance": 3200.0, "currency": "USD"},
}
account_lock = threading.Lock()


def simulated_latency(operation):
    # A small tail-latency population makes bottleneck detection realistic.
    if random.random() < 0.08:
        delay = random.uniform(0.8, 2.5)
        log_event("WARN", "slow dependency simulation", operation=operation, delay_ms=round(delay * 1000))
        time.sleep(delay)
    else:
        time.sleep(random.uniform(0.01, 0.08))


def create_app(mode):
    app = Flask(SERVICE_NAME)
    FlaskInstrumentor().instrument_app(app)

    @app.before_request
    def start_request_timer():
        request._started_at = time.perf_counter()

    @app.after_request
    def record_request(response):
        duration_ms = (time.perf_counter() - request._started_at) * 1000
        attributes = {"http.method": request.method, "http.route": request.path, "http.status_code": response.status_code}
        request_counter.add(1, attributes)
        request_duration.record(duration_ms, attributes)
        return response

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "service": SERVICE_NAME})

    if mode == "account":
        @app.get("/accounts/<account_id>/balance")
        def balance(account_id):
            with tracer.start_as_current_span("account.lookup_balance") as span:
                span.set_attribute("bank.account_id", account_id)
                simulated_latency("balance_lookup")
                account = accounts.get(account_id)
                if not account:
                    span.set_attribute("bank.result", "not_found")
                    log_event("WARN", "account not found", operation="balance", account_id=account_id)
                    return jsonify({"error": "account not found"}), 404
                log_event("INFO", "balance checked", operation="balance", account_id=account_id)
                return jsonify({"account_id": account_id, **account})

        @app.post("/accounts/<account_id>/debit")
        def debit(account_id):
            body = request.get_json(silent=True) or {}
            amount = float(body.get("amount", 0))
            with tracer.start_as_current_span("account.debit") as span:
                span.set_attribute("bank.account_id", account_id)
                span.set_attribute("bank.amount", amount)
                simulated_latency("debit")
                with account_lock:
                    account = accounts.get(account_id)
                    if not account or amount <= 0 or account["balance"] < amount:
                        transaction_counter.add(1, {"operation": "debit", "result": "rejected"})
                        log_event("WARN", "debit rejected", operation="debit", account_id=account_id, amount=amount, reason="insufficient_funds_or_invalid_account")
                        return jsonify({"error": "debit rejected"}), 409
                    account["balance"] -= amount
                transaction_counter.add(1, {"operation": "debit", "result": "approved"})
                log_event("INFO", "debit approved", operation="debit", account_id=account_id, amount=amount)
                return jsonify({"status": "approved", "account_id": account_id, "amount": amount})

    if mode == "payment":
        @app.post("/transfers")
        def transfer():
            body = request.get_json(silent=True) or {}
            source = body.get("from", "alice")
            destination = body.get("to", "bob")
            amount = float(body.get("amount", 100))
            with tracer.start_as_current_span("payment.transfer") as span:
                span.set_attributes({"bank.source_account": source, "bank.destination_account": destination, "bank.amount": amount})
                simulated_latency("payment_risk_check")
                # This controlled error rate creates data for anomaly detection.
                if random.random() < 0.04:
                    transaction_counter.add(1, {"operation": "transfer", "result": "risk_rejected"})
                    log_event("WARN", "transfer rejected by risk simulation", operation="transfer", amount=amount, reason="risk_rule")
                    return jsonify({"status": "rejected", "reason": "risk_rule"}), 403
                account_url = os.getenv("ACCOUNT_SERVICE_URL", "http://localhost:8081")
                response = requests.post(
                    f"{account_url}/accounts/{source}/debit", json={"amount": amount}, timeout=5
                )
                if response.status_code != 200:
                    transaction_counter.add(1, {"operation": "transfer", "result": "failed"})
                    log_event("ERROR", "transfer failed during debit", operation="transfer", amount=amount, upstream_status=response.status_code)
                    return jsonify({"status": "failed", "reason": "debit_failed"}), 502
                # Credit is local to this simulation; the inter-service debit call provides a distributed trace.
                with account_lock:
                    if destination in accounts:
                        accounts[destination]["balance"] += amount
                transaction_counter.add(1, {"operation": "transfer", "result": "approved"})
                log_event("INFO", "transfer approved", operation="transfer", source=source, destination=destination, amount=amount)
                return jsonify({"status": "approved", "from": source, "to": destination, "amount": amount})

    return app


def run_traffic():
    logging.basicConfig(level=logging.INFO)
    payment_url = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8082")
    accounts_to_use = ["alice", "bob", "carol"]
    session = requests.Session()
    while True:
        started = time.perf_counter()
        source, destination = random.sample(accounts_to_use, 2)
        amount = random.choice([25, 50, 100, 250, 500, 2500])
        try:
            with tracer.start_as_current_span("traffic.scenario") as span:
                span.set_attribute("scenario.source", source)
                span.set_attribute("scenario.destination", destination)
                response = session.post(
                    f"{payment_url}/transfers",
                    json={"from": source, "to": destination, "amount": amount},
                    timeout=8,
                )
                log_event("INFO" if response.ok else "WARN", "traffic scenario completed", operation="transfer", source=source, destination=destination, amount=amount, status_code=response.status_code, duration_ms=round((time.perf_counter() - started) * 1000))
        except requests.RequestException as exc:
            log_event("ERROR", "traffic scenario failed", operation="transfer", error=str(exc))
        time.sleep(1.5)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "account"
    if mode == "traffic":
        run_traffic()
    else:
        port = int(os.getenv("SERVICE_PORT", "8080"))
        create_app(mode).run(host="0.0.0.0", port=port)
