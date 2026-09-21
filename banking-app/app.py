import json
import logging
import math
import os
import random
import shutil
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone

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
from opentelemetry.trace import Status, StatusCode

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", os.getenv("SERVICE_NAME", "banking-service"))
TEAM = os.getenv("TEAM", "unknown")
SERVICE_ROLE = os.getenv("SERVICE_ROLE", "microservice")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8080"))
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "localhost")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", "5044"))
DEPENDENCIES = [item.strip() for item in os.getenv("SERVICE_DEPENDENCIES", "").split(",") if item.strip()]
TARGETS = [item.strip() for item in os.getenv("TARGETS", "").split(",") if item.strip()]
CHAOS_AUTO_ENABLED = os.getenv("CHAOS_AUTO_ENABLED", "true").lower() in {"true", "1", "yes"}
CHAOS_INTERVAL_SECONDS = int(os.getenv("CHAOS_INTERVAL_SECONDS", "45"))
CHAOS_DURATION_SECONDS = int(os.getenv("CHAOS_DURATION_SECONDS", "25"))

ALL_KNOWN_SERVICES = [
    "api-gateway", "ipg", "switch", "esb", "core-banking",
    "mobile-banking", "internet-banking", "branch-channel", "atm-channel",
    "payment-service", "transfer-service", "card-service", "bill-payment-service", "atm-service",
    "account-service", "customer-service", "ledger-service", "transaction-service", "balance-service",
    "fraud-service", "aml-service", "risk-engine",
    "audit-service", "reconciliation-service", "settlement-service", "statement-service",
    "reporting-service", "identity-service", "notification-service"
]

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
chaos_events_counter = meter.create_counter(
    "banking_chaos_events_total", description="Chaos and anomaly injection events"
)
chaos_impact_counter = meter.create_counter(
    "banking_chaos_impact_total", description="Requests impacted by active chaos anomalies"
)
chaos_active_gauge = meter.create_up_down_counter(
    "banking_chaos_active", description="Number of currently active chaos anomalies"
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


class ChaosEngine:
    """Manages active anomalies including latency, error rates, unresponsiveness,
    and system resource pressure (CPU, memory, disk I/O, disk space)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active_anomalies = {}
        self.cleanup_timers = {}

    def get_status(self):
        with self.lock:
            status_list = []
            now = time.time()
            for anomaly_id, info in self.active_anomalies.items():
                remaining = max(0, info["expires_at"] - now)
                status_list.append({
                    "id": anomaly_id,
                    "type": info["type"],
                    "params": info["params"],
                    "started_at": info["started_at"],
                    "duration_seconds": info["duration_seconds"],
                    "remaining_seconds": round(remaining, 1),
                })
            return {
                "service": SERVICE_NAME,
                "team": TEAM,
                "active_anomalies_count": len(status_list),
                "anomalies": status_list,
            }

    def inject(self, anomaly_type, duration_seconds=30, params=None):
        params = params or {}
        duration_seconds = max(1, int(duration_seconds))
        anomaly_id = f"{anomaly_type}_{int(time.time() * 1000)}_{random.randint(100, 999)}"
        stop_event = threading.Event()
        worker_threads = []
        extra_resources = {}

        now = time.time()
        expires_at = now + duration_seconds

        # Resource pressure background workers
        if anomaly_type == "cpu_pressure":
            threads_count = int(params.get("cpu_threads", params.get("threads", 2)))
            for _ in range(threads_count):
                thread = threading.Thread(
                    target=self._run_cpu_pressure,
                    args=(stop_event, float(params.get("utilization_pct", 85.0))),
                    daemon=True,
                )
                thread.start()
                worker_threads.append(thread)

        elif anomaly_type == "memory_pressure":
            memory_mb = int(params.get("memory_mb", 150))
            thread = threading.Thread(
                target=self._run_memory_pressure,
                args=(stop_event, memory_mb, extra_resources),
                daemon=True,
            )
            thread.start()
            worker_threads.append(thread)

        elif anomaly_type == "disk_io_pressure":
            chunk_size_kb = int(params.get("chunk_size_kb", 1024))
            thread = threading.Thread(
                target=self._run_disk_io_pressure,
                args=(stop_event, chunk_size_kb, extra_resources),
                daemon=True,
            )
            thread.start()
            worker_threads.append(thread)

        elif anomaly_type == "disk_space_pressure":
            space_mb = int(params.get("disk_space_mb", 200))
            thread = threading.Thread(
                target=self._run_disk_space_pressure,
                args=(stop_event, space_mb, extra_resources),
                daemon=True,
            )
            thread.start()
            worker_threads.append(thread)

        with self.lock:
            self.active_anomalies[anomaly_id] = {
                "id": anomaly_id,
                "type": anomaly_type,
                "duration_seconds": duration_seconds,
                "params": params,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": expires_at,
                "stop_event": stop_event,
                "worker_threads": worker_threads,
                "extra_resources": extra_resources,
            }

            timer = threading.Timer(duration_seconds, self.clear, args=[anomaly_id])
            timer.daemon = True
            timer.start()
            self.cleanup_timers[anomaly_id] = timer

        chaos_active_gauge.add(1, {"service": SERVICE_NAME, "team": TEAM, "anomaly_type": anomaly_type})
        chaos_events_counter.add(1, {"service": SERVICE_NAME, "team": TEAM, "anomaly_type": anomaly_type, "action": "inject"})
        log_event(
            "WARN",
            f"Chaos anomaly injected: {anomaly_type}",
            chaos_event="injected",
            anomaly_id=anomaly_id,
            anomaly_type=anomaly_type,
            duration_seconds=duration_seconds,
            params=params,
        )
        return {
            "status": "injected",
            "anomaly_id": anomaly_id,
            "service": SERVICE_NAME,
            "team": TEAM,
            "type": anomaly_type,
            "duration_seconds": duration_seconds,
            "params": params,
        }

    def clear(self, anomaly_id=None, anomaly_type=None):
        cleared_count = 0
        to_clear = []
        with self.lock:
            if anomaly_id and anomaly_id in self.active_anomalies:
                to_clear.append((anomaly_id, self.active_anomalies.pop(anomaly_id)))
            else:
                for aid, info in list(self.active_anomalies.items()):
                    if anomaly_type is None or info["type"] == anomaly_type:
                        to_clear.append((aid, self.active_anomalies.pop(aid)))

            for aid, info in to_clear:
                timer = self.cleanup_timers.pop(aid, None)
                if timer:
                    timer.cancel()

        for aid, info in to_clear:
            info["stop_event"].set()
            # Clean up temp resources if any
            temp_dir = info["extra_resources"].get("temp_dir")
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
            temp_file = info["extra_resources"].get("temp_file")
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    pass

            chaos_active_gauge.add(-1, {"service": SERVICE_NAME, "team": TEAM, "anomaly_type": info["type"]})
            chaos_events_counter.add(1, {"service": SERVICE_NAME, "team": TEAM, "anomaly_type": info["type"], "action": "clear"})
            log_event(
                "INFO",
                f"Chaos anomaly cleared: {info['type']}",
                chaos_event="cleared",
                anomaly_id=aid,
                anomaly_type=info["type"],
            )
            cleared_count += 1

        return {"status": "cleared", "cleared_count": cleared_count, "service": SERVICE_NAME}

    def intercept_request(self, span, operation):
        """Evaluates active anomalies and applies latency, errors, or unresponsiveness."""
        active_list = []
        now = time.time()
        with self.lock:
            for info in list(self.active_anomalies.values()):
                if info["expires_at"] > now:
                    active_list.append(info)

        if not active_list:
            return None

        for anomaly in active_list:
            atype = anomaly["type"]
            params = anomaly["params"]
            span.set_attribute("chaos.active", True)
            span.set_attribute(f"chaos.active_{atype}", True)

            if atype == "unresponsive":
                hold_seconds = float(params.get("hold_seconds", 15.0))
                pct = float(params.get("percentage", 100.0))
                if random.random() * 100 < pct:
                    chaos_impact_counter.add(1, {"service": SERVICE_NAME, "anomaly_type": "unresponsive"})
                    span.set_attribute("chaos.unresponsive_held_seconds", hold_seconds)
                    log_event("ERROR", "simulating unresponsive service timeout", anomaly_type=atype, hold_seconds=hold_seconds)
                    time.sleep(hold_seconds)
                    span.set_status(Status(StatusCode.ERROR, "service unresponsive timeout"))
                    return jsonify({"status": "failed", "service": SERVICE_NAME, "reason": "chaos_unresponsive_timeout"}), 504

            elif atype in {"error", "drop"}:
                error_rate = float(params.get("error_rate", 1.0))
                status_code = int(params.get("status_code", 503))
                if random.random() <= error_rate:
                    chaos_impact_counter.add(1, {"service": SERVICE_NAME, "anomaly_type": atype})
                    span.set_attribute("chaos.injected_error", True)
                    span.set_attribute("chaos.status_code", status_code)
                    span.set_status(Status(StatusCode.ERROR, f"chaos injected error {status_code}"))
                    log_event(
                        "ERROR",
                        f"Chaos request failure injected ({status_code})",
                        anomaly_type=atype,
                        status_code=status_code,
                        operation=operation,
                    )
                    return jsonify({
                        "status": "failed",
                        "service": SERVICE_NAME,
                        "team": TEAM,
                        "reason": f"chaos_{atype}_injected",
                        "status_code": status_code,
                    }), status_code

            elif atype == "latency":
                delay_ms = float(params.get("delay_ms", 2500.0))
                jitter_ms = float(params.get("jitter_ms", 300.0))
                actual_delay = max(0.1, (delay_ms + random.uniform(-jitter_ms, jitter_ms)) / 1000.0)
                pct = float(params.get("percentage", 100.0))
                if random.random() * 100 < pct:
                    chaos_impact_counter.add(1, {"service": SERVICE_NAME, "anomaly_type": "latency"})
                    span.set_attribute("chaos.injected_delay_ms", round(actual_delay * 1000, 2))
                    log_event(
                        "WARN",
                        f"Chaos latency spike injected ({round(actual_delay * 1000)}ms)",
                        anomaly_type=atype,
                        delay_ms=round(actual_delay * 1000),
                        operation=operation,
                    )
                    time.sleep(actual_delay)

        return None

    def _run_cpu_pressure(self, stop_event, target_pct):
        """Spins CPU worker doing intensive math calculations with duty-cycle sleep."""
        duty_cycle = max(0.1, min(1.0, target_pct / 100.0))
        interval = 0.1
        work_time = interval * duty_cycle
        sleep_time = interval * (1.0 - duty_cycle)
        while not stop_event.is_set():
            start = time.perf_counter()
            while (time.perf_counter() - start) < work_time:
                _ = [math.sin(i) * math.cos(i) for i in range(2000)]
            if sleep_time > 0:
                time.sleep(sleep_time)

    def _run_memory_pressure(self, stop_event, memory_mb, extra_resources):
        """Allocates and holds large memory chunks, periodically accessing it to prevent swapping."""
        try:
            chunk_size = 1024 * 1024
            chunks = [bytearray(chunk_size) for _ in range(memory_mb)]
            extra_resources["memory_chunks_count"] = len(chunks)
            while not stop_event.is_set():
                for idx in range(0, len(chunks), max(1, len(chunks) // 10)):
                    chunks[idx][0] = (chunks[idx][0] + 1) % 255
                time.sleep(1.0)
        except MemoryError:
            log_event("ERROR", "Memory allocation limit reached during memory pressure chaos")
        finally:
            chunks = None

    def _run_disk_io_pressure(self, stop_event, chunk_size_kb, extra_resources):
        """Repeatedly writes, flushes, and reads disk chunks in a temporary file to create high IOPS."""
        temp_dir = tempfile.mkdtemp(prefix="chaos_io_")
        extra_resources["temp_dir"] = temp_dir
        file_path = os.path.join(temp_dir, "io_stress.dat")
        data = bytearray(chunk_size_kb * 1024)
        while not stop_event.is_set():
            try:
                with open(file_path, "wb") as f:
                    for _ in range(10):
                        if stop_event.is_set():
                            break
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        while f.read(chunk_size_kb * 1024):
                            if stop_event.is_set():
                                break
            except Exception as exc:
                log_event("ERROR", "Disk I/O pressure error", error=str(exc))
                time.sleep(1.0)
            time.sleep(0.05)

    def _run_disk_space_pressure(self, stop_event, space_mb, extra_resources):
        """Creates a large file on disk to simulate low disk space conditions."""
        temp_file = tempfile.NamedTemporaryFile(prefix="chaos_space_", delete=False)
        extra_resources["temp_file"] = temp_file.name
        temp_file.close()
        chunk = bytearray(1024 * 1024)
        try:
            with open(temp_file.name, "wb") as f:
                for _ in range(space_mb):
                    if stop_event.is_set():
                        break
                    f.write(chunk)
                    f.flush()
            while not stop_event.is_set():
                time.sleep(1.0)
        except Exception as exc:
            log_event("ERROR", "Disk space pressure error", error=str(exc))


chaos_engine = ChaosEngine()


def normalize_anomaly_payload(data):
    """Accept both the documented nested format and the flat legacy format."""
    data = data if isinstance(data, dict) else {}
    anomaly = data.get("anomaly") if isinstance(data.get("anomaly"), dict) else {}
    params = {}
    if isinstance(data.get("params"), dict):
        params.update(data["params"])
    if isinstance(anomaly.get("params"), dict):
        params.update(anomaly["params"])

    inline_keys = {
        "delay_ms", "jitter_ms", "percentage", "error_rate", "status_code",
        "hold_seconds", "cpu_threads", "threads", "utilization_pct", "memory_mb",
        "chunk_size_kb", "disk_space_mb",
    }
    for source in (data, anomaly):
        for key in inline_keys:
            if key in source:
                params[key] = source[key]

    return {
        "target": data.get("target", SERVICE_NAME),
        "type": anomaly.get("type", data.get("type", "latency")),
        "duration_seconds": int(anomaly.get("duration_seconds", data.get("duration_seconds", 30))),
        "params": params,
    }


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

    # --- Chaos & Anomaly Endpoints ---

    @app.get("/chaos/status")
    def chaos_status():
        return jsonify(chaos_engine.get_status())

    @app.post("/chaos/inject")
    def chaos_inject():
        payload = normalize_anomaly_payload(request.get_json(silent=True) or {})
        result = chaos_engine.inject(
            payload["type"],
            duration_seconds=payload["duration_seconds"],
            params=payload["params"],
        )
        return jsonify(result)

    @app.post("/chaos/clear")
    def chaos_clear():
        data = request.get_json(silent=True) or {}
        anomaly_id = data.get("id")
        anomaly_type = data.get("type")
        result = chaos_engine.clear(anomaly_id=anomaly_id, anomaly_type=anomaly_type)
        return jsonify(result)

    @app.post("/chaos/cluster")
    @app.post("/chaos/cluster/inject")
    def chaos_cluster_inject():
        """Orchestrate chaos across one, several, or all services."""
        data = request.get_json(silent=True) or {}
        payload = normalize_anomaly_payload(data)
        target_service = payload["target"]
        anomaly_type = payload["type"]
        duration = payload["duration_seconds"]
        params = payload["params"]

        target_services = []
        if target_service == "all":
            target_services = ALL_KNOWN_SERVICES
        elif target_service == "random":
            target_services = [random.choice(ALL_KNOWN_SERVICES)]
        elif isinstance(target_service, list):
            target_services = target_service
        else:
            target_services = [target_service]

        results = []
        for sname in target_services:
            if sname == SERVICE_NAME:
                results.append(chaos_engine.inject(anomaly_type, duration_seconds=duration, params=params))
            else:
                try:
                    res = requests.post(
                        f"http://{sname}:8080/chaos/inject",
                        json={"type": anomaly_type, "duration_seconds": duration, "params": params},
                        timeout=3,
                    )
                    results.append(res.json() if res.ok else {"service": sname, "error": res.status_code})
                except Exception as exc:
                    results.append({"service": sname, "error": str(exc)})

        return jsonify({"orchestrator": SERVICE_NAME, "results": results})

    @app.post("/chaos/cluster/clear")
    def chaos_cluster_clear():
        """Clear chaos across all or selected services."""
        data = request.get_json(silent=True) or {}
        target_service = data.get("target", "all")
        anomaly_type = data.get("type")

        target_services = ALL_KNOWN_SERVICES if target_service == "all" else [target_service]
        results = []
        for sname in target_services:
            if sname == SERVICE_NAME:
                results.append(chaos_engine.clear(anomaly_type=anomaly_type))
            else:
                try:
                    res = requests.post(
                        f"http://{sname}:8080/chaos/clear",
                        json={"type": anomaly_type},
                        timeout=3,
                    )
                    results.append(res.json() if res.ok else {"service": sname, "error": res.status_code})
                except Exception as exc:
                    results.append({"service": sname, "error": str(exc)})

        return jsonify({"orchestrator": SERVICE_NAME, "results": results})

    @app.get("/chaos/cluster/status")
    def chaos_cluster_status():
        """Query chaos status from all services in the cluster."""
        cluster_status = {}
        for sname in ALL_KNOWN_SERVICES:
            if sname == SERVICE_NAME:
                cluster_status[sname] = chaos_engine.get_status()
            else:
                try:
                    res = requests.get(f"http://{sname}:8080/chaos/status", timeout=2)
                    cluster_status[sname] = res.json() if res.ok else {"error": res.status_code}
                except Exception as exc:
                    cluster_status[sname] = {"error": str(exc)}
        return jsonify({"orchestrator": SERVICE_NAME, "services": cluster_status})

    @app.route("/chaos/scheduler", methods=["GET", "POST"])
    def chaos_scheduler():
        if request.method == "GET":
            with scheduler_lock:
                return jsonify({
                    "enabled": auto_scheduler_enabled,
                    "running": auto_scheduler_running,
                    "interval_seconds": CHAOS_INTERVAL_SECONDS,
                    "duration_seconds": CHAOS_DURATION_SECONDS,
                    "last_triggered_at": auto_scheduler_last_trigger,
                    "next_trigger_at": auto_scheduler_next_trigger,
                    "current_scenario": auto_scheduler_current_scenario,
                    "history": list(auto_scheduler_history[-10:]),
                    "scenarios": [scenario["name"] for scenario in CHAOS_SCENARIOS],
                })

        data = request.get_json(silent=True) or {}
        action = str(data.get("action", "")).lower()
        if action in {"enable", "start"}:
            set_scheduler_enabled(True)
        elif action in {"disable", "stop"}:
            set_scheduler_enabled(False)
        elif action == "toggle":
            set_scheduler_enabled(not auto_scheduler_enabled)
        elif action in {"trigger", "run", "run_now"}:
            threading.Thread(
                target=trigger_automated_scenario,
                args=(data.get("scenario"),),
                daemon=True,
            ).start()
        elif action:
            return jsonify({"error": "unknown scheduler action"}), 400
        return jsonify(get_scheduler_status())

    # --- Business Request Processing ---

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

            # Check for active chaos interception (latency, error, unresponsive)
            chaos_response = chaos_engine.intercept_request(span, operation)
            if chaos_response:
                return chaos_response

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


# --- Automated Chaos Scenarios Scheduler ---

CHAOS_SCENARIOS = [
    {
        "name": "Payment Gateway Latency Spike",
        "targets": ["ipg", "switch", "payment-service"],
        "type": "latency",
        "params": {"delay_ms": 3000, "jitter_ms": 600, "percentage": 90},
    },
    {
        "name": "Core Banking Unresponsiveness & Outage",
        "targets": ["core-banking", "ledger-service"],
        "type": "unresponsive",
        "params": {"hold_seconds": 15, "percentage": 75},
    },
    {
        "name": "Customer Identity Service CPU & Memory Pressure",
        "targets": ["customer-service", "identity-service"],
        "type": "cpu_pressure",
        "params": {"cpu_threads": 3, "utilization_pct": 95},
    },
    {
        "name": "Transaction Database Disk I/O & Space Exhaustion",
        "targets": ["transaction-service", "audit-service"],
        "type": "disk_io_pressure",
        "params": {"chunk_size_kb": 2048},
    },
    {
        "name": "Integration ESB Cascading Fault",
        "targets": ["esb", "transfer-service"],
        "type": "error",
        "params": {"status_code": 503, "error_rate": 0.8},
    },
    {
        "name": "Risk & Fraud Verification Bottleneck",
        "targets": ["fraud-service", "aml-service", "risk-engine"],
        "type": "latency",
        "params": {"delay_ms": 2200, "jitter_ms": 400, "percentage": 100},
    },
    {
        "name": "Channel Edge Gateway Degradation",
        "targets": ["api-gateway", "mobile-banking", "internet-banking"],
        "type": "error",
        "params": {"status_code": 500, "error_rate": 0.5},
    },
]

auto_scheduler_running = False
auto_scheduler_enabled = CHAOS_AUTO_ENABLED
auto_scheduler_last_trigger = None
auto_scheduler_next_trigger = None
auto_scheduler_current_scenario = None
auto_scheduler_history = []
scheduler_lock = threading.Lock()


def get_scheduler_status():
    with scheduler_lock:
        return {
            "enabled": auto_scheduler_enabled,
            "running": auto_scheduler_running,
            "interval_seconds": CHAOS_INTERVAL_SECONDS,
            "duration_seconds": CHAOS_DURATION_SECONDS,
            "last_triggered_at": auto_scheduler_last_trigger,
            "next_trigger_at": auto_scheduler_next_trigger,
            "current_scenario": auto_scheduler_current_scenario,
            "history": list(auto_scheduler_history[-10:]),
            "scenarios": [scenario["name"] for scenario in CHAOS_SCENARIOS],
        }


def set_scheduler_enabled(enabled):
    global auto_scheduler_enabled
    with scheduler_lock:
        auto_scheduler_enabled = bool(enabled)
    log_event("INFO", "automated chaos scheduler updated", scheduler_enabled=auto_scheduler_enabled)


def trigger_automated_scenario(scenario_name=None):
    global auto_scheduler_last_trigger, auto_scheduler_current_scenario
    scenario = next(
        (item for item in CHAOS_SCENARIOS if item["name"] == scenario_name),
        None,
    ) if scenario_name else random.choice(CHAOS_SCENARIOS)
    if scenario is None:
        raise ValueError(f"unknown chaos scenario: {scenario_name}")

    started_at = datetime.now(timezone.utc).isoformat()
    with scheduler_lock:
        auto_scheduler_last_trigger = started_at
        auto_scheduler_current_scenario = scenario["name"]
        auto_scheduler_history.append({"scenario": scenario["name"], "started_at": started_at})

    log_event(
        "WARN",
        f"AUTOMATED CHAOS TRIGGERED: {scenario['name']}",
        chaos_event="automated_scenario_started",
        scenario_name=scenario["name"],
        targets=scenario["targets"],
        anomaly_type=scenario["type"],
        duration_seconds=CHAOS_DURATION_SECONDS,
        params=scenario["params"],
    )

    for target in scenario["targets"]:
        try:
            requests.post(
                f"http://{target}:8080/chaos/inject",
                json={
                    "type": scenario["type"],
                    "duration_seconds": CHAOS_DURATION_SECONDS,
                    "params": scenario["params"],
                },
                timeout=3,
            )
        except requests.RequestException as exc:
            log_event("WARN", f"Failed to inject automated chaos into {target}", error=str(exc))

    time.sleep(CHAOS_DURATION_SECONDS)
    with scheduler_lock:
        auto_scheduler_current_scenario = None
    log_event(
        "INFO",
        f"AUTOMATED CHAOS RESOLVED: {scenario['name']}",
        chaos_event="automated_scenario_resolved",
        scenario_name=scenario["name"],
        targets=scenario["targets"],
    )


def run_automated_chaos_scheduler():
    global auto_scheduler_running, auto_scheduler_next_trigger
    with scheduler_lock:
        auto_scheduler_running = True
    logging.info(
        "Automated Chaos Scheduler started (interval=%ss, duration=%ss)",
        CHAOS_INTERVAL_SECONDS,
        CHAOS_DURATION_SECONDS,
    )

    time.sleep(20)  # Grace period on startup
    while True:
        with scheduler_lock:
            enabled = auto_scheduler_enabled
            auto_scheduler_next_trigger = datetime.now(timezone.utc).isoformat()
        if enabled:
            try:
                trigger_automated_scenario()
            except Exception as exc:
                log_event("ERROR", "automated chaos scenario failed", error=str(exc))
            with scheduler_lock:
                auto_scheduler_next_trigger = (
                    datetime.now(timezone.utc) + timedelta(seconds=max(10, CHAOS_INTERVAL_SECONDS))
                ).isoformat()
        else:
            with scheduler_lock:
                auto_scheduler_next_trigger = None
        time.sleep(max(10, CHAOS_INTERVAL_SECONDS))


def run_control_server():
    create_app().run(host="0.0.0.0", port=SERVICE_PORT, threaded=True, use_reloader=False)


def run_traffic():
    if not TARGETS:
        log_event("ERROR", "no traffic targets configured")
        while True:
            time.sleep(60)

    # Expose health and chaos controls while this process generates traffic.
    control_thread = threading.Thread(target=run_control_server, daemon=True)
    control_thread.start()

    # Start automated chaos scheduler in background thread if enabled
    if CHAOS_AUTO_ENABLED:
        scheduler_thread = threading.Thread(target=run_automated_chaos_scheduler, daemon=True)
        scheduler_thread.start()

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
