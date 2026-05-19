---
name: infra-opentelemetry-instrumentation
description: OpenTelemetry instrumentation for data platforms — Python auto-instrumentation (opentelemetry-bootstrap), manual span creation for ETL tasks, trace context propagation across Airflow→Spark→dbt, OTLP exporter configuration, W3C TraceContext headers in Kafka messages, resource attributes (service.name/environment/version), Baggage for partition metadata propagation, Collector pipeline (tail sampling), correlation between traces/logs/metrics, Grafana Tempo integration
---

# OpenTelemetry Instrumentation

## When to Use

- Tracing end-to-end latency through a multi-component data pipeline
- Correlating Airflow task execution with Spark job spans
- Understanding where time is spent in a slow ETL pipeline
- Adding observability to custom Python ETL code
- Propagating context (partition date, run ID) across service boundaries

---

## Python Auto-Instrumentation (Zero-Code)

```bash
# Install OTel distro + auto-detection
pip install opentelemetry-distro opentelemetry-exporter-otlp

# Auto-detect and install instrumentation for installed packages
opentelemetry-bootstrap -a install
# Installs: opentelemetry-instrumentation-requests, sqlalchemy, psycopg2, kafka-python, etc.
```

```bash
# Run with auto-instrumentation
OTEL_SERVICE_NAME=etl-orders \
OTEL_RESOURCE_ATTRIBUTES="environment=production,team=data-engineering,version=1.2.0" \
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=otlp \
OTEL_LOGS_EXPORTER=otlp \
opentelemetry-instrument python etl_orders.py
```

---

## Manual Span Creation for ETL Tasks

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Initialize tracer (once per service)
resource = Resource.create({
    "service.name": "etl-orders",
    "service.version": "1.2.0",
    "deployment.environment": "production",
    "team": "data-engineering",
})

provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("etl.orders")


def run_etl(partition_date: str):
    with tracer.start_as_current_span("etl.run", kind=trace.SpanKind.INTERNAL) as root_span:
        root_span.set_attributes({
            "partition.date": partition_date,
            "pipeline.name": "etl-orders",
        })

        # Sub-span for extraction
        with tracer.start_as_current_span("etl.extract") as extract_span:
            rows = extract_orders(partition_date)
            extract_span.set_attribute("rows.extracted", len(rows))

        # Sub-span for transformation
        with tracer.start_as_current_span("etl.transform") as transform_span:
            transformed = transform_orders(rows)
            transform_span.set_attribute("rows.output", len(transformed))

        # Sub-span for load
        with tracer.start_as_current_span("etl.load") as load_span:
            loaded = load_orders(transformed, partition_date)
            load_span.set_attributes({
                "rows.loaded": loaded,
                "target.table": "silver.orders",
            })
```

---

## Context Propagation in Airflow Tasks

```python
from opentelemetry import trace, propagate
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

tracer = trace.get_tracer("airflow.etl")

@task
def extract_task(ds: str, **context) -> dict:
    # Start span, inject context into XCom for downstream tasks
    with tracer.start_as_current_span("airflow.extract") as span:
        span.set_attribute("partition.date", ds)
        span.set_attribute("dag.run_id", context["run_id"])

        result = extract_orders(ds)

        # Propagate trace context to next task via XCom
        carrier = {}
        propagate.inject(carrier)   # injects traceparent header
        return {"data": result, "otel_context": carrier}


@task
def transform_task(extract_result: dict, ds: str) -> dict:
    # Extract and restore trace context from upstream task
    carrier = extract_result.get("otel_context", {})
    ctx = propagate.extract(carrier)

    with tracer.start_as_current_span("airflow.transform", context=ctx) as span:
        span.set_attribute("partition.date", ds)
        transformed = transform_orders(extract_result["data"])
        span.set_attribute("rows.processed", len(transformed))

        carrier = {}
        propagate.inject(carrier)
        return {"data": transformed, "otel_context": carrier}
```

---

## Context Propagation in Kafka

```python
from opentelemetry import trace, propagate
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from confluent_kafka import Producer, Consumer, KafkaMessage

tracer = trace.get_tracer("kafka.producer")

def produce_with_trace(topic: str, key: str, value: bytes, partition_date: str):
    with tracer.start_as_current_span("kafka.send") as span:
        span.set_attributes({
            "messaging.system": "kafka",
            "messaging.destination": topic,
            "messaging.message_key": key,
            "partition.date": partition_date,
        })

        # Inject trace context into Kafka message headers
        headers = {}
        propagate.inject(headers)

        producer = Producer({"bootstrap.servers": "kafka:9092"})
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            headers=[(k, v.encode()) for k, v in headers.items()],
        )
        producer.flush()


def consume_with_trace(msg: KafkaMessage):
    # Restore trace context from Kafka message headers
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    ctx = propagate.extract(headers)

    with tracer.start_as_current_span("kafka.receive", context=ctx) as span:
        span.set_attributes({
            "messaging.system": "kafka",
            "messaging.source": msg.topic(),
            "messaging.partition": msg.partition(),
        })
        process_message(msg.value())
```

---

## Baggage for Partition Metadata

```python
from opentelemetry import baggage, context

# Producer: add partition metadata to baggage
ctx = baggage.set_baggage("partition.date", "2024-01-15")
ctx = baggage.set_baggage("pipeline.run_id", "manual__2024-01-15T02:00:00+00:00", context=ctx)
token = context.attach(ctx)

try:
    run_pipeline()
finally:
    context.detach(token)

# Anywhere downstream: read baggage
partition_date = baggage.get_baggage("partition.date")
run_id = baggage.get_baggage("pipeline.run_id")
```

---

## OTel Collector — Tail Sampling

```yaml
# Tail sampling: keep only traces with errors or slow operations
processors:
  tail_sampling:
    decision_wait: 30s
    num_traces: 50000
    expected_new_traces_per_sec: 100
    policies:
      # Always keep error traces
      - name: errors-policy
        type: status_code
        status_code: {status_codes: [ERROR]}

      # Keep slow traces (> 5 seconds)
      - name: slow-traces-policy
        type: latency
        latency: {threshold_ms: 5000}

      # Sample 10% of healthy fast traces
      - name: probabilistic-policy
        type: probabilistic
        probabilistic: {sampling_percentage: 10}
```

---

## Trace-to-Log Correlation

```python
import logging
from opentelemetry import trace

class OtelLogHandler(logging.Handler):
    """Inject trace_id and span_id into every log record."""
    def emit(self, record):
        span = trace.get_current_span()
        if span.is_recording():
            ctx = span.get_span_context()
            record.trace_id = format(ctx.trace_id, "032x")
            record.span_id  = format(ctx.span_id, "016x")
        else:
            record.trace_id = "0" * 32
            record.span_id  = "0" * 16

# Configure logging
logging.basicConfig(
    format='{"time": "%(asctime)s", "level": "%(levelname)s", '
           '"trace_id": "%(trace_id)s", "span_id": "%(span_id)s", '
           '"message": "%(message)s"}',
    handlers=[OtelLogHandler()]
)
```

---

## Instrumentation Checklist

```
[ ] opentelemetry-distro installed and bootstrap run
[ ] OTEL_SERVICE_NAME set (service.name, not default "unknown_service")
[ ] OTEL_RESOURCE_ATTRIBUTES includes environment and team
[ ] OTLP exporter pointing to OTel Collector (not directly to backend)
[ ] Manual spans on critical ETL operations (extract/transform/load)
[ ] span.set_attribute() with row counts and partition dates
[ ] Context propagated via XCom in Airflow task chains
[ ] Kafka message headers carry traceparent/tracestate
[ ] trace_id injected into log records for correlation
[ ] Tail sampling configured (don't store 100% of healthy traces)
[ ] Grafana Tempo datasource linked to Loki for log correlation
```

---

## Anti-Patterns

1. **Service name = "unknown_service"** — all traces aggregated under one name; always set `OTEL_SERVICE_NAME` uniquely per component.
2. **Exporting directly from app to Tempo/Jaeger** — bypasses sampling and processing; always route through OTel Collector.
3. **No context propagation between Airflow tasks** — each task starts a new disconnected trace; inject context via XCom to link the full pipeline trace.
4. **Storing 100% of traces without sampling** — trace storage costs scale with throughput; use tail sampling to keep errors and slow traces.
5. **No attributes on spans** — spans without attributes (rows processed, partition date, table name) are useless for debugging; annotate spans with business-relevant data.

---

## References

- OTel Python: `opentelemetry.io/docs/languages/python/`
- OTel Collector: `opentelemetry.io/docs/collector/`
- Context propagation: `opentelemetry.io/docs/concepts/context-propagation/`
- Grafana Tempo: `grafana.com/docs/tempo/latest/`
- Related skills: `[[infra-observability-stack-review]]`, `[[dataops-airflow-observability]]`, `[[de-production-readiness]]`
