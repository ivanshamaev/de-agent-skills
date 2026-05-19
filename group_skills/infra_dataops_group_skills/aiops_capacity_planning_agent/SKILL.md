---
name: aiops-capacity-planning-agent
description: AIOps capacity planning agent — predictive resource forecasting (Prophet/linear regression on Prometheus metrics), Kubernetes HPA/VPA right-sizing recommendations, node capacity headroom calculator, Kafka partition and broker capacity model, Spark executor sizing from job history, data lake storage growth forecast (S3/GCS), cluster autoscaler simulation, automatic HPA threshold tuning, cost-per-job attribution, capacity planning report generator
---

# AIOps Capacity Planning Agent

## When to Use

- Forecasting when a cluster will run out of CPU/memory/disk before it happens
- Right-sizing HPA min/max replicas based on historical utilization patterns
- Estimating Kafka broker capacity for projected message volume growth
- Analyzing Spark job history to recommend executor configuration
- Generating quarterly capacity planning reports for budget planning

---

## Prometheus-Based Resource Forecasting

```python
import pandas as pd
import numpy as np
from prophet import Prophet
from prometheus_api_client import PrometheusConnect
from datetime import datetime, timedelta

class ResourceForecaster:
    """Forecast resource usage to predict capacity exhaustion."""

    def __init__(self, prometheus_url: str):
        self.prom = PrometheusConnect(url=prometheus_url)

    def fetch_metric_history(
        self,
        query: str,
        days_back: int = 30,
        step: str = "1h",
    ) -> pd.DataFrame:
        end = datetime.now()
        start = end - timedelta(days=days_back)
        results = self.prom.custom_query_range(
            query=query,
            start_time=start,
            end_time=end,
            step=step,
        )
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results[0]["values"], columns=["ds", "y"])
        df["ds"] = pd.to_datetime(df["ds"], unit="s")
        df["y"] = df["y"].astype(float)
        return df

    def forecast(
        self,
        metric_name: str,
        query: str,
        forecast_days: int = 30,
        capacity_limit: float | None = None,
    ) -> dict:
        df = self.fetch_metric_history(query)
        if df.empty:
            return {"error": f"No data for {metric_name}"}

        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,
            changepoint_prior_scale=0.05,  # conservative — data infra grows smoothly
        )
        model.fit(df)

        future = model.make_future_dataframe(periods=forecast_days * 24, freq="H")
        forecast = model.predict(future)

        result = {
            "metric": metric_name,
            "current_value": round(df["y"].iloc[-1], 2),
            "forecast_30d_p50": round(forecast["yhat"].iloc[-1], 2),
            "forecast_30d_p95": round(forecast["yhat_upper"].iloc[-1], 2),
        }

        if capacity_limit:
            # Find when p95 forecast exceeds capacity
            exceed = forecast[forecast["yhat_upper"] >= capacity_limit]
            if not exceed.empty:
                days_until_full = (exceed["ds"].iloc[0] - datetime.now()).days
                result["days_until_capacity_breach"] = max(0, days_until_full)
                result["breach_date"] = exceed["ds"].iloc[0].strftime("%Y-%m-%d")
                result["action_required"] = days_until_full < 14
            else:
                result["days_until_capacity_breach"] = None
                result["breach_date"] = None

        return result


def run_capacity_forecast(prom_url: str) -> list[dict]:
    """Run full capacity forecast for all critical platform resources."""
    forecaster = ResourceForecaster(prom_url)
    reports = []

    checks = [
        {
            "name": "airflow_scheduler_cpu",
            "query": 'avg(rate(container_cpu_usage_seconds_total{pod=~"airflow-scheduler.*"}[5m]))',
            "capacity_limit": 3.8,  # 4 cores limit
        },
        {
            "name": "kafka_broker_disk_pct",
            "query": 'avg(1 - kubelet_volume_stats_available_bytes{pvc=~"data-production-kafka.*"} / kubelet_volume_stats_capacity_bytes) * 100',
            "capacity_limit": 85.0,  # alert at 85% full
        },
        {
            "name": "spark_node_memory_gib",
            "query": 'sum(container_memory_working_set_bytes{namespace="spark"}) / 1073741824',
            "capacity_limit": 200.0,  # 200 GiB cluster limit
        },
    ]

    for check in checks:
        report = forecaster.forecast(
            metric_name=check["name"],
            query=check["query"],
            capacity_limit=check.get("capacity_limit"),
        )
        reports.append(report)

    return reports
```

---

## Kubernetes HPA Right-Sizing

```python
def recommend_hpa_settings(
    deployment_name: str,
    namespace: str,
    prom_url: str,
    target_cpu_utilization: float = 0.70,
    p99_multiplier: float = 1.5,  # headroom above p99 load
) -> dict:
    """
    Analyze historical CPU utilization to recommend HPA min/max replicas
    and target utilization threshold.
    """
    prom = PrometheusConnect(url=prom_url)

    # Fetch 30-day CPU utilization history (% of request)
    query = f"""
        avg(
            rate(container_cpu_usage_seconds_total{{namespace="{namespace}", pod=~"{deployment_name}-.*"}}[5m])
        )
        /
        avg(
            kube_pod_container_resource_requests{{namespace="{namespace}", pod=~"{deployment_name}-.*", resource="cpu"}}
        ) * 100
    """
    results = prom.custom_query_range(
        query=query,
        start_time=datetime.now() - timedelta(days=30),
        end_time=datetime.now(),
        step="5m",
    )

    if not results:
        return {"error": "No metrics found"}

    values = [float(v[1]) for v in results[0]["values"]]
    values_arr = np.array(values)

    # Current replica count
    import subprocess, json
    replicas_raw = subprocess.run(
        ["kubectl", "get", "deployment", deployment_name, "-n", namespace,
         "-o", "jsonpath={.spec.replicas}"],
        capture_output=True, text=True
    ).stdout
    current_replicas = int(replicas_raw.strip() or "1")

    p50_util = np.percentile(values_arr, 50)
    p95_util = np.percentile(values_arr, 95)
    p99_util = np.percentile(values_arr, 99)

    # At target utilization, how many replicas do we need for p99 load?
    recommended_max = int(np.ceil(current_replicas * (p99_util * p99_multiplier) / (target_cpu_utilization * 100)))
    recommended_min = max(1, int(np.ceil(current_replicas * p50_util / (target_cpu_utilization * 100))))

    return {
        "deployment": deployment_name,
        "namespace": namespace,
        "current_replicas": current_replicas,
        "cpu_p50_pct": round(p50_util, 1),
        "cpu_p95_pct": round(p95_util, 1),
        "cpu_p99_pct": round(p99_util, 1),
        "recommended_hpa": {
            "minReplicas": recommended_min,
            "maxReplicas": recommended_max,
            "targetCPUUtilizationPercentage": int(target_cpu_utilization * 100),
        },
        "current_hpa_adequate": current_replicas >= recommended_min,
    }
```

---

## Kafka Capacity Model

```python
def kafka_capacity_model(
    current_brokers: int,
    current_topics: list[dict],   # [{name, partitions, replication_factor, MB_per_sec}]
    growth_rate_monthly_pct: float = 20.0,   # 20% MoM growth
    forecast_months: int = 6,
    broker_max_throughput_MBps: float = 150.0,  # per broker network capacity
    broker_disk_TB: float = 2.0,
    retention_days: int = 7,
) -> dict:
    """Forecast when current Kafka cluster needs scaling."""

    total_write_MBps = sum(t["MB_per_sec"] * t["replication_factor"] for t in current_topics)
    total_storage_TB = sum(
        t["MB_per_sec"] * t["replication_factor"] * retention_days * 86400 / 1e6
        for t in current_topics
    )

    cluster_throughput_capacity_MBps = current_brokers * broker_max_throughput_MBps
    cluster_disk_TB = current_brokers * broker_disk_TB

    # Monthly projections
    monthly_projections = []
    for month in range(1, forecast_months + 1):
        factor = (1 + growth_rate_monthly_pct / 100) ** month
        proj_throughput = total_write_MBps * factor
        proj_storage = total_storage_TB * factor

        throughput_pct = proj_throughput / cluster_throughput_capacity_MBps * 100
        storage_pct = proj_storage / cluster_disk_TB * 100
        needs_scaling = throughput_pct > 80 or storage_pct > 80

        monthly_projections.append({
            "month": month,
            "throughput_MBps": round(proj_throughput, 1),
            "storage_TB": round(proj_storage, 2),
            "throughput_utilization_pct": round(throughput_pct, 1),
            "storage_utilization_pct": round(storage_pct, 1),
            "needs_broker_scaling": needs_scaling,
        })

    # When does first scaling need to happen?
    scaling_month = next(
        (p["month"] for p in monthly_projections if p["needs_broker_scaling"]),
        None
    )
    brokers_needed_at_6m = int(np.ceil(
        monthly_projections[-1]["throughput_MBps"] / (broker_max_throughput_MBps * 0.8)
    ))

    return {
        "current_state": {
            "brokers": current_brokers,
            "write_throughput_MBps": round(total_write_MBps, 1),
            "storage_TB": round(total_storage_TB, 2),
        },
        "capacity_headroom": {
            "throughput_pct_used": round(total_write_MBps / cluster_throughput_capacity_MBps * 100, 1),
            "storage_pct_used": round(total_storage_TB / cluster_disk_TB * 100, 1),
        },
        "scaling_needed_in_month": scaling_month,
        "brokers_needed_at_month_6": brokers_needed_at_6m,
        "monthly_projections": monthly_projections,
    }
```

---

## Data Lake Storage Forecast

```python
def forecast_s3_storage(
    bucket_size_queries: list[dict],   # [{prefix, current_TB, monthly_growth_TB}]
    forecast_months: int = 12,
    storage_budget_usd_month: float = 5000.0,
    s3_standard_cost_per_gb: float = 0.023,
    s3_ia_cost_per_gb: float = 0.0125,
) -> dict:
    """Forecast S3 storage costs and identify prefixes for tiering."""

    projections = []
    for month in range(1, forecast_months + 1):
        monthly_data = []
        for prefix_info in bucket_size_queries:
            projected_tb = prefix_info["current_TB"] + prefix_info["monthly_growth_TB"] * month
            monthly_data.append({
                "prefix": prefix_info["prefix"],
                "size_TB": round(projected_tb, 2),
            })

        total_tb = sum(d["size_TB"] for d in monthly_data)
        cost_standard = total_tb * 1000 * s3_standard_cost_per_gb
        cost_with_tiering = (
            min(total_tb * 0.2, total_tb) * 1000 * s3_standard_cost_per_gb +  # 20% hot
            max(0, total_tb * 0.8) * 1000 * s3_ia_cost_per_gb                 # 80% IA
        )

        projections.append({
            "month": month,
            "total_TB": round(total_tb, 2),
            "cost_standard_usd": round(cost_standard, 0),
            "cost_with_tiering_usd": round(cost_with_tiering, 0),
            "over_budget": cost_standard > storage_budget_usd_month,
        })

    budget_breach_month = next(
        (p["month"] for p in projections if p["over_budget"]),
        None
    )

    return {
        "budget_breach_month": budget_breach_month,
        "monthly_savings_from_tiering_usd": round(
            projections[-1]["cost_standard_usd"] - projections[-1]["cost_with_tiering_usd"], 0
        ),
        "projections": projections,
    }
```

---

## Capacity Planning Report Generator

```python
def generate_capacity_report(prom_url: str, output_path: str = "capacity_report.md"):
    """Generate quarterly capacity planning report."""
    forecasts = run_capacity_forecast(prom_url)

    critical = [f for f in forecasts if f.get("days_until_capacity_breach") is not None and f["days_until_capacity_breach"] < 30]
    warning = [f for f in forecasts if f.get("days_until_capacity_breach") is not None and 30 <= f["days_until_capacity_breach"] < 60]

    lines = [
        f"# Capacity Planning Report — {datetime.now().strftime('%Y-%m')}",
        "",
        f"**Generated**: {datetime.now().isoformat()}",
        f"**Critical items** (< 30 days): {len(critical)}",
        f"**Warning items** (30–60 days): {len(warning)}",
        "",
        "## Critical: Action Required",
    ]

    for item in critical:
        lines.append(
            f"- **{item['metric']}**: breach in {item['days_until_capacity_breach']}d "
            f"(~{item['breach_date']}), current={item['current_value']}, "
            f"p95 forecast={item.get('forecast_30d_p95', 'N/A')}"
        )

    lines += ["", "## Warning: Monitor Closely"]
    for item in warning:
        lines.append(
            f"- **{item['metric']}**: breach in {item['days_until_capacity_breach']}d "
            f"(~{item['breach_date']})"
        )

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    return output_path
```

---

## Anti-Patterns

1. **Planning only to current capacity** — always plan to 1.5× current capacity; unexpected spikes (marketing campaigns, product launches) are the norm not the exception.
2. **Single-metric forecasting** — a service can breach disk while CPU is healthy; forecast all dimensions: CPU, memory, disk, network, partitions.
3. **Not accounting for seasonal peaks** — a 20% YoY growth rate ignores Black Friday 10× spikes; segment seasonality in the forecast model.
4. **Reacting to capacity instead of forecasting** — adding brokers after the cluster is at 95% creates operational emergencies; start scaling at 70%.
5. **Capacity planning without cost modeling** — scaling resources has a budget impact; always include cost forecasts alongside resource forecasts.

---

## References

- Kubernetes HPA: `kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/`
- Prometheus API client: `github.com/prometheus-api-client-python/prometheus-api-client-python`
- Prophet forecasting: `facebook.github.io/prophet/`
- Related skills: `[[aiops-infrastructure-anomaly-detection]]`, `[[de-cost-optimization]]`, `[[infra-kafka-platform-review]]`, `[[infra-aws-data-platform-review]]`
