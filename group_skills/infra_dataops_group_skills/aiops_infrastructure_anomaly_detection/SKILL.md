---
name: aiops-infrastructure-anomaly-detection
description: AIOps infrastructure anomaly detection — statistical baseline (z-score/IQR/seasonal decomposition), Prometheus anomaly rules (predict_linear/stddev_over_time/MAD), ML-based detection (Isolation Forest/Prophet/LSTM autoencoder on metrics), metric correlation clustering, automated alert threshold tuning from historical data, Kubernetes resource anomaly detection, data pipeline anomaly signals (row count z-score/freshness drift/throughput drop), Python sklearn/Prophet integration, Grafana anomaly panel
---

# AIOps Infrastructure Anomaly Detection

## When to Use

- Detecting infrastructure anomalies (CPU/memory spikes, disk fill rate, network saturation) without static thresholds
- Alerting on data pipeline anomalies (row count drops, latency spikes) using statistical baselines
- Building ML models to predict failures before they become incidents
- Auto-tuning alert thresholds using historical metric distributions
- Identifying correlated failures across services using metric clustering

---

## Statistical Anomaly Detection in Prometheus

### Z-Score Based Alerts

```yaml
# PromQL: alert when metric is > 3 sigma from its 7-day rolling mean
groups:
  - name: anomaly_detection
    rules:
      # CPU anomaly: current value > mean + 3*stddev over 7 days
      - alert: CPUAnomalyDetected
        expr: |
          (
            rate(container_cpu_usage_seconds_total{namespace="airflow"}[5m])
            -
            avg_over_time(rate(container_cpu_usage_seconds_total{namespace="airflow"}[5m])[7d:5m])
          )
          /
          stddev_over_time(rate(container_cpu_usage_seconds_total{namespace="airflow"}[5m])[7d:5m])
          > 3
        for: 10m
        labels:
          severity: warning
          anomaly_type: cpu
        annotations:
          summary: "CPU anomaly: {{ $labels.pod }} is {{ $value | printf \"%.1f\" }}σ above baseline"

      # Memory fill rate prediction: predict OOM in < 2 hours
      - alert: MemoryFillRateCritical
        expr: |
          predict_linear(
            container_memory_working_set_bytes{namespace="spark"}[1h],
            7200   # predict 2h ahead
          ) > container_spec_memory_limit_bytes * 0.95
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.pod }} predicted to OOM in < 2h"

      # Data pipeline throughput drop (MAD-based)
      - alert: PipelineThroughputAnomaly
        expr: |
          abs(
            kafka_consumer_group_lag - 
            quantile_over_time(0.5, kafka_consumer_group_lag[7d:5m])
          )
          /
          (
            quantile_over_time(0.75, kafka_consumer_group_lag[7d:5m])
            - quantile_over_time(0.25, kafka_consumer_group_lag[7d:5m])
            + 1
          )
          > 5   # 5 MADs above median
        for: 15m
        labels:
          severity: warning
```

---

## Python: Statistical Anomaly Detector

```python
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional

@dataclass
class AnomalyResult:
    metric: str
    timestamp: pd.Timestamp
    value: float
    z_score: float
    is_anomaly: bool
    severity: str   # normal / warning / critical


class StatisticalAnomalyDetector:
    """Z-score and IQR based anomaly detection for infrastructure metrics."""

    def __init__(self, z_warning: float = 2.5, z_critical: float = 3.5):
        self.z_warning = z_warning
        self.z_critical = z_critical
        self._baselines: dict[str, dict] = {}

    def fit(self, metric_name: str, historical_values: np.ndarray):
        """Build baseline statistics from historical data."""
        clean = historical_values[~np.isnan(historical_values)]
        # Use robust statistics (median/IQR) to resist outliers in training data
        q25, q75 = np.percentile(clean, [25, 75])
        self._baselines[metric_name] = {
            "median": np.median(clean),
            "mad": np.median(np.abs(clean - np.median(clean))),  # Median Absolute Deviation
            "q25": q25,
            "q75": q75,
            "iqr": q75 - q25,
            "mean": np.mean(clean),
            "std": np.std(clean),
        }

    def score(self, metric_name: str, value: float) -> AnomalyResult:
        b = self._baselines[metric_name]
        # Modified Z-score using MAD (more robust than std-based z-score)
        mad = b["mad"] if b["mad"] > 0 else 1e-10
        z = 0.6745 * (value - b["median"]) / mad

        severity = "normal"
        if abs(z) > self.z_critical:
            severity = "critical"
        elif abs(z) > self.z_warning:
            severity = "warning"

        return AnomalyResult(
            metric=metric_name,
            timestamp=pd.Timestamp.now(),
            value=value,
            z_score=round(z, 2),
            is_anomaly=abs(z) > self.z_warning,
            severity=severity,
        )
```

---

## Isolation Forest for Multivariate Anomalies

```python
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np

def detect_infrastructure_anomalies(
    metrics_df: pd.DataFrame,
    feature_cols: list[str],
    contamination: float = 0.02,   # expect ~2% anomalies
) -> pd.DataFrame:
    """
    Detect multivariate anomalies across infrastructure metrics.

    metrics_df: time-series DataFrame with columns like cpu_usage, memory_pct,
                disk_iops, network_in_mb, kafka_lag
    """
    X = metrics_df[feature_cols].copy()

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X.fillna(X.median()))

    # Isolation Forest: fast, no assumptions about distribution
    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    metrics_df = metrics_df.copy()
    metrics_df["anomaly_score"] = model.decision_function(X_scaled)
    metrics_df["is_anomaly"] = model.predict(X_scaled) == -1

    # Rank features contributing to each anomaly
    metrics_df["top_anomaly_feature"] = X.apply(
        lambda row: feature_cols[np.argmax(np.abs(
            (row[feature_cols].values - X[feature_cols].median().values)
            / (X[feature_cols].std().values + 1e-10)
        ))],
        axis=1,
    )

    return metrics_df


# Example usage with Prometheus data
from prometheus_api_client import PrometheusConnect

def fetch_infra_metrics(prom_url: str, lookback_hours: int = 168) -> pd.DataFrame:
    prom = PrometheusConnect(url=prom_url, disable_ssl=False)

    metrics = {
        "cpu_usage": 'avg(rate(container_cpu_usage_seconds_total{namespace="airflow"}[5m]))',
        "memory_pct": 'avg(container_memory_working_set_bytes{namespace="airflow"} / container_spec_memory_limit_bytes)',
        "kafka_lag": 'sum(kafka_consumer_group_lag)',
    }

    dfs = {}
    for name, query in metrics.items():
        result = prom.custom_query_range(
            query=query,
            start_time=pd.Timestamp.now() - pd.Timedelta(hours=lookback_hours),
            end_time=pd.Timestamp.now(),
            step="5m",
        )
        if result:
            df = pd.DataFrame(result[0]["values"], columns=["timestamp", name])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            df[name] = df[name].astype(float)
            dfs[name] = df.set_index("timestamp")

    return pd.concat(dfs.values(), axis=1).reset_index()
```

---

## Prophet-Based Seasonal Anomaly Detection

```python
from prophet import Prophet
import pandas as pd

def detect_seasonal_anomaly(
    df: pd.DataFrame,           # columns: ds (datetime), y (metric value)
    metric_name: str,
    uncertainty_interval: float = 0.99,
) -> pd.DataFrame:
    """Detect anomalies that deviate from seasonal patterns (daily/weekly)."""

    model = Prophet(
        interval_width=uncertainty_interval,
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=False,
    )

    # Add custom regressor for known events (deployments, batch jobs)
    # model.add_regressor('deployment')

    model.fit(df[df["ds"] < df["ds"].max() - pd.Timedelta(days=1)])  # train on all but last day

    # Forecast for the evaluation period
    future = model.make_future_dataframe(periods=24 * 12, freq="5min")
    forecast = model.predict(future)

    # Merge actual values with forecast
    result = df.merge(
        forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]],
        on="ds",
        how="inner",
    )

    # Flag values outside prediction interval
    result["is_anomaly"] = (result["y"] < result["yhat_lower"]) | (result["y"] > result["yhat_upper"])
    result["anomaly_direction"] = "none"
    result.loc[result["y"] > result["yhat_upper"], "anomaly_direction"] = "high"
    result.loc[result["y"] < result["yhat_lower"], "anomaly_direction"] = "low"

    return result[result["is_anomaly"]]
```

---

## Automated Alert Threshold Tuning

```python
def auto_tune_thresholds(
    metric_name: str,
    historical_data: np.ndarray,
    target_false_positive_rate: float = 0.01,  # 1% FPR
) -> dict:
    """
    Calculate alert thresholds from historical data that achieve
    the desired false positive rate.
    """
    clean = historical_data[~np.isnan(historical_data)]

    # Percentile-based thresholds
    warning_pct = (1 - target_false_positive_rate * 2) * 100  # e.g., 98th percentile
    critical_pct = (1 - target_false_positive_rate) * 100      # e.g., 99th percentile

    warning_threshold = np.percentile(clean, warning_pct)
    critical_threshold = np.percentile(clean, critical_pct)

    return {
        "metric": metric_name,
        "warning_threshold": round(float(warning_threshold), 4),
        "critical_threshold": round(float(critical_threshold), 4),
        "based_on_samples": len(clean),
        "target_fpr": target_false_positive_rate,
        "percentiles_used": {
            "warning": f"p{warning_pct:.1f}",
            "critical": f"p{critical_pct:.1f}",
        },
    }

# Batch threshold generation for all metrics
def generate_alert_thresholds(metrics: dict[str, np.ndarray]) -> list[dict]:
    thresholds = []
    for name, data in metrics.items():
        t = auto_tune_thresholds(name, data)
        thresholds.append(t)
        print(f"{name}: warning={t['warning_threshold']}, critical={t['critical_threshold']}")
    return thresholds
```

---

## Data Pipeline Anomaly Signals

```python
# Detect row count anomalies in data pipeline outputs
def check_pipeline_row_count_anomaly(
    table: str,
    partition_date: str,
    lookback_days: int = 14,
    z_threshold: float = 3.0,
) -> dict:
    from sqlalchemy import create_engine, text

    engine = create_engine(TRINO_URI)

    result = engine.execute(text("""
        WITH history AS (
            SELECT
                partition_date,
                COUNT(*) AS row_count
            FROM :table
            WHERE partition_date >= DATE(:partition_date) - INTERVAL ':lookback_days' DAY
              AND partition_date < DATE(:partition_date)
            GROUP BY partition_date
        ),
        stats AS (
            SELECT
                AVG(row_count) AS mean,
                STDDEV(row_count) AS std,
                APPROX_PERCENTILE(row_count, 0.5) AS median
            FROM history
        )
        SELECT
            s.mean,
            s.std,
            s.median,
            t.row_count AS today_count,
            (t.row_count - s.mean) / NULLIF(s.std, 0) AS z_score
        FROM stats s,
        (SELECT COUNT(*) AS row_count FROM :table WHERE partition_date = DATE(:partition_date)) t
    """), {"table": table, "partition_date": partition_date, "lookback_days": lookback_days})

    row = result.fetchone()
    z = row.z_score or 0

    return {
        "table": table,
        "partition_date": partition_date,
        "today_count": row.today_count,
        "expected_mean": round(row.mean, 0),
        "z_score": round(z, 2),
        "is_anomaly": abs(z) > z_threshold,
        "severity": "critical" if abs(z) > z_threshold * 1.5 else ("warning" if abs(z) > z_threshold else "normal"),
    }
```

---

## Anti-Patterns

1. **Static thresholds for dynamic workloads** — batch pipelines have predictable hourly spikes that static thresholds alert on constantly; use seasonal baselines.
2. **Training on data containing anomalies** — the baseline model learns anomalies as "normal"; filter known incidents from training data or use robust statistics (median/MAD).
3. **Too many anomaly dimensions** — anomaly detectors with 50+ features learn noise; select 5–10 highly informative metrics per system.
4. **No alert fatigue management** — anomaly detection generates more alerts than static rules; pair with severity scoring and inhibition to route only actionable anomalies.
5. **Detecting anomalies without action** — anomaly detection is only valuable if it triggers a response; wire each alert type to a runbook or self-healing action.

---

## References

- Prometheus anomaly detection: `prometheus.io/docs/prometheus/latest/querying/functions/`
- Prophet forecasting: `facebook.github.io/prophet/docs/quick_start.html`
- Isolation Forest: `scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html`
- Related skills: `[[aiops-autonomous-incident-response]]`, `[[infra-prometheus-optimization]]`, `[[dataops-root-cause-analysis]]`, `[[dataops-sla-monitoring]]`
