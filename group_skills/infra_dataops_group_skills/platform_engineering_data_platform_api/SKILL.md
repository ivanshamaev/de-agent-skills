---
name: platform-engineering-data-platform-api
description: Data platform self-service API — FastAPI REST API for platform operations (Kafka topic CRUD/Airflow DAG trigger/dbt job run/Trino query execution), OAuth2+JWT authentication with team-based RBAC, async job tracking with status polling, rate limiting per team, OpenAPI spec generation, Terraform provider for API resources, versioned API (v1/v2), audit logging of all platform operations, SDK generation (Python/TypeScript), Kubernetes deployment with HPA
---

# Data Platform Self-Service API

## When to Use

- Building a unified API layer over data platform services (Airflow, Kafka, Trino, dbt)
- Enabling other teams to trigger pipelines, create topics, or query data without direct access
- Enforcing RBAC and rate limits on platform resource access
- Generating a typed Python/TypeScript SDK for programmatic platform access
- Creating a Terraform provider for platform resources (topics, DAGs, tables)

---

## API Architecture

```
POST /api/v1/auth/token              — OAuth2 token exchange
GET  /api/v1/pipelines               — list DAGs with status
POST /api/v1/pipelines/{dag_id}/runs — trigger DAG run
GET  /api/v1/pipelines/{dag_id}/runs/{run_id} — poll run status

GET  /api/v1/kafka/topics            — list topics
POST /api/v1/kafka/topics            — create topic (provisioning)
DELETE /api/v1/kafka/topics/{name}   — delete topic

POST /api/v1/queries                 — submit Trino query (async)
GET  /api/v1/queries/{query_id}      — poll query status + results

POST /api/v1/dbt/jobs                — trigger dbt Cloud / dbt Core job
GET  /api/v1/dbt/jobs/{job_id}       — poll job status

GET  /api/v1/catalog/datasets        — list registered datasets
GET  /api/v1/catalog/datasets/{name}/lineage — get lineage graph
```

---

## FastAPI App with JWT Auth

```python
from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from datetime import datetime, timedelta, timezone
from typing import Annotated
import jwt, uuid, asyncio

# --- App Setup ---
app = FastAPI(
    title="Data Platform API",
    version="1.0.0",
    description="Self-service API for the data platform",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SECRET_KEY = "replace-with-vault-secret"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


# --- Token & Auth ---
def create_access_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = {**data, "exp": datetime.now(timezone.utc) + expires_delta}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        if sub is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"user": sub, "team": payload.get("team"), "roles": payload.get("roles", [])}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def require_role(required_role: str):
    async def check_role(current_user: dict = Depends(get_current_user)):
        if required_role not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail=f"Role '{required_role}' required")
        return current_user
    return check_role


@app.post("/api/v1/auth/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    # In production: validate against LDAP/SSO, not hardcoded
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect credentials")

    token = create_access_token(
        data={"sub": user["username"], "team": user["team"], "roles": user["roles"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return {"access_token": token, "token_type": "bearer"}
```

---

## Pipeline Trigger API

```python
from airflow.api.client.local_client import Client as AirflowClient

class DagRunRequest(BaseModel):
    conf: dict = {}
    logical_date: str | None = None   # ISO 8601, e.g. "2024-01-15T00:00:00"
    note: str | None = None

class DagRunResponse(BaseModel):
    run_id: str
    dag_id: str
    state: str
    logical_date: str
    started_at: str

@app.post("/api/v1/pipelines/{dag_id}/runs", response_model=DagRunResponse)
async def trigger_dag(
    dag_id: str,
    request: DagRunRequest,
    current_user: dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Trigger a DAG run. Accessible by any authenticated user."""
    # Log audit event
    audit_log("trigger_dag", user=current_user["user"], resource=dag_id, details=request.dict())

    # Validate DAG exists and is active
    client = AirflowClient(None, None)
    try:
        dag_info = client.get_dag(dag_id)
    except Exception:
        raise HTTPException(status_code=404, detail=f"DAG {dag_id} not found")

    if not dag_info.get("is_active"):
        raise HTTPException(status_code=400, detail=f"DAG {dag_id} is paused")

    run_id = f"api_{current_user['user']}_{uuid.uuid4().hex[:8]}"
    client.trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf={**request.conf, "_triggered_by": current_user["user"]},
        execution_date=request.logical_date,
    )

    return DagRunResponse(
        run_id=run_id,
        dag_id=dag_id,
        state="queued",
        logical_date=request.logical_date or datetime.now(timezone.utc).isoformat(),
        started_at=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/api/v1/pipelines/{dag_id}/runs/{run_id}")
async def get_dag_run_status(
    dag_id: str,
    run_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Poll DAG run status."""
    from sqlalchemy import create_engine, text
    engine = create_engine(AIRFLOW_DB_URI)
    row = engine.execute(text("""
        SELECT state, start_date, end_date, conf
        FROM dag_run WHERE dag_id = :dag_id AND run_id = :run_id
    """), {"dag_id": dag_id, "run_id": run_id}).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Run not found")

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "state": row.state,
        "started_at": str(row.start_date),
        "finished_at": str(row.end_date),
    }
```

---

## Async Query API (Trino)

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

query_store: dict[str, dict] = {}  # production: use Redis
executor = ThreadPoolExecutor(max_workers=10)

class QueryState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

class QueryRequest(BaseModel):
    sql: str
    catalog: str = "iceberg"
    schema: str = "gold"
    max_rows: int = 10000

    @validator("sql")
    def no_dangerous_statements(cls, v):
        forbidden = ["DROP ", "TRUNCATE ", "DELETE FROM", "ALTER TABLE"]
        if any(v.upper().startswith(f) for f in forbidden):
            raise ValueError("DDL and destructive statements are not allowed via API")
        return v

@app.post("/api/v1/queries")
async def submit_query(
    request: QueryRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
):
    query_id = str(uuid.uuid4())
    query_store[query_id] = {
        "state": QueryState.QUEUED,
        "submitted_by": current_user["user"],
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }

    audit_log("submit_query", user=current_user["user"], resource=query_id)
    background_tasks.add_task(_execute_query, query_id, request, current_user["team"])

    return {"query_id": query_id, "state": QueryState.QUEUED}


async def _execute_query(query_id: str, request: QueryRequest, team: str):
    query_store[query_id]["state"] = QueryState.RUNNING
    try:
        loop = asyncio.get_event_loop()
        rows, cols = await loop.run_in_executor(
            executor,
            _run_trino_query,
            request.sql, request.catalog, request.schema, request.max_rows, team
        )
        query_store[query_id].update({
            "state": QueryState.FINISHED,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        query_store[query_id].update({
            "state": QueryState.FAILED,
            "error": str(e),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })


def _run_trino_query(sql, catalog, schema, max_rows, team):
    from trino.dbapi import connect
    conn = connect(
        host=TRINO_HOST, port=443, user=f"api__{team}",
        catalog=catalog, schema=schema,
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchmany(max_rows)
    cols = [d[0] for d in cur.description]
    return rows, cols


@app.get("/api/v1/queries/{query_id}")
async def get_query(query_id: str, current_user: dict = Depends(get_current_user)):
    if query_id not in query_store:
        raise HTTPException(status_code=404, detail="Query not found")
    return query_store[query_id]
```

---

## Rate Limiting Middleware

```python
from fastapi import Request
from collections import defaultdict
import time

RATE_LIMITS = {
    "default": (100, 60),     # 100 requests per 60 seconds
    "admin":   (1000, 60),
}

request_counts: dict = defaultdict(list)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Extract team from JWT (without full auth flow)
    team = "default"
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        try:
            payload = jwt.decode(auth[7:], SECRET_KEY, algorithms=[ALGORITHM], options={"verify_exp": False})
            team = payload.get("team", "default")
        except Exception:
            pass

    limit, window = RATE_LIMITS.get(team, RATE_LIMITS["default"])
    now = time.time()
    key = f"{team}:{request.client.host}"

    # Clean old requests
    request_counts[key] = [ts for ts in request_counts[key] if now - ts < window]

    if len(request_counts[key]) >= limit:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=429,
            content={"detail": f"Rate limit exceeded: {limit} requests per {window}s"},
            headers={"Retry-After": str(window)},
        )

    request_counts[key].append(now)
    return await call_next(request)
```

---

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-platform-api
  namespace: platform
spec:
  replicas: 2
  selector:
    matchLabels:
      app: data-platform-api
  template:
    metadata:
      labels:
        app: data-platform-api
    spec:
      containers:
        - name: api
          image: ghcr.io/myorg/data-platform-api:v1.0.0
          ports:
            - containerPort: 8000
          env:
            - name: SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: platform-api-secrets
                  key: jwt-secret
          resources:
            requests:
              cpu: "0.5"
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 1Gi
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: data-platform-api-hpa
  namespace: platform
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: data-platform-api
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

---

## Anti-Patterns

1. **Synchronous Trino queries in API handlers** — long-running queries block the event loop; always use async execution with polling (submit + poll pattern).
2. **No RBAC on destructive operations** — allowing any authenticated user to delete Kafka topics or DAGs; require an `admin` or `platform-operator` role for destructive actions.
3. **JWT secret in code** — hardcoding `SECRET_KEY` in source; fetch from HashiCorp Vault or Kubernetes Secret at startup.
4. **No audit log** — platform operations without audit trail make compliance impossible; log every API call with user, action, resource, timestamp.
5. **One API version forever** — breaking API changes without versioning breaks all consumers; use `/api/v1/` prefix and maintain the old version for 6 months after introducing v2.

---

## References

- FastAPI: `fastapi.tiangolo.com/tutorial/security/oauth2-jwt/`
- Airflow REST API: `airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html`
- Trino Python client: `trinodb.github.io/trino-python-client/`
- Related skills: `[[platform-engineering-internal-developer-platform]]`, `[[platform-engineering-agentic-control-plane]]`, `[[infra-rbac-audit]]`
