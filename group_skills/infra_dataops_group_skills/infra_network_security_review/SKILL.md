---
name: infra-network-security-review
description: Network security review for data platforms — Kubernetes NetworkPolicy (default-deny + allow patterns), VPC/subnet design (private subnets for all data services), security group rules audit (0.0.0.0/0 detection), TLS everywhere (Kafka mTLS/Trino HTTPS/DB SSL), service mesh (Istio mTLS), DNS security (private hosted zones), egress filtering (no unrestricted outbound), VPC peering vs PrivateLink, network flow logs analysis, firewall rules review
---

# Network Security Review

## When to Use

- Designing network isolation for a new data platform
- Auditing existing security groups for overly permissive rules
- Enabling TLS/mTLS for all inter-service communication
- Investigating a potential lateral movement incident
- Planning VPC peering or PrivateLink connectivity

---

## Kubernetes NetworkPolicy — Default Deny + Explicit Allow

```yaml
# Step 1: Block all ingress to namespace by default
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: airflow
spec:
  podSelector: {}           # applies to ALL pods
  policyTypes:
    - Ingress
    - Egress

---
# Step 2: Allow only specific traffic
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: airflow-scheduler
  namespace: airflow
spec:
  podSelector:
    matchLabels:
      component: scheduler
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              component: webserver
      ports:
        - port: 8793    # Airflow scheduler port
  egress:
    # Allow scheduler to connect to metadata DB
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: postgres
      ports:
        - port: 5432
    # Allow scheduler to connect to Kafka
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kafka
      ports:
        - port: 9092
        - port: 9093   # Kafka TLS
    # Allow DNS resolution
    - to:
        - namespaceSelector: {}
      ports:
        - port: 53
          protocol: UDP
        - port: 53
          protocol: TCP
```

---

## AWS Security Group Audit

```bash
#!/bin/bash
# Find security groups with 0.0.0.0/0 ingress rules

aws ec2 describe-security-groups \
  --filters "Name=ip-permission.cidr,Values=0.0.0.0/0" \
  --query 'SecurityGroups[*].{
    ID: GroupId,
    Name: GroupName,
    VPC: VpcId,
    Rules: IpPermissions
  }' \
  --output json | jq '.[] |
  "SG: \(.ID) (\(.Name)) in VPC \(.VPC)
   Ports: \(.Rules | map("\(.FromPort)-\(.ToPort)/\(.IpProtocol)") | join(", "))"'

# Find RDS instances with public access
aws rds describe-db-instances \
  --query "DBInstances[?PubliclyAccessible==\`true\`].{
    ID: DBInstanceIdentifier,
    Endpoint: Endpoint.Address
  }" \
  --output table

# Find MSK clusters with unauthenticated access
aws kafka list-clusters-v2 \
  --query "ClusterInfoList[?ClusterState=='ACTIVE'].{
    Name: ClusterName,
    Auth: ClientAuthentication
  }" | jq '.[] | select(.Auth.Unauthenticated.Enabled == true)'
```

---

## TLS Configuration

### Kafka mTLS

```yaml
# MSK cluster: require TLS in transit
encryption_info {
  encryption_in_transit {
    client_broker = "TLS"        # TLS_PLAINTEXT or TLS
    in_cluster    = true
  }
}

# Java Kafka client with mTLS
spring:
  kafka:
    bootstrap-servers: kafka:9093
    ssl:
      trust-store-location: classpath:kafka-truststore.jks
      trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD}
      key-store-location: classpath:kafka-keystore.jks
      key-store-password: ${KAFKA_KEYSTORE_PASSWORD}
    properties:
      security.protocol: SSL
```

### PostgreSQL TLS

```python
# Require SSL for all PostgreSQL connections
import psycopg2

conn = psycopg2.connect(
    host="postgres.internal",
    database="airflow",
    user="airflow",
    password=os.environ["DB_PASSWORD"],
    sslmode="verify-full",              # reject if cert doesn't match
    sslcert="/certs/client.crt",
    sslkey="/certs/client.key",
    sslrootcert="/certs/ca.crt",
)

# Enforce SSL in PostgreSQL server
# postgresql.conf:
# ssl = on
# ssl_cert_file = 'server.crt'
# ssl_key_file = 'server.key'
# pg_hba.conf:
# hostssl all all 0.0.0.0/0 scram-sha-256
```

### Istio Service Mesh mTLS (Zero-Trust)

```yaml
# Enable strict mTLS across entire mesh
apiVersion: security.istio.io/v1beta1
kind: PeerAuthentication
metadata:
  name: default
  namespace: data-platform
spec:
  mtls:
    mode: STRICT    # reject all plaintext traffic

---
# Define allowed traffic (Authorization Policy)
apiVersion: security.istio.io/v1beta1
kind: AuthorizationPolicy
metadata:
  name: trino-access
  namespace: trino
spec:
  selector:
    matchLabels:
      app: trino-coordinator
  rules:
    - from:
        - source:
            principals:
              - "cluster.local/ns/airflow/sa/airflow-worker"
              - "cluster.local/ns/dbt/sa/dbt-runner"
      to:
        - operation:
            ports: ["8080", "8443"]
```

---

## VPC Network Design

```hcl
# Production: data services only in private subnets
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  cidr = "10.0.0.0/16"

  private_subnets  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets   = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  database_subnets = ["10.0.201.0/24", "10.0.202.0/24", "10.0.203.0/24"]

  # S3/Glue/ECR via VPC endpoints (no internet)
  enable_s3_endpoint               = true
  enable_glue_endpoint             = true
  enable_ecr_dkr_endpoint          = true
  enable_ecr_api_endpoint          = true

  # No public IPs for data services
  map_public_ip_on_launch          = false
}

# Egress: restrict outbound traffic
resource "aws_security_group" "egress_restricted" {
  name   = "${local.prefix}-egress-restricted"
  vpc_id = module.vpc.vpc_id

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]    # HTTPS only (for package downloads)
  }
  # No blanket 0.0.0.0/0 egress
}
```

---

## VPC Flow Logs Analysis

```bash
# Enable VPC flow logs
aws ec2 create-flow-logs \
  --resource-type VPC \
  --resource-ids vpc-1234567890abcdef0 \
  --traffic-type ALL \
  --log-destination-type cloud-watch-logs \
  --log-group-name /vpc/flow-logs \
  --deliver-logs-permission-arn arn:aws:iam::123456789012:role/flowlogsRole

# Athena query: find suspicious connections to unusual ports
SELECT
  srcaddr, dstaddr, dstport, action, packets, bytes,
  from_unixtime(start) AS start_time
FROM vpc_flow_logs
WHERE action = 'REJECT'
  AND dstport NOT IN (80, 443, 5432, 9092, 9093)
  AND start > to_unixtime(current_timestamp - interval '1' hour)
ORDER BY packets DESC
LIMIT 50;

# Find data exfiltration patterns (large outbound transfers)
SELECT
  srcaddr, dstaddr, SUM(bytes) AS total_bytes
FROM vpc_flow_logs
WHERE direction = 'egress'
  AND dstaddr NOT LIKE '10.%'   -- non-RFC1918 destinations
  AND start > to_unixtime(current_timestamp - interval '24' hour)
GROUP BY srcaddr, dstaddr
HAVING SUM(bytes) > 100000000   -- > 100MB
ORDER BY total_bytes DESC;
```

---

## Private Endpoints vs VPC Peering

| Pattern | Use Case | Security |
|---------|----------|---------|
| VPC Endpoints (AWS PrivateLink) | AWS services (S3/RDS/MSK) | Traffic stays in AWS network |
| VPC Peering | Connect two VPCs (same/cross-account) | Transitive routing blocked |
| Transit Gateway | Hub-and-spoke, many VPCs | Centralized routing |
| PrivateLink (custom service) | Expose internal service to consumer VPC | No VPC peering required |

```hcl
# AWS PrivateLink for RDS
resource "aws_vpc_endpoint" "rds" {
  vpc_id              = module.vpc.vpc_id
  service_name        = "com.amazonaws.us-east-1.rds"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = module.vpc.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
}
```

---

## Network Security Checklist

```
[ ] All data services in private subnets (no public IPs)
[ ] Security groups: no 0.0.0.0/0 ingress except load balancers
[ ] Kubernetes NetworkPolicy: default-deny in all namespaces
[ ] Kafka: TLS required (client_broker = TLS)
[ ] PostgreSQL: SSL required (hostssl only in pg_hba.conf)
[ ] Trino/ClickHouse: HTTPS endpoints only
[ ] VPC endpoints for AWS services (S3/ECR/Glue/SSM)
[ ] VPC flow logs enabled and analyzed weekly
[ ] No SSH (port 22) open to 0.0.0.0/0
[ ] mTLS via Istio or Linkerd for inter-service communication
[ ] DNS: private hosted zones, no public record for internal services
```

---

## Anti-Patterns

1. **`0.0.0.0/0` ingress on any port** — even SSH on jump hosts should be restricted to VPN CIDR; open internet access to any data service is critical risk.
2. **No NetworkPolicy in Kubernetes** — any compromised pod can reach any other pod and any database; deploy default-deny before any workloads.
3. **Plaintext Kafka** — credentials and messages transmitted in clear text on network; always enforce TLS between clients and brokers.
4. **Database accessible from public subnets** — RDS/ClickHouse should never be in public subnets; use a bastion host or VPN for admin access.
5. **No VPC flow logs** — lateral movement and data exfiltration go undetected; enable flow logs before any production traffic.

---

## References

- Kubernetes NetworkPolicy: `kubernetes.io/docs/concepts/services-networking/network-policies/`
- Istio security: `istio.io/docs/concepts/security/`
- AWS VPC endpoints: `docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html`
- Related skills: `[[infra-kubernetes-security-audit]]`, `[[infra-rbac-audit]]`, `[[infra-aws-data-platform-review]]`
