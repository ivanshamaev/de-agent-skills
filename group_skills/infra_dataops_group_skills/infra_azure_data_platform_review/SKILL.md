---
name: infra-azure-data-platform-review
description: Azure data platform review — ADLS Gen2 (hierarchical namespace/RBAC/lifecycle), Azure Synapse vs Databricks vs HDInsight trade-offs, Event Hubs (Kafka protocol compatible), ADF vs Azure Databricks for ETL, Azure Managed Airflow, AKS with Workload Identity (AAD Pod Identity), Key Vault for secrets, Azure Monitor/Log Analytics, Purview for data catalog/lineage, Private Endpoints for data services, cost management (reserved instances/spot VMs)
---

# Azure Data Platform Review

## When to Use

- Designing or reviewing an Azure-native data platform
- Auditing ADLS Gen2 access controls and RBAC
- Choosing between Azure Synapse, Databricks, and HDInsight
- Setting up Managed Identity for AKS workloads
- Implementing Azure Purview for data governance

---

## ADLS Gen2 Data Lake

```hcl
# Terraform: ADLS Gen2 with hierarchical namespace
resource "azurerm_storage_account" "data_lake" {
  name                     = "${replace(local.prefix, "-", "")}datalake"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"           # zone-redundant storage
  account_kind             = "StorageV2"
  is_hns_enabled           = true            # hierarchical namespace = ADLS Gen2

  blob_properties {
    versioning_enabled  = true
    delete_retention_policy { days = 30 }
  }

  identity {
    type = "SystemAssigned"
  }

  # Encrypt with customer-managed key
  customer_managed_key {
    key_vault_key_id          = azurerm_key_vault_key.storage.id
    user_assigned_identity_id = azurerm_user_assigned_identity.storage.id
  }

  tags = local.common_tags
}

# Hierarchical namespace containers (zones)
resource "azurerm_storage_data_lake_gen2_filesystem" "zones" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.data_lake.id
}

# RBAC: Airflow service principal gets Storage Blob Data Contributor on bronze/silver
resource "azurerm_role_assignment" "airflow_bronze" {
  scope                = azurerm_storage_data_lake_gen2_filesystem.zones["bronze"].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.airflow.principal_id
}

# Lifecycle policy
resource "azurerm_storage_management_policy" "data_lake" {
  storage_account_id = azurerm_storage_account.data_lake.id
  rule {
    name    = "bronze-tiering"
    enabled = true
    filters {
      prefix_match = ["bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
        delete_after_days_since_modification_greater_than          = 365
      }
    }
  }
}
```

---

## AKS Workload Identity

```hcl
# AKS cluster with OIDC issuer (required for Workload Identity)
resource "azurerm_kubernetes_cluster" "main" {
  name                = "${local.prefix}-aks"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  oidc_issuer_enabled = true
  workload_identity_enabled = true

  identity {
    type = "SystemAssigned"
  }

  default_node_pool {
    name       = "system"
    vm_size    = "Standard_D4s_v3"
    node_count = 3
    vnet_subnet_id = azurerm_subnet.aks.id
  }
}

# Managed Identity for Airflow
resource "azurerm_user_assigned_identity" "airflow" {
  name                = "${local.prefix}-airflow"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
}

# Federated credential: link K8s SA to Managed Identity
resource "azurerm_federated_identity_credential" "airflow_worker" {
  name                = "airflow-worker"
  resource_group_name = azurerm_resource_group.main.name
  parent_id           = azurerm_user_assigned_identity.airflow.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.main.oidc_issuer_url
  subject             = "system:serviceaccount:airflow:airflow-worker"
}
```

```yaml
# K8s ServiceAccount annotated with Managed Identity
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-worker
  namespace: airflow
  annotations:
    azure.workload.identity/client-id: "<managed-identity-client-id>"
  labels:
    azure.workload.identity/use: "true"
```

---

## Azure Event Hubs (Kafka-Compatible)

```python
# Event Hubs works with standard Kafka clients
from confluent_kafka import Producer, Consumer

# Connection string auth (or Managed Identity via OAUTHBEARER)
producer_conf = {
    "bootstrap.servers": "my-namespace.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": "Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
}

# Event Hubs = Kafka topic; Namespace = Kafka cluster
# Partition = Event Hub partition (fixed at creation time)
producer = Producer(producer_conf)
producer.produce("orders-raw", key="order-1", value='{"order_id": "1"}')
producer.flush()
```

```hcl
resource "azurerm_eventhub_namespace" "main" {
  name                = "${local.prefix}-events"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "Standard"
  capacity            = 4           # throughput units (1 TU = 1 MB/s ingress)

  kafka_enabled            = true   # enable Kafka protocol
  auto_inflate_enabled     = true
  maximum_throughput_units = 20
}

resource "azurerm_eventhub" "orders" {
  name                = "orders-raw"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 32
  message_retention   = 7           # days
}
```

---

## Azure Synapse vs Databricks

| Aspect | Azure Synapse | Azure Databricks |
|--------|---------------|-----------------|
| SQL analytics | Dedicated/serverless SQL pool | Databricks SQL Warehouse |
| Spark | Synapse Spark pools | Full Databricks Runtime |
| Delta Lake | Via Spark pool | Native (Delta Engine) |
| Orchestration | Synapse Pipelines | Databricks Workflows |
| MLflow | Limited | Built-in |
| Unity Catalog | No | Yes (data governance) |
| Cost | SQL pool: per DWU-hour | Per DBU-hour |
| Best for | SQL warehouse workloads | Lakehouse + ML platform |

---

## Key Vault for Secrets

```hcl
resource "azurerm_key_vault" "data_platform" {
  name                       = "${local.prefix}-kv"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    ip_rules       = [var.corp_vpn_cidr]
    virtual_network_subnet_ids = [azurerm_subnet.aks.id]
  }
}

resource "azurerm_key_vault_access_policy" "airflow" {
  key_vault_id = azurerm_key_vault.data_platform.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_user_assigned_identity.airflow.principal_id

  secret_permissions = ["Get", "List"]
}
```

```python
# Access Key Vault from AKS pod with Workload Identity
from azure.keyvault.secrets import SecretClient
from azure.identity import WorkloadIdentityCredential

credential = WorkloadIdentityCredential()
client = SecretClient(
    vault_url="https://my-kv.vault.azure.net/",
    credential=credential,
)

db_password = client.get_secret("airflow-db-password").value
```

---

## Azure Monitor and Log Analytics

```hcl
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${local.prefix}-logs"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = 90
}

# AKS diagnostic settings → Log Analytics
resource "azurerm_monitor_diagnostic_setting" "aks" {
  name               = "aks-diagnostics"
  target_resource_id = azurerm_kubernetes_cluster.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "kube-apiserver"
  }
  enabled_log {
    category = "kube-controller-manager"
  }
  metric {
    category = "AllMetrics"
  }
}
```

```kusto
// KQL: Airflow task failures in last 24h
ContainerLog
| where TimeGenerated > ago(24h)
| where ContainerName == "airflow-scheduler"
| where LogEntry contains "ERROR" or LogEntry contains "FAILED"
| parse LogEntry with * "dag_id=" dag_id "," * "task_id=" task_id ","*
| summarize count() by dag_id, task_id
| order by count_ desc
```

---

## Private Endpoints for Data Services

```hcl
# Private endpoint: ADLS Gen2 accessible only within VNet
resource "azurerm_private_endpoint" "storage" {
  name                = "${local.prefix}-storage-pe"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  subnet_id           = azurerm_subnet.private_endpoints.id

  private_service_connection {
    name                           = "storage"
    private_connection_resource_id = azurerm_storage_account.data_lake.id
    subresource_names              = ["dfs"]   # ADLS Gen2 endpoint
    is_manual_connection           = false
  }
}
```

---

## Review Checklist

```
[ ] ADLS Gen2: hierarchical namespace, RBAC (not SAS tokens), lifecycle rules
[ ] No SAS tokens with long expiry — use Managed Identity instead
[ ] AKS: Workload Identity enabled (not AAD Pod Identity v1)
[ ] Key Vault: network deny default, private endpoint or VNet rules
[ ] Event Hubs: RBAC auth (not connection strings in code), min TLS 1.2
[ ] Private endpoints for all data services (no public internet access)
[ ] Azure Purview registered and scanning key datasets
[ ] Diagnostic settings: all services → Log Analytics (retention 90d)
[ ] ADLS Gen2: versioning enabled, soft delete configured
[ ] Cost: Reserved capacity for Synapse DWU or Databricks DBUs
```

---

## Anti-Patterns

1. **SAS tokens with long expiry or no expiry** — leaked token gives permanent access; use Managed Identity and RBAC for service-to-service auth.
2. **Storage account key rotation neglected** — keys are valid indefinitely unless rotated; use RBAC and disable shared key access.
3. **No hierarchical namespace on ADLS** — flat storage doesn't support directory-level RBAC or efficient rename operations; enable HNS from creation.
4. **Public endpoint on Key Vault** — accessible from any IP; add network deny default + VNet rules.
5. **Event Hubs with basic SKU** — no consumer groups beyond $Default, no Kafka protocol; use Standard SKU minimum for data pipelines.

---

## References

- ADLS Gen2: `docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction`
- AKS Workload Identity: `docs.microsoft.com/azure/aks/workload-identity-overview`
- Azure Databricks: `docs.microsoft.com/azure/databricks/`
- Azure Purview: `docs.microsoft.com/azure/purview/`
- Related skills: `[[terraform-data]]`, `[[infra-kubernetes-security-audit]]`, `[[de-cost-optimization]]`
