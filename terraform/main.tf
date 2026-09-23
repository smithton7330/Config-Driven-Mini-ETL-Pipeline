locals {
  name_prefix = "${var.project_name}-${var.environment}"
  # Storage account names must be globally unique, lowercase, no hyphens, <=24 chars.
  storage_account_name = substr(
    replace("st${var.project_name}${var.environment}", "-", ""),
    0, 24
  )
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = var.tags
}

# --- Container registry for the pipeline image ---
# In an org running many similar feeds this would usually be one shared
# registry referenced via a data source rather than created per pipeline;
# it's created here to keep this snippet self-contained.
resource "azurerm_container_registry" "this" {
  name                = replace("acr${local.name_prefix}", "-", "")
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "Basic"
  admin_enabled       = false # pull via managed identity only, no admin credentials
  tags                = var.tags
}

# --- Storage for the SQLite source file and pipeline outputs ---
resource "azurerm_storage_account" "this" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
  # Shared keys stay enabled: Container Apps' Azure Files mount does not yet
  # support identity-based auth, so the environment storage below has to use
  # an account key. The key is read straight from this resource's attribute
  # (never typed into code or committed) and lands only in Terraform state -
  # keep state in a remote, encrypted, access-controlled backend.
}

resource "azurerm_storage_share" "data" {
  name                 = "encounters-data"
  storage_account_name = azurerm_storage_account.this.name
  quota                = 5 # GB; sample data is tiny, generous headroom for growth
}

# config.yaml is ours to own, so Terraform uploads it directly: every
# `apply` keeps the deployed config in sync with this repo's copy. This is
# deliberately NOT how the source SQLite data gets there, that's expected
# to be kept current by a separate sync process reading from the
# hospital's live source system, which is out of scope for this
# Terraform (see variables.tf's config_file_path description).
resource "azurerm_storage_share_file" "config" {
  name             = "config.yaml"
  storage_share_id = azurerm_storage_share.data.id
  source           = var.config_file_path
  content_md5      = filemd5(var.config_file_path)
}

# --- Identity the job runs as (no secrets, RBAC-only access) ---
resource "azurerm_user_assigned_identity" "job" {
  name                = "id-${local.name_prefix}-job"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags
}

resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.job.principal_id
}

# --- Container Apps environment (required host for a scheduled Job) ---
resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

resource "azurerm_container_app_environment" "this" {
  name                       = "cae-${local.name_prefix}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = azurerm_resource_group.this.location
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
  tags                       = var.tags
}

resource "azurerm_container_app_environment_storage" "data" {
  name                         = "encounters-data"
  container_app_environment_id = azurerm_container_app_environment.this.id
  account_name                 = azurerm_storage_account.this.name
  share_name                   = azurerm_storage_share.data.name
  access_mode                  = "ReadWrite"
  access_key                   = azurerm_storage_account.this.primary_access_key
}

# --- The scheduled pipeline run itself ---
resource "azurerm_container_app_job" "pipeline" {
  name                         = "caj-${local.name_prefix}"
  resource_group_name          = azurerm_resource_group.this.name
  location                     = azurerm_resource_group.this.location
  container_app_environment_id = azurerm_container_app_environment.this.id
  replica_timeout_in_seconds   = 600
  replica_retry_limit          = 1

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.job.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.job.id
  }

  schedule_trigger_config {
    cron_expression          = var.schedule_cron
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "encounters-etl"
      image  = var.container_image
      cpu    = var.cpu
      memory = var.memory

      # /data is the mounted Azure Files share: config.yaml, the source
      # SQLite file and the output/ directory all live there, so relative
      # paths inside config.yaml resolve correctly regardless of the
      # image's own working directory.
      command = ["python3", "/app/pipeline.py", "--config", "/data/config.yaml"]

      volume_mounts {
        name = "data"
        path = "/data"
      }

      env {
        name  = "PIPELINE_ENVIRONMENT"
        value = var.environment
      }
    }

    volume {
      name         = "data"
      storage_type = "AzureFile"
      storage_name = azurerm_container_app_environment_storage.data.name
    }
  }

  tags = var.tags
}
