output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "container_registry_login_server" {
  value = azurerm_container_registry.this.login_server
}

output "job_name" {
  value = azurerm_container_app_job.pipeline.name
}

output "job_identity_principal_id" {
  description = "Principal ID of the job's managed identity, for granting it access to additional resources (e.g. a hospital-specific source system) without any secrets."
  value       = azurerm_user_assigned_identity.job.principal_id
}
