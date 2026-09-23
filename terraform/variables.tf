variable "environment" {
  description = "Deployment environment, used in resource names (e.g. dev, prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

variable "location" {
  description = "Azure region to deploy into."
  type        = string
  default     = "australiaeast"
}

variable "project_name" {
  description = "Short name used as a prefix for all resource names."
  type        = string
  default     = "encounters-etl"
}

variable "container_image" {
  description = "Fully qualified container image (registry/repo:tag) running pipeline.py. Built and pushed by CI, not by this Terraform."
  type        = string
}

variable "schedule_cron" {
  description = "Cron expression (UTC) controlling how often the pipeline job runs."
  type        = string
  default     = "0 2 * * *" # 02:00 UTC daily
}

variable "cpu" {
  description = "vCPU allocation for the job's container."
  type        = number
  default     = 0.5
}

variable "memory" {
  description = "Memory allocation for the job's container, e.g. '1Gi'."
  type        = string
  default     = "1Gi"
}

variable "tags" {
  description = "Common resource tags."
  type        = map(string)
  default = {
    system = "encounters-etl"
  }
}

variable "config_file_path" {
  description = "Path to the config.yaml uploaded into the Azure Files share alongside the pipeline, relative to wherever `terraform apply` is run from (conventionally this terraform/ directory). The source SQLite data itself is NOT managed here: in production that would be kept current by a separate sync process reading from the hospital's live source system, not by Terraform."
  type        = string
  default     = "../config.yaml"
}
