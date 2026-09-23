terraform {
  required_version = ">= 1.7.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}

provider "azurerm" {
  features {}
  # subscription_id / tenant_id are intentionally not set here.
  # Provide them via ARM_SUBSCRIPTION_ID / ARM_TENANT_ID env vars or
  # `az login`, so no subscription ID is ever committed to this repo.
}
