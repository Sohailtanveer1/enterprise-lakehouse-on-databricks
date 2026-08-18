# REFERENCE ONLY — NEVER APPLIED IN THIS PROJECT
#
# Unity Catalog groups, users and service principals are ACCOUNT-level
# resources. Databricks Free Edition has no account console and no
# account-level API, so `terraform apply` on this directory fails at the first
# resource.
#
# It is committed rather than omitted because the knowledge is the point: this
# is what the account layer looks like, and an interviewer asking "how would
# you set up RBAC properly?" gets a concrete answer instead of a hand-wave.
#
# To use it on a workspace with account access:
#   1. Point the provider at the ACCOUNT host (accounts.gcp.databricks.com)
#   2. terraform apply here first, to create groups and the service principal
#   3. Then set create_group_grants = true in the workspace module
#
# There is deliberately no backend, no tfvars and no CI wiring here. Nothing
# should be able to apply it by accident.

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

# Account-level provider — a different host from the workspace provider.
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.gcp.databricks.com"
  account_id = var.databricks_account_id
}

variable "databricks_account_id" {
  type      = string
  sensitive = true
}

locals {
  # Groups, not individual users. Per-user grants become unauditable within a
  # year — nobody can answer "who can read PII?" without a spreadsheet.
  groups = {
    northpeak_platform_admins = "Owns the metastore, catalogs and grants"
    northpeak_data_engineers  = "Builds and operates the pipelines"
    northpeak_data_analysts   = "Builds reports. Gold and masked Silver only."
    northpeak_business_users  = "Consumes dashboards. Curated Gold views only."
    northpeak_finance         = "Financial reporting. The only role that sees product cost."
    northpeak_pii_readers     = "Support and compliance. Unmasked PII — deliberately separate."
  }
}

resource "databricks_group" "roles" {
  provider     = databricks.account
  for_each     = local.groups
  display_name = each.key

  # Groups may hold entitlements but never direct workspace admin. Admin is
  # granted to named individuals, not inherited by group membership.
  allow_cluster_create = false
}

# Service principal for CI/CD and scheduled runs.
#
# A pipeline running as a person breaks when that person leaves — the single
# most common cause of "the pipeline broke and nobody knows why". It is also
# why no human holds MODIFY on prod (SECURITY.md §6).
resource "databricks_service_principal" "pipeline" {
  provider     = databricks.account
  display_name = "northpeak_sp_pipeline"
}

resource "databricks_service_principal" "cicd" {
  provider     = databricks.account
  display_name = "northpeak_sp_cicd"
}

# Secret rotated at project milestones; 90 days in production.
resource "databricks_service_principal_secret" "cicd" {
  provider             = databricks.account
  service_principal_id = databricks_service_principal.cicd.id
}

output "cicd_client_id" {
  value = databricks_service_principal.cicd.application_id
}

output "cicd_client_secret" {
  value     = databricks_service_principal_secret.cicd.secret
  sensitive = true
}
