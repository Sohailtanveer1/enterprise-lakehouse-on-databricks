# Unity Catalog structure and grants — workspace-scoped only.
#
# Terraform owns the slow-moving governance objects; Asset Bundles own the
# fast-moving jobs and code (ADR-07). Mixing them on one resource causes state
# conflicts.
#
# WHAT IS NOT HERE, AND WHY
# Groups, users and service principals are **account-level** resources.
# Free Edition has no account console and no account-level API, so
# `databricks_group` cannot be applied at all. That code lives in
# terraform/reference-only/ — written, reviewed, never applied. Pretending
# otherwise would produce a plan that fails on the first apply.
#
#   terraform init
#   terraform plan  -var-file=envs/dev.tfvars
#   terraform apply -var-file=envs/dev.tfvars

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
  # Local state. A real deployment uses a GCS backend with locking; a solo
  # project with one operator does not need the extra moving part, and saying
  # so is more honest than configuring a backend nobody shares.
  #
  # backend "gcs" {
  #   bucket = "northpeak-tfstate"
  #   prefix = "unity-catalog"
  # }
}

provider "databricks" {
  # Auth comes from the DATABRICKS_HOST / DATABRICKS_TOKEN environment or a
  # CLI profile. Never a literal here (NFR-MAINT-05).
  profile = var.databricks_profile
}

locals {
  schemas = {
    bronze = "Raw, immutable, append-only. Engineers only — unvalidated by definition."
    silver = "Cleansed, deduplicated, CDC applied, SCD history. Analysts read masked."
    gold   = "Conformed star schema and aggregate marts. The business-facing layer."
    audit  = "pipeline_run_audit, data_quality_results, reconciliation_results."
  }

  # Grants are declared per schema so least privilege is visible rather than
  # implied. See SECURITY.md §2 for the full role model.
  schema_grants = {
    bronze = {
      data_engineer = ["SELECT", "MODIFY"]
      sp_pipeline   = ["SELECT", "MODIFY"]
    }
    silver = {
      data_engineer = ["SELECT", "MODIFY"]
      data_analyst  = ["SELECT"]
      pii_reader    = ["SELECT"]
      sp_pipeline   = ["SELECT", "MODIFY"]
    }
    gold = {
      data_engineer = ["SELECT", "MODIFY"]
      data_analyst  = ["SELECT"]
      business_user = ["SELECT"]
      finance       = ["SELECT"]
      pii_reader    = ["SELECT"]
      sp_pipeline   = ["SELECT", "MODIFY"]
    }
    audit = {
      data_engineer = ["SELECT"]
      sp_pipeline   = ["SELECT", "MODIFY"]
    }
  }

  # Only grant to principals that actually exist. On Free Edition this
  # collapses to the single human identity, and the multi-role matrix above
  # becomes documentation rather than enforcement — which is stated plainly
  # rather than hidden.
  active_principals = var.create_group_grants ? var.principal_names : {}
}

resource "databricks_catalog" "this" {
  name    = var.catalog_name
  comment = "NorthPeak e-commerce lakehouse — ${var.environment}"

  properties = {
    environment = var.environment
    owner       = "data-platform"
    managed_by  = "terraform"
  }

  # Deliberately not force_destroy. A `terraform destroy` that silently drops
  # a catalog full of tables is a footgun with no upside here.
  force_destroy = false
}

resource "databricks_schema" "this" {
  for_each = local.schemas

  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = each.value
  properties = {
    layer      = each.key
    managed_by = "terraform"
  }
}

# Landing zone as a Unity Catalog Volume.
#
# This is the Spike-1 fallback made concrete: if a GCS external location is
# unavailable on Free Edition, files are pushed here via the Files API and
# Auto Loader reads the Volume path instead. Creating it unconditionally costs
# nothing and means the fallback needs no infrastructure change.
resource "databricks_volume" "landing" {
  count = var.create_landing_volume ? 1 : 0

  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.this["bronze"].name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Landing zone fallback when a GCS external location is unavailable"
}

resource "databricks_grants" "catalog" {
  count   = var.create_group_grants ? 1 : 0
  catalog = databricks_catalog.this.name

  dynamic "grant" {
    for_each = local.active_principals
    content {
      principal = grant.value
      # USE_CATALOG alone grants no data access — it only makes the catalog
      # traversable. Table-level SELECT still has to be granted below.
      privileges = ["USE_CATALOG"]
    }
  }
}

resource "databricks_grants" "schemas" {
  for_each = var.create_group_grants ? local.schema_grants : {}

  schema = "${databricks_catalog.this.name}.${databricks_schema.this[each.key].name}"

  dynamic "grant" {
    for_each = {
      for role, privileges in each.value :
      role => privileges if contains(keys(local.active_principals), role)
    }
    content {
      principal  = local.active_principals[grant.key]
      privileges = concat(["USE_SCHEMA"], grant.value)
    }
  }
}
