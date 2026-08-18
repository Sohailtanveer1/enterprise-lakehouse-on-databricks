variable "databricks_profile" {
  description = "Databricks CLI profile. Auth never appears as a literal in code."
  type        = string
  default     = "DEFAULT"
}

variable "environment" {
  description = "dev | test | prod"
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be dev, test or prod."
  }
}

variable "catalog_name" {
  description = "Unity Catalog catalog for this environment"
  type        = string
}

variable "create_group_grants" {
  description = <<-EOT
    Apply the role-based grant matrix.

    Set false on Free Edition. Unity Catalog groups are ACCOUNT-level objects
    and Free Edition exposes no account console or account API, so the groups
    referenced below cannot exist and every grant would fail on apply.

    Set true on any workspace with account access, after creating the groups
    with terraform/reference-only/.
  EOT
  type        = bool
  default     = false
}

variable "principal_names" {
  description = <<-EOT
    Role -> principal name. Kept as a variable rather than hard-coded so the
    same module serves a Free Edition workspace (where these collapse to one
    identity) and a real account (where they are distinct groups).
  EOT
  type        = map(string)
  default = {
    data_engineer = "northpeak_data_engineers"
    data_analyst  = "northpeak_data_analysts"
    business_user = "northpeak_business_users"
    finance       = "northpeak_finance"
    pii_reader    = "northpeak_pii_readers"
    sp_pipeline   = "northpeak_sp_pipeline"
  }
}

variable "create_landing_volume" {
  description = <<-EOT
    Create a Unity Catalog Volume for the landing zone.

    This is the Spike-1 fallback. If the GCS external location works, the
    Volume is harmless and unused; if it does not, the fallback path already
    exists. Cheap insurance either way.
  EOT
  type        = bool
  default     = true
}
