variable "subscription_id" {
  type        = string
  description = "Azure subscription ID for the runner infrastructure"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group to create for runner infrastructure (must NOT pre-exist)"
}

variable "location" {
  type        = string
  description = "Azure region"
  default     = "canadacentral"
}

variable "github_repo" {
  type        = string
  description = "Full HTTPS URL of the GitHub repository (e.g. https://github.com/owner/repo)"
}

variable "github_runner_token" {
  type        = string
  description = "GitHub Actions runner registration token (expires in 1 hour)"
  sensitive   = true
}

variable "ssh_public_key" {
  type        = string
  description = "Public SSH key (OpenSSH format) authorized for azureuser on the VMSS"
  sensitive   = true
}

variable "runner_labels" {
  type        = list(string)
  description = "Custom labels added to the runner. The default 'self-hosted, Linux, X64' set is added by GitHub automatically."
  default     = ["ivm-bench-azure"]
}

variable "instance_count" {
  type        = number
  description = "Number of VMSS instances (manual scale-out supported)"
  default     = 2
}

variable "instance_sku" {
  type        = string
  description = "VMSS instance SKU"
  default     = "Standard_E32as_v4"
}

variable "os_disk_size_gb" {
  type        = number
  description = "OS disk size in GB"
  # At SF=1000 the ivm-bench workload coexists across one disk:
  #   ~300 GB digen flat files
  #   ~200 GB Delta staging tables
  #   ~300 GB DuckDB-OpenIVM DuckLake data after batch 1
  #   ~800 GB peak DuckDB spill (max_temp_directory_size) during the
  #          fact_market_history IVM refresh in batch 2
  # That sums to ~1.6 TB so 1 TB was not enough — run 25745170653
  # failed batch 2 with "No space left on device" while DuckDB was
  # writing duckdb_temp_storage_DEFAULT-8.tmp. 2 TB leaves ~1.2 TB of
  # headroom after the resident working set, which fits the observed
  # peak spill with margin.
  default     = 2048
}

variable "vm_image" {
  type = object({
    publisher = string
    offer     = string
    sku       = string
    version   = string
  })
  description = "Source image for the VMSS instances"
  default = {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }
}

variable "admin_username" {
  type        = string
  description = "Admin username on each VMSS instance"
  default     = "azureuser"
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to every resource"
  default = {
    project = "ivm-bench"
    purpose = "github-actions-runner"
    managed = "terraform"
  }
}
