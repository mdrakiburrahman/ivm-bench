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
  default     = "eastus2"
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
  default     = 1
}

variable "instance_sku" {
  type        = string
  description = "VMSS instance SKU"
  default     = "Standard_E32as_v4"
}

variable "os_disk_size_gb" {
  type        = number
  description = "OS disk size in GB"
  default     = 1024
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
