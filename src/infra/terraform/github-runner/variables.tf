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
  description = "Azure region. Defaults to the module default (`canadacentral`)."
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

variable "instance_sku" {
  type        = string
  description = "VMSS instance SKU (e.g. Standard_E32as_v4, Standard_E16as_v4). Overrides the module default."
  default     = "Standard_E32as_v4"
}

variable "instance_count" {
  type        = number
  description = "Number of VMSS instances. The VMSS is not autoscaled, so this is both the current and the max node count."
  default     = 2
}
