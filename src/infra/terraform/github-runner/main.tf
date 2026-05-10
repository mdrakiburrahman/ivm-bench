resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
  numeric = true
}

locals {
  name_suffix    = random_string.suffix.result
  vnet_name      = "vnet-ivm-bench-runner"
  runners_subnet = "snet-runners"
  bastion_subnet = "AzureBastionSubnet"
  nsg_name       = "nsg-ivm-bench-runners"
  bastion_name   = "bas-ivm-bench"
  bastion_pip    = "pip-bas-ivm-bench"
  vmss_name      = "vmss-ivm-bench-runner"
}
