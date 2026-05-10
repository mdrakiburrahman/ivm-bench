resource "azurerm_public_ip" "bastion" {
  name                = local.bastion_pip
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
  domain_name_label   = "bas-ivm-bench-${local.name_suffix}"

  # Skip the `hybrid-vm-migration-publicip-mustbefirstpartytagged` policy.
  # The policy denies PIPs without ipTags of FirstPartyUsage, but exempts
  # resources tagged with both skip-flag and skip-justification.
  tags = merge(var.tags, {
    "hybrid-vm-migration-publicip-mustbefirstpartytagged-skip-flag"          = "true"
    "hybrid-vm-migration-publicip-mustbefirstpartytagged-skip-justification" = "Azure Bastion ingress for ivm-bench self-hosted GitHub runner — runner VMSS itself has no public IP"
  })
}

resource "azurerm_bastion_host" "this" {
  name                = local.bastion_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  tags                = var.tags

  copy_paste_enabled     = true
  file_copy_enabled      = false
  ip_connect_enabled     = true
  shareable_link_enabled = false
  tunneling_enabled      = true

  ip_configuration {
    name                 = "configuration"
    subnet_id            = azurerm_subnet.bastion.id
    public_ip_address_id = azurerm_public_ip.bastion.id
  }
}
