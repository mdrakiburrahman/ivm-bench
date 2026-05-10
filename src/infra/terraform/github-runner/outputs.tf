output "resource_group_name" {
  description = "Resource group containing all runner infrastructure"
  value       = azurerm_resource_group.rg.name
}

output "location" {
  description = "Azure region"
  value       = azurerm_resource_group.rg.location
}

output "vnet_name" {
  description = "Virtual network name"
  value       = azurerm_virtual_network.vnet.name
}

output "vmss_name" {
  description = "VMSS name (used as --target-resource-id base for SSH via Bastion)"
  value       = azurerm_linux_virtual_machine_scale_set.this.name
}

output "vmss_resource_id" {
  description = "VMSS resource ID"
  value       = azurerm_linux_virtual_machine_scale_set.this.id
}

output "bastion_name" {
  description = "Azure Bastion host name"
  value       = azurerm_bastion_host.this.name
}

output "bastion_resource_id" {
  description = "Azure Bastion resource ID"
  value       = azurerm_bastion_host.this.id
}

output "ssh_via_bastion_hint" {
  description = "Sample command to SSH into the first VMSS instance via Bastion. Replace 0 with the instance index from `az vmss list-instances`."
  value = format(
    "INSTANCE_ID=$(az vmss list-instances -g %s -n %s --query '[0].id' -o tsv) && az network bastion ssh --resource-group %s --name %s --target-resource-id \"$INSTANCE_ID\" --auth-type ssh-key --username %s --ssh-key ~/.ssh/id_ed25519",
    azurerm_resource_group.rg.name,
    azurerm_linux_virtual_machine_scale_set.this.name,
    azurerm_resource_group.rg.name,
    azurerm_bastion_host.this.name,
    var.admin_username,
  )
}

output "tunnel_via_bastion_hint" {
  description = "Sample command to open a local tunnel to the first VMSS instance via Bastion."
  value = format(
    "INSTANCE_ID=$(az vmss list-instances -g %s -n %s --query '[0].id' -o tsv) && az network bastion tunnel --resource-group %s --name %s --target-resource-id \"$INSTANCE_ID\" --resource-port 22 --port 50022",
    azurerm_resource_group.rg.name,
    azurerm_linux_virtual_machine_scale_set.this.name,
    azurerm_resource_group.rg.name,
    azurerm_bastion_host.this.name,
  )
}
