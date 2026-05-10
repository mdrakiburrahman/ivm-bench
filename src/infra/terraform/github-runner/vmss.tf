locals {
  runner_version   = "2.331.0"
  bootstrap_script = file("${path.module}/bootstrap-github-runner.sh")

  cloud_init_rendered = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    github_repo          = var.github_repo
    github_runner_token  = var.github_runner_token
    runner_labels        = join(",", var.runner_labels)
    runner_version       = local.runner_version
    runner_user          = var.admin_username
    bootstrap_script_b64 = base64encode(local.bootstrap_script)
  })
}

resource "azurerm_linux_virtual_machine_scale_set" "this" {
  name                = local.vmss_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = var.instance_sku
  instances           = var.instance_count
  admin_username      = var.admin_username
  tags                = var.tags

  # Automatic: when `terraform apply` changes the model (e.g. new custom_data
  # because gh api minted a fresh registration token), Azure rolls the model
  # out to all instances. For Linux VMSS this re-runs cloud-init via reimage,
  # so the bootstrap re-registers the runner with the latest token.
  upgrade_mode                    = "Automatic"
  single_placement_group          = true
  platform_fault_domain_count     = 1
  disable_password_authentication = true
  overprovision                   = false

  rolling_upgrade_policy {
    max_batch_instance_percent              = 50
    max_unhealthy_instance_percent          = 50
    max_unhealthy_upgraded_instance_percent = 50
    pause_time_between_batches              = "PT2M"
  }

  custom_data = base64encode(local.cloud_init_rendered)

  identity {
    type = "SystemAssigned"
  }

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  source_image_reference {
    publisher = var.vm_image.publisher
    offer     = var.vm_image.offer
    sku       = var.vm_image.sku
    version   = var.vm_image.version
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = var.os_disk_size_gb
  }

  network_interface {
    name    = "nic-runners"
    primary = true

    ip_configuration {
      name      = "internal"
      primary   = true
      subnet_id = azurerm_subnet.runners.id
    }
  }

  boot_diagnostics {
    storage_account_uri = null
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.runners,
    azurerm_bastion_host.this,
  ]
}
