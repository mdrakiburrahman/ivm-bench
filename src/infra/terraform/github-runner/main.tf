module "github_runner" {
  source = "git::https://github.com/KangarooKube/terraform-infrastructure-modules.git//modules/github-runner/azure-vmss?ref=5cb723be64457a458965b7e33fd2d6efdd78c5e9"

  resource_group_name      = var.resource_group_name
  location                 = var.location
  name_prefix              = "ivm-bench"
  github_repo              = var.github_repo
  github_runner_token      = var.github_runner_token
  ssh_public_key           = var.ssh_public_key
  runner_labels            = ["ivm-bench-azure"]
  instance_sku             = var.instance_sku
  instance_count           = var.instance_count
  health_extension_enabled = true
  tags = {
    project = "ivm-bench"
    purpose = "github-actions-runner"
    managed = "terraform"
  }
}

# A fresh runner token is minted each apply, so this trigger changes every apply
# and forces an all-instance reimage (re-runs cloud-init -> re-registers with the
# new token). plan/destroy pass a stable placeholder token, so they don't churn.
resource "terraform_data" "reimage_runners" {
  triggers_replace = var.github_runner_token

  provisioner "local-exec" {
    command = "az vmss reimage --subscription ${var.subscription_id} --resource-group ${module.github_runner.resource_group_name} --name ${module.github_runner.vmss_name}"
  }

  depends_on = [module.github_runner]
}
