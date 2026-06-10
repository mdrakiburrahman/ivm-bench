# Pinned to KangarooKube/terraform-infrastructure-modules dev branch HEAD.
# Will be repinned to the post-merge main commit before the ivm-bench PR.
module "github_runner" {
  source = "git::https://github.com/KangarooKube/terraform-infrastructure-modules.git//modules/github-runner/azure-vmss?ref=ebb5e6bc4a94fa9ca055b9b8ca84d1454b300692"

  resource_group_name = var.resource_group_name
  location            = var.location
  name_prefix         = "ivm-bench"
  github_repo         = var.github_repo
  github_runner_token = var.github_runner_token
  ssh_public_key      = var.ssh_public_key
  runner_labels       = ["ivm-bench-azure"]
  instance_sku        = var.instance_sku
  instance_count      = var.instance_count
  tags = {
    project = "ivm-bench"
    purpose = "github-actions-runner"
    managed = "terraform"
  }
}
