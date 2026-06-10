# Pinned to a merged main commit of KangarooKube/terraform-infrastructure-modules
# from PR https://github.com/KangarooKube/terraform-infrastructure-modules/pull/1.
module "github_runner" {
  source = "git::https://github.com/KangarooKube/terraform-infrastructure-modules.git//modules/github-runner/azure-vmss?ref=d62879b869b9ac927686d20c816b73b19152aa7a"

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
