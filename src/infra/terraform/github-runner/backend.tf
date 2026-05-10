terraform {
  backend "azurerm" {
    # Partial backend — populated by deploy-gh-runner.sh from .env:
    #   storage_account_name, container_name, key, access_key
    #
    # No `resource_group_name` is required when `access_key` is supplied,
    # which keeps the state storage account decoupled from TF_RESOURCE_GROUP.
  }
}
