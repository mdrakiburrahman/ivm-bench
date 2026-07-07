# GitHub self-hosted runner on Azure

Thin wrapper around [`KangarooKube/terraform-infrastructure-modules//modules/github-runner/azure-vmss`](https://github.com/KangarooKube/terraform-infrastructure-modules/tree/main/modules/github-runner/azure-vmss).

```bash
src/.scripts/deploy-gh-runner.sh apply     # ~15 min end-to-end
src/.scripts/deploy-gh-runner.sh destroy
src/.scripts/deploy-gh-runner.sh plan
```

Requires `gh auth status` logged in (fresh runner token is minted per apply).

Every `apply` reimages all VMSS instances so they re-run cloud-init and
re-register with the freshly minted token. The instances reboot simultaneously,
so expect a brief runner outage mid-apply.

## Override VMSS sizing

Set in `.env`:

```bash
TF_VAR_location=eastus2
TF_VAR_instance_sku=Standard_E16as_v4
TF_VAR_instance_count=4
```

## SSH

```bash
RG=$(grep ^TF_RESOURCE_GROUP .env | cut -d= -f2)
INSTANCE_ID=$(az vmss list-instances -g "$RG" -n ivm-bench-vmss-runner --query '[0].id' -o tsv)
az network bastion ssh -g "$RG" -n ivm-bench-bas \
  --target-resource-id "$INSTANCE_ID" \
  --auth-type ssh-key --username azureuser --ssh-key ~/.ssh/id_ed25519
```

Or read `ssh_via_bastion_hint` / `tunnel_via_bastion_hint` from `terraform apply` output.

## Bump pinned module

Edit `?ref=<SHA>` in `main.tf`, then `deploy-gh-runner.sh plan`.
