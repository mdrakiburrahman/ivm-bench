# GitHub Self-Hosted Runner on Azure (VMSS + Bastion)

Thin Terraform wrapper around the upstream
[`github-runner/azure-vmss`](https://github.com/KangarooKube/terraform-infrastructure-modules/tree/main/modules/github-runner/azure-vmss)
module. This directory just pins to a specific commit of that module and
passes through the `ivm-bench`-specific naming, labels, and tags. All the
actual Azure resources (RG, VNet, NSG, Bastion, VMSS, cloud-init,
bootstrap script) live in the upstream module — see its README for the
full resource list and inputs.

Single-instance Linux VMSS (`Standard_E32as_v4`) behind Azure Bastion,
auto-registered as a runner against the repo named in `GH_REPO`.

## Apply / destroy / loop

Make sure `gh auth status` is logged in — the deploy script mints a fresh
runner registration token on every `apply` via `gh api`. Then:

```bash
src/.scripts/deploy-gh-runner.sh apply     # default
src/.scripts/deploy-gh-runner.sh destroy
src/.scripts/deploy-gh-runner.sh plan
```

## Overriding VMSS sizing without touching the module

[`variables.tf`](./variables.tf) exposes `instance_sku`, `instance_count`,
and `location` as pass-through inputs to the module. Override any of them
by setting the corresponding `TF_VAR_*` environment variable in `.env`
(the deploy script auto-exports everything from `.env`):

```bash
# .env
TF_VAR_location=eastus2
TF_VAR_instance_sku=Standard_E16as_v4
TF_VAR_instance_count=4
```

No code changes required. `terraform plan` will pick the new values up on
the next `deploy-gh-runner.sh` invocation.

Estimated wall time:

|                                |                                    |
| ------------------------------ | ---------------------------------- |
| `apply` (initial)              | ~10–12 min (Bastion ~7m, VMSS ~3m) |
| Cloud-init runner registration | ~3–5 min after VMSS is `Running`   |
| `destroy`                      | ~5–7 min                           |
| `apply` (no diff)              | ~30 s                              |

## SSH from this host

```bash
RG=$(grep ^TF_RESOURCE_GROUP .env | cut -d= -f2)
INSTANCE_ID=$(az vmss list-instances -g "$RG" -n ivm-bench-vmss-runner --query '[0].id' -o tsv)

# Interactive shell:
az network bastion ssh -g "$RG" -n ivm-bench-bas \
  --target-resource-id "$INSTANCE_ID" \
  --auth-type ssh-key --username azureuser --ssh-key ~/.ssh/id_ed25519

# Or tunnel to localhost:50022 (for scp/rsync/VS Code):
az network bastion tunnel -g "$RG" -n ivm-bench-bas \
  --target-resource-id "$INSTANCE_ID" \
  --resource-port 22 --port 50022 &
ssh -i ~/.ssh/id_ed25519 azureuser@localhost -p 50022
```

The same commands are echoed by `terraform apply` as `ssh_via_bastion_hint`
and `tunnel_via_bastion_hint`.

## Updating the pinned module

The `?ref=` in [`main.tf`](./main.tf) is a commit SHA in the upstream repo.
To pull in upstream changes:

1. Pick the new SHA (e.g. `git ls-remote https://github.com/KangarooKube/terraform-infrastructure-modules.git main`).
2. Update the `?ref=...` value in `main.tf`.
3. Run `src/.scripts/deploy-gh-runner.sh plan` — the deploy script runs
   `terraform get -update` before `init`, so the new module version is
   fetched automatically.
