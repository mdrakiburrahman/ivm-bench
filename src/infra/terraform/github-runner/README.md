# GitHub Self-Hosted Runner on Azure (VMSS + Bastion)

Single-instance Linux VMSS (`Standard_E32as_v4`) behind Azure Bastion.

## Apply / destroy / loop

Make sure `gh auth status` is logged in — the deploy script mints a fresh
runner registration token on every `apply` via `gh api`. Then:

```bash
src/.scripts/deploy-gh-runner.sh apply     # default
src/.scripts/deploy-gh-runner.sh destroy
src/.scripts/deploy-gh-runner.sh plan
```

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
INSTANCE_ID=$(az vmss list-instances -g "$RG" -n vmss-ivm-bench-runner --query '[0].id' -o tsv)

# Interactive shell:
az network bastion ssh -g "$RG" -n bas-ivm-bench \
  --target-resource-id "$INSTANCE_ID" \
  --auth-type ssh-key --username azureuser --ssh-key ~/.ssh/id_ed25519

# Or tunnel to localhost:50022 (for scp/rsync/VS Code):
az network bastion tunnel -g "$RG" -n bas-ivm-bench \
  --target-resource-id "$INSTANCE_ID" \
  --resource-port 22 --port 50022 &
ssh -i ~/.ssh/id_ed25519 azureuser@localhost -p 50022
```

The same commands are echoed by `terraform apply` as `ssh_via_bastion_hint`
and `tunnel_via_bastion_hint`.