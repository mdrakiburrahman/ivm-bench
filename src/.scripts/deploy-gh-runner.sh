#!/bin/bash
# ---------------------------------------------------------------------------
# deploy-gh-runner.sh — Wraps `terraform init/apply/destroy/plan` for the
# Azure self-hosted runner stack at src/infra/terraform/github-runner/.
#
# Reads /home/mdrrahman/ivm-bench/.env (or repo-root .env) for:
#   GH_REPO
#   TF_RESOURCE_GROUP, TF_SUBSCRIPTION_ID
#   TF_STATE_STORAGE_ACCOUNT_NAME, TF_STATE_STORAGE_ACCOUNT_CONTAINER,
#   TF_STATE_STORAGE_ACCOUNT_KEY
#
# The runner registration token is auto-minted on each `apply` from the
# already-authenticated `gh` CLI session — no need to copy from the GitHub UI.
#
# Usage:
#   src/.scripts/deploy-gh-runner.sh                # apply (default)
#   src/.scripts/deploy-gh-runner.sh apply
#   src/.scripts/deploy-gh-runner.sh destroy
#   src/.scripts/deploy-gh-runner.sh plan
#   src/.scripts/deploy-gh-runner.sh output         # show terraform outputs
# ---------------------------------------------------------------------------
set -euo pipefail

# Anchor REPO_ROOT to the script's own location (src/.scripts/) rather than
# `git rev-parse --show-toplevel`, which picks up an outer git repo when this
# checkout is nested inside one (e.g. ~/openivm-spark/.temp/ivm-bench).
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." &>/dev/null && pwd)"
cd "$REPO_ROOT"

if [[ ! -f "$REPO_ROOT/.env" ]]; then
  echo "ERROR: .env not found at $REPO_ROOT/.env" >&2
  echo "Copy .env.example to .env and fill in the values." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$REPO_ROOT/.env"
set +a

ACTION="${1:-apply}"

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "ERROR: required env var $name is empty in .env" >&2
    exit 1
  fi
}

require_env GH_REPO
require_env TF_RESOURCE_GROUP
require_env TF_SUBSCRIPTION_ID
require_env TF_STATE_STORAGE_ACCOUNT_NAME
require_env TF_STATE_STORAGE_ACCOUNT_CONTAINER
require_env TF_STATE_STORAGE_ACCOUNT_KEY

if ! command -v terraform >/dev/null 2>&1; then
  echo "ERROR: terraform not found. Run contrib/bootstrap-dev-env.sh first." >&2
  exit 1
fi

if ! command -v az >/dev/null 2>&1; then
  echo "ERROR: az CLI not found. Run contrib/bootstrap-dev-env.sh first." >&2
  exit 1
fi

# `gh` is required only for `apply` (to mint a fresh runner registration token).
# Fail fast before any cloud calls so we don't waste init/plan time.
if [[ "$ACTION" == "apply" ]]; then
  if ! command -v gh >/dev/null 2>&1; then
    echo "ERROR: gh CLI not found." >&2
    echo "Install it: https://github.com/cli/cli/blob/trunk/docs/install_linux.md" >&2
    exit 1
  fi
  if ! gh auth status >/dev/null 2>&1; then
    echo "ERROR: gh is not authenticated. Run:" >&2
    echo "    gh auth login" >&2
    exit 1
  fi
fi

if ! az account show >/dev/null 2>&1; then
  echo "az is not logged in, running az login..."
  az login >/dev/null
fi

az account set --subscription "$TF_SUBSCRIPTION_ID" >/dev/null
echo "Active subscription: $(az account show --query name -o tsv) ($TF_SUBSCRIPTION_ID)"

# --- gh CLI: mint a fresh registration token for `apply` ---------------------
mint_runner_token() {
  local repo_path="${GH_REPO#http://}"
  repo_path="${repo_path#https://}"
  repo_path="${repo_path#github.com/}"
  repo_path="${repo_path%/}"
  repo_path="${repo_path%.git}"

  echo "Minting fresh runner registration token via gh api for ${repo_path}..." >&2
  gh api -X POST "/repos/${repo_path}/actions/runners/registration-token" --jq .token
}

SSH_KEY_PATH="${HOME}/.ssh/id_ed25519"
SSH_PUB_PATH="${SSH_KEY_PATH}.pub"
if [[ ! -f "$SSH_PUB_PATH" ]]; then
  echo "Generating SSH key at $SSH_KEY_PATH (no passphrase)..."
  ssh-keygen -t ed25519 -N "" -f "$SSH_KEY_PATH" -C "ivm-bench-runner@$(hostname)"
fi
SSH_PUBLIC_KEY="$(cat "$SSH_PUB_PATH")"

TF_DIR="$REPO_ROOT/src/infra/terraform/github-runner"
STATE_KEY="github-runner-ivm-bench.tfstate"

# Refresh the upstream module pinned via `git::` source in main.tf before
# init/plan/apply. Cheap when the ref is a commit SHA (cached after first
# fetch); ensures upstream changes are picked up if the ref is moved.
echo "=== terraform get -update (refresh pinned KangarooKube module) ==="
terraform -chdir="$TF_DIR" get -update

echo "=== terraform init (backend: $TF_STATE_STORAGE_ACCOUNT_NAME / $TF_STATE_STORAGE_ACCOUNT_CONTAINER / $STATE_KEY) ==="
terraform -chdir="$TF_DIR" init -reconfigure \
  -backend-config="storage_account_name=${TF_STATE_STORAGE_ACCOUNT_NAME}" \
  -backend-config="container_name=${TF_STATE_STORAGE_ACCOUNT_CONTAINER}" \
  -backend-config="key=${STATE_KEY}" \
  -backend-config="access_key=${TF_STATE_STORAGE_ACCOUNT_KEY}"

# Mint a fresh token only for `apply` (destroy/plan don't need to talk to GitHub,
# but Terraform still requires the variable to be set — pass a placeholder).
RUNNER_TOKEN="unused"
if [[ "$ACTION" == "apply" ]]; then
  RUNNER_TOKEN="$(mint_runner_token)"
fi

TF_VARS=(
  -var "subscription_id=${TF_SUBSCRIPTION_ID}"
  -var "resource_group_name=${TF_RESOURCE_GROUP}"
  -var "github_repo=${GH_REPO}"
  -var "github_runner_token=${RUNNER_TOKEN}"
  -var "ssh_public_key=${SSH_PUBLIC_KEY}"
)

case "$ACTION" in
  apply)
    echo "=== terraform apply ==="
    terraform -chdir="$TF_DIR" apply -auto-approve "${TF_VARS[@]}"
    echo ""
    echo "=== terraform outputs ==="
    terraform -chdir="$TF_DIR" output
    echo ""
    echo "=== Next steps ==="
    echo "  1. Wait ~3-5 min for cloud-init to finish, then check the runner here:"
    echo "       ${GH_REPO}/settings/actions/runners"
    echo "  2. SSH (copy/paste from the ssh_via_bastion_hint output above)."
    ;;
  destroy)
    echo "=== terraform destroy ==="
    terraform -chdir="$TF_DIR" destroy -auto-approve "${TF_VARS[@]}"
    ;;
  plan)
    terraform -chdir="$TF_DIR" plan "${TF_VARS[@]}"
    ;;
  output|outputs)
    terraform -chdir="$TF_DIR" output
    ;;
  *)
    echo "Usage: $0 [apply|destroy|plan|output]" >&2
    exit 2
    ;;
esac
