#!/bin/bash
#
# bootstrap-github-runner.sh — runs once on each new VMSS instance via cloud-init.
#
# Idempotent:
#   - Docker install is skipped if already present.
#   - GitHub Actions runner uses --replace so re-registering with the same name
#     overwrites the previous registration.
#
# Required environment (sourced from /opt/ivm-bench/runner.env):
#   GH_REPO            — full HTTPS URL of the repo
#   GH_RUNNER_TOKEN    — registration token (1-hour TTL)
#   RUNNER_LABELS      — comma-separated custom labels (e.g. "ivm-bench-azure")
#   RUNNER_NAME        — runner name (defaults to ivm-bench-runner-<hostname>)
#   RUNNER_VERSION     — actions-runner release version (e.g. 2.331.0)
#   RUNNER_USER        — local user that owns and runs the runner (e.g. azureuser)
#
set -euo pipefail

LOG_FILE="/var/log/ivm-bench-bootstrap.log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "=== ivm-bench runner bootstrap starting at $(date -u +%FT%TZ) ==="

ENV_FILE="/opt/ivm-bench/runner.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found" >&2
  exit 1
fi
# shellcheck disable=SC1090
set -a
source "$ENV_FILE"
set +a

: "${GH_REPO:?GH_REPO not set}"
: "${GH_RUNNER_TOKEN:?GH_RUNNER_TOKEN not set}"
RUNNER_LABELS="${RUNNER_LABELS:-ivm-bench-azure}"
RUNNER_NAME="${RUNNER_NAME:-$(hostname)}"
RUNNER_VERSION="${RUNNER_VERSION:-2.331.0}"
RUNNER_USER="${RUNNER_USER:-azureuser}"
DOCKER_VERSION="${DOCKER_VERSION:-5:27.5.1-1~ubuntu.24.04~noble}"

# ----- Base packages ---------------------------------------------------------
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y --no-install-recommends \
  ca-certificates curl gnupg lsb-release jq sudo tar

# ----- Docker (idempotent) ---------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker..."
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -q
  apt-get install -y --allow-downgrades \
    docker-ce="${DOCKER_VERSION}" \
    docker-ce-cli="${DOCKER_VERSION}" \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
else
  echo "Docker already installed: $(docker --version)"
fi

mkdir -p /etc/docker
echo '{"max-concurrent-downloads": 32}' > /etc/docker/daemon.json
systemctl enable --now docker
systemctl restart docker

# Ensure the runner user exists and is in the docker group
if ! id "$RUNNER_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$RUNNER_USER"
fi
usermod -aG docker "$RUNNER_USER"

# ----- GitHub Actions runner -------------------------------------------------
RUNNER_HOME="/home/${RUNNER_USER}/actions-runner"
RUNNER_TARBALL="actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
RUNNER_URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${RUNNER_TARBALL}"

mkdir -p "$RUNNER_HOME"
chown -R "$RUNNER_USER:$RUNNER_USER" "$RUNNER_HOME"

if [[ ! -f "${RUNNER_HOME}/config.sh" ]]; then
  echo "Downloading GitHub Actions runner v${RUNNER_VERSION}..."
  sudo -u "$RUNNER_USER" -H bash -c "cd '$RUNNER_HOME' && curl -fsSLo '$RUNNER_TARBALL' '$RUNNER_URL' && tar xzf '$RUNNER_TARBALL' && rm -f '$RUNNER_TARBALL'"
fi

# Stop and uninstall any prior systemd service so we can register cleanly.
SVC_NAME="$(ls /etc/systemd/system/actions.runner.*.service 2>/dev/null | head -1 || true)"
if [[ -n "$SVC_NAME" ]]; then
  echo "Removing prior runner service: $SVC_NAME"
  ( cd "$RUNNER_HOME" && ./svc.sh stop || true )
  ( cd "$RUNNER_HOME" && ./svc.sh uninstall || true )
fi

# Best-effort de-register if a previous .runner config is present.
if [[ -f "${RUNNER_HOME}/.runner" ]]; then
  echo "Removing prior runner registration (best effort)..."
  sudo -u "$RUNNER_USER" -H bash -c "cd '$RUNNER_HOME' && ./config.sh remove --token '$GH_RUNNER_TOKEN'" || true
fi

echo "Registering runner: name=${RUNNER_NAME}, labels=${RUNNER_LABELS}, repo=${GH_REPO}"
sudo -u "$RUNNER_USER" -H bash -c "cd '$RUNNER_HOME' && ./config.sh \
  --unattended \
  --replace \
  --url '$GH_REPO' \
  --token '$GH_RUNNER_TOKEN' \
  --name '$RUNNER_NAME' \
  --labels '$RUNNER_LABELS' \
  --runnergroup default \
  --work _work"

echo "Installing runner as systemd service..."
( cd "$RUNNER_HOME" && ./svc.sh install "$RUNNER_USER" )
( cd "$RUNNER_HOME" && ./svc.sh start )

echo "=== ivm-bench runner bootstrap complete at $(date -u +%FT%TZ) ==="
systemctl status "actions.runner.*" --no-pager || true
