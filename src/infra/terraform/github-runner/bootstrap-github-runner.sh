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
SWAP_SIZE_GB="${SWAP_SIZE_GB:-64}"

# ----- Base packages ---------------------------------------------------------
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y --no-install-recommends \
  ca-certificates curl gnupg lsb-release jq sudo tar cron logrotate

# ----- Swap file -------------------------------------------------------------
# DuckDB / Spark can briefly spike past RAM during shuffle/spill flushes; the
# GitHub Actions runner agent has been observed losing communication with the
# server when the kernel OOM-kills random processes under memory pressure.
# A generous swap file on the 2 TB OS disk absorbs those spikes.
SWAP_FILE="/var/swap"
if ! swapon --show=NAME --noheadings | grep -q "^${SWAP_FILE}$"; then
  echo "Creating ${SWAP_SIZE_GB} GB swap file at ${SWAP_FILE}..."
  fallocate -l "${SWAP_SIZE_GB}G" "$SWAP_FILE" || \
    dd if=/dev/zero of="$SWAP_FILE" bs=1M count=$((SWAP_SIZE_GB * 1024)) status=progress
  chmod 600 "$SWAP_FILE"
  mkswap "$SWAP_FILE"
  swapon "$SWAP_FILE"
  if ! grep -q "^${SWAP_FILE} " /etc/fstab; then
    echo "${SWAP_FILE} none swap sw 0 0" >> /etc/fstab
  fi
else
  echo "Swap already enabled at ${SWAP_FILE}"
fi

# ----- Sysctls (DuckDB / Spark / runner agent friendly) ----------------------
cat > /etc/sysctl.d/99-ivm-bench.conf <<'EOF'
# Let processes overcommit. DuckDB's documentation explicitly recommends this
# under memory pressure; Spark also benefits during shuffle.
vm.overcommit_memory = 1
# Use swap, but only when truly under memory pressure (we have lots of RAM).
vm.swappiness = 10
# Larger virtual memory area limit (Java/Spark needs this for many regions).
vm.max_map_count = 262144
# More file descriptors / socket backlog so dbt + duckdb + docker can scale.
fs.file-max = 2097152
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 8192
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
EOF
sysctl --system >/dev/null

# ----- ulimits ---------------------------------------------------------------
cat > /etc/security/limits.d/99-ivm-bench.conf <<'EOF'
*               soft    nofile          1048576
*               hard    nofile          1048576
*               soft    nproc           unlimited
*               hard    nproc           unlimited
root            soft    nofile          1048576
root            hard    nofile          1048576
EOF
# Make sure systemd-spawned services see the new limits too.
mkdir -p /etc/systemd/system.conf.d
cat > /etc/systemd/system.conf.d/99-ivm-bench.conf <<'EOF'
[Manager]
DefaultLimitNOFILE=1048576
DefaultLimitNPROC=infinity
EOF
systemctl daemon-reexec || true

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
cat > /etc/docker/daemon.json <<'EOF'
{
  "max-concurrent-downloads": 32,
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "5"
  },
  "default-ulimits": {
    "nofile": {
      "Name": "nofile",
      "Hard": 1048576,
      "Soft": 1048576
    }
  },
  "storage-driver": "overlay2"
}
EOF
systemctl enable --now docker
systemctl restart docker

# ----- Daily docker prune cron ----------------------------------------------
# Even with the per-job prune, image layers from interrupted runs and stale
# build caches accumulate. A daily prune keeps the disk from creeping up
# across the 5 sequential SF=1000 benchmark runs.
cat > /etc/cron.daily/ivm-bench-docker-prune <<'EOF'
#!/bin/bash
# Prune anything not used in the last 24h. -af includes images + build cache.
# --volumes catches anonymous compose volumes from crashed runs.
/usr/bin/docker system prune -af --volumes --filter "until=24h" \
  >> /var/log/ivm-bench-docker-prune.log 2>&1 || true
EOF
chmod 0755 /etc/cron.daily/ivm-bench-docker-prune
systemctl enable --now cron || systemctl enable --now cronie || true

# ----- Log rotation for our own bootstrap + prune logs -----------------------
cat > /etc/logrotate.d/ivm-bench <<'EOF'
/var/log/ivm-bench-*.log {
  weekly
  rotate 4
  compress
  missingok
  notifempty
  copytruncate
}
EOF

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
