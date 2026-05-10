#!/bin/bash
#
#
#       Bootstraps a Linux Devbox host for the VS Code devcontainer idempotently.
#       If your Devbox restarts, rerun this script.
#
# ---------------------------------------------------------------------------------------
#

DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"
TERRAFORM_VERSION="1.9.8"

PACKAGES=""
if ! command -v jq &> /dev/null; then PACKAGES="$PACKAGES jq"; fi
if [ ! -z "$PACKAGES" ]; then
    echo "Packages $PACKAGES not found - installing..."
    sudo apt-get update 2>&1 > /dev/null
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $PACKAGES 2>&1 > /dev/null
fi

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
else
  echo "docker is already installed."
fi

sudo mkdir -p /etc/docker
echo '{"max-concurrent-downloads": 32}' | sudo tee /etc/docker/daemon.json > /dev/null

echo "docker is installed, restarting..."
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
docker container ls
docker ps -q | xargs -r docker kill

# Remove Windows paths from PATH to avoid using Windows az CLI
# This allows us to mount ~/.azure from WSL.
#
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
AZ_PATH=$(which az 2>/dev/null)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi

az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

if ! [ -x "$(command -v terraform)" ]; then
  echo "terraform is not installed on your devbox, installing v${TERRAFORM_VERSION}..."
  sudo apt-get update -q
  sudo apt-get install -y gnupg software-properties-common curl
  curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
    | sudo tee /etc/apt/sources.list.d/hashicorp.list > /dev/null
  sudo apt-get update -q
  sudo apt-get install -y --allow-downgrades "terraform=${TERRAFORM_VERSION}-*"
else
  echo "terraform is already installed."
fi

if ! [ -x "$(command -v gh)" ]; then
  (type -p wget >/dev/null || (sudo apt update && sudo apt install wget -y)) \
    && sudo mkdir -p -m 755 /etc/apt/keyrings \
    && out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    && cat $out | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
    && sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && sudo mkdir -p -m 755 /etc/apt/sources.list.d \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && sudo apt update \
    && sudo apt install gh -y
else
  echo "GitHub CLI is already installed."
fi

if ! gh auth status >/dev/null 2>&1; then
    echo "gh is not logged in, running 'gh auth login'..."
    gh auth login
else
    echo "GitHub CLI is already logged in as: $(gh api user --jq .login 2>/dev/null || echo unknown)"
fi

echo "Docker: $(docker --version)"
echo "Terraform: $(terraform -version | head -1)"
echo "GitHub CLI: $(gh --version | head -1)"
echo "Azure CLI: $(az --version | head -1)"