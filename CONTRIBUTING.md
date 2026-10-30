# Contributing

## How to use, on a Windows machine by installing WSL

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

1. Clone the repo on Windows, then check out and push the branch you want to work on:

   ```powershell
   git clone https://github.com/mdrakiburrahman/ivm-bench.git
   ```

1. From an **elevated PowerShell 7** prompt in the checkout, install [Task](https://taskfile.dev) and run
   [`contrib/bootstrap-dev-env.yaml`](contrib/bootstrap-dev-env.yaml):

   > ⚠️ This **deletes every WSL distribution** on the machine before recreating `Ubuntu-24.04`, and removes
   > Docker Desktop if you have it installed. Push any work inside WSL first.

   ```powershell
   winget install Task.Task
   $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
   task --taskfile contrib/bootstrap-dev-env.yaml
   ```

   Without any prompts, this recreates WSL on the drive with the most free space and clones your current
   branch into `~/ivm-bench` with this checkout's git identity; Linux > Windows drive commits via `/mnt/c`
   are extremely slow for Spark I/O. It then runs
   [`contrib/bootstrap-dev-env.sh`](contrib/bootstrap-dev-env.sh), which idempotently installs or validates
   `jq`, Docker Engine, the Azure CLI, Terraform and the GitHub CLI, then opens the clone in VS Code.
   `az login` finishes by itself in the browser that is signed in on Windows (single sign-on), and
   `gh auth login` only prompts when a terminal is attached, so run it yourself otherwise. After a restart,
   run `task --taskfile contrib/bootstrap-dev-env.yaml up` instead; it keeps WSL and reopens the clone.

1. Fill up `.env` based on `.env.example` for interaction with cloud infra (e.g. Databricks).

1. Install AI:

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo
   ```
