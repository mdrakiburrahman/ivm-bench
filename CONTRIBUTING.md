# Contributing

## How to use, on a Windows machine by installing WSL

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

1. Get a fresh new WSL machine up:

   > ⚠️ Warning: this removes Docker Desktop if you have it installed

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\contrib\bootstrap-dev-env.ps1"
   ```

1. Clone the repo, and open VSCode in it:

    > ⚠️ Important: We use WSL in `~/` because Linux > Windows drive commits via `/mnt/c` is extremely slow for Spark I/O.
    > You can technically run the Devcontainer using Windows Docker Desktop, but the I/O experience is slow and poor.

   ```bash
   cd ~/

   read -p "Enter your name (e.g. 'FirstName LastName'): " user_name
   read -p "Enter your GitHub email (e.g. 'your-email@blah.com'): " user_email
    
   git clone https://github.com/mdrakiburrahman/ivm-bench.git

   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   cd ivm-bench/
   git pull origin

   code .
   ```

1. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   echo "$USER ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/$USER
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```