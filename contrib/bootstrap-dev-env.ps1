<#

.SYNOPSIS
  Bootstraps a Windows DevBox with WSL.

.NOTES

  - Non-interactive: recreates WSL and clones -Branch (default: this checkout's branch) into ~/ivm-bench.
  - The script uninstalls Docker Desktop as it interferes with WSL2.
  - Must be run as Administrator in PowerShell 7+.

#>

#Requires -RunAsAdministrator

param(
    [string]$Branch = (git -C $PSScriptRoot rev-parse --abbrev-ref HEAD),
    [string]$LinuxUser = ($env:USERNAME.ToLower() -replace '[^a-z0-9_-]', '')
)

code --install-extension ms-vscode-remote.remote-wsl
code --install-extension ms-vscode-remote.remote-containers

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Error "This script requires PowerShell 7+. You are running PowerShell $($PSVersionTable.PSVersion).`nTo launch PowerShell 7 as Administrator:`n  Start Menu > search 'pwsh' > right-click 'PowerShell 7' > 'Run as administrator'"
    exit 1
}

# Git Credential Manager lets git inside WSL reuse this checkout's sign-in; install it if Git for Windows lacks it.
if (-not (git credential-manager --version 2>$null)) {
    winget install -e --id Git.GCM --silent --accept-package-agreements --accept-source-agreements --disable-interactivity
    $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
}
$gcm = @((Get-Command git-credential-manager -ErrorAction SilentlyContinue).Source, "$(Split-Path (Split-Path (git --exec-path)))\bin\git-credential-manager.exe") |
    Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1

# Fail before deleting WSL if the branch can't be cloned without a prompt.
$origin = git -C $PSScriptRoot remote get-url origin
$env:GCM_INTERACTIVE = 'never'
$env:GIT_TERMINAL_PROMPT = '0'
git ls-remote --exit-code --heads $origin $Branch | Out-Null
if ($LASTEXITCODE) { throw "Cannot read $Branch from $origin without a prompt; push it, or run 'git fetch' in $PSScriptRoot once to sign in, then rerun." }

$dockerProcesses = @("Docker Desktop")
foreach ($process in $dockerProcesses) {
    try {
        Get-Process -Name $process -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    } catch {

    }
}

winget uninstall "Docker Desktop" --silent --force --accept-source-agreements 2>$null
$pkg = Get-ChildItem 'HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall','HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall' | Get-ItemProperty | Where-Object { $_.DisplayName -like "Docker Desktop*" };
if ($pkg) {
    $cmd = $pkg.UninstallString
    Start-Process "cmd.exe" -ArgumentList "/c $cmd /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /FORCECLOSEAPPLICATIONS" -Wait -ErrorAction SilentlyContinue
}
Remove-Item -Path "$env:PROGRAMFILES\Docker", "$env:PROGRAMDATA\Docker*", "$env:LOCALAPPDATA\Docker*", "$env:APPDATA\Docker*" -Recurse -Force -ErrorAction SilentlyContinue

# VS Code windows attached to WSL go stale when it is recreated, and VS Code would focus them instead of reconnecting.
Add-Type -AssemblyName UIAutomationClient
[Windows.Automation.AutomationElement]::RootElement.FindAll('Children', [Windows.Automation.Condition]::TrueCondition) |
    Where-Object { $_.Current.Name -match '\[(Dev Container|WSL)\b.*Visual Studio Code' } |
    ForEach-Object { Write-Host "Closing VS Code window: $($_.Current.Name)"; $_.GetCurrentPattern([Windows.Automation.WindowPattern]::Pattern).Close() }

$distros = (wsl -l -q) | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim().Replace("`0", "") } | Where-Object { $_ }
foreach ($distro in $distros) {
    Write-Host "Unregistering WSL distro: $distro"
    wsl --unregister $distro
}

$RECOMMENDED_CORES = 32
$RECOMMENDED_FREE_GB = 512

$memGB=[math]::Floor((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB)
$cpu=[Environment]::ProcessorCount
$swap=[math]::Floor($memGB/4)

if ($cpu -lt $RECOMMENDED_CORES) {
    Write-Host "WARNING: This machine has $cpu cores, which is below the recommended $RECOMMENDED_CORES cores." -ForegroundColor DarkYellow
} else {
    Write-Host "(detected $cpu cores, ${memGB}GB RAM)" -ForegroundColor Green
}

$drive = Get-CimInstance Win32_LogicalDisk -Filter "DriveType=3" |
    Where-Object { $_.VolumeName -notmatch 'Temporary' } |
    Sort-Object -Property FreeSpace -Descending |
    Select-Object -First 1
$wslLocation = "$($drive.DeviceID)\VHD"
$freeGB = [math]::Floor($drive.FreeSpace / 1GB)

if ($freeGB -lt $RECOMMENDED_FREE_GB) {
    Write-Host "WARNING: $wslLocation, the drive with the most free space, has only $freeGB GB free, which is below the recommended $RECOMMENDED_FREE_GB GB; WSL crashes when the disk fills up." -ForegroundColor DarkYellow
} else {
    Write-Host "Using $wslLocation, the drive with the most free space ($freeGB GB free), for the WSL VHD." -ForegroundColor Green
}
New-Item -ItemType Directory -Path $wslLocation -Force | Out-Null

@"
[wsl2]
memory=${memGB}GB
processors=$cpu
swap=${swap}GB
networkingMode=NAT

[general]
instanceIdleTimeout=-1
"@ | Set-Content -Path "$env:USERPROFILE\.wslconfig"

Write-Host "Restarting WSL to apply settings"
wsl --shutdown

Write-Host "Updating WSL (the --location flag requires WSL 2.4.4+)"
wsl --update

Write-Host "Installing Ubuntu to $wslLocation"
wsl --install -d Ubuntu-24.04 --location $wslLocation --no-launch --web-download
if ($LASTEXITCODE) { throw "wsl --install failed with exit code $LASTEXITCODE." }

Write-Host "Creating default Linux user $LinuxUser with passwordless sudo"
wsl -d Ubuntu-24.04 -u root --exec bash -c 'useradd -m -s /bin/bash -G sudo "$1" && echo "$1 ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/$1"' bash $LinuxUser
if ($LASTEXITCODE) { throw "Could not create Linux user $LinuxUser." }
wsl --manage Ubuntu-24.04 --set-default-user $LinuxUser
if ($LASTEXITCODE) { throw "Could not make $LinuxUser the default WSL user." }

Write-Host "Cloning $Branch into ~/ivm-bench"
wsl -d Ubuntu-24.04 --cd ~ --exec bash -c 'git config --global user.name "$1" && git config --global user.email "$2" && git config --global credential.helper "$(printf %q "$(wslpath -u "$5")")" && GIT_TERMINAL_PROMPT=0 git clone --branch "$3" "$4" ~/ivm-bench' bash (git -C $PSScriptRoot config user.name) (git -C $PSScriptRoot config user.email) $Branch $origin $gcm
if ($LASTEXITCODE) { throw "Could not clone $Branch into ~/ivm-bench." }