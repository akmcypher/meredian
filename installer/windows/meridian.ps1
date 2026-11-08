<#
.SYNOPSIS
  Meridian Community launcher for Windows 11 (x64 and ARM64).

.DESCRIPTION
  Wraps scripts/self-host.mjs, the single-machine preview installer. Docker Desktop must be
  installed and running. Node.js, Bun and the Supabase CLI are used from PATH when the
  required version is there; otherwise pinned releases are downloaded, verified against the
  SHA-256 hashes in this file, and kept under %LOCALAPPDATA%\Meridian\tools. Nothing is
  installed system-wide and PATH is not changed.

  Installation state and generated credentials stay in .meridian\ next to this script.
  This is a local preview installation, not a hosted or certified deployment.

.EXAMPLE
  .\meridian.cmd install
  .\meridian.cmd start
  .\meridian.cmd status
  .\meridian.cmd stop
  .\meridian.cmd uninstall
#>
[CmdletBinding(PositionalBinding = $false)]
param(
  [Parameter(Position = 0)] [string] $Command = 'help',
  [Parameter(Position = 1, ValueFromRemainingArguments = $true)] [string[]] $Arguments = @(),
  [int] $Port = 0,
  [int] $BackendPort = 0,
  [switch] $NoBrowser,
  [switch] $Force,
  [switch] $RemoveTools,
  [int] $TimeoutSeconds = 300
)

Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

# ---------------------------------------------------------------------------------------------
# Pinned toolchain. Versions match .github CI and QUICKSTART.md; hashes come from each project's
# published checksum file for that exact release asset.
# ---------------------------------------------------------------------------------------------
$Pins = @{
  node = @{
    version = '26.13.0'
    x64   = @{ url = 'https://nodejs.org/dist/v26.13.0/node-v26.13.0-win-x64.zip';   sha256 = 'dcc60d59a0746999f99064a01c020445d54d84228c691cdbf25b49ac3b59150a'; exe = 'node-v26.13.0-win-x64\node.exe' }
    arm64 = @{ url = 'https://nodejs.org/dist/v26.13.0/node-v26.13.0-win-arm64.zip'; sha256 = 'a9427139595156ef484e1fb08957676e09d4232368dd1743a2a9f93f3d241b14'; exe = 'node-v26.13.0-win-arm64\node.exe' }
  }
  bun = @{
    version = '1.4.4'
    x64   = @{ url = 'https://github.com/oven-sh/bun/releases/download/bun-v1.4.4/bun-windows-x64-baseline.zip'; sha256 = '0f20ce6674d4f2dbcc8c5d59fdf99f7ba45f8c3b367e0f5781cbfa30a1c78d99'; exe = 'bun-windows-x64-baseline\bun.exe' }
    arm64 = @{ url = 'https://github.com/oven-sh/bun/releases/download/bun-v1.4.4/bun-windows-aarch64.zip';     sha256 = '2d57a0eec0cb44d95c918c3f9734f60455a1306ae5085d76fc68f44e3ee30d09'; exe = 'bun-windows-aarch64\bun.exe' }
  }
  supabase = @{
    version = '2.122.0'
    x64   = @{ url = 'https://github.com/supabase/cli/releases/download/v2.122.0/supabase_2.122.0_windows_amd64.zip'; sha256 = '60031fd5bf050fc12a0c6aef68997600d143870f944bf13109c7765dca329af8'; exe = 'supabase.exe' }
    arm64 = @{ url = 'https://github.com/supabase/cli/releases/download/v2.122.0/supabase_2.122.0_windows_arm64.zip'; sha256 = 'dda0185f8b56a5c35c36fba7c08dbe6683b0f99180df7018ea2b863b73fb3775'; exe = 'supabase.exe' }
  }
}

# The launcher sits at the package root in the release zip, and at installer\windows\ in a
# source checkout.
$Root = $PSScriptRoot
if (-not (Test-Path (Join-Path $Root 'scripts\self-host.mjs'))) {
  $Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}
$SelfHost = Join-Path $Root 'scripts\self-host.mjs'
if (-not (Test-Path $SelfHost)) { throw "scripts\self-host.mjs not found next to $PSCommandPath." }
$State = Join-Path $Root '.meridian'
$Manifest = Join-Path $State 'installation.json'
$PidFile = Join-Path $State 'app.pid'
$LogDir = Join-Path $State 'logs'
$ToolsDir = Join-Path $env:LOCALAPPDATA 'Meridian\tools'

function Write-Step([string] $Text) { Write-Host "==> $Text" -ForegroundColor Cyan }
function Write-Note([string] $Text) { Write-Host "    $Text" }
function Fail([string] $Text) { Write-Host "error: $Text" -ForegroundColor Red; exit 1 }

function Get-NativeArch {
  # The registry reports the machine architecture even when this PowerShell runs emulated.
  $value = (Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment').PROCESSOR_ARCHITECTURE
  switch ($value) {
    'ARM64' { 'arm64' }
    'AMD64' { 'x64' }
    default { Fail "Unsupported processor architecture '$value'. Meridian supports Windows x64 and ARM64." }
  }
}

function Get-ToolVersion([string] $Exe) {
  # Windows PowerShell 5.1 turns redirected native stderr into a terminating error under Stop.
  $ErrorActionPreference = 'Continue'
  try {
    $output = & $Exe --version 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return (($output | Select-Object -First 1) -replace '^v', '').Trim()
  } catch { return $null }
}

function Test-VersionOk([string] $Name, [string] $Version) {
  if (-not $Version) { return $false }
  switch ($Name) {
    'node'     { return ([int]($Version.Split('.')[0]) -ge 26) }
    'bun'      { return $Version.StartsWith('1.4.') }
    'supabase' { return $Version -eq $Pins.supabase.version }
  }
}

function Install-PinnedTool([string] $Name, [string] $Arch) {
  $pin = $Pins[$Name][$Arch]
  $target = Join-Path $ToolsDir "$Name-$($Pins[$Name].version)-$Arch"
  $exe = Join-Path $target $pin.exe
  if (Test-Path $exe) { return $exe }
  New-Item -ItemType Directory -Force $ToolsDir | Out-Null
  $zip = Join-Path $ToolsDir "$Name-$($Pins[$Name].version)-$Arch.download.zip"
  Write-Note "downloading $Name $($Pins[$Name].version) ($Arch) from $($pin.url)"
  Invoke-WebRequest -UseBasicParsing -Uri $pin.url -OutFile $zip
  $actual = (Get-FileHash -Algorithm SHA256 $zip).Hash.ToLowerInvariant()
  if ($actual -ne $pin.sha256) {
    Remove-Item -Force $zip
    Fail "$Name download failed SHA-256 verification (expected $($pin.sha256), got $actual). Nothing was installed."
  }
  $staging = "$target.partial"
  if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
  Expand-Archive -Path $zip -DestinationPath $staging
  Remove-Item -Force $zip
  Move-Item $staging $target
  if (-not (Test-Path $exe)) { Fail "$Name archive did not contain $($pin.exe)." }
  return $exe
}

# Resolves node, bun and supabase, downloading pinned copies when allowed, and puts their
# directories first on PATH for this process and its children (self-host.mjs calls them by name).
function Resolve-Toolchain([bool] $AllowDownload) {
  $arch = Get-NativeArch
  $found = [ordered]@{}
  foreach ($name in 'node', 'bun', 'supabase') {
    $exe = $null
    $command = Get-Command "$name.exe" -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($command -and (Test-VersionOk $name (Get-ToolVersion $command.Source))) { $exe = $command.Source }
    if (-not $exe) {
      $pin = $Pins[$name][$arch]
      $cached = Join-Path (Join-Path $ToolsDir "$name-$($Pins[$name].version)-$arch") $pin.exe
      if (Test-Path $cached) { $exe = $cached }
      elseif ($AllowDownload) { $exe = Install-PinnedTool $name $arch }
    }
    $found[$name] = $exe
  }
  $dirs = @($found.Values | Where-Object { $_ } | ForEach-Object { Split-Path -Parent $_ } | Select-Object -Unique)
  if ($dirs.Count) { $env:PATH = (($dirs -join ';') + ';' + $env:PATH) }
  return $found
}

function Test-Docker([bool] $Quiet = $false) {
  # Windows PowerShell 5.1 turns redirected native stderr into a terminating error under Stop.
  $ErrorActionPreference = 'Continue'
  $docker = Get-Command 'docker.exe' -CommandType Application -ErrorAction SilentlyContinue
  if (-not $docker) {
    if ($Quiet) { return 'missing' }
    Write-Host ''
    Write-Host 'Docker Desktop is required and was not found.' -ForegroundColor Yellow
    Write-Note 'Install it (x64 and ARM64 builds are both available), then start it once:'
    Write-Note '  winget install --exact --id Docker.DockerDesktop'
    Write-Note '  or download from https://docs.docker.com/desktop/setup/install/windows-install/'
    Write-Note 'Docker Desktop uses WSL 2; accept its prompt to enable it, restart if asked, and wait'
    Write-Note 'until Docker Desktop reports "Engine running". Then run this command again.'
    exit 1
  }
  $os = & $docker.Source version --format '{{.Server.Os}}' 2>$null
  if ($LASTEXITCODE -ne 0 -or -not $os) {
    if ($Quiet) { return 'stopped' }
    Fail 'Docker Desktop is installed but its engine is not running. Start Docker Desktop, wait for "Engine running", and retry.'
  }
  if ("$os".Trim() -ne 'linux') {
    if ($Quiet) { return 'windows-containers' }
    Fail 'Docker is in Windows containers mode. Right-click the Docker tray icon and choose "Switch to Linux containers".'
  }
  return 'running'
}

# Docker Desktop for Windows publishes container ports through its own port exposer, which ignores
# the per-network loopback option self-host.mjs sets (and the engine "ip" setting). Unless Docker
# Desktop is set to bind to localhost, the backend is reachable from the network and self-host.mjs
# refuses it; this explains the one-time fix before anything is created.
function Get-DockerPortBinding {
  $store = Join-Path $env:APPDATA 'Docker\settings-store.json'
  if (-not (Test-Path $store)) { return $null }
  try { return (Get-Content -Raw $store | ConvertFrom-Json).PortBindingBehavior } catch { return $null }
}

function Test-DockerLoopbackDefault {
  if ((Get-DockerPortBinding) -in 'default-local-port-binding', 'local-only-port-binding') { return }
  Write-Host ''
  Write-Host 'Docker Desktop must publish container ports on localhost only, so the local database and' -ForegroundColor Yellow
  Write-Host 'backend are not reachable from your network. It is not set up that way yet.' -ForegroundColor Yellow
  Write-Note 'Docker Desktop > Settings > Resources > Network > Port binding behavior:'
  Write-Note '  choose "Localhost by default", then "Apply & restart".'
  Write-Note 'This applies to every container that does not name an address. Restarting Docker Desktop'
  Write-Note 'stops running containers; ones with a restart policy come back.'
  exit 1
}

function Get-BusyPorts {
  # Windows PowerShell 5.1 turns redirected native stderr into a terminating error under Stop.
  $ErrorActionPreference = 'Continue'
  $ports = @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | ForEach-Object { [int]$_.LocalPort })
  $published = & docker ps --format '{{.Ports}}' 2>$null
  foreach ($match in [regex]::Matches(($published -join ' '), ':(\d+)->')) { $ports += [int]$match.Groups[1].Value }
  return $ports | Sort-Object -Unique
}

function Get-Installation {
  if (-not (Test-Path $Manifest)) { return $null }
  return Get-Content -Raw $Manifest | ConvertFrom-Json
}

function Invoke-SelfHost([string[]] $SelfHostArgs) {
  # Out-Host keeps the child's output on screen instead of in this function's return value.
  & node $SelfHost @SelfHostArgs | Out-Host
  return $LASTEXITCODE
}

# Returns the running app process recorded in app.pid, verifying it is still our node process
# (PIDs are reused by Windows).
function Get-AppProcess {
  if (-not (Test-Path $PidFile)) { return $null }
  $id = [int](Get-Content -Raw $PidFile).Trim()
  $process = Get-CimInstance Win32_Process -Filter "ProcessId = $id" -ErrorAction SilentlyContinue
  if ($process -and $process.CommandLine -and ($process.CommandLine.Contains('run-app.cmd') -or
      ($process.CommandLine.Contains('self-host.mjs') -and $process.CommandLine.Contains('start')))) {
    return $process
  }
  Remove-Item -Force $PidFile -ErrorAction SilentlyContinue
  return $null
}

function Test-AppHttp([int] $AppPort) {
  try {
    $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 -Uri "http://localhost:$AppPort/auth"
    return ($response.StatusCode -eq 200)
  } catch { return $false }
}

function Stop-App {
  # Windows PowerShell 5.1 turns redirected native stderr into a terminating error under Stop.
  $ErrorActionPreference = 'Continue'
  $process = Get-AppProcess
  if (-not $process) { return $false }
  # /T ends the web server child that self-host.mjs spawned.
  & taskkill.exe /PID $process.ProcessId /T /F *> $null
  Remove-Item -Force $PidFile -ErrorAction SilentlyContinue
  return $true
}

function Show-Help {
  @'
Meridian Community launcher (local preview installation)

Usage: .\meridian.cmd <command> [options]

  install [-Port 8080] [-BackendPort 58321]
                   Check Docker, get Node/Bun/Supabase CLI, create the local backend, build the app.
                   Safe to re-run; it resumes with the same installation and ports.
  start [-NoBrowser]
                   Start the backend and the web app in the background, then open it.
  stop             Stop the web app and the backend. Data is kept.
  status           Show tools, Docker, the installation and app health.
  logs             Show the end of the web app log.
  uninstall [-Force] [-RemoveTools]
                   Stop everything and permanently delete this installation's database,
                   storage and generated files. Back up first if you need the data.
  doctor           Check prerequisites without downloading anything.

  bootstrap-admin <verified-email>, backup, restore <dir>, upgrade, check
                   Passed through to scripts\self-host.mjs (stop the app before backup,
                   restore and upgrade).
'@ | Write-Host
}

# ---------------------------------------------------------------------------------------------
switch ($Command.ToLowerInvariant()) {
  { $_ -in 'help', '-h', '--help', '/?' } { Show-Help; exit 0 }

  'doctor' {
    $arch = Get-NativeArch
    Write-Step "Windows $([Environment]::OSVersion.Version) ($arch)"
    $tools = Resolve-Toolchain $false
    foreach ($name in $tools.Keys) {
      if ($tools[$name]) { Write-Note ("{0,-9} {1}  {2}" -f $name, (Get-ToolVersion $tools[$name]), $tools[$name]) }
      else { Write-Note ("{0,-9} not found; install will download {1} {2}" -f $name, $name, $Pins[$name].version) }
    }
    Write-Note ("docker    {0}" -f (Test-Docker $true))
    $binding = Get-DockerPortBinding
    Write-Note ("ports     {0}" -f $(if ($binding -in 'default-local-port-binding', 'local-only-port-binding') { "$binding (ok)" } else { 'open to the network; install will explain the Docker Desktop setting' }))
    exit 0
  }

  'install' {
    Write-Step 'Checking Docker Desktop'
    Test-Docker | Out-Null
    Test-DockerLoopbackDefault
    Write-Step 'Locating Node.js, Bun and the Supabase CLI'
    $tools = Resolve-Toolchain $true
    foreach ($name in $tools.Keys) { Write-Note ("{0,-9} {1}  {2}" -f $name, (Get-ToolVersion $tools[$name]), $tools[$name]) }
    if ($Port) { $env:MERIDIAN_PORT = "$Port" }
    if ($BackendPort) { $env:MERIDIAN_BACKEND_PORT = "$BackendPort" }
    if (-not (Get-Installation)) {
      # A resumed install owns its ports already; a new one must not collide with another local
      # backend (Docker-published ports do not always show up as Windows listeners).
      $appPort = if ($Port) { $Port } else { 8080 }
      $apiPort = if ($BackendPort) { $BackendPort } else { 58321 }
      $busy = @(Get-BusyPorts | Where-Object { $_ -eq $appPort -or ($_ -ge $apiPort -and $_ -le $apiPort + 4) })
      if ($busy.Count) {
        Fail ("Port(s) $($busy -join ', ') are already in use. Choose free ones, for example:" +
          "`n  .\meridian.cmd install -Port 8090 -BackendPort 59421   (backend uses BackendPort to BackendPort+4)")
      }
    }
    Write-Step 'Installing (first run downloads backend images and dependencies; this can take 10+ minutes)'
    $code = Invoke-SelfHost @('install')
    if ($code -ne 0) {
      Write-Note 'If the error says ports are not loopback-only, set Docker Desktop port binding to "Localhost by default"'
      Write-Note '(Settings > Resources > Network), Apply & restart, then run .\meridian.cmd uninstall -Force and install again.'
      Fail 'Install did not finish. Fix the reported problem and run install again; state is preserved in .meridian\.'
    }
    Write-Host ''
    Write-Host 'Installed. Next: .\meridian.cmd start' -ForegroundColor Green
    exit 0
  }

  'start' {
    $installed = Get-Installation
    if (-not $installed) { Fail 'Not installed yet. Run: .\meridian.cmd install' }
    if (Get-AppProcess) { Write-Note "Already running: http://localhost:$($installed.port)"; exit 0 }
    Test-Docker | Out-Null
    Resolve-Toolchain $false | Out-Null
    if (-not (Test-Path (Join-Path $Root '.output\server\index.mjs'))) { Fail 'The app is not built. Run: .\meridian.cmd install' }
    New-Item -ItemType Directory -Force $LogDir | Out-Null
    $out = Join-Path $LogDir 'app.log'
    $err = Join-Path $LogDir 'app.err.log'
    Write-Step 'Starting backend and web app'
    $node = (Get-Command node.exe -CommandType Application | Select-Object -First 1).Source
    # Start-Process without redirection goes through ShellExecute, which does not hand this
    # console's handles to the app, so the launcher returns even when its output is piped or
    # captured. The runner script does the log redirection instead.
    $runner = Join-Path $State 'run-app.cmd'
    Set-Content -Path $runner -Encoding ascii -Value @(
      '@echo off',
      "cd /d `"$Root`"",
      "`"$node`" `"$SelfHost`" start > `"$out`" 2> `"$err`""
    )
    $process = Start-Process -FilePath 'cmd.exe' -ArgumentList "/d /c `"`"$runner`"`"" -WorkingDirectory $Root `
      -WindowStyle Hidden -PassThru
    Set-Content -Path $PidFile -Value $process.Id -Encoding ascii
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
      if ($process.HasExited) {
        Remove-Item -Force $PidFile -ErrorAction SilentlyContinue
        Get-Content -Tail 20 $err, $out -ErrorAction SilentlyContinue | Write-Host
        Fail "The app exited during startup (logs: $LogDir)."
      }
      if (Test-AppHttp $installed.port) { break }
      Start-Sleep -Seconds 2
    }
    if (-not (Test-AppHttp $installed.port)) { Fail "The app did not answer within $TimeoutSeconds s; see .\meridian.cmd logs. It is still starting in the background." }
    $url = "http://localhost:$($installed.port)"
    Write-Host ''
    Write-Host "Meridian is running: $url" -ForegroundColor Green
    Write-Note "Verification mail (Mailpit): http://localhost:$($installed.apiPort + 3)"
    Write-Note 'Sign up, confirm the email in Mailpit, then make that account the platform administrator:'
    Write-Note '  .\meridian.cmd bootstrap-admin you@example.com'
    Write-Note 'Stop with: .\meridian.cmd stop'
    if (-not $NoBrowser) { Start-Process $url }
    exit 0
  }

  'stop' {
    $installed = Get-Installation
    if (-not $installed) { Fail 'Not installed.' }
    if (Stop-App) { Write-Step 'Web app stopped' } else { Write-Note 'Web app was not running.' }
    if ((Test-Docker $true) -ne 'running') { Write-Note 'Docker is not running; backend containers are already stopped.'; exit 0 }
    Resolve-Toolchain $false | Out-Null
    Write-Step 'Stopping backend (data is kept)'
    $code = Invoke-SelfHost @('stop')
    exit $code
  }

  'status' {
    $installed = Get-Installation
    $tools = Resolve-Toolchain $false
    Write-Step 'Tools'
    foreach ($name in $tools.Keys) { Write-Note ("{0,-9} {1}" -f $name, $(if ($tools[$name]) { $tools[$name] } else { 'not installed' })) }
    Write-Note ("docker    {0}" -f (Test-Docker $true))
    if (-not $installed) { Write-Step 'Not installed (run .\meridian.cmd install)'; exit 0 }
    Write-Step "Installation $($installed.id)"
    Write-Note "folder    $Root"
    Write-Note "app       http://localhost:$($installed.port)"
    Write-Note "backend   http://127.0.0.1:$($installed.apiPort)"
    $process = Get-AppProcess
    Write-Note ("process   {0}" -f $(if ($process) { "running (pid $($process.ProcessId))" } else { 'stopped' }))
    if ($process) {
      Write-Step 'Health'
      $code = Invoke-SelfHost @('check')
      exit $code
    }
    exit 0
  }

  'logs' {
    foreach ($file in (Join-Path $LogDir 'app.log'), (Join-Path $LogDir 'app.err.log')) {
      if (Test-Path $file) { Write-Step $file; Get-Content -Tail 40 $file | Write-Host }
    }
    exit 0
  }

  'uninstall' {
    $installed = Get-Installation
    if (-not $installed) {
      Write-Note 'No installation found in this folder.'
    } else {
      Write-Host "This permanently deletes installation $($installed.id): its database, stored files," -ForegroundColor Yellow
      Write-Host 'accounts, generated keys and build output. Run .\meridian.cmd backup first to keep data.' -ForegroundColor Yellow
      if (-not $Force) {
        $answer = Read-Host "Type the installation id ($($installed.id)) to confirm"
        if ($answer -ne $installed.id) { Write-Note 'Cancelled; nothing was deleted.'; exit 1 }
      }
      if (Stop-App) { Write-Step 'Web app stopped' }
      if ((Test-Docker $true) -eq 'running') {
        $ErrorActionPreference = 'Continue'
        Resolve-Toolchain $false | Out-Null
        if (Get-Command supabase.exe -CommandType Application -ErrorAction SilentlyContinue) {
          Write-Step 'Removing backend containers and data volumes'
          & supabase stop --no-backup --workdir $State
        }
        # Remove anything left with this installation's label (for example if the CLI is missing).
        $ids = & docker ps -aq --filter "label=com.supabase.cli.project=$($installed.id)"
        if ($ids) { & docker rm -f $ids | Out-Null }
        $volumes = & docker volume ls -q --filter "label=com.supabase.cli.project=$($installed.id)"
        if ($volumes) { & docker volume rm $volumes | Out-Null }
        & docker network rm "$($installed.id)_local" *> $null
      } else {
        Write-Host 'Docker is not running, so backend containers and volumes were not removed.' -ForegroundColor Yellow
        Write-Note "Start Docker Desktop and run uninstall again, or remove resources labelled com.supabase.cli.project=$($installed.id)."
        exit 1
      }
    }
    Write-Step 'Removing generated files'
    # rd with the \\?\ prefix removes paths beyond 260 characters, which deep node_modules trees
    # reach when the package is extracted under a long folder; Remove-Item in 5.1 cannot.
    $leftover = @()
    foreach ($path in '.meridian', '.output', 'node_modules') {
      $full = Join-Path $Root $path
      if (Test-Path $full) { & cmd.exe /d /c "rd /s /q `"\\?\$full`"" 2>$null }
      if (Test-Path $full) { $leftover += $full }
    }
    if ($RemoveTools -and (Test-Path $ToolsDir)) {
      Write-Step "Removing downloaded tools ($ToolsDir)"
      & cmd.exe /d /c "rd /s /q `"\\?\$ToolsDir`"" 2>$null
      if (Test-Path $ToolsDir) { $leftover += $ToolsDir }
    }
    if ($leftover.Count) {
      Write-Host "Backend data was removed, but these folders could not be deleted (a program may be using them):" -ForegroundColor Yellow
      $leftover | ForEach-Object { Write-Note $_ }
      exit 1
    }
    Write-Host "Uninstalled. You can now delete the folder $Root." -ForegroundColor Green
    exit 0
  }

  default {
    $installed = Get-Installation
    if (-not $installed) { Fail 'Not installed yet. Run: .\meridian.cmd install' }
    Resolve-Toolchain $false | Out-Null
    exit (Invoke-SelfHost (@($Command) + $Arguments))
  }
}
