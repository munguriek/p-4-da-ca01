#!/usr/bin/env bash
# Prepares the routewatch VM (Ubuntu 26.04 LTS) once. Copy this file and
# new-env.sh to the VM, then run:  sudo bash vm-bootstrap.sh
# Safe to rerun: each step keeps what is already there, and .env is never
# replaced once written.
#  1. Docker Engine and the Compose plugin, from Docker's apt repository
#  2. /opt/routewatch, with a new .env (fresh secrets, mode 600)
#  3. /usr/local/bin/routewatch, the current release's release.sh
#  4. a timer that stops the collector when the disk is 85% full

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root=/opt/routewatch

[[ $EUID -eq 0 ]] || {
  echo "vm-bootstrap.sh: run it with sudo" >&2
  exit 1
}
# shellcheck source=/dev/null
. /etc/os-release
[[ $ID == ubuntu ]] || {
  echo "vm-bootstrap.sh: expected Ubuntu, found $ID" >&2
  exit 1
}

echo "== 1. Docker Engine"
if ! docker compose version >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt_get() { apt-get -o DPkg::Lock::Timeout=300 "$@"; }
  apt_get update
  apt_get install -y ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  cat > /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
  apt_get update
  apt_get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
    docker-compose-plugin
fi
systemctl enable --now docker
docker version --format 'Docker Engine {{.Server.Version}}'
docker compose version

echo "== 2. /opt/routewatch"
install -d -m 0750 "$root" "$root/releases"
if [[ -e $root/.env ]]; then
  echo "$root/.env exists; left unchanged"
else
  bash "$here/new-env.sh" "$root/.env"
fi

echo "== 3. the routewatch command"
ln -sfn "$root/current/release.sh" /usr/local/bin/routewatch
echo "/usr/local/bin/routewatch -> $root/current/release.sh (live after the first deploy)"

echo "== 4. the disk guard"
cat > /etc/systemd/system/routewatch-disk-guard.service <<EOF
[Unit]
Description=Stop the routewatch collector when the disk is nearly full
ConditionPathExists=$root/current/release.sh
After=docker.service

[Service]
Type=oneshot
ExecStart=$root/current/release.sh disk-guard
EOF
cat > /etc/systemd/system/routewatch-disk-guard.timer <<'EOF'
[Unit]
Description=Run the routewatch disk guard every five minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now routewatch-disk-guard.timer

echo
echo "The VM is ready. Merge to main and approve the deployment in GitHub;"
echo "afterwards: sudo routewatch status"
