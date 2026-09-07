#!/bin/sh
# Install or update barclock on the Pi. Run as root from the unpacked deploy tree:
#   sudo ./deploy/install.sh /path/to/barclock-binary
# Idempotent: existing /etc/barclock.env and /etc/barclock.toml are never overwritten.
set -eu
BIN=${1:-./barclock}
HERE=$(cd "$(dirname "$0")" && pwd)
[ -x "$BIN" ] || { echo "usage: $0 /path/to/barclock (built for aarch64)"; exit 1; }

if ! id barclock >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/barclock --shell /usr/sbin/nologin \
        --groups video,input,render barclock
    echo "created user barclock"
fi
install -d -m 0755 /opt/barclock
install -m 0755 "$BIN" /opt/barclock/barclock.new && mv /opt/barclock/barclock.new /opt/barclock/barclock
[ -f "$HERE/../README.md" ] && install -m 0644 "$HERE/../README.md" /opt/barclock/README.md
install -d -m 0750 -o barclock -g barclock /var/lib/barclock

[ -f /etc/barclock.toml ] || install -m 0644 "$HERE/barclock.toml" /etc/barclock.toml
if [ ! -f /etc/barclock.env ]; then
    install -m 0640 -o root -g barclock "$HERE/barclock.env.example" /etc/barclock.env
    echo ">>> edit /etc/barclock.env: HA_TOKEN, MQTT_USER, MQTT_PASSWORD"
fi
install -m 0644 "$HERE/udev/99-barclock-touch.rules" /etc/udev/rules.d/99-barclock-touch.rules
udevadm control --reload && udevadm trigger --subsystem-match=input || true
install -m 0644 "$HERE/barclock.service" /etc/systemd/system/barclock.service
systemctl daemon-reload
# The panel is barclock's alone: no login prompt on tty1.
systemctl disable --now getty@tty1.service >/dev/null 2>&1 || true
systemctl mask getty@tty1.service >/dev/null 2>&1 || true
systemctl enable barclock.service
systemctl restart barclock.service
sleep 2
systemctl --no-pager --lines=20 status barclock.service || true
