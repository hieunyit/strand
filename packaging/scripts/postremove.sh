#!/bin/sh
# Không xóa /etc/strand, /var/lib/strand (state cục bộ của pipeline) hay user strand, kể cả khi
# purge: mất state cục bộ buộc snapshot lại; xóa là việc của người vận hành.
set -e
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
exit 0
