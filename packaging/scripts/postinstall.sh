#!/bin/sh
# /etc/strand: cấu hình (root:strand 0640) và file .env chứa DSN/mật khẩu (root:strand 0600 theo 03 §4).
# Không tự enable/start: mỗi pipeline cần /etc/strand/<name>.yaml trước.
set -e
install -d -m 0750 -o root -g strand /etc/strand
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
fi
# Nâng cấp: pipeline đang chạy vẫn dùng binary cũ tới khi restart (an toàn: checkpoint ở target).
running=""
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
	running=$(systemctl list-units --type=service --state=running --no-legend 'strand@*' 2>/dev/null | awk '{print $1}')
fi
if [ -n "$running" ]; then
	echo "strand: các pipeline sau vẫn chạy binary cũ; restart khi thuận tiện:"
	for u in $running; do echo "  systemctl restart $u"; done
fi
exit 0
