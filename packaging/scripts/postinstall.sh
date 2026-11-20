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
# Web UI: khóa mã hóa bí mật pipeline trong DB (bắt buộc), sinh một lần; nâng cấp giữ nguyên file có sẵn.
if [ ! -f /etc/strand/web.env ]; then
	(
		umask 027
		key=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
		printf '# strand-web environment (EnvironmentFile of strand-web.service). Back up this key OUTSIDE the database backup.\nSTRAND_WEB_SECRET_KEY=%s\n' "$key" >/etc/strand/web.env
	)
	chown root:strand /etc/strand/web.env
	chmod 0640 /etc/strand/web.env
fi
echo "strand: Web UI needs PostgreSQL: sudo strand web init --database-dsn postgres://user:pass@host:5432/strand_web --enable"
echo "strand: (STRAND_WEB_SECRET_KEY is in /etc/strand/web.env: keep a copy outside the database backup)"
exit 0
