#!/bin/sh
# Gỡ hẳn (không phải nâng cấp): dừng mọi pipeline. SIGTERM → strand xả writer, ghi checkpoint
# rồi thoát; chạy lại sau này tiếp tục đúng chỗ.
#   deb: $1 = remove | upgrade | deconfigure ...   rpm: $1 = 0 (gỡ) | 1 (nâng cấp)
set -e
case "$1" in
	remove|0) ;;
	*) exit 0 ;;
esac
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
	# Instance đang nạp (list-units) và instance đã enable mà chưa nạp kể từ boot (symlink trong *.wants).
	units=$(systemctl list-units --type=service --all --no-legend 'strand@*' 2>/dev/null | awk '{print $1}')
	for l in /etc/systemd/system/*.wants/strand@*.service; do
		[ -e "$l" ] || [ -L "$l" ] || continue
		units="$units $(basename "$l")"
	done
	for u in $(printf '%s\n' $units | sort -u); do
		systemctl disable --now "$u" || true
	done
	# Web UI (nếu đã bật).
	systemctl disable --now strand-web.service >/dev/null 2>&1 || true
fi
# Pipeline chế độ nền do web khởi động (strand sync --background) không thuộc unit nào và (KillMode=process
# của strand-web.service) không chết theo web: dừng êm bằng strand stop cho từng /etc/strand/*.yaml.
# strand stop ở pipeline không chạy nền chỉ báo "not running"; lỗi bỏ qua.
if [ -x /usr/bin/strand ] && command -v runuser >/dev/null 2>&1; then
	for c in /etc/strand/*.yaml; do
		[ -f "$c" ] || continue
		case "$(basename "$c")" in web.yaml) continue ;; esac
		runuser -u strand -- /usr/bin/strand stop -c "$c" >/dev/null 2>&1 || true
	done
fi
exit 0
