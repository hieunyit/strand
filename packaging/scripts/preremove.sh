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
	units=$(systemctl list-units --type=service --all --no-legend 'strand@*' 2>/dev/null | awk '{print $1}')
	for u in $units; do
		systemctl disable --now "$u" || true
	done
fi
exit 0
