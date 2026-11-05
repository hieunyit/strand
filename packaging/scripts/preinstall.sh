#!/bin/sh
# Tạo system user/group strand (chạy strand@.service). Không đổi user đã có.
set -e
if ! getent group strand >/dev/null 2>&1; then
	groupadd --system strand
fi
if ! getent passwd strand >/dev/null 2>&1; then
	useradd --system --gid strand --home-dir /var/lib/strand --no-create-home \
		--shell /usr/sbin/nologin --comment "Strand CDC" strand
fi
exit 0
