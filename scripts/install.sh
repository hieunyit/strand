#!/bin/sh
# Cài Strand lên máy không dùng .deb/.rpm (chạy bằng root). Chạy lại an toàn (idempotent).
#
#   sudo scripts/install.sh --binary ./strand [--prefix /usr/local/bin] [--with-web [web-init options]] [--no-start]
#
# Việc làm: cài binary 0755; tạo group/user strand; /etc/strand (root:strand 0750); chép strand@.service và
# strand-web.service vào /etc/systemd/system (ExecStart trỏ binary đã cài); chép quy tắc polkit nếu có
# /etc/polkit-1/rules.d; systemctl daemon-reload; chép ví dụ vào /usr/local/share/doc/strand.
# --with-web: chạy `strand web init --enable` (thêm --no-start thì không bật dịch vụ); các tùy chọn sau đây
# được chuyển nguyên cho nó: --listen X, --state-dir X, --scan X, --tls-cert X, --tls-key X, --secure-cookies,
# --trust-forwarded-for, --allow-plain-http, --generate-password, --password-stdin, --token, --force.
set -eu

usage() {
	sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
	exit "${1:-2}"
}

binary=""
prefix=/usr/local/bin
with_web=0
no_start=0
webargs=""

# Thêm một tham số vào danh sách chuyển cho `strand web init` (đã quote an toàn bằng '...').
add_webarg() {
	q=$(printf '%s' "$1" | sed "s/'/'\\\\''/g")
	webargs="$webargs '$q'"
}

while [ $# -gt 0 ]; do
	case "$1" in
	--binary)
		[ $# -ge 2 ] || usage
		binary=$2
		shift 2
		;;
	--prefix)
		[ $# -ge 2 ] || usage
		prefix=$2
		shift 2
		;;
	--with-web)
		with_web=1
		shift
		;;
	--no-start)
		no_start=1
		shift
		;;
	--listen | --state-dir | --scan | --tls-cert | --tls-key)
		[ $# -ge 2 ] || usage
		add_webarg "$1"
		add_webarg "$2"
		shift 2
		;;
	--secure-cookies | --trust-forwarded-for | --allow-plain-http | --generate-password | --password-stdin | --token | --force)
		add_webarg "$1"
		shift
		;;
	-h | --help)
		usage 0
		;;
	*)
		echo "install.sh: unknown option: $1" >&2
		usage
		;;
	esac
done

if [ "$(id -u)" -ne 0 ]; then
	echo "install.sh: must run as root (use sudo)" >&2
	exit 1
fi

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo=$(dirname -- "$here")

if [ -z "$binary" ]; then
	if [ -x ./strand ]; then
		binary=./strand
	else
		echo "install.sh: need --binary PATH (or a ./strand binary in the current directory)" >&2
		exit 1
	fi
fi
if [ ! -f "$binary" ]; then
	echo "install.sh: binary not found: $binary" >&2
	exit 1
fi
# Tìm file mẫu: cạnh script (repo) hoặc trong tarball phát hành (cùng thư mục với script).
find_src() {
	for c in "$repo/$1" "$here/$(basename -- "$1")"; do
		if [ -f "$c" ]; then
			printf '%s\n' "$c"
			return 0
		fi
	done
	return 1
}
svc_tmpl=$(find_src deploy/systemd/strand@.service) || {
	echo "install.sh: strand@.service not found next to the script" >&2
	exit 1
}
web_tmpl=$(find_src deploy/systemd/strand-web.service) || {
	echo "install.sh: strand-web.service not found next to the script" >&2
	exit 1
}

bin="$prefix/strand"

echo "==> binary -> $bin"
install -d -m 0755 "$prefix"
install -m 0755 "$binary" "$bin"

echo "==> user/group strand"
if ! getent group strand >/dev/null 2>&1; then
	groupadd --system strand
fi
if ! getent passwd strand >/dev/null 2>&1; then
	nologin=/usr/sbin/nologin
	[ -x "$nologin" ] || nologin=/sbin/nologin
	useradd --system --gid strand --home-dir /var/lib/strand --no-create-home \
		--shell "$nologin" --comment "Strand CDC" strand
fi
install -d -m 0750 -o root -g strand /etc/strand

# Đổi đường dẫn binary trong ExecStart (dùng | làm dấu phân cách; prefix không chứa | hay &).
esc=$(printf '%s' "$bin" | sed 's/[&|\\]/\\&/g')
echo "==> systemd units -> /etc/systemd/system"
install -d -m 0755 /etc/systemd/system
sed "s|^ExecStart=/usr/local/bin/strand |ExecStart=$esc |" "$svc_tmpl" >/etc/systemd/system/strand@.service
sed "s|^ExecStart=/usr/local/bin/strand |ExecStart=$esc |" "$web_tmpl" >/etc/systemd/system/strand-web.service
chmod 0644 /etc/systemd/system/strand@.service /etc/systemd/system/strand-web.service

echo "==> polkit rule"
if polkit_src=$(find_src packaging/polkit/50-strand.rules); then
	if [ -d /etc/polkit-1/rules.d ]; then
		install -m 0644 "$polkit_src" /etc/polkit-1/rules.d/50-strand.rules
	else
		echo "warning: /etc/polkit-1/rules.d not found (polkit not installed): the Web UI cannot start/stop strand@ units;" >&2
		echo "         install polkit and copy $polkit_src to /etc/polkit-1/rules.d/50-strand.rules" >&2
	fi
else
	echo "warning: 50-strand.rules not found next to the script; skipping" >&2
fi

if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload
else
	echo "warning: systemd is not running; skipped daemon-reload" >&2
fi

echo "==> examples -> /usr/local/share/doc/strand/examples"
install -d -m 0755 /usr/local/share/doc/strand/examples
for pair in pipeline.yaml.example:pipeline.yaml pipeline.env.example:pipeline.env web.yaml.example:web.yaml; do
	src=${pair%%:*}
	dst=${pair##*:}
	if f=$(find_src "packaging/$src"); then
		install -m 0644 "$f" "/usr/local/share/doc/strand/examples/$dst"
	fi
done

if [ "$with_web" -eq 1 ]; then
	echo "==> strand web init"
	enable="--enable"
	[ "$no_start" -eq 1 ] && enable=""
	# webargs đã được quote từng phần, nên eval an toàn.
	eval "set -- $webargs"
	if [ -n "$enable" ]; then
		"$bin" web init --config /etc/strand/web.yaml --enable "$@"
	else
		"$bin" web init --config /etc/strand/web.yaml "$@"
	fi
else
	echo
	echo "Done. Web UI: sudo $bin web init --enable"
	echo "Pipeline: copy /usr/local/share/doc/strand/examples/pipeline.yaml to /etc/strand/<name>.yaml, then"
	[ "$no_start" -eq 1 ] || echo "  sudo systemctl enable --now strand@<name>"
fi
