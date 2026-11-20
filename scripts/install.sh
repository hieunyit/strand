#!/bin/sh
# Cài Strand lên máy không dùng .deb/.rpm (chạy bằng root). Chạy lại an toàn (idempotent).
#
#   sudo scripts/install.sh --binary ./strand [--prefix /usr/local/bin] [--with-web [--with-postgres | --database-dsn DSN]
#        [web-init options]] [--no-start]
#
# Việc làm: cài binary 0755; tạo group/user strand; /etc/strand (root:strand 0750); chép strand@.service và
# strand-web.service vào /etc/systemd/system (ExecStart trỏ binary đã cài); chép quy tắc polkit nếu có
# /etc/polkit-1/rules.d; systemctl daemon-reload; chép ví dụ vào /usr/local/share/doc/strand.
# --with-web: chạy `strand web init --enable` (thêm --no-start thì không bật dịch vụ). Mặc định web nghe
# 0.0.0.0:8080 bằng HTTP thường (--allow-plain-http; mật khẩu không mã hoá, chỉ dùng trong mạng tin cậy);
# --tls-cert/--tls-key thì dùng HTTPS; chỉ cho máy chủ truy cập: --listen 127.0.0.1:8080. web.yaml có sẵn đang
# nghe loopback mà không đưa --listen thì được chuyển sang 0.0.0.0:8080. Các tùy chọn sau đây
# được chuyển nguyên cho nó: --listen X, --state-dir X, --scan X, --tls-cert X, --tls-key X, --secure-cookies,
# --trust-forwarded-for, --allow-plain-http, --generate-password, --password-stdin, --token, --force,
# Web UI cần PostgreSQL (kho metadata, bắt buộc): --database-dsn postgres://user:pass@host:5432/db (ghi vào
# /etc/strand/web.yaml 0640 root:strand), hoặc --with-postgres: cài PostgreSQL cục bộ bằng apt/dnf, tạo role
# strand_web + database strand_web với mật khẩu ngẫu nhiên. web.yaml có sẵn database.dsn thì giữ nguyên.
# Khóa STRAND_WEB_SECRET_KEY (bắt buộc) được sinh một lần vào /etc/strand/web.env (root:strand 0640), nạp qua
# EnvironmentFile của strand-web.service. Giữ một bản khóa ngoài máy: mất khóa = mất bí mật pipeline trong DB.
set -eu

usage() {
	sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
	exit "${1:-2}"
}

binary=""
prefix=/usr/local/bin
with_web=0
with_pg=0
dsn_set=0
no_start=0
webargs=""
listen_set=0
tls_set=0

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
	--with-postgres)
		with_pg=1
		shift
		;;
	--no-start)
		no_start=1
		shift
		;;
	--listen | --state-dir | --scan | --tls-cert | --tls-key | --database-dsn)
		[ $# -ge 2 ] || usage
		case "$1" in
		--listen) listen_set=1 ;;
		--tls-cert | --tls-key) tls_set=1 ;;
		--database-dsn) dsn_set=1 ;;
		esac
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

# setup_local_postgres: cài PostgreSQL bằng trình quản lý gói, tạo role/database strand_web (chạy lại an toàn:
# role có sẵn thì đặt lại mật khẩu), in DSN ra stdout.
setup_local_postgres() {
	if ! command -v psql >/dev/null 2>&1; then
		if command -v apt-get >/dev/null 2>&1; then
			DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql >&2
		elif command -v dnf >/dev/null 2>&1; then
			dnf install -y postgresql-server >&2
			[ -f /var/lib/pgsql/data/PG_VERSION ] || postgresql-setup --initdb >&2
		else
			echo "install.sh: --with-postgres needs apt-get or dnf; install PostgreSQL yourself and pass --database-dsn" >&2
			return 1
		fi
	fi
	systemctl enable --now postgresql >&2 2>/dev/null || true
	pw=$(od -An -tx1 -N24 /dev/urandom | tr -d ' \n')
	su postgres -c "psql -v ON_ERROR_STOP=1 -q" >&2 <<SQL
DO \$\$ BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'strand_web') THEN
    ALTER ROLE strand_web WITH LOGIN PASSWORD '$pw';
  ELSE
    CREATE ROLE strand_web WITH LOGIN PASSWORD '$pw';
  END IF;
END \$\$;
SQL
	if ! su postgres -c "psql -tAc \"SELECT 1 FROM pg_database WHERE datname = 'strand_web'\"" | grep -q 1; then
		su postgres -c "createdb -O strand_web strand_web" >&2
	fi
	printf 'postgres://strand_web:%s@127.0.0.1:5432/strand_web?sslmode=disable\n' "$pw"
}

if [ "$with_web" -eq 1 ]; then
	echo "==> web: PostgreSQL metadata database"
	wy=/etc/strand/web.yaml
	if [ "$with_pg" -eq 1 ] && [ "$dsn_set" -eq 0 ]; then
		dsn=$(setup_local_postgres)
		add_webarg --database-dsn
		add_webarg "$dsn"
		dsn_set=1
		echo "    local PostgreSQL ready: database strand_web, role strand_web"
	fi
	if [ "$dsn_set" -eq 0 ] && ! { [ -f "$wy" ] && grep -q '^[[:space:]]*dsn:' "$wy"; } && [ -z "${STRAND_WEB_DATABASE_DSN:-}" ]; then
		echo "install.sh: the web UI needs a PostgreSQL database: pass --database-dsn postgres://user:pass@host:5432/db," >&2
		echo "  or --with-postgres to install one locally (apt/dnf). Create one by hand with:" >&2
		echo "    sudo -u postgres psql -c \"CREATE ROLE strand_web LOGIN PASSWORD '<pw>'\" -c \"CREATE DATABASE strand_web OWNER strand_web\"" >&2
		exit 1
	fi
	echo "==> web: secret key /etc/strand/web.env"
	if ! grep -q '^STRAND_WEB_SECRET_KEY=..*' /etc/strand/web.env 2>/dev/null; then
		umask 027
		key=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
		printf '# strand-web environment (EnvironmentFile of strand-web.service). Back up this key OUTSIDE the database backup:\n# losing it makes the pipeline secrets stored in the database unreadable.\nSTRAND_WEB_SECRET_KEY=%s\n' "$key" >>/etc/strand/web.env
		chown root:strand /etc/strand/web.env
		chmod 0640 /etc/strand/web.env
		echo "    generated STRAND_WEB_SECRET_KEY (copy /etc/strand/web.env somewhere safe)"
	fi
	echo "==> strand web init"
	if [ "$listen_set" -eq 0 ]; then
		# Mặc định mở cho mạng: 0.0.0.0:8080 (HTTPS nếu có --tls-cert, không thì HTTP thường).
		add_webarg --listen
		add_webarg 0.0.0.0:8080
		[ "$tls_set" -eq 1 ] || add_webarg --allow-plain-http
		if [ -f "$wy" ] && cur=$(sed -n 's/^listen:[[:space:]]*["'\'']\{0,1\}\([^"'\''[:space:]#]*\).*/\1/p' "$wy" | head -n 1) &&
			case "$cur" in 127.*:* | localhost:* | \[::1\]:*) true ;; *) false ;; esac; then
			# web.yaml có sẵn nghe loopback: chuyển sang 0.0.0.0:8080 (web init không --force sẽ giữ listen cũ).
			echo "    $wy: listen $cur -> 0.0.0.0:8080"
			sed -i 's/^listen:.*/listen: 0.0.0.0:8080/' "$wy"
			if [ "$tls_set" -eq 0 ] && ! grep -q '^allow_plain_http:' "$wy"; then
				printf 'allow_plain_http: true\n' >>"$wy"
			fi
		fi
	fi
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
	echo "Done. Web UI (needs PostgreSQL): sudo scripts/install.sh --binary $bin --with-web --with-postgres   (or --database-dsn postgres://...)"
	echo "Pipeline: copy /usr/local/share/doc/strand/examples/pipeline.yaml to /etc/strand/<name>.yaml, then"
	[ "$no_start" -eq 1 ] || echo "  sudo systemctl enable --now strand@<name>"
fi
