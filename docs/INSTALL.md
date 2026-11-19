# Cài đặt và vận hành Strand (CLI + Web UI)

Hướng dẫn từng bước cho người vận hành cài Strand lên một máy Linux mới, tạo pipeline đầu tiên và bật Web UI. Đây là bản "làm theo", không lặp lại tham chiếu: mọi khóa cấu hình, lệnh, metric, exit code nằm ở [USAGE.md](USAGE.md); REST API ở [WEB-API.md](WEB-API.md).

Mục lục: Cài nhanh, 1 Yêu cầu và thành phần, 2 Cài đặt, 3 DB nguồn/đích, 4 Pipeline đầu tiên bằng CLI, 5 Cài Web UI, 6 Dùng Web UI, 7 Giám sát, 8 Nâng cấp, gỡ, sao lưu, 9 Sự cố thường gặp, 10 Chạy bằng docker compose (Web UI + PostgreSQL metadata).

## Cài nhanh

**Chọn chế độ lưu trạng thái của Web UI.** Khuyến nghị: **PostgreSQL** (kho metadata). Mọi trạng thái của web (tài khoản admin, phiên đăng nhập, danh sách pipeline kể cả pipeline khai báo/quét, nội dung cấu hình và `.env` mã hóa, trạng thái mong muốn, lịch sử và output job, wizard, cài đặt trên trang Settings, lịch sử số liệu 7 ngày, audit log chỉ-thêm) nằm trong DB: khởi động lại hay dựng lại máy không mất gì, sao lưu bằng `pg_dump`. **Chế độ file** (không có DB) vẫn được hỗ trợ cho cài đặt nhỏ: trạng thái nằm trong `web.yaml` và `state_dir`, biểu đồ chỉ 1 giờ trong RAM, phiên mất khi khởi động lại.

```bash
# Chế độ PostgreSQL (khuyến nghị): container, xem mục 10
cp .env.example .env && docker compose up -d --build
# hoặc cài trên máy, DB có sẵn (DSN ghi vào web.yaml 0640; muốn giữ DSN ngoài file: đặt STRAND_WEB_DATABASE_DSN cho dịch vụ)
sudo strand web init --database-dsn 'postgres://strand_web:PASS@db-host:5432/strand_web' --enable
sudo scripts/install.sh --binary ./strand --with-web --database-dsn 'postgres://...' --generate-password
```

Đang chạy chế độ file và muốn chuyển sang PostgreSQL: mục 11.

Hai lệnh là có Web UI (chế độ file). `strand web init` mặc định nghe `127.0.0.1:8080` (chỉ máy chủ); `scripts/install.sh --with-web` mặc định mở `0.0.0.0:8080` bằng HTTP thường cho cả mạng (xem dưới):

```bash
# Gói .deb / .rpm
sudo dpkg -i strand_<ver>_amd64.deb          # hoặc: sudo rpm -i strand-<ver>.x86_64.rpm
sudo strand web init --enable                # hỏi mật khẩu hai lần, bật strand-web, chờ /login trả 200

# Cài tay (không có gói): cần binary và repo/tarball phát hành
sudo scripts/install.sh --binary ./strand --with-web --generate-password
```

- `strand web init` tạo `/etc/strand/web.yaml` (root:strand 0640) và `/var/lib/strand-web` (strand:strand 0750), đặt mật khẩu admin, rồi với `--enable` chạy `systemctl enable --now strand-web`. Mật khẩu: hỏi trên terminal, `--password-stdin` (một dòng), hoặc `--generate-password` (in MỘT lần ra stdout, ghi lại ngay). `--token` tạo thêm token API (cũng in một lần).
- Chạy lại an toàn: `web.yaml` có sẵn giữ nguyên `listen`/`state_dir`/`scan` (lệnh nói rõ điều này; thêm `--force` để ghi đè), chỉ mật khẩu/token được đặt lại; không đưa mật khẩu thì giữ hash cũ. Đổi mật khẩu sau này: `strand web passwd`.
- Các cờ khác: `--listen`, `--state-dir`, `--scan` (lặp lại được; mặc định `/etc/strand/*.yaml`), `--tls-cert`/`--tls-key`, `--secure-cookies`, `--trust-forwarded-for`. `--listen` không phải loopback mà thiếu TLS thì lệnh báo lỗi và không ghi gì.
- `scripts/install.sh` (chạy bằng root, idempotent) cài binary vào `--prefix` (mặc định `/usr/local/bin`), tạo user/group `strand`, `/etc/strand`, unit systemd (ExecStart trỏ đúng binary), quy tắc polkit và ví dụ; `--with-web` gọi tiếp `strand web init --enable` và chuyển nguyên `--listen`, `--generate-password`, `--token`, `--tls-cert`... Thêm `--no-start` để không bật dịch vụ.
- `scripts/install.sh --with-web` mặc định: `--listen 0.0.0.0:8080 --allow-plain-http` (có `--tls-cert`/`--tls-key` thì HTTPS, không `--allow-plain-http`). Máy đã có `web.yaml` nghe loopback thì script tự chuyển sang `0.0.0.0:8080` (in dòng `listen ... -> 0.0.0.0:8080`). Chỉ cho máy chủ: `--listen 127.0.0.1:8080 --force`. Nhớ mở cổng 8080 trên tường lửa.
- Truy cập từ máy khác (khi nghe loopback): `ssh -L 8080:127.0.0.1:8080 user@strand-host`, mở `http://127.0.0.1:8080` (hoặc reverse proxy/TLS: mục 5.4).
- Mở thẳng cho mọi máy trong mạng nội bộ, không TLS: `sudo strand web init --listen 0.0.0.0:8080 --allow-plain-http --enable` (web.yaml có sẵn thì thêm `--force`; cài tay: `scripts/install.sh ... --with-web --listen 0.0.0.0:8080 --allow-plain-http`), rồi mở `http://<IP-máy-chủ>:8080` và mở cổng 8080 trên tường lửa. Mật khẩu, cookie phiên và token đi dạng rõ trên mạng: chỉ dùng trong mạng tin cậy; web in cảnh báo mỗi lần khởi động. Có TLS thì dùng `--tls-cert`/`--tls-key` thay cho `--allow-plain-http`.

Triển khai bằng container (Web UI lưu trạng thái trong PostgreSQL, tự chạy lại pipeline khi container khởi động lại): mục 10, `cp .env.example .env && docker compose up -d --build`.

Phần dưới là cài từng bước (khi cần kiểm soát từng việc).

## 1. Yêu cầu và thành phần

- Linux có systemd (amd64 hoặc arm64). Binary tĩnh, không cần thư viện hay Internet. Web UI cũng nằm trong binary (không cần Node, CDN).
- Máy chạy Strand nối được tới source và target (mặc định MySQL 3306, PostgreSQL 5432). Strand không cần chạy trên máy DB.
- Build từ source cần Go 1.25 trở lên.
- Có `sudo` (hoặc root) để cài gói, tạo user, chép unit.

| Thành phần | Đường dẫn | Ghi chú |
| --- | --- | --- |
| Binary `strand` | `/usr/bin/strand` (gói) hoặc `/usr/local/bin/strand` (cài tay) | CLI và Web UI là cùng một binary (`strand web`) |
| Unit pipeline | `strand@<name>.service` | Một instance mỗi pipeline; đọc `/etc/strand/<name>.yaml` và `/etc/strand/<name>.env` |
| Unit Web UI | `strand-web.service` | Đọc `/etc/strand/web.yaml`; chạy `User=strand` |
| Cấu hình | `/etc/strand/` | `<name>.yaml`, `<name>.env`, `web.yaml` |
| State pipeline | `/var/lib/strand/<name>/` | systemd tạo (`StateDirectory`); spool, log chế độ nền |
| Socket quản trị | `/run/strand/<name>/admin.sock` | systemd tạo (`RuntimeDirectory`) |
| State Web UI | `/var/lib/strand-web/` | `audit.log`, `jobs/`, `wizards/`, `pipelines.json` (danh sách pipeline tạo từ web), `pipelines/` (file của chúng nếu không ghi được `/etc/strand`) |

Checkpoint của pipeline nằm ở **target** (bảng `_strand_*`), nên mất `/var/lib/strand/<name>` không mất vị trí đồng bộ nhưng buộc snapshot lại phần local (xem mục 8).

## 2. Cài đặt

### 2a. Gói `.deb` / `.rpm`

Gói dựng bằng `VERSION=1.0.0 scripts/package.sh` (kết quả ở `dist/`: `.deb`, `.rpm`, tarball + SHA-256, cho amd64 và arm64).

```bash
sudo dpkg -i strand_<ver>_amd64.deb          # Debian/Ubuntu
sudo rpm -i strand-<ver>.x86_64.rpm          # RHEL/Rocky
strand version
```

Gói làm các việc sau (đối chiếu `packaging/scripts/*.sh`, `packaging/nfpm.yaml`):

- Tạo group và user hệ thống `strand` (home `/var/lib/strand`, shell `/usr/sbin/nologin`), không đổi user đã có.
- Tạo `/etc/strand` (root:strand, 0750).
- Cài `/usr/bin/strand`, `strand@.service`, `strand-web.service` vào `/usr/lib/systemd/system/` rồi `systemctl daemon-reload`.
- Cài quy tắc polkit `/usr/share/polkit-1/rules.d/50-strand.rules`: user `strand` (Web UI) được start/stop/restart đúng các unit `strand@<name>.service`, không gì khác (mục 5.3).
- Chép tài liệu và ví dụ vào `/usr/share/doc/strand/`: `INSTALL.md`, `USAGE.md`, `examples/pipeline.yaml`, `examples/pipeline.env`, `examples/web.yaml`.
- **Không** bật pipeline hay Web UI nào.

### 2b. Cài tay (binary hoặc build từ source)

```bash
git clone https://github.com/hieunyit/strand.git && cd strand
CGO_ENABLED=0 go build -trimpath -o strand ./cmd/strand
sudo install -m 0755 strand /usr/local/bin/strand

sudo groupadd --system strand
sudo useradd --system --gid strand --home-dir /var/lib/strand --no-create-home \
     --shell /usr/sbin/nologin --comment "Strand CDC" strand
sudo install -d -m 0750 -o root -g strand /etc/strand

sudo cp deploy/systemd/strand@.service deploy/systemd/strand-web.service /etc/systemd/system/
sudo cp packaging/polkit/50-strand.rules /etc/polkit-1/rules.d/     # cho Web UI start/stop strand@* (polkit >= 0.106)
sudo systemctl daemon-reload
```

Unit mẫu trong repo dùng `ExecStart=/usr/local/bin/strand ...` (khớp cách cài tay ở trên). Gói `.deb`/`.rpm` tự đổi thành `/usr/bin/strand`. Nếu để binary chỗ khác, sửa `ExecStart` trong hai unit.

## 3. Chuẩn bị DB nguồn và đích

Strand cần: user có quyền replication ở source (MySQL: `REPLICATION SLAVE, REPLICATION CLIENT`, binlog ROW, GTID khuyến nghị; PostgreSQL: `wal_level=logical` và quyền tạo slot/publication), user ghi được ở target, và (target MySQL) database metadata `strand` đã tồn tại:

```sql
CREATE DATABASE strand;
```

Chi tiết từng engine, quyền tối thiểu và các biến cần bật: [USAGE.md](USAGE.md) mục 4 (4.1 MySQL, 4.2 PostgreSQL, 4.3 Quyền) và mục 3 (chuẩn bị target). Chạy `strand validate` (mục 4) để Strand tự kiểm các điều kiện này.

## 4. Pipeline đầu tiên bằng CLI

### 4.1 Tạo file cấu hình và `.env`

```bash
sudo strand config init --from mysql --to postgres --name orders-sync \
     --source-host 10.0.0.11 --target-host 10.0.0.21 --database app \
     --systemd -o /etc/strand/orders-sync.yaml
```

`--systemd` đặt state ở `/var/lib/strand/<name>` và admin socket ở `/run/strand/<name>/admin.sock`, đúng với `StateDirectory`/`RuntimeDirectory` của `strand@.service`. Cờ khác (`--exclude`, `--snapshot-window`, `--metrics`...): [USAGE.md](USAGE.md) mục 7.0. Có thể chép `/usr/share/doc/strand/examples/pipeline.yaml` thay vì dùng `config init`. Mật khẩu không nằm trong YAML, mà đọc từ biến môi trường `SRC_PW` / `DST_PW`.

Tạo file `.env` (định dạng `KEY=VALUE` mỗi dòng, systemd nạp qua `EnvironmentFile`) và đặt quyền:

```bash
sudo cp /usr/share/doc/strand/examples/pipeline.env /etc/strand/orders-sync.env
sudoedit /etc/strand/orders-sync.env          # SRC_PW=..., DST_PW=...
sudo chown root:strand /etc/strand/orders-sync.yaml /etc/strand/orders-sync.env
sudo chmod 0640 /etc/strand/orders-sync.yaml
sudo chmod 0600 /etc/strand/orders-sync.env   # chỉ root đọc; systemd (root) nạp env rồi mới hạ xuống user strand
```

Lưu ý: `strand config init -o` ghi file mode 0600 cho chính người chạy; phải `chown`/`chmod` như trên để user `strand` đọc được YAML. Khi Web UI cần xem/sửa `.env` hoặc chạy pipeline chế độ nền với `env_file`, user `strand` phải đọc được `.env`: đổi thành `0640` (xem mục 5.3).

### 4.2 Validate và plan

Nạp `.env` vào môi trường rồi chạy (root đọc được `.env` 0600):

```bash
sudo bash -c 'set -a; . /etc/strand/orders-sync.env; set +a
  strand validate -c /etc/strand/orders-sync.yaml && strand plan -c /etc/strand/orders-sync.yaml'
```

`validate` kiểm kết nối, quyền, binlog/GTID hoặc `wal_level`, khóa hàng của bảng (PK, hoặc unique index NOT NULL; bảng thiếu khóa được in kèm lệnh `ALTER TABLE` gợi ý, xem [USAGE.md](USAGE.md) 5.1); `plan` ước lượng thời gian snapshot. Ý nghĩa kiểm tra và exit code: [USAGE.md](USAGE.md) 7.1, 7.2, 8.3.

### 4.3 Chạy

**Cách A, systemd (khuyến nghị cho production):**

```bash
sudo systemctl enable --now strand@orders-sync
systemctl status strand@orders-sync
journalctl -u strand@orders-sync -f
sudo -u strand strand top -c /etc/strand/orders-sync.yaml      # trạng thái, lag
```

Unit tự restart khi exit 1 (lỗi tạm) và **không** restart với exit 2, 3, 4, 5, 10 (cần người xử lý: sửa cấu hình, thiếu khóa hàng (`missing_pk`), đang bị khóa/fenced, `log_gap`, chờ quyết định promote); xem [USAGE.md](USAGE.md) 8.3.

**Cách B, chế độ nền (không cần unit, tiện cho thử nghiệm):**

```bash
set -a; . ./orders-sync.env; set +a
strand sync -c orders-sync.yaml --background      # hoặc -d
strand status -c orders-sync.yaml                 # còn chạy không + 5 dòng log cuối
strand stop   -c orders-sync.yaml                 # SIGTERM, chờ tối đa 150s (--timeout)
```

Log và file pid mặc định ở `<resources.spool.path>/strand.log` và `strand.pid` (đổi bằng `--log-file`, `--pid-file`). Chế độ nền **không tự restart** khi pipeline lỗi hoặc máy khởi động lại.

## 5. Cài Web UI (từng bước)

> Chỉ cần một lệnh: `sudo strand web init --enable` (xem "Cài nhanh"). Các mục 5.1 đến 5.4 là cách làm tay tương đương.

### 5.1 Tạo `web.yaml`

```bash
sudo cp /usr/share/doc/strand/examples/web.yaml /etc/strand/web.yaml     # cài tay: packaging/web.yaml.example
sudo chown root:strand /etc/strand/web.yaml; sudo chmod 0640 /etc/strand/web.yaml
```

Các khóa hay dùng (đầy đủ: [USAGE.md](USAGE.md) 7.15; file nhận khóa lạ là lỗi):

| Khóa | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `listen` | `127.0.0.1:8080` | Không phải loopback (ví dụ `0.0.0.0:8080`) thì cần `tls_cert` + `tls_key`, hoặc `allow_plain_http: true` |
| `allow_plain_http` | `false` | Cho phép bind ngoài loopback bằng HTTP thường (không mã hoá; chỉ mạng tin cậy) |
| `state_dir` | `/var/lib/strand-web` | `audit.log`, `jobs/`, `wizards/` |
| `scan` | (không) | Glob các file pipeline, ví dụ `/etc/strand/*.yaml`; tên pipeline = tên file bỏ `.yaml`; `<name>.env` cùng thư mục được nhận làm `env_file`; bản thân `web.yaml` bị bỏ qua |
| `pipelines` | (không) | Khai báo tường minh `name`, `config`, `env_file`, `systemd_unit`, `log_file`, `pid_file`; thắng `scan` cùng tên |
| `config_dir` | `/etc/strand` nếu ghi được, không thì `<state_dir>/pipelines` | Nơi ghi pipeline tạo từ web |
| `secure_cookies`, `trust_forwarded_for` | `false` | Dùng khi đứng sau reverse proxy TLS (mục 5.4) |

Cách quản lý một pipeline: có `systemd_unit` (hoặc, với `scan`, unit `strand@<name>` tồn tại: gói luôn cài template nên điều này thường đúng) thì dùng systemctl/journalctl; còn lại là chế độ nền (`strand sync --background`, pid/log trong spool).

### 5.2 Đặt mật khẩu và token

```bash
sudo strand web passwd --config /etc/strand/web.yaml        # hỏi hai lần; 8 đến 72 ký tự
sudo strand web token  --config /etc/strand/web.yaml        # tùy chọn: in token MỘT lần, lưu sha256
```

- Không có TTY: `echo 'mật-khẩu' | sudo strand web passwd --config /etc/strand/web.yaml`.
- Một tài khoản admin duy nhất, không có mật khẩu mặc định. Chưa đặt mật khẩu thì `strand web` thoát exit 2.
- Cả hai lệnh ghi lại `web.yaml` nguyên tử (file tạm cùng thư mục rồi đổi tên), giữ chú thích và khóa khác, và **giữ nguyên chủ sở hữu và mode** của file cũ: root:strand 0640 vẫn là root:strand 0640, nên user `strand` vẫn đọc được. Vì file chứa hash, bit "other" và quyền ghi của group luôn bị bỏ (0644 thành 0640, có in một dòng ghi chú). File chưa có thì tạo 0600. `/etc/strand` là thư mục của root nên chạy bằng `sudo`; chạy không đủ quyền để giữ chủ sở hữu thì lệnh báo lỗi và không sửa file.
- Token chỉ hiện một lần (chạy lại `token` thì token cũ mất hiệu lực). Giữ như mật khẩu.

### 5.3 Quyền để web quản lý được pipeline

`strand-web.service` chạy `User=strand`, không phải root, không dùng polkit hay sudo. Vì vậy web chỉ quản lý được pipeline mà user `strand` có quyền:

| Cần | Cách |
| --- | --- |
| Đọc YAML pipeline | `/etc/strand/<name>.yaml` root:strand 0640 |
| Đọc `.env` (xem/sửa trên tab Config, nạp env cho `validate`, `verify`, chế độ nền) | Đổi `.env` thành root:strand `0640` |
| Gọi admin socket | Pipeline phải chạy bằng user `strand` (unit mẫu đã vậy); socket 0700 của user khác thì web báo `unreachable`/`stopped` |
| Xem log systemd | `sudo usermod -aG systemd-journal strand`, rồi restart `strand-web` |
| Start/Stop/Restart pipeline systemd từ web | Quy tắc polkit `50-strand.rules` (gói cài sẵn; cài tay: mục 2b) cho user `strand` đúng ba động từ start/stop/restart trên `strand@<name>.service`. Unit khác, `daemon-reload` vẫn bị từ chối |
| Enable/Disable at boot từ web | **Không cấp**: quyền này (`manage-unit-files`) không giới hạn được theo unit. Web trả 502 kèm lệnh cần chạy: `sudo systemctl enable strand@<name>` |

Unit web đặt `ProtectSystem=strict`: web (và tiến trình con của nó) **chỉ ghi được** `/var/lib/strand-web` (và `/dev`, `/tmp`...), không ghi được `/etc`. Hệ quả:

- Tạo pipeline từ web: `config_dir` mặc định rơi về `/var/lib/strand-web/pipelines`; YAML và `.env` nằm ở đó, spool ở `/var/lib/strand-web/pipelines/<name>/spool`. Danh sách pipeline tạo từ web lưu ở `/var/lib/strand-web/pipelines.json` (0600), nên còn nguyên sau khi `strand-web` restart; không cần ghi được `web.yaml`. Khai báo cùng tên trong `web.yaml` (`pipelines:`) thắng, kèm cảnh báo.
- Pipeline chế độ nền do web khởi động cũng chỉ ghi được dưới `/var/lib/strand-web`: spool/state của nó phải nằm ở đó (pipeline tạo từ web đã vậy). Pipeline khai tay mà spool ở chỗ khác (ví dụ `/var/lib/strand/<name>`) thì thêm drop-in `ReadWritePaths=` (dưới đây) hoặc chạy bằng `strand@<name>`.
- Sửa cấu hình trên tab Config (ghi `.bak.<timestamp>` và đổi tên file tạm trong cùng thư mục) với file ở `/etc/strand` sẽ thất bại. Muốn cho phép: tạo drop-in `sudo systemctl edit strand-web` với `[Service]` / `ReadWritePaths=/etc/strand`, và cho nhóm strand quyền ghi thư mục (`sudo chmod 0770 /etc/strand`). Đây là lựa chọn bảo mật của bạn: user `strand` khi đó sửa được mọi cấu hình pipeline. Khi lưu, file giữ mode và (nếu được) chủ sở hữu cũ; bản `.bak` không bao giờ thoáng quyền hơn bản gốc.

`strand-web.service` có `KillMode=process`: `systemctl restart|stop strand-web` chỉ dừng tiến trình web. Pipeline chế độ nền và job verify/repair/promote do web khởi động **vẫn chạy**, web mới nhận lại chúng qua pid file. Muốn dừng chúng: nút Stop trên web hoặc `strand stop -c <cfg>`. Gỡ gói thì tự `strand stop` các pipeline này (mục 8). Pipeline production vẫn nên chạy bằng `strand@<name>` (tự restart khi lỗi tạm).

### 5.4 Bật và truy cập

```bash
sudo systemctl enable --now strand-web
journalctl -u strand-web -n 20      # "strand web listening on http://127.0.0.1:8080 (audit log: ...)"
```

Mặc định chỉ nghe loopback. Ba cách truy cập từ xa:

**a) SSH tunnel (đơn giản nhất, không cần TLS):**

```bash
ssh -L 8080:127.0.0.1:8080 user@strand-host      # rồi mở http://127.0.0.1:8080
```

**b) Reverse proxy TLS (khuyến nghị):** giữ `listen: 127.0.0.1:8080`, thêm vào `web.yaml`:

```yaml
secure_cookies: true        # cookie có cờ Secure
trust_forwarded_for: true   # lấy IP client từ X-Forwarded-For (chỉ khi proxy tin cậy)
```

Ví dụ nginx (TLS do nginx giữ):

```nginx
server {
    listen 443 ssl;
    server_name strand.example.internal;
    ssl_certificate     /etc/ssl/strand.crt;
    ssl_certificate_key /etc/ssl/strand.key;
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_read_timeout 180s;      # thao tác stop/restart có thể chờ tới ~170 giây
    }
}
```

`trust_forwarded_for` chỉ đúng khi nginx **ghi đè** `X-Forwarded-For` như trên (không nối thêm giá trị client gửi). Nếu không, kẻ tấn công giả IP để né khóa đăng nhập.

**c) TLS trực tiếp:** đặt `listen: 0.0.0.0:8443`, `tls_cert`, `tls_key` (cả hai, đọc được bởi user `strand`). Bind khác loopback mà thiếu TLS thì web từ chối khởi động (exit 2), trừ khi bật `allow_plain_http`.

**d) HTTP thường trên mọi interface (mạng nội bộ tin cậy):** `listen: 0.0.0.0:8080` và `allow_plain_http: true` (hoặc `strand web init --listen 0.0.0.0:8080 --allow-plain-http --force`). Mở `http://<IP-máy-chủ>:8080`. Không có mã hoá: mật khẩu admin, cookie phiên và token đi dạng rõ; web in cảnh báo khi khởi động. Giới hạn bằng tường lửa chỉ cho các máy cần vào.

## 6. Dùng Web UI

Mở URL, đăng nhập bằng mật khẩu admin. Phiên hết hạn sau 30 phút không thao tác hoặc 12 giờ; restart `strand-web` thì đăng nhập lại. Giao diện bằng tiếng Anh.

### 6.1 Điều hướng

- **Overview**: số pipeline theo trạng thái (`synced`, `bootstrapping`, `paused`, `starting`, `stopped`, `failed`, `unreachable`, `unknown`), lag lớn nhất, tổng rows/s; cảnh báo (tên pipeline không hợp lệ...) ở đầu trang.
- **Pipelines**: danh sách; **+ New pipeline**; **Jobs**: mọi job verify/repair/promote.
- Trang chi tiết một pipeline có các tab: **Graph** (Source, Assembler, Scheduler, Writer, Target kèm số liệu và biểu đồ 1 giờ, chọn 5 phút / 15 phút / 1 giờ, điểm mới thêm trực tiếp; nút Start/Stop/Restart/Pause/Resume, Throttle), **Tables** (snapshot từng bảng, khung "Copying now" liệt kê mọi bảng đang chép song song; Snapshot add), **Checkpoint**, **Errors**, **Config**, **Logs** (bám đuôi trực tiếp, mặc định bật), **Jobs**, **Repair**, **Promote**.
- Các thẻ trạng thái ở Overview bấm được (mở `/pipelines?state=failed`...). Danh sách Pipelines xếp pipeline cần chú ý lên đầu (failed, unreachable, paused...), có ô lọc theo tên/trạng thái và bấm tiêu đề cột (Name, State, Lag, Rows/s) để sắp xếp. Đầu mỗi trang có dòng cho biết chu kỳ lấy mẫu và giờ render trang (trang tĩnh: `Data as of HH:MM:SS <múi giờ>`); đầu trang pipeline có huy hiệu trạng thái, lý do (failed/paused/breaker), lag, và các nút xếp theo nhóm (Start/Resume, Pause/Restart, Stop màu đỏ); nút không áp dụng được ở trạng thái hiện tại bị mờ và có chú thích lý do. Tên tab trình duyệt: `<pipeline> · <Tab> · Strand`. Phím mũi tên chuyển giữa các tab; màn hình hẹp thì thanh điều hướng chuyển lên đầu trang.
- **Số liệu sống**: chỉ bộ lấy mẫu chạy nền của `strand web` hỏi admin socket và `/metrics` (mỗi 5 giây, mỗi pipeline một lần, có hạn giờ, lưu 1 giờ trong RAM, mất khi restart web); trang không bao giờ gọi socket nên luôn mở nhanh. Trình duyệt nhận mẫu mới qua Server-Sent Events (`/api/v1/stream`) và cập nhật số tại chỗ: không nhảy trang, không mất vị trí cuộn, ô đang gõ hay chữ đang bôi đen. Ô góc trên phải cho biết kết nối: **Live**, **Live (polling)** (SSE bị chặn, ví dụ proxy có buffering: hỏi JSON mỗi chu kỳ), **Reconnecting**, **No new data** (không có tin nào từ server quá 3 chu kỳ).
- Số ổn định: Rows/s là trung bình 30 giây (không phải tốc độ tức thời); lag hiển thị đơn vị cố định (`0.25 s`, `12.3 s`, `4m 05s`), đổi màu ở 30 s / 300 s và chỉ hết màu khi xuống dưới 80% ngưỡng; huy hiệu trạng thái chỉ đổi khi hai mẫu liên tiếp đồng ý (riêng `failed`, `stopped` đổi ngay). Khi một lần hỏi pipeline thất bại hoặc quá hạn (pipeline treo, socket bận), số cũ vẫn hiện nhưng mờ đi, kèm biểu tượng cảnh báo và "updated N s ago"; hai lần liên tiếp thì trạng thái thành `unreachable`.
- Sau reverse proxy (nginx...): tắt buffering cho `/api/v1/stream` (`proxy_buffering off;`, `proxy_read_timeout` > 15 s) để SSE chạy; không thì trang tự chuyển sang hỏi định kỳ.

### 6.2 Tạo pipeline mới từ web

`+ New pipeline` (`/pipelines/new`) hoặc `POST /api/v1/pipelines` bằng JSON (trường: `name`, `from`, `to`, `source_host`, `target_host`, `source_user`, `target_user`, `source_password`, `target_password`, `database`/`databases`, `target_dsn`, `metrics`... xem [WEB-API.md](WEB-API.md)). Web:

1. Kiểm tên (`[A-Za-z0-9][A-Za-z0-9_.-]{0,62}`, không trùng) và cổng `/metrics` (tự chọn cổng trống đầu tiên từ `127.0.0.1:9464`; cổng bạn đặt mà trùng pipeline khác bị từ chối 409).
2. Ghi `<config_dir>/<name>.yaml` (0640) và `<config_dir>/<name>.env` (0600, chứa mật khẩu; YAML chỉ tham chiếu `password_env`). Spool ở `<state_dir>/pipelines/<name>/spool`. File đã tồn tại thì từ chối.
3. Chạy `strand validate` và `strand plan`, hiện kết quả. Validate lỗi thì pipeline vẫn được tạo (để sửa ở tab Config) nhưng kết quả là `ok: false` và không có nút Start.
4. Nút **Start** khởi động ở chế độ nền (`strand sync --background`). Muốn chạy bằng systemd: chép file về `/etc/strand/<name>.{yaml,env}` (`.env` web ghi đã ở dạng `KEY=VALUE` mà systemd `EnvironmentFile` đọc được; mật khẩu có ký tự đặc biệt được đặt trong nháy kép), sửa `resources.spool.path`/`observability.admin_socket` về `/var/lib/strand/<name>` và `/run/strand/<name>/admin.sock`, rồi `systemctl enable --now strand@<name>`. Mật khẩu chứa ký tự xuống dòng bị từ chối (400).

Form chỉ hiện ô hợp với cặp engine đã chọn (PG chỉ sang PG, MariaDB chỉ sang MySQL; cổng tự đổi 3306/5432) và có dòng **Will copy** xem trước phạm vi:

- Bước 2 (nguồn PG): **Database** và **Schema** (thường `public`).
- Bước 3: đích PG có **Target database** (database PostgreSQL để chép vào; trống = cùng tên database nguồn; bắt buộc khi nguồn MySQL là `*`), phải tạo trước bằng `CREATE DATABASE`. Đích MySQL có **Strand metadata database** (`strand`, chỉ chứa checkpoint `_strand_*`, không phải dữ liệu), cũng phải tạo trước.
- Bước 4 (nguồn MySQL/MariaDB): **Databases** cần chép, cách nhau dấu phẩy, `*` = tất cả (bỏ database hệ thống). Mục "Only some tables?" (tùy chọn): **Include only** / **Skip** theo mẫu `db.bảng` (PG: `schema.bảng`), ví dụ `shop.orders` hay `shop.log_*`; để trống là lấy hết.
- Bước 5 (Advanced, tùy chọn): commit order, số worker, thư mục spool, tên biến mật khẩu.
- Bước 6 (**Snapshot speed & safety**, tùy chọn, gập sẵn): giới hạn đọc source soft/hard (mặc định soft 600), chunk rows (mặc định tự chỉnh, 5000), khung giờ snapshot (`22:00-06:00`), governor algorithm (`static|onoff|grace-v2`), lag SLO, ngưỡng breaker (tỉ lệ độ trễ query, replica lag max). Để trống là dùng mặc định. Snapshot chậm hoặc breaker mở trên source MariaDB/MySQL thì chỉnh ở đây (hoặc sau này ở tab Config).

Validate sẽ gợi ý `CREATE DATABASE` nếu database đích hoặc metadata chưa có. Nơi lưu file và quyền ghi: mục 5.3.

### 6.3 Xem và sửa cấu hình

Tab **Config**: hiện đúng văn bản file (giữ chú thích), mật khẩu/bí mật che bằng `********`, kèm cảnh báo khóa chưa có tác dụng. Chế độ sửa: ghi file tạm, chạy `strand validate` với env của pipeline, hiện kết quả và **diff**, rồi mới ghi đè nguyên tử và giữ bản sao lưu `<file>.bak.<timestamp>`; có tùy chọn Restart sau khi lưu. Validate lỗi thì không ghi đè. Giá trị `.env` là write-only: để nguyên `********` thì giữ giá trị cũ. Mọi lần xem/sửa đều vào audit log. Đa số khóa chỉ có hiệu lực sau restart ([USAGE.md](USAGE.md) 8.1).

Thẻ **Tuning** (tab Config) cho chỉnh các khóa tốc độ/an toàn snapshot bằng form thay vì sửa YAML: `resources.source.iops.soft/hard`, `bootstrap.logical.chunk_rows`, `bootstrap.logical.windows`, `governor.algorithm`, `governor.lag_slo`, `governor.breaker.strand_query_p99.max_ratio`, `governor.breaker.replica_lag.max`, `resources.workers.max`. Form hiện giá trị hiện có trong file (khóa vắng ghi "default: ..."); ô để trống thì khóa bị xóa (về mặc định). Lưu đi qua đúng đường ở trên (validate, diff, sao lưu `.bak`, ghi nguyên tử, Restart tùy chọn) và vẫn giữ chú thích cùng các khóa khác; bấm **Validate & Preview Diff** để xem trước. API: `GET/POST /api/v1/pipelines/{name}/tuning` ([WEB-API.md](WEB-API.md)). Giá trị mới có hiệu lực sau restart; muốn đổi tốc độ đọc ngay thì dùng **Throttle** ở tab Graph (không ghi vào file).

### 6.4 Thao tác

| Thao tác | Nơi | Ghi chú |
| --- | --- | --- |
| Start, Stop, Restart | đầu trang | Stop/Restart có hộp xác nhận. Chế độ systemd: `systemctl`; chế độ nền: `strand sync --background` / `strand stop` |
| Enable/Disable at boot | đầu trang | Chỉ chế độ systemd; không được polkit cấp cho web (mục 5.3), web chỉ ra lệnh `sudo systemctl ...` cần chạy |
| Pause, Resume | đầu trang | Qua admin socket |
| Pause snapshot, Resume (snapshot only), Snapshot add | tab Tables | Snapshot add: chọn bảng từ source (bảng cần PK) |
| Throttle (soft IOPS) | tab Graph | Chỉ tiến trình đang chạy, đổi ngay, không ghi vào file cấu hình. Muốn lưu lâu dài: thẻ Tuning ở tab Config (6.3) |
| Checkpoint Rewind | tab Checkpoint | Chỉ MySQL → MySQL, đang chạy; gõ lại tên pipeline |
| Verify sample/full/delta | tab Jobs | Tiến trình con `strand verify ... --repair report`; mỗi pipeline tối đa **một** job (409 nếu bấm thêm); output trực tiếp ở `/jobs/<id>`, có Cancel |

Wizard **Repair** (bốn bước: chờ lag gần 0, Stop, Run repair, Start) và **Promote** (Check state, Promote, quyết định nếu exit 10, chuyển ứng dụng) chỉ gọi CLI `verify --repair fix` và `promote`; trạng thái lưu ở `<state_dir>/wizards/` nên đóng trình duyệt hay restart web vẫn tiếp tục đúng bước. DSN target/fence của Promote nhập mỗi lần, không lưu, không vào audit. Đọc [USAGE.md](USAGE.md) 7.10 và 7.15 trước khi dùng trên production.

### 6.5 Tìm lỗi: Errors, Logs, trạng thái `failed`

- Tab **Errors**: WARN/ERROR gần nhất, exit code lần dừng gần nhất kèm cách xử lý (tra [USAGE.md](USAGE.md) 8.3).
- Trạng thái `failed`: pipeline đã dừng và lần chạy cuối kết thúc bằng lỗi. Với chế độ systemd lấy từ `systemctl show`; chế độ nền suy ra từ đuôi log (dòng `Error [code]: ...`, hoặc bản ghi ERROR `pipeline failed`). Trang hiện `last_exit` (mã và lý do). Đã kiểm chạy thật: pipeline chế độ nền trỏ vào MySQL không có thật báo `failed`, `exit_code: 1`, `last_error: read source gtid_mode: dial tcp ... connection refused`.
- Chế độ nền **không tự restart**: `failed` ở đây nghĩa là bạn phải sửa nguyên nhân rồi bấm **Start**.
- Tab **Logs**: `journalctl -u <unit>` (systemd) hoặc đuôi `<spool>/strand.log` (nền).

### 6.6 Audit log

`<state_dir>/audit.log` (JSON lines, 0600): đăng nhập (ok/denied, lý do `bad_password` hoặc `rate_limited`), đăng xuất, xem/sửa config, tạo pipeline và mọi thao tác ghi (dòng `started` trước khi chạy, rồi dòng kết quả kèm exit code). Không ghi được audit thì thao tác không chạy.

```bash
sudo tail -f /var/lib/strand-web/audit.log
```

### 6.7 Gọi API bằng token

```bash
TOKEN=...   # in một lần bởi strand web token
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/overview
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/pipelines
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/pipelines/orders-sync/status
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{}' \
     http://127.0.0.1:8080/api/v1/pipelines/orders-sync/actions/start
```

Token tĩnh không cần cookie hay CSRF. Đăng nhập bằng session (`GET /login` lấy cookie `strand_login`, rồi `POST /api/v1/login` với header `X-CSRF-Token` bằng giá trị cookie đó) và đầy đủ endpoint: [WEB-API.md](WEB-API.md).

## 7. Giám sát

- **Prometheus**: mỗi pipeline bật `observability.metrics: 127.0.0.1:9464` thì có `/metrics` và `/healthz` ở đó. **Mỗi pipeline một cổng riêng** (`config init` mặc định 9464 cho mọi pipeline nên pipeline thứ hai phải đổi, ví dụ 9465), nếu trùng thì pipeline thứ hai không khởi động được. Danh sách metric và ngưỡng cảnh báo gợi ý: [USAGE.md](USAGE.md) 8.2. Bind loopback thì Prometheus phải chạy cùng máy hoặc đặt `0.0.0.0:<cổng>` kèm tường lửa.

```yaml
scrape_configs:
  - job_name: strand
    static_configs:
      - targets: ['10.0.0.5:9464', '10.0.0.5:9465']
```

- **Log**: pipeline systemd: `journalctl -u strand@<name>` (log JSON mặc định); chế độ nền: `<resources.spool.path>/strand.log`; Web UI: `journalctl -u strand-web`.
- Trạng thái nhanh: `strand top -c <cfg>` (chạy bằng user `strand`, hoặc user cùng quyền với socket quản trị), hoặc Web UI.

## 8. Nâng cấp, gỡ cài đặt, sao lưu

**Nâng cấp** (cài gói mới đè lên):

```bash
sudo dpkg -i strand_<ver_moi>_amd64.deb
sudo systemctl restart strand@orders-sync     # từng pipeline, khi thuận tiện
sudo systemctl restart strand-web
```

Pipeline đang chạy giữ binary cũ tới khi restart (an toàn nhờ checkpoint; gói in danh sách unit cần restart). `restart strand-web` không dừng các pipeline chế độ nền (mục 5.3); chúng dùng binary mới sau lần Restart kế tiếp. Cài tay: thay `/usr/local/bin/strand` rồi restart.

**Gỡ**: `sudo apt remove strand` / `sudo rpm -e strand`. Gói dừng và disable mọi `strand@*` và `strand-web`, và `strand stop` các pipeline chế độ nền (cấu hình trong `/etc/strand/*.yaml` và `/var/lib/strand-web/pipelines/*.yaml`). Gói **không** xóa `/etc/strand`, `/var/lib/strand`, `/var/lib/strand-web` hay user `strand`, kể cả khi purge. Xóa hẳn là việc của bạn:

```bash
sudo rm -rf /etc/strand /var/lib/strand /var/lib/strand-web
sudo userdel strand; sudo groupdel strand
```

Trước đó nên dừng pipeline êm (`systemctl stop strand@x`, tự ghi checkpoint) và dọn replication slot/publication ở source PG nếu không dùng nữa.

**Sao lưu** (cái cần giữ):

| Đường dẫn | Vì sao |
| --- | --- |
| `/etc/strand/*.yaml`, `*.env`, `web.yaml` | Cấu hình, mật khẩu DB, hash mật khẩu web và token (`*.env` là bí mật) |
| `/var/lib/strand-web/` | `audit.log`, lịch sử job, trạng thái wizard, pipeline tạo từ web |
| `/var/lib/strand/<name>/` | State và spool cục bộ; mất thì pipeline phải snapshot lại phần local (checkpoint chính vẫn ở target) |

## 9. Xử lý sự cố thường gặp với web

| Triệu chứng | Nguyên nhân và cách xử lý |
| --- | --- |
| `strand-web` thoát ngay, log `no admin password set` (exit 2) | Chưa đặt mật khẩu: `sudo strand web passwd --config /etc/strand/web.yaml` |
| Log `read web config ... permission denied` | User `strand` không đọc được `web.yaml`: `sudo chown root:strand /etc/strand/web.yaml; sudo chmod 0640 /etc/strand/web.yaml` (làm một lần; `passwd`/`token` giữ nguyên quyền này) |
| `passwd`/`token` báo `preserve owner ...` | Chạy không đủ quyền để giữ chủ sở hữu file: chạy lại bằng `sudo` |
| `listen "0.0.0.0:..." is not a loopback address: TLS (tls_cert, tls_key) is required` | Bind khác loopback cần TLS: đặt cả `tls_cert` và `tls_key`; hoặc trong mạng tin cậy thêm `allow_plain_http: true` (`strand web init --listen 0.0.0.0:8080 --allow-plain-http --force`); hoặc dùng `127.0.0.1` + reverse proxy/SSH tunnel |
| `tls_cert and tls_key must be set together` | Thiếu một trong hai khóa |
| Pipeline hiện `unreachable` hoặc `stopped` dù đang chạy | Web (user `strand`) không đọc được YAML/`.env` hoặc không vào được admin socket vì pipeline chạy bằng user khác (ví dụ root). Chạy pipeline bằng user `strand`, kiểm quyền file; xem mục 5.3 |
| Trang chi tiết báo lỗi cấu hình (`config error`) | Web không đọc/parse được YAML pipeline; kiểm quyền và `strand validate -c` |
| Tab Logs báo lỗi `journalctl` | `sudo usermod -aG systemd-journal strand` rồi restart `strand-web` |
| Start/Stop trả 502 `Interactive authentication required` | Thiếu quy tắc polkit `50-strand.rules` (cài tay: mục 2b), hoặc polkit < 0.106 không đọc rules JS |
| Enable/Disable trả 502 | Đúng thiết kế (mục 5.3): chạy `sudo systemctl enable|disable strand@<name>` trên máy chủ |
| Lưu cấu hình lỗi `read-only file system`, hoặc pipeline nền do web khởi động lỗi ghi spool | `ProtectSystem=strict` của unit web: xem mục 5.3 (drop-in `ReadWritePaths`, hoặc đặt spool dưới `/var/lib/strand-web`) |
| `observability.admin_socket is too long: N bytes (Unix socket path limit is 107)` | Đường dẫn socket tối đa 107 byte. Đặt đường dẫn ngắn, ví dụ `/run/strand/<name>/admin.sock`. Pipeline tạo từ web tự chọn `<state_dir>/sock/<tên>.sock`, rồi `/run/strand/<tên>.sock`, rồi `<tmp>/strand-<tên>.sock` |
| Pipeline thứ hai thoát ngay với `start metrics server: listen on ...: address already in use` | Trùng cổng `observability.metrics` (lỗi này làm `sync` dừng hẳn). Mỗi pipeline một cổng; trên web, cổng trùng bị từ chối 409 |
| Đăng nhập sai nhiều lần ra `429` | Quá 5 lần sai/phút từ một IP thì khóa 1 phút, tái phạm gấp đôi, tối đa 15 phút. Đợi hết `retry_after_s` (xem `audit.log`). Sau reverse proxy mà ai cũng bị khóa chung một IP: bật `trust_forwarded_for` (và để proxy đặt đúng `X-Forwarded-For`) |
| `403` khi đăng nhập/POST bằng curl | Thiếu token CSRF (mục 6.7); script nên dùng Bearer token |
| Trình duyệt báo `missing or invalid CSRF token` khi đăng nhập | Bản mới chấp nhận form đăng nhập khi trình duyệt xác nhận cùng origin (`Origin`/`Sec-Fetch-Site`), kể cả cookie `strand_login` thiếu hoặc cũ; vẫn gặp thì: tải lại trang, kiểm tra proxy giữ nguyên header `Host` (`proxy_set_header Host $host`), xem lý do trong `audit.log` (`login_csrf_rejected`, `reason`) |
| Quên mật khẩu | Chạy lại `sudo strand web passwd --config /etc/strand/web.yaml`, sửa quyền file, restart `strand-web` (session cũ mất). Không có cách khôi phục mật khẩu cũ (chỉ lưu bcrypt hash) |
| Quên/lộ token | Chạy lại `strand web token`: token mới thay token cũ |
| Pipeline `failed` ở chế độ nền và không tự chạy lại | Đúng thiết kế: chế độ nền không có supervisor (`auto_restart=false`). Đọc `last_exit`/tab Errors, sửa nguyên nhân, bấm Start. Muốn tự restart khi lỗi tạm: dùng `strand@<name>` (`Restart=on-failure`) |
| `systemctl stop strand-web` nhưng pipeline nền vẫn chạy | Đúng thiết kế (`KillMode=process`): dừng bằng nút Stop hoặc `strand stop -c <cfg>` |

Sự cố của chính pipeline (lag, `log_gap`, `missing_pk`, exit code): [USAGE.md](USAGE.md) 8.3 và 10.2.

## 10. Chạy bằng docker compose

Triển khai thật bằng container: `compose.yaml` ở gốc repo chạy hai dịch vụ, `db` (PostgreSQL 17, lưu **metadata của Web UI**) và `web` (`strand web` + các pipeline chế độ nền chạy trong chính container này). Cần Docker Engine với Compose v2. Đây là kho metadata của web, **không** phải DB nguồn hay đích của pipeline.

### 10.1 Chạy nhanh (3 lệnh)

```bash
cp .env.example .env                       # đặt POSTGRES_PASSWORD (openssl rand -hex 24) và, nên có, STRAND_WEB_SECRET_KEY (openssl rand -hex 32)
docker compose up -d --build               # build image, db khỏe rồi mới chạy web
docker compose logs web | grep -A1 "GENERATED ADMIN PASSWORD"   # chỉ cần khi KHÔNG đặt STRAND_WEB_ADMIN_PASSWORD
```

Mở `http://<host>:8080` và đăng nhập bằng mật khẩu admin. Nếu `STRAND_WEB_ADMIN_PASSWORD` trống, entrypoint sinh mật khẩu ngẫu nhiên và in **một lần** ở lần khởi động đầu (khi database chưa có admin); các lần sau không in và không đổi gì. Kiểm tra: `docker compose ps` (cả hai `healthy`), `curl -s http://localhost:8080/healthz`.

### 10.2 Biến môi trường

Mọi biến nằm trong `.env.example` (kèm giải thích). Các biến chính:

| Biến | Ý nghĩa |
| --- | --- |
| `POSTGRES_PASSWORD` | Bắt buộc. Mật khẩu của `db`; nằm trong DSN dạng URL nên dùng chữ-số (`openssl rand -hex 24`). |
| `STRAND_WEB_DATABASE_DSN` | DSN metadata (compose tự dựng từ `POSTGRES_PASSWORD`). Thắng khóa `database.dsn` của `web.yaml`. Không bao giờ được ghi vào log; lỗi kết nối đã che mật khẩu. |
| `STRAND_WEB_SECRET_KEY` | Khóa AES-256-GCM (64 ký tự hex, hoặc base64 32 byte, hoặc passphrase từ 16 ký tự) để lưu `.env` của pipeline (mật khẩu nguồn/đích) **mã hóa** trong DB. Không đặt: `.env` chỉ nằm trên volume `/etc/strand`, DB không giữ bí mật nào của pipeline (web ghi một dòng log nói rõ). Mất khóa thì `.env` trong DB không giải mã được: giữ khóa ngoài bản sao lưu DB. Khởi động với khóa **khác** khóa đã mã hóa các `.env` trong DB: web từ chối khởi động với lỗi nói rõ (không chạy tiếp với bản sao không giải mã được). |
| `STRAND_WEB_SECRET_KEY_PREVIOUS` | Chỉ khi **đổi khóa**: đặt khóa mới vào `STRAND_WEB_SECRET_KEY`, khóa cũ vào biến này, khởi động lại; web mã hóa lại mọi `.env` trong DB bằng khóa mới (log `re-encrypted ...`), sau đó xóa biến này. |
| `STRAND_WEB_ADMIN_PASSWORD` hoặc `STRAND_WEB_ADMIN_PASSWORD_FILE` | Mật khẩu admin, chỉ dùng khi database chưa có admin (tối thiểu 8 ký tự). `_FILE` dành cho docker secrets (`/run/secrets/...`), tránh để mật khẩu lộ qua `docker inspect`. Đổi sau này: `docker compose exec web strand web passwd`; token script: `docker compose exec web strand web token`. |
| `STRAND_WEB_BIND`, `STRAND_WEB_PORT` | Địa chỉ/cổng publish trên host (mặc định `0.0.0.0:8080`). |
| `STRAND_WEB_ALLOW_PLAIN_HTTP`, `STRAND_WEB_TLS_CERT`, `STRAND_WEB_TLS_KEY`, `STRAND_WEB_SECURE_COOKIES`, `STRAND_WEB_TRUST_FORWARDED_FOR` | Mạng và TLS, xem 10.5. |
| `WEB_CPUS`, `WEB_MEMORY`, `DB_CPUS`, `DB_MEMORY` | Giới hạn tài nguyên (mặc định web 2 CPU/2 GB, db 1 CPU/1 GB). Log container giới hạn 10 MB x 5 file. |

### 10.3 Dữ liệu nằm ở đâu (volume)

| Volume | Chứa | Ghi chú |
| --- | --- | --- |
| `pg_data` | PostgreSQL metadata: tài khoản admin (hash), phiên đăng nhập (chỉ sha256 id), danh sách pipeline do web tạo + YAML cấu hình (+ `.env` mã hóa nếu có khóa) + **trạng thái mong muốn** (running/stopped), lịch sử job, trạng thái wizard, audit log (bảng chỉ-thêm) | nguồn sự thật |
| `strand_config` (`/etc/strand`) | File cấu hình pipeline `<name>.yaml` (0640) và `<name>.env` (0600), được **dựng lại từ DB** trước khi validate/start nếu thiếu hoặc khác | thả thêm file YAML vào đây thì web tự quét (`scan`) và **đăng ký vào DB** ở lần quét đầu |
| `strand_state` (`/var/lib/strand-web`) | Spool, state, checkpoint cục bộ của pipeline, spool output job đang chạy (`jobs/<id>.log`, chuyển vào bảng `job_output` của DB rồi xóa khi job xong), bản sao `audit.log` | mất thì pipeline phải dựng lại phần local (checkpoint chính ở target) |

Pipeline chạy **trong container web** ở chế độ nền (không có systemd). Container dừng (`docker compose stop/restart`, `down`): web dừng êm các pipeline đang chạy (chờ ghi checkpoint, tối đa ~100 giây; `stop_grace_period: 120s`) mà **không đổi trạng thái mong muốn**; khi container chạy lại, web tự chạy lại mọi pipeline chế độ nền có trạng thái mong muốn `running` mà chưa chạy (audit `autostart`). Bấm Stop trên web đặt trạng thái `stopped` nên sẽ không tự chạy lại. Dừng **ngoài web** cũng vậy: `strand stop` (hoặc SIGTERM) từ CLI làm pipeline thoát êm ("pipeline stopped cleanly"), web active nhận ra (kể cả khi lúc đó web không chạy: kiểm lúc khởi động), đặt trạng thái mong muốn `stopped` (audit `desired.observed_stop`, trang ghi "stopped outside the web") và không tự chạy lại. Pipeline **chết** (thoát lỗi, bị kill/OOM) vẫn giữ `running` và được chạy lại. Phiên đăng nhập nằm trong DB nên khởi động lại web không đăng xuất ai (hạn idle 30 phút / tối đa 12 giờ như cũ).

Nguồn/đích ở máy khác: dùng IP/DNS của chúng trong cấu hình pipeline (container đi ra ngoài bình thường; MySQL nguồn phải cho phép kết nối từ host docker). Nguồn/đích là dịch vụ trên chính host docker: bỏ comment `extra_hosts: ["host.docker.internal:host-gateway"]` trong `compose.yaml` và dùng host `host.docker.internal`; hoặc `network_mode: host` cho `web` (khi đó bỏ `ports`, web nghe trực tiếp trên host, và `db` phải publish cổng 5432 cho web, DSN trỏ `127.0.0.1`). Nguồn/đích là container cùng máy: gắn `web` vào network của chúng (`networks:` ngoài) và dùng tên service.

### 10.4 Sao lưu, nâng cấp

**Sao lưu** = `pg_dump` của metadata + volume pipeline + khóa `STRAND_WEB_SECRET_KEY`:

```bash
docker compose exec -T db pg_dump -U strand -Fc strand > strand-web-$(date +%F).dump
docker run --rm -v strand_strand_config:/c -v strand_strand_state:/s -v "$PWD":/b debian:bookworm-slim \
  tar czf /b/strand-volumes-$(date +%F).tgz /c /s          # tên volume: <tên project>_<volume>, xem docker volume ls
# khôi phục metadata: docker compose exec -T db pg_restore -U strand -d strand --clean --if-exists < strand-web-XXXX.dump
```

**Nâng cấp**: `git pull && docker compose up -d --build`. Web mới áp dụng migration schema còn thiếu lúc khởi động (đánh số `NNN_tên.sql`, mỗi file một transaction, bảng `schema_migrations`, advisory lock nên hai instance khởi động cùng lúc vẫn an toàn). Pipeline được dừng êm khi container cũ dừng và tự chạy lại bằng binary mới. Nâng cấp major của PostgreSQL (17 lên 18) cần dump/restore, không đổi tag tại chỗ.

**Chuyển từ cài bằng gói (file) sang compose**: mount thư mục cũ của `state_dir` và `/etc/strand` vào đúng đường dẫn trong container; lần khởi động đầu với DSN web tự nhập trạng thái file đúng một lần (USAGE.md 7.15). Muốn xem trước và có báo cáo kiểm chứng: `docker compose run --rm web strand web db migrate --config <web.yaml cũ> --dry-run`, rồi bỏ `--dry-run` (mục 11).

### 10.5 TLS và cổng

Mặc định web nghe `0.0.0.0:8080` bằng HTTP thường (`allow_plain_http`): mật khẩu và cookie đi dạng rõ. Chỉ dùng trong mạng tin cậy. Hai cách có TLS:

- **Reverse proxy (khuyến nghị)**: đặt `STRAND_WEB_BIND=127.0.0.1`, `STRAND_WEB_SECURE_COOKIES=true`, `STRAND_WEB_TRUST_FORWARDED_FOR=true`, rồi proxy (nginx, Caddy, Traefik) kết thúc TLS và chuyển tới `127.0.0.1:8080` (mục 5.4).
- **TLS tại web**: mount chứng chỉ (`./certs:/certs:ro` trong `compose.yaml`) và đặt `STRAND_WEB_TLS_CERT=/certs/tls.crt`, `STRAND_WEB_TLS_KEY=/certs/tls.key`, `STRAND_WEB_ALLOW_PLAIN_HTTP=false`. Healthcheck tự chuyển sang HTTPS.

Nếu cần `web.yaml` riêng (ví dụ `pipelines:` khai báo tường minh), mount file vào container và đặt `STRAND_WEB_CONFIG=/etc/strand/web.yaml`: entrypoint khi đó không sinh file mà dùng file của bạn (vẫn đọc DSN và khóa từ biến môi trường).

### 10.6 Sự cố thường gặp (compose)

| Triệu chứng | Nguyên nhân / cách xử lý |
| --- | --- |
| `web` khởi động lại liên tục, log `connect to metadata database` | `db` chưa sẵn sàng hoặc sai `POSTGRES_PASSWORD` (đổi mật khẩu sau khi `pg_data` đã tạo thì không có hiệu lực: `ALTER USER strand PASSWORD '...'`, hoặc `docker compose down -v` nếu chưa có dữ liệu). Web tự thử lại tối đa 60 giây với backoff. |
| `/healthz` trả 503 | Mất kết nối DB; web vẫn chạy nhưng đăng nhập/thao tác ghi sẽ lỗi (audit từ chối thao tác ghi khi không ghi được). Pipeline đang chạy không bị ảnh hưởng. |
| `web` không khởi động, log `STRAND_WEB_SECRET_KEY does not match the key ...` | Khóa đã đổi so với lúc mã hóa `.env` trong DB. Dùng lại khóa cũ, hoặc đổi khóa đúng cách với `STRAND_WEB_SECRET_KEY_PREVIOUS` (10.2). |
| `.env` của pipeline không còn sau khi tạo lại container | Chưa đặt `STRAND_WEB_SECRET_KEY` nên DB không giữ `.env`; đặt khóa rồi mở tab Config > Env và lưu lại để đưa vào DB. |
| Sửa tay `/etc/strand/<name>.yaml`, bị thay bằng bản DB | DB là nguồn sự thật (mọi pipeline đã đăng ký, kể cả từ `scan`): bản sửa tay được cất thành `<name>.yaml.conflict-<thời gian>`, trang pipeline có banner **Adopt disk version** để dùng bản đó (USAGE.md 7.15 "Registry trong DB"). Nên sửa qua web (Config) hoặc API. |

## 11. Chuyển từ chế độ file sang PostgreSQL (và quay lại)

Không xóa hay sửa file nào của bản cài file; chạy lại an toàn (dừng giữa chừng thì chạy lại sẽ tiếp tục).

```bash
# 0. Tạo database và user (trên máy PostgreSQL)
sudo -u postgres psql -c "CREATE USER strand_web PASSWORD '...'" -c "CREATE DATABASE strand_web OWNER strand_web"
export STRAND_WEB_DATABASE_DSN='postgres://strand_web:...@db-host:5432/strand_web'   # tránh để DSN lộ qua ps
export STRAND_WEB_SECRET_KEY=$(openssl rand -hex 32)   # để .env của pipeline được lưu (mã hóa) trong DB; giữ khóa này

# 1. Xem trước: không ghi gì vào DB (kể cả lược đồ)
sudo -E -u strand strand web db migrate --config /etc/strand/web.yaml --dry-run

# 2. Dừng web, nhập và kiểm chứng
sudo systemctl stop strand-web
sudo -E -u strand strand web db migrate --config /etc/strand/web.yaml

# 3. Bật chế độ DB: thêm database.dsn vào web.yaml (hoặc Environment=STRAND_WEB_DATABASE_DSN=... và
#    STRAND_WEB_SECRET_KEY=... trong drop-in của strand-web.service), rồi
sudo systemctl start strand-web
```

Kế hoạch in theo loại (`admin`, `api_token`, `pipeline` gồm `pipelines.json`, `pipelines:` của web.yaml và file `scan`, `job`, `job_output`, `wizard`, `audit`) với số `new` / `present` / `conflict` / `skipped`; các dòng `conflict` (DB đã có giá trị khác; bản DB được giữ) và `skipped` (file không đọc được) được liệt kê. Sau khi nhập, lệnh lập lại kế hoạch trên DB để **kiểm chứng**: mọi mục `new` phải thành `present` (cấu hình so bằng sha256, job/wizard theo id, audit theo số dòng); có mục chưa đạt thì in `PROBLEM:` và thoát với mã khác 0. Cài đặt trong `web.yaml` (scan, config_dir, ...) không bị chép: chúng vẫn là giá trị của `web.yaml` cho tới khi được đổi trên trang Settings.

**Quay lại chế độ file** (hoặc sao lưu dạng file):

```bash
sudo -E -u strand strand web db export --config /etc/strand/web.yaml --dir /var/backups/strand-web-$(date +%F)
```

Thư mục xuất ra có `web.yaml` (hash mật khẩu/token và cài đặt đã lưu trên trang Settings, không có mục `database`), `state/` (`pipelines.json`, `jobs/<id>.json|.log`, `wizards/`, `audit.log`) và `pipelines/<name>.yaml|.env` kèm `MANIFEST.json` (đường dẫn gốc; `.env` chỉ được giải mã khi có `STRAND_WEB_SECRET_KEY`). Quay lại: dừng web, chép `state/*` vào `state_dir`, chép cấu hình về đường dẫn trong `MANIFEST.json` nếu khác, dùng `web.yaml` vừa xuất (bỏ `STRAND_WEB_DATABASE_DSN`), khởi động web.

## 12. Vận hành chế độ PostgreSQL: lưu giữ audit, nhiều instance web

### 12.1 Lưu giữ audit

Bảng `audit_log` chỉ-thêm nên không tự nhỏ lại. Đặt thời gian giữ trên trang Settings (`audit_retention_days`, mặc định 365 ngày; 0 = giữ mãi), rồi định kỳ:

```bash
sudo -E -u strand strand web db audit-archive --config /etc/strand/web.yaml --dry-run       # xem trước
sudo -E -u strand strand web db audit-archive --config /etc/strand/web.yaml                 # mặc định: cũ hơn audit_retention_days
sudo -E -u strand strand web db audit-archive --config /etc/strand/web.yaml --before 2025-01-01 --out /backup/audit-2024.jsonl.gz
# compose: docker compose exec web strand web db audit-archive
```

Lệnh xuất các dòng cũ ra `.jsonl.gz` (mặc định trong `audit_archive_dir`, rỗng = `<state_dir>/audit-archive`), kiểm chứng file (số dòng, id, sha256), ghi `<file>.sha256` và `<file>.manifest.json`, rồi mới xóa đúng các dòng đó qua hàm `strand_audit_archive()`; dòng `audit.archive` (số dòng, khoảng thời gian, tên file, sha256) và bảng `audit_archives` giữ dấu vết vĩnh viễn. Có dòng mới chen vào khoảng đã xuất thì không xóa gì (chạy lại). Kiểm một file lưu trữ: `sha256sum -c audit-....jsonl.gz.sha256`; đọc: `zcat audit-....jsonl.gz | jq .`. Muốn web tự làm hằng ngày: bật `audit_archive_auto` (chỉ instance active chạy). Chép file lưu trữ ra nơi sao lưu: Strand không bao giờ xóa chúng.

Chế độ file: `audit.log` tự xoay khi vượt `audit_rotate_mb` (mặc định 100 MiB) hoặc, nếu đặt, `audit_rotate_days` thành `audit-<thời điểm>.log.gz` + `.sha256` trong `state_dir`; `audit.log` mới bắt đầu bằng dòng `audit.rotate` chứa sha256 của file trước (WEB-API.md §5.1).

### 12.2 Hai (hoặc nhiều) instance web trên một database

Được hỗ trợ ở dạng **active / standby**: một instance giữ lease (bảng `web_lease`) và làm mọi việc ghi; instance khác chạy **standby chỉ đọc** (xem được, đăng nhập được, thao tác ghi trả 503 kèm địa chỉ instance active, có banner), và **tự tiếp quản** khi lease hết hạn (mặc định 30 s, `STRAND_WEB_LEASE_TTL`) hoặc ngay khi instance active dừng êm. Lý do chọn standby thay vì từ chối khởi động: nâng cấp không gián đoạn và có máy dự phòng nóng mà không bao giờ có hai instance cùng tự chạy lại pipeline, cùng ghi lịch sử số liệu hay đánh dấu job của nhau là `interrupted`. `/healthz` có `role` và `lease` (instance standby vẫn 200; load balancer nên gửi người dùng tới instance có `role: active`, hoặc dùng sticky session: đăng nhập dùng được ở cả hai).

Lưu ý: pipeline chế độ nền là tiến trình con **của instance đã khởi động nó** (trong container của nó). Standby ở container/máy khác không thấy chúng (hiển thị là không chạy). Khi tiếp quản, nó chạy các pipeline có trạng thái mong muốn `running` trên máy của nó; nếu instance cũ vẫn sống nhưng mất DB, pipeline cũ vẫn chạy và bản thứ hai bị khóa pipeline chặn (`pipeline_locked`, exit 5). Pipeline systemd không phụ thuộc instance web nào.

Kiểm tra nhanh: `curl -s http://web-a:8080/healthz | jq .role` và `... web-b ... | jq .role`; `SELECT holder, epoch, expires_at FROM web_lease; SELECT id, role, last_seen FROM web_instances;`.

