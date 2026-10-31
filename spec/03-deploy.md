# Strand CDC — 03 Deployment Guide

Oct 31, 2026 · @hieuny

## 1. Yêu cầu chung

Mỗi pipeline chạy một process `strand sync`, là một binary Go tĩnh. Pipeline cần kết nối tới source (nên là replica), target, VictoriaMetrics, và object storage nếu dùng chế độ B hoặc spool tầng bucket. Tên gọi theo glossary trong tài liệu 02; ma trận phiên bản theo tài liệu 01, mục 2.

| Hạng mục | Yêu cầu |
| --- | --- |
| Kiến trúc | linux/amd64, linux/arm64 |
| RAM | Request 1–2 GB, limit 4 GB. Chốt lại sau khi đo ở P0-08 |
| Spool | SSD/NVMe local có fsync, không dùng NFS. Chế độ B cần dung lượng ≥ tốc độ sinh log ở peak × ETA restore × 2 |
| Mạng ra | Source 3306/5432, target, S3 endpoint, VictoriaMetrics query API (chỉ khi dùng metrics\_source) |
| Mạng vào | `:9464` cho scrape metric. Admin socket chỉ là Unix socket local |
| Đồng hồ | NTP đồng bộ. Lag tính từ timestamp của event nên lệch giờ làm sai `strand_lag_seconds` |

**Cấu hình source MySQL** (`validate` kiểm):

```ini
binlog_format            = ROW
binlog_row_image         = FULL
binlog_row_value_options = ''      # tắt partial JSON
binlog_row_metadata      = FULL    # khuyến nghị
gtid_mode                = ON
enforce_gtid_consistency = ON
log_replica_updates      = ON      # khi Strand đọc từ replica
```

Retention binlog (trên source hoặc trong archive) phải ≥ 1,5 × (tuổi backup + thời gian restore) nếu dùng chế độ B. Mỗi pipeline cần một `server_id` riêng, không trùng với bất kỳ replica nào.

**Cấu hình source PostgreSQL:** `wal_level=logical`, đủ `max_replication_slots` và `max_wal_senders` cho số pipeline. Nên đặt `max_slot_wal_keep_size` để một slot bị treo không làm đầy đĩa primary. Decode trên standby cần PG 16+ và `hot_standby_feedback=on`, đổi lại có thể gây bloat trên primary.

**Chặn ứng dụng ghi vào target trước khi promote:**

- **MySQL:** bật `read_only=ON` nhưng **tắt** `super_read_only`. User của Strand có `CONNECTION_ADMIN` nên vẫn ghi được; user của ứng dụng thì không. `super_read_only=ON` chặn mọi client, kể cả Strand, chỉ chừa replication thread nội bộ, nên dùng nó thì Strand không apply được.
- **PostgreSQL:** đặt `default_transaction_read_only=on` cho role của ứng dụng và chưa cấp quyền ghi. Session của Strand tự mở transaction read-write.

Thiết kế rev 89 ghi `super_read_only` cho bước 2 của promote. Điểm này được sửa trong rev 90.

## 2. Quyền DB tối thiểu

Mỗi pipeline dùng hai user: `strand` cho đọc và apply hằng ngày, `strand_admin` chỉ dùng cho `promote` (fence source, mở ghi target). Tách user để credential chạy thường trực không thể mở quyền ghi cho ứng dụng. Mọi kết nối bắt buộc TLS (mục 7).

**MySQL source** (nên là replica):

```sql
CREATE USER 'strand'@'10.%' IDENTIFIED WITH caching_sha2_password BY '<secret>' REQUIRE SSL;
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'10.%';
GRANT SELECT ON app.* TO 'strand'@'10.%';          -- snapshot chế độ A, verify
-- Chỉ khi seed bằng CLONE: BACKUP_ADMIN trên donor
```

**MySQL target:**

```sql
CREATE USER 'strand'@'10.%' IDENTIFIED WITH caching_sha2_password BY '<secret>' REQUIRE SSL;
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON app.* TO 'strand'@'10.%';
GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'10.%';  -- ghi được khi read_only=ON
-- strand_admin dùng cho promote:
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'strand_admin'@'10.%';  -- đổi read_only ở target, super_read_only ở source cũ
```

**PostgreSQL source** (primary, hoặc standby PG 16+):

```sql
CREATE ROLE strand LOGIN REPLICATION PASSWORD '<secret>';
GRANT SELECT ON ALL TABLES IN SCHEMA app TO strand;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA app TO strand;   -- đồng bộ last_value cho promote
GRANT pg_monitor TO strand;                             -- wait event cho governor
-- Publication do DBA tạo trước (cần quyền owner của bảng):
CREATE PUBLICATION strand_orders_sync FOR TABLE app.orders, app.users;
```

Trên primary, `pg_hba.conf` cần một dòng `hostssl replication strand <cidr> scram-sha-256` cho kết nối replication.

**PostgreSQL target:**

```sql
CREATE ROLE strand LOGIN PASSWORD '<secret>';
GRANT CONNECT, TEMPORARY ON DATABASE appdb TO strand;   -- bảng tạm cho COPY + MERGE
GRANT USAGE, CREATE ON SCHEMA app TO strand;            -- tạo _strand_* và apply DDL
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO strand;
GRANT UPDATE ON ALL SEQUENCES IN SCHEMA app TO strand;  -- setval khi promote
-- Role của ứng dụng: chỉ SELECT cho đến khi promote
ALTER ROLE app_rw SET default_transaction_read_only = on;
```

Thiếu quyền nào thì `validate` báo `insufficient_privilege` và thoát với exit code 3. Quyền trên bảng mới tạo về sau cần `ALTER DEFAULT PRIVILEGES`, nếu không schema evolution ở P3 sẽ dừng lại.

## 3. Docker compose (dev, CI, PoC)

Compose chỉ dùng cho máy dev, CI e2e và PoC, không dùng cho production vì không có fencing hạ tầng và spool nằm trên named volume. Toxiproxy đứng giữa Strand và source để test fault injection (tài liệu 04). Profile `mysql` và `pg` cho phép chỉ bật engine cần thiết. Mục tiêu P0-01 là `make up` dựng sạch dưới 5 phút.

```yaml
# compose.yaml
services:
  mysql-src:
    image: mysql:8.4
    profiles: [mysql]
    command: >
      --server-id=1 --gtid-mode=ON --enforce-gtid-consistency=ON
      --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL
    environment: { MYSQL_ROOT_PASSWORD: dev }
  mysql-dst:
    image: mysql:8.4
    profiles: [mysql]
    command: --server-id=2 --read-only=ON
    environment: { MYSQL_ROOT_PASSWORD: dev }
  pg-src:
    image: postgres:18
    profiles: [pg]
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=8"]
    environment: { POSTGRES_PASSWORD: dev }
  pg-dst:
    image: postgres:18
    profiles: [pg]
    environment: { POSTGRES_PASSWORD: dev }
  toxiproxy:
    image: ghcr.io/shopify/toxiproxy
    volumes: ["./dev/toxiproxy.json:/config/toxiproxy.json:ro"]
    command: ["-host=0.0.0.0", "-config=/config/toxiproxy.json"]
  victoriametrics:
    image: victoriametrics/victoria-metrics
    command: ["-promscrape.config=/etc/vm/scrape.yaml"]
    volumes: ["./dev/scrape.yaml:/etc/vm/scrape.yaml:ro"]
    ports: ["8428:8428"]
  grafana:
    image: grafana/grafana
    ports: ["3000:3000"]
  strand:
    build: .
    command: ["sync", "-c", "/etc/strand/pipeline.yaml"]
    volumes:
      - "./dev/pipeline.yaml:/etc/strand/pipeline.yaml:ro"
      - "strand-spool:/var/lib/strand"
    env_file: ["./dev/strand.env"]          # SRC_DSN trỏ vào toxiproxy, không trỏ thẳng source
    stop_grace_period: 2m
volumes:
  strand-spool: {}
```

- Muốn chạy MySQL 9.7 thì đổi image sang `mysql:9.7`. CI chạy ma trận phiên bản bằng biến môi trường, xem tài liệu 04.
- `stop_grace_period` phải lớn hơn thời gian Strand xả writer và ghi checkpoint khi nhận SIGTERM.
- Script init tạo user và publication theo mục 2 nằm trong `dev/init/`.

## 4. VM / bare metal

Mỗi pipeline là một instance của unit template `strand@.service`. P1 phát hành dạng tarball (binary + checksum SHA-256 + chữ ký); gói `.deb`/`.rpm` có từ P3. Luôn kiểm checksum và chữ ký trước khi cài.

| Đường dẫn | Nội dung | Quyền |
| --- | --- | --- |
| `/usr/local/bin/strand` | Binary | root:root 0755 |
| `/etc/strand/<pipeline>.yaml` | Cấu hình pipeline, không chứa mật khẩu | root:strand 0640 |
| `/etc/strand/<pipeline>.env` | `SRC_DSN`, `DST_DSN`, key S3 | root:strand 0600 |
| `/var/lib/strand/<pipeline>/` | Spool, trên SSD/NVMe local | strand:strand 0700 |
| `/run/strand/<pipeline>/admin.sock` | Admin socket cho `top`, `pause`, `throttle` | strand:strand 0700 |

```ini
# /etc/systemd/system/strand@.service
[Unit]
Description=Strand CDC pipeline %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=strand
Group=strand
EnvironmentFile=/etc/strand/%i.env
ExecStart=/usr/local/bin/strand sync -c /etc/strand/%i.yaml
KillSignal=SIGTERM
TimeoutStopSec=120
Restart=on-failure
RestartSec=10
# Không restart khi lỗi cần người xử lý: cấu hình, validate, locked/fenced, log_gap, chờ quyết định
RestartPreventExitStatus=2 3 4 5 10
StateDirectory=strand/%i
RuntimeDirectory=strand/%i
MemoryMax=4G
LimitNOFILE=65536
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable --now strand@orders-sync
journalctl -u strand@orders-sync -f        # log JSON
strand top --socket /run/strand/orders-sync/admin.sock
```

- `RestartPreventExitStatus` dựa vào bảng exit code ở tài liệu 02, mục 10. Restart tự động sau `fenced` hay `log_gap` chỉ tạo vòng lặp và che mất sự cố.
- `TimeoutStopSec=120` là giá trị ban đầu. Đo thời gian dừng an toàn thật ở P1 rồi đặt lại.
- `admin_socket` trong YAML đặt thành `/run/strand/<pipeline>/admin.sock` cho khớp `RuntimeDirectory`.
- Cài MySQL từ repo Oracle trên cùng máy: ghim dòng 8.4 hoặc 9.7 để tránh lỗi Bug #120315, vốn lặng lẽ chuyển repo từ 8.4 sang 9.7 (tài liệu 01, mục 3).

## 5. FKE

Từ P1 đến P3, mỗi pipeline là một StatefulSet `replicas: 1` có PVC block storage. Bộ manifest quản lý bằng Kustomize qua ArgoCD; Helm chart chính thức có ở P4. StatefulSet không tạo lại pod khi node mất liên lạc cho đến khi node được xác nhận đã mất. Nhưng khi người vận hành force-delete pod, vẫn có thể có hai pod. Lúc đó fencing bằng `epoch` (tài liệu 02, mục 4) chặn pod cũ.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: strand-orders-sync, namespace: strand }
spec:
  replicas: 1
  serviceName: strand-orders-sync
  selector: { matchLabels: { app: strand, pipeline: orders-sync } }
  template:
    metadata: { labels: { app: strand, pipeline: orders-sync } }
    spec:
      terminationGracePeriodSeconds: 150     # > thời gian dừng an toàn đo ở P1
      securityContext: { runAsNonRoot: true, runAsUser: 10001, fsGroup: 10001 }
      containers:
        - name: strand
          image: <registry>/strand:<version>
          args: ["sync", "-c", "/etc/strand/pipeline.yaml"]
          envFrom: [{ secretRef: { name: strand-orders-sync } }]   # SRC_DSN, DST_DSN, key S3
          ports: [{ name: metrics, containerPort: 9464 }]
          livenessProbe: { httpGet: { path: /healthz, port: metrics }, periodSeconds: 10, failureThreshold: 6 }
          resources:
            requests: { cpu: "1", memory: 2Gi }
            limits: { memory: 4Gi }
          securityContext: { readOnlyRootFilesystem: true, allowPrivilegeEscalation: false }
          volumeMounts:
            - { name: config, mountPath: /etc/strand, readOnly: true }
            - { name: spool, mountPath: /var/lib/strand }
            - { name: run, mountPath: /run/strand }
      volumes:
        - { name: config, configMap: { name: strand-orders-sync } }
        - { name: run, emptyDir: {} }
  volumeClaimTemplates:
    - metadata: { name: spool }
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: <block-storage-class-cua-FKE>
        resources: { requests: { storage: 50Gi } }   # = resources.spool.hard
```

- **Không dùng PDB `minAvailable: 1`.** Với một replica, PDB này chặn drain node. Restart do drain vốn an toàn nhờ checkpoint.
- **Không đặt CPU limit**, để tránh throttling CFS làm sai latency mà governor đo được. Memory limit bằng `resources.memory.hard` cộng phần dư cho Pebble.
- **`/healthz` (đặc tả mới, cần thêm vào 02):** trả 200 khi vòng lặp chính còn tiến. Không dùng readiness probe, vì pod không nhận traffic.
- **Không tự restart sau exit code 2–5 và 10:** Kubernetes vẫn restart theo `restartPolicy: Always`. Strand vì vậy phải ghi lại mã lỗi vào metric và log trước khi thoát. Alert `StrandFenced` và `CrashLoopBackOff` là tín hiệu để người vận hành scale về 0 (tài liệu 05).
- **Pod đặt gần source:** dùng node affinity theo zone của replica, vì đọc binlog qua WAN làm tăng lag.

## 6. Observability

Strand expose metric ở `:9464/metrics` theo metrics contract (tài liệu 02, mục 11). VictoriaMetrics scrape metric này, vmalert chạy rule. Governor và breaker không cần VictoriaMetrics: mặc định mọi tín hiệu đọc trực tiếp từ DB. Chỉ khi khai báo metrics\_source tùy chọn thì endpoint `vmselect` phải truy cập được từ pod/VM chạy Strand.

**FKE** (VictoriaMetrics operator):

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMServiceScrape
metadata: { name: strand, namespace: strand }
spec:
  selector: { matchLabels: { app: strand } }
  endpoints: [{ port: metrics, interval: 15s }]
```

Cần thêm một headless Service có port `metrics` cho mỗi StatefulSet.

**VM / bare metal** (`scrape.yaml` của vmagent):

```yaml
scrape_configs:
  - job_name: strand
    scrape_interval: 15s
    static_configs:
      - targets: ["strand-vm-01:9464"]
        labels: { pipeline: orders-sync }
```

**Rule cho vmalert** (ngưỡng ban đầu, tài liệu 05 hiệu chỉnh):

```yaml
groups:
  - name: strand
    rules:
      - alert: StrandCompactionStuck
        expr: strand_checkpoint_compaction_lag_seconds > 5
        for: 1m
      - alert: StrandLagSLO
        expr: strand_lag_seconds{phase="synced"} > 5      # = lag_slo của pipeline
        for: 5m
      - alert: StrandRetentionRisk
        expr: strand_retention_headroom_seconds < 3600
      - alert: StrandVerifyMismatch
        expr: increase(strand_verify_mismatch_total[15m]) > 0
      - alert: StrandFenced
        expr: increase(strand_fenced_total[5m]) > 0
      - alert: StrandBreakerOpen
        expr: strand_breaker_open == 1
        for: 10m
```

`StrandSpoolNearFull` cần biết dung lượng spool. Strand sẽ expose thêm `strand_spool_capacity_bytes{tier}` (bổ sung vào metrics contract) để viết rule `sum(strand_spool_bytes) / sum(strand_spool_capacity_bytes) > 0.8`.

## 7. Bảo mật

Strand đọc toàn bộ dữ liệu của các bảng trong pipeline, nên spool và bucket staging có mức nhạy cảm ngang với chính DB. Các yêu cầu compliance cụ thể vẫn đang mở (tài liệu 01, mục 8). Bảng dưới là mức tối thiểu.

| Lớp | Yêu cầu |
| --- | --- |
| Kết nối DB | TLS bắt buộc, kiểm tra CA và hostname (`sslmode=verify-full` với PG, `ssl-mode=VERIFY_IDENTITY` với MySQL) |
| Credential | Chỉ nằm trong Secret hoặc `EnvironmentFile` 0600; YAML chỉ tham chiếu `${VAR}`; `strand_admin` tách riêng (mục 2) |
| Spool | Pebble không tự mã hóa. Mã hóa ở tầng đĩa: LUKS trên VM, StorageClass có mã hóa trên FKE |
| Bucket staging, archive | Mã hóa phía server, bật versioning; bật Object Lock nếu muốn dùng legal hold cho seed |
| Admin socket | Unix socket 0700, không bao giờ mở qua TCP |
| Metric | Không chứa giá trị dữ liệu hay PK, chỉ có tên bảng |
| Log | Không log giá trị row; lỗi apply chỉ log PK đã hash |
| Masking | `tables.mask` chỉ áp được cho snapshot logic và seed logic. Seed vật lý (XtraBackup, CLONE, pgBackRest) mang nguyên dữ liệu gốc |
| Binary | Kiểm SHA-256 và chữ ký trước khi cài; image chạy non-root, root filesystem read-only |

**NetworkPolicy trên FKE:**

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: strand, namespace: strand }
spec:
  podSelector: { matchLabels: { app: strand } }
  policyTypes: [Ingress, Egress]
  ingress:
    - from: [{ namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: monitoring } } }]
      ports: [{ port: 9464 }]
  egress:
    - to: [{ ipBlock: { cidr: <cidr-cua-source-va-target> } }]
      ports: [{ port: 3306 }, { port: 5432 }]
    - to: [{ ipBlock: { cidr: <cidr-cua-object-storage> } }]
      ports: [{ port: 443 }]
    - to: [{ namespaceSelector: { matchLabels: { kubernetes.io/metadata.name: monitoring } } }]
      ports: [{ port: 8481 }]          # vmselect, chỉ khi dùng metrics_source
    - to: [{ namespaceSelector: {} }]
      ports: [{ port: 53, protocol: UDP }]
```

## 8. Nâng cấp, rollback và chi phí

**Nâng cấp Strand** giống nhau trên cả ba đích: dừng bằng SIGTERM để Strand ghi checkpoint, thay binary hoặc image, rồi chạy lại. Checkpoint nằm trên target nên không phụ thuộc máy chạy Strand.

- **Metadata:** bảng `_strand_pipeline` cần thêm cột `meta_version` (bổ sung vào 02, mục 4). Binary mới tự migrate tiến; binary cũ gặp `meta_version` cao hơn thì từ chối chạy.
- **Rollback binary:** chỉ an toàn khi release notes ghi "không đổi `meta_version`". Nếu đã đổi, rollback phải chạy lệnh migrate lùi do bản mới cung cấp.
- **Spool khác định dạng:** spool có thể xóa bất cứ lúc nào, miễn log trên source hoặc archive còn phủ từ `lwd`. Reader sẽ đọc lại từ checkpoint. Không được xóa spool trong lúc chế độ B đang dựa vào prefetch.
- **Nâng cấp major của PG source:** slot logic chỉ được giữ qua `pg_upgrade` khi nâng từ PG 17 trở lên. Nâng từ PG 16 thì phải tạo lại slot và bootstrap lại pipeline.

**Hủy một pipeline:** `strand pause`, rồi `strand slot drop` (PG) để slot không giữ WAL trên source. Sau đó xóa `_strand_*` trên target và gỡ legal hold nếu còn. Quên drop slot là lỗi vận hành nguy hiểm nhất, vì WAL trên primary sẽ tăng cho đến khi chạm `max_slot_wal_keep_size`.

**Ước tính chi phí.** Chưa có bảng giá FPT Cloud nên mục này chỉ đưa công thức tính lượng tài nguyên. Đơn giá sẽ điền khi có input.

| Thành phần | Lượng cần | Thời gian dùng |
| --- | --- | --- |
| Compute | 1 vCPU, 2–4 GB RAM mỗi pipeline (chốt ở P0-08) | Suốt đời pipeline |
| Spool local | `resources.spool.hard`, ví dụ 50 GiB SSD | Suốt đời pipeline |
| Bucket staging | Tốc độ sinh log ở peak × ETA restore × 2 | Chỉ trong lúc seed |
| Archive binlog/WAL | Dung lượng log mỗi ngày × số ngày retention | Thường trực, nếu dùng chế độ B |
| Replica dành riêng cho Strand (tùy chọn) | Một replica MySQL/PG | Khi `plan` khuyến nghị |
| Băng thông liên DC | Dung lượng log mỗi ngày, cộng dữ liệu snapshot hoặc backup một lần | Thường trực, cộng đợt bootstrap |
