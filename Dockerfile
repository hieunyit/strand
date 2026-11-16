# syntax=docker/dockerfile:1
# Strand CDC: image cho `strand web` (Web UI + pipeline chế độ nền chạy trong cùng container).
# Build: docker build -t strand:local .   (hoặc: docker compose up -d --build, xem compose.yaml)

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/strand ./cmd/strand

FROM debian:bookworm-slim
# tini: PID 1 thu hoạch zombie (web sinh pipeline nền bằng `strand sync --background`, tiến trình mồ côi về PID 1).
# ca-certificates: kết nối TLS tới nguồn/đích. curl: HEALTHCHECK.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --user-group --create-home --home-dir /home/strand --shell /usr/sbin/nologin strand \
 && mkdir -p /etc/strand /var/lib/strand-web \
 && chown strand:strand /etc/strand /var/lib/strand-web \
 && chmod 0750 /var/lib/strand-web

COPY --from=build /out/strand /usr/local/bin/strand
COPY deploy/compose/entrypoint.sh /usr/local/bin/strand-entrypoint
COPY deploy/compose/healthcheck.sh /usr/local/bin/strand-healthcheck
RUN chmod 0755 /usr/local/bin/strand-entrypoint /usr/local/bin/strand-healthcheck

USER strand
WORKDIR /home/strand
ENV STRAND_WEB_STATE_DIR=/var/lib/strand-web \
    STRAND_CONFIG_DIR=/etc/strand
VOLUME ["/etc/strand", "/var/lib/strand-web"]
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 CMD ["/usr/local/bin/strand-healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/strand-entrypoint"]
CMD ["serve"]
