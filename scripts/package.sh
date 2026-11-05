#!/usr/bin/env bash
# Dựng bản phát hành của strand vào dist/: tarball + SHA-256 (dạng phát hành P1, 03 §4) và gói
# .deb/.rpm (P3) cho linux/amd64 và linux/arm64. Binary tĩnh (CGO_ENABLED=0).
#
#   scripts/package.sh                 # VERSION = git describe
#   VERSION=1.2.0 scripts/package.sh
#   ARCHES="amd64" scripts/package.sh
#
# Gói được dựng bằng nfpm (go run, không thêm vào go.mod). Chữ ký (03 §4) chưa làm: ký dist/*.sha256
# bằng khóa phát hành của tổ chức sau bước này.
set -euo pipefail
cd "$(dirname "$0")/.."

NFPM="github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.48.0"
ARCHES="${ARCHES:-amd64 arm64}"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
if [ -z "${VERSION:-}" ]; then
	VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo 0.0.0-dev)"
	VERSION="${VERSION#v}"
fi
# deb/rpm cần version bắt đầu bằng chữ số.
case "$VERSION" in
	[0-9]*) PKGVER="$VERSION" ;;
	*) PKGVER="0.0.0+${VERSION//[^A-Za-z0-9.+~]/.}" ;;
esac

rm -rf dist
mkdir -p dist/pkgroot
# Unit cho gói: binary ở /usr/bin (gói không cài vào /usr/local); nguồn duy nhất là deploy/systemd.
sed 's#/usr/local/bin/strand#/usr/bin/strand#g' deploy/systemd/strand@.service > dist/pkgroot/strand@.service
grep -q 'ExecStart=/usr/bin/strand ' dist/pkgroot/strand@.service

for arch in $ARCHES; do
	bin="dist/strand-linux-$arch/strand"
	mkdir -p "$(dirname "$bin")"
	CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath \
		-ldflags "-s -w -X main.version=$VERSION -X main.commit=$COMMIT" -o "$bin" ./cmd/strand
	tar -C dist -czf "dist/strand-$VERSION-linux-$arch.tar.gz" "strand-linux-$arch"
	for fmt in deb rpm; do
		ARCH="$arch" VERSION="$PKGVER" BIN="$bin" go run "$NFPM" package \
			--config packaging/nfpm.yaml --packager "$fmt" --target dist/
	done
done
rm -rf dist/pkgroot
(cd dist && sha256sum *.tar.gz *.deb *.rpm > "strand-$VERSION.sha256")
ls -l dist
