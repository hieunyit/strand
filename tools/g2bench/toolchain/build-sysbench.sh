#!/bin/sh
# Dựng sysbench 1.0.20 đã ghim: bản patch (đo raw event) và bản nguyên bản (chỉ để so overhead).
# Dùng: build-sysbench.sh <upstream.tar> <out-dir> [image]
#   upstream.tar: `git archive ebf1c90da05dea94648165e4f149abc20c979557` của akopytov/sysbench (sha256 ghim bên dưới).
# Chạy trong container toolchain, không mạng; out-dir chứa sysbench-patched, sysbench-pristine, SHA256SUMS.
set -eu
tar_file=$(readlink -f "$1")
out=$(mkdir -p "$2" && readlink -f "$2")
image=${3:-strand-sysbench-toolchain:20261108}
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
patch_file="$here/../../benchclient/patches/sysbench-1.0.20-raw-events.patch"
printf '%s  %s\n' 53c0b2859373c0500f6465c08791aecb14493ce066553259f272f8b996f1f32b "$tar_file" | sha256sum -c -
printf '%s  %s\n' 8cfb1da36ba9b5aa3febd24d9ed4a56da8981b2ac0fb0df04a23b5d09d7b43c4 "$patch_file" | sha256sum -c -
cp "$tar_file" "$out/upstream.tar"
cp "$patch_file" "$out/sysbench.patch"
docker run --rm --network none --cpus=2 --memory=2g --pids-limit=256 \
  --mount "type=bind,src=$out,dst=/work" "$image" sh -ec '
  for v in patched pristine; do
    d=/tmp/build-$v
    mkdir -p "$d" && tar -xf /work/upstream.tar -C "$d" && cd "$d"
    [ "$v" = patched ] && patch -p1 < /work/sysbench.patch
    ./autogen.sh >/work/build-$v.log 2>&1
    ./configure --with-mysql --without-pgsql --with-system-luajit >>/work/build-$v.log 2>&1
    make -j2 >>/work/build-$v.log 2>&1
    cp src/sysbench /work/sysbench-$v
    cd /
  done
  # script Lua dựng sẵn của upstream (oltp_*.lua, không sửa): mount vào /usr/local/share/sysbench trong container client
  mkdir -p /work/lua && cp /tmp/build-pristine/src/lua/*.lua /work/lua/
  chmod -R a+rX /work/lua && chmod a+rx /work/sysbench-patched /work/sysbench-pristine
  cd /work && sha256sum sysbench-patched sysbench-pristine lua/*.lua > SHA256SUMS'
cat "$out/SHA256SUMS"
