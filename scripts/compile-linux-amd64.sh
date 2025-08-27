#!/bin/sh
export KUBEDOCK_VERSION=0.18.1-1-custom-lifetime

rm -rf dist

go mod download
LDFLAGS="-s -w -X main.version=${KUBEDOCK_VERSION}"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -buildvcs=false -ldflags="${LDFLAGS}" \
  -o dist/kubedock .

mkdir -p dist/pkg
cp dist/kubedock dist/pkg/
cp LICENSE dist/pkg/
tar -C dist/pkg -czf "dist/kubedock_${KUBEDOCK_VERSION}_linux_amd64.tar.gz" .
shasum -a 256 "dist/kubedock_${KUBEDOCK_VERSION}_linux_amd64.tar.gz" > "dist/kubedock_${KUBEDOCK_VERSION}_linux_amd64.tar.gz.sha256"