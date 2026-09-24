#!/bin/sh
set -eu
base_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cert_dir="$base_dir/env/judge-tls"
if [ -f "$cert_dir/server.crt" ] && [ -f "$cert_dir/server.key" ]; then
  exit 0
fi
if [ -e "$cert_dir/server.crt" ] || [ -e "$cert_dir/server.key" ]; then
  echo 'Incomplete judge TLS key pair; refusing to silently replace trusted credentials.' >&2
  exit 1
fi
umask 077
mkdir -p "$cert_dir"
openssl req -x509 -newkey rsa:3072 -nodes -days 365 \
  -keyout "$cert_dir/server.key" -out "$cert_dir/server.crt" \
  -subj '/CN=ZOJ internal judge API' \
  -addext 'subjectAltName=IP:10.10.10.110' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
  -addext 'extendedKeyUsage=serverAuth'
chmod 644 "$cert_dir/server.crt"
echo 'Created judge TLS certificate. Distribute the public certificate to authorized agents over a trusted channel.'
