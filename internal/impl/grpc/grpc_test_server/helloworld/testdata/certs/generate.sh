#!/bin/sh
# Regenerates the TLS fixtures of the gRPC client tests: a CA, a server
# certificate for localhost/127.0.0.1 and a client certificate, all signed by
# the CA and valid for 10 years. Run from this directory: ./generate.sh
set -eu

days=3650
subj="/C=DE/ST=NRW/L=Earth/O=Random Company/OU=IT"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$days" \
  -keyout ca.key -out ca.pem \
  -subj "$subj/CN=www.random.com/emailAddress=KryptoKings@random.com"

for name in server client; do
  case "$name" in
    server) cn=localhost; ext="subjectAltName=DNS:localhost,IP:127.0.0.1" ;;
    client) cn=client.com; ext="extendedKeyUsage=clientAuth" ;;
  esac
  openssl req -newkey rsa:2048 -nodes -sha256 \
    -keyout "$name.key" -out "$name.csr" \
    -subj "$subj/CN=$cn/emailAddress=foo@foo.com"
  printf '%s\n' "$ext" > "$name.ext"
  openssl x509 -req -sha256 -days "$days" -in "$name.csr" \
    -CA ca.pem -CAkey ca.key -CAcreateserial \
    -extfile "$name.ext" -out "$name.pem"
  rm "$name.csr" "$name.ext"
done
