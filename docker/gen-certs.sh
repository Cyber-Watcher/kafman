#!/usr/bin/env bash
#
# Generates the certificates used by the SASL_SSL listener of the test cluster.
#
# Produces, in docker/certs/:
#   ca.key / ca.crt         the test certificate authority
#   server.key / server.crt the broker certificate, signed by the CA
#   server.p12              PKCS#12 keystore for the brokers (Kafka reads this)
#   keystore_creds          keystore password, read by the Kafka container
#   key_creds               private key password, read by the Kafka container
#
# Only ca.crt is needed by kafman itself: it is the CA the client must trust.
#
# THIS IS A THROWAWAY LOCAL TEST CA. The password is hard-coded on purpose.
# Never use these certificates anywhere but this compose stack.

set -euo pipefail

cd "$(dirname "$0")"
CERTS_DIR="certs"
PASSWORD="kafman-test"
DAYS=825 # the maximum many TLS clients accept for a leaf certificate

if [[ -f "$CERTS_DIR/server.p12" && -f "$CERTS_DIR/ca.crt" ]]; then
	echo "certificates already present in docker/$CERTS_DIR — nothing to do"
	echo "(delete the directory to regenerate)"
	exit 0
fi

command -v openssl >/dev/null 2>&1 || {
	echo "openssl is required but was not found" >&2
	exit 1
}

mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

echo "==> certificate authority"
openssl req -new -x509 -nodes -days 3650 \
	-newkey rsa:2048 \
	-keyout ca.key -out ca.crt \
	-subj "/C=RU/O=kafman test/CN=kafman test CA" 2>/dev/null

echo "==> broker key and signing request"
openssl req -new -nodes \
	-newkey rsa:2048 \
	-keyout server.key -out server.csr \
	-subj "/C=RU/O=kafman test/CN=kafka-1" 2>/dev/null

# The brokers are reached as localhost from the host and as kafka-N from inside
# the compose network, so every name has to be in the SAN list.
cat >server.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = kafka-1
DNS.3 = kafka-2
DNS.4 = kafka-3
IP.1  = 127.0.0.1
EOF

echo "==> signing the broker certificate"
openssl x509 -req -in server.csr \
	-CA ca.crt -CAkey ca.key -CAcreateserial \
	-out server.crt -days "$DAYS" -sha256 \
	-extfile server.ext 2>/dev/null

echo "==> PKCS#12 keystore for the brokers"
# PKCS#12 avoids needing keytool, so no JDK is required on the host.
openssl pkcs12 -export \
	-in server.crt -inkey server.key -certfile ca.crt \
	-name kafka -out server.p12 \
	-password "pass:$PASSWORD" 2>/dev/null

printf '%s' "$PASSWORD" >keystore_creds
printf '%s' "$PASSWORD" >key_creds

# The container runs as a non-root user and must be able to read these.
chmod 644 server.p12 keystore_creds key_creds ca.crt
chmod 600 ca.key server.key
rm -f server.csr server.ext ca.srl

echo
echo "certificates written to docker/$CERTS_DIR:"
ls -1 .
echo
openssl x509 -in server.crt -noout -subject -dates -ext subjectAltName |
	sed 's/^/  /'
