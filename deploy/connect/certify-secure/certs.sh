#!/bin/bash
# Test-only PKI for deploy/connect/certify-secure.compose.yml, made fresh on every start with the
# JDK keytool already in the Kafka image (no keys in Git, no extra image). Writes to /certs:
#   ca.pem             CA the clients trust
#   broker.p12         broker key + chain (localhost only, so 127.0.0.1 fails hostname checks), truststore.p12 for client certs
#   client.p12         client key + chain for the mTLS listener (CN=meridian-client)
#   rogue-ca.pem       an unrelated CA (negative test: not trusted by anyone)
#   rogue-client.p12   a client cert signed by the rogue CA (negative test: refused by the broker)
# Every store uses the fixed test password below; nothing here protects real data.
set -euo pipefail
cd /certs
rm -f ./*.p12 ./*.pem ./*.csr ./*.crt ./ready
P=meridian-test-store
K="keytool -noprompt -storetype PKCS12 -storepass $P"
ca() { # alias dname
  $K -genkeypair -alias "$1" -dname "$2" -keyalg RSA -keysize 2048 -validity 7 -ext bc:c -keystore "$1.p12"
  $K -exportcert -alias "$1" -keystore "$1.p12" -rfc -file "$1.pem"
}
leaf() { # name dname ca san
  $K -genkeypair -alias "$1" -dname "$2" -keyalg RSA -keysize 2048 -validity 7 -keystore "$1.p12"
  $K -certreq -alias "$1" -keystore "$1.p12" -file "$1.csr"
  $K -gencert -alias "$3" -keystore "$3.p12" -infile "$1.csr" -outfile "$1.crt" -rfc -validity 7 ${4:+-ext "SAN=$4"}
  $K -importcert -alias "$3" -file "$3.pem" -keystore "$1.p12"
  $K -importcert -alias "$1" -file "$1.crt" -keystore "$1.p12"
}
ca ca "CN=meridian-test-ca"
ca rogue-ca "CN=meridian-rogue-ca"
leaf broker "CN=localhost" ca "dns:localhost,dns:kafka-secure"
leaf client "CN=meridian-client" ca
leaf rogue-client "CN=meridian-client" rogue-ca
$K -importcert -alias ca -file ca.pem -keystore truststore.p12
rm -f ./*.csr ./*.crt ca.p12 rogue-ca.p12
chmod 644 ./*
touch ready
