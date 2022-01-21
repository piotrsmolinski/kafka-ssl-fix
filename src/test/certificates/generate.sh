#!/bin/bash -e

[ ! -f ca.key ] \
&& openssl genrsa \
-out ca.key \
2048 \
&& rm -f ca.crt \
&& echo "ca.key generated"

[ ! -f ca.crt ] \
&& openssl req -x509 \
-new \
-key ca.key \
-days 3650 \
-sha256 \
-subj "/CN=CA" \
-out ca.crt \
&& rm -f ca.jks \
&& rm -f kafka.crt \
&& echo "ca.crt generated"

[ ! -f ca.jks ] \
&& keytool -importcert -v \
-noprompt \
-file ca.crt \
-alias ca \
-destkeystore ca.jks \
-deststoretype PKCS12 \
-deststorepass changeit \
&& echo "ca.jks generated"

[ ! -f kafka.key ] \
&& openssl genrsa \
-out kafka.key \
2048 \
&& rm -f kafka.crt \
&& echo "kafka.key generated"

[ ! -f kafka.csr ] \
&& openssl req \
-new \
-key kafka.key \
-sha256 \
-subj "/CN=kafka" \
-out kafka.csr \
&& rm -f kafka.crt \
&& echo "kafka.csr generated"

[ ! -f kafka.crt ] \
&& openssl x509 \
-req \
-CA ca.crt \
-CAkey ca.key \
-CAcreateserial \
-in kafka.csr \
-out kafka.crt \
-days 3650 \
-sha256  \
-extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth") \
&& rm -f kafka.p12 \
&& echo "kafka.crt generated"

[ ! -f kafka.p12 ] \
&& openssl pkcs12 \
-export \
-name kafka \
-in kafka.crt \
-inkey kafka.key \
-chain \
-CAfile ca.crt \
-passout pass:changeit \
-out kafka.p12 \
&& rm -f kafka.jks \
&& echo "kafka.p12 generated"

[ ! -f kafka.jks ] \
&& keytool -importkeystore -v \
-srckeystore kafka.p12 \
-srcstoretype PKCS12 \
-srcstorepass changeit \
-destkeystore kafka.jks \
-deststoretype PKCS12 \
-deststorepass changeit \
&& echo "kafka.jks generated"
