#!/usr/bin/env bash
# Generates a throwaway CA and one certificate for the nodes, for LOCAL USE AND TESTS ONLY.
#
# Mutual TLS needs three files per node: its certificate, its key, and the CA both ends verify against.
# A real deployment issues one certificate per node from a CA it controls and rotates them; this makes
# a single shared one so `DGS_TLS_*` can be exercised end to end without pretending to be a PKI.
set -euo pipefail
out="${1:-/tmp/dgs-tls}"
mkdir -p "$out"

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$out/ca.key" -out "$out/ca.crt" -subj "/CN=dgs-test-ca" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
    -keyout "$out/node.key" -out "$out/node.csr" -subj "/CN=dgs-node" >/dev/null 2>&1

openssl x509 -req -in "$out/node.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" \
    -CAcreateserial -out "$out/node.crt" -days 3650 >/dev/null 2>&1

# And an IMPOSTOR: a certificate from a DIFFERENT CA. Without one there is no way to tell "TLS works"
# from "TLS accepts anybody", which is the whole point of verifying.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$out/rogue-ca.key" -out "$out/rogue-ca.crt" -subj "/CN=dgs-rogue-ca" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
    -keyout "$out/rogue.key" -out "$out/rogue.csr" -subj "/CN=dgs-rogue" >/dev/null 2>&1
openssl x509 -req -in "$out/rogue.csr" -CA "$out/rogue-ca.crt" -CAkey "$out/rogue-ca.key" \
    -CAcreateserial -out "$out/rogue.crt" -days 3650 >/dev/null 2>&1

rm -f "$out"/*.csr "$out"/*.srl
echo "$out"
