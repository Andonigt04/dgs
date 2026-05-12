#!/bin/bash
set -e

echo "=== Applying manifests ==="
kubectl apply -f k8s/namespace.yaml

kubectl apply -f k8s/mongodb/
kubectl apply -f k8s/head-server/
kubectl apply -f k8s/zone-node/
kubectl apply -f k8s/cache/
kubectl apply -f k8s/anticheat/
kubectl apply -f k8s/persistence/


echo "=== Rollout Restart ==="
kubectl apply -f k8s/namespace.yaml

kubectl rollout restart deployment/head-server -n dgs
kubectl rollout restart deployment/zone-node -n dgs
kubectl rollout restart deployment/cache -n dgs
kubectl rollout restart deployment/anticheat -n dgs
kubectl rollout restart deployment/persistence -n dgs

echo "=== Estado del cluster ==="
kubectl get pods -n dgs
