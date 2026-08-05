#!/bin/bash
set -e

echo "=== Building images in minikube ==="
eval $(minikube docker-env)

docker build -f docker/Dockerfile.base -t dgs-base .
docker build -f docker/Dockerfile.head_server    -t dgs-head-server:latest .
docker build -f docker/Dockerfile.zone_node      -t dgs-zone-node:latest .
docker build -f docker/Dockerfile.cache_node     -t dgs-cache-node:latest .
docker build -f docker/Dockerfile.validador_node -t dgs-validador-node:latest .
docker build -f docker/Dockerfile.persistence_node -t dgs-persistence-node:latest .

echo "=== Applying manifests ==="
kubectl apply -f k8s/namespace.yaml

kubectl apply -f k8s/mongodb/
kubectl apply -f k8s/head-server/
kubectl apply -f k8s/zone-node/
kubectl apply -f k8s/cache/
kubectl apply -f k8s/validador/
kubectl apply -f k8s/persistence/

echo "=== Estado del cluster ==="
kubectl get pods -n dgs
