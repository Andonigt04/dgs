# Módulo k8s — clúster k8s (EKS/GKE/AKS o k3s en VM) + worker IPs para los NodePort UDP (§3.8, P8).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "cluster_name" {
  type    = string
  default = "dgs"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_id" {
  type = string
}

# Ejemplo con k3s en una VM (portable, sin cuota de EKS). En cloud managed se sustituye este bloque
# por el data source del clúster existente (EKS/GKE/AKS) y el kubeconfig se exporta igual.
output "kubeconfig" {
  description = "Ruta del kubeconfig para kubectl"
  value       = "/tmp/dgs-kubeconfig.yaml"
}

output "worker_ips" {
  description = "IPs de nodos worker (MY_NODE_IP para los NodePort UDP)"
  value       = ["10.0.1.10", "10.0.1.11", "10.0.1.12"]
}
