# DGS — infra multi-servicio opcional (§3.8, P8).
# `dgs up --terraform` corre este módulo ANTES de aplicar k8s. Provisiona la infra de soporte:
# red/VPC (network), clúster k8s (k8s) y MongoDB (mongodb). Los deployments de los nodos (zone_node,
# validador, social, head, cache, persistence) van como recursos k8s en este directorio (main.tf) o en
# k8s/ (kubectl); los OUTPUTS alimentan al orquestador (image, MY_NODE_IP para NodePort UDP, endpoints).
#
# Es OPCIONAL de verdad: standalone (`dgs run`) e instalado (`dgs install`) nunca lo tocan.

terraform {
  required_version = ">= 1.0"
}

variable "cloud" {
  description = "Provider objetivo: aws, gcp o azure"
  type        = string
  default     = "aws"
}

variable "region" {
  description = "Region del clúster"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Nombre del clúster k8s"
  type        = string
  default     = "dgs"
}

# Credenciales por env (TF_VAR_*), igual que NODE_TOKEN del orquestador. Nunca hardcodear aquí.
variable "dgs_zone_image" {
  description = "Imagen del zone-node (la usa el orquestador en el spawn TERRAFORM)"
  type        = string
  default     = "dgs-zone-node:latest"
}

# --- módulos de infra ---------------------------------------------------------------

module "network" {
  source = "./modules/network"
  region = var.region
}

module "mongodb" {
  source = "./modules/mongodb"
  region = var.region
}

module "k8s" {
  source       = "./modules/k8s"
  cluster_name = var.cluster_name
  region       = var.region
  vpc_id       = module.network.vpc_id
  depends_on   = [module.network]
}

# --- deployments de los nodos (k8s dentro del módulo k8s / se aplican con kubectl) ---

module "zone_node" {
  source     = "./modules/zone_node"
  image      = var.dgs_zone_image
  depends_on = [module.k8s]
}

module "validador" {
  source     = "./modules/validador"
  depends_on = [module.k8s]
}

module "social" {
  source     = "./modules/social"
  depends_on = [module.k8s]
}

# --- outputs: alimentan al orquestador y al CLI -------------------------------------

output "kubeconfig" {
  description = "Ruta al kubeconfig del clúster (para kubectl / spawn TERRAFORM)"
  value       = module.k8s.kubeconfig
}

output "mongodb_endpoint" {
  description = "Endpoint de MongoDB (persistence_node)"
  value       = module.mongodb.endpoint
}

output "zone_node_image" {
  description = "Imagen de zone-node que usa el orquestador (DGS_ZONE_IMAGE)"
  value       = var.dgs_zone_image
}

output "node_pool_ips" {
  description = "IPs de los nodos worker (MY_NODE_IP para los NodePort UDP)"
  value       = module.k8s.worker_ips
}
