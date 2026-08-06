# Módulo zone_node — deployment + Service NodePort UDP del zone-node (§3.8, P8).
# Refleja k8s/zone-node/ (deployment + service + HPA). El orquestador genera deployments DINÁMICOS
# para las réplicas de split con el MISMO env (orchestrator.h zoneSpawnEnv) — la paridad LOCAL/K8S
# está garantizada porque ambos backends construyen el env desde la misma función.

terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.0" }
  }
}

variable "image" {
  type    = string
  default = "dgs-zone-node:latest"
}

resource "kubernetes_deployment" "zone_node" {
  metadata {
    name      = "zone-node"
    namespace = "dgs"
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "zone-node" }
    }
    template {
      metadata {
        labels = { app = "zone-node" }
      }
      spec {
        container {
          name  = "zone-node"
          image = var.image
          image_pull_policy = "IfNotPresent"
          port {
            container_port = 42425
            protocol       = "UDP"
          }
          env {
            name  = "HEAD_SERVER_HOST"
            value = "head-server"
          }
          env {
            name  = "HEAD_SERVER_PORT"
            value = "42424"
          }
          env {
            name  = "CHUNK_X_MIN"
            value = "0"
          }
          env {
            name  = "CHUNK_X_MAX"
            value = "100"
          }
          env {
            name  = "CHUNK_Y_MIN"
            value = "0"
          }
          env {
            name  = "CHUNK_Y_MAX"
            value = "100"
          }
          env {
            name  = "CHUNK_Z_MIN"
            value = "0"
          }
          env {
            name  = "CHUNK_Z_MAX"
            value = "100"
          }
          env {
            name  = "ZONE_UDP_PORT"
            value = "42425"
          }
          env {
            name  = "MY_POD_IP"
            value = "127.0.0.1"
          }
          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = "1",    memory = "512Mi" }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "zone_node" {
  metadata {
    name      = "zone-node"
    namespace = "dgs"
  }
  spec {
    type = "NodePort"
    selector = { app = "zone-node" }
    port {
      name        = "udp"
      port        = 42425
      target_port = 42425
      protocol    = "UDP"
      node_port   = 30425
    }
  }
}
