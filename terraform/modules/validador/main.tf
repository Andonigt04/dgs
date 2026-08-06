# Módulo validador — deployment del validador (§3.8, P8). Refleja k8s/validador/.
# Init container espera al head-server (orden de conexión standalone/cluster idéntico).

terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.0" }
  }
}

resource "kubernetes_deployment" "validador" {
  metadata {
    name      = "validador"
    namespace = "dgs"
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "validador" }
    }
    template {
      metadata {
        labels = { app = "validador" }
      }
      spec {
        container {
          name  = "validador"
          image = "dgs-validador-node:latest"
          image_pull_policy = "IfNotPresent"
          port {
            container_port = 42427
            protocol       = "UDP"
          }
          port {
            container_port = 42428
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
            name  = "PERSISTENCE_HOST"
            value = "persistence"
          }
          env {
            name  = "PERSISTENCE_PORT"
            value = "42429"
          }
          env {
            name  = "VALIDADOR_UDP_PORT"
            value = "42427"
          }
          env {
            name  = "VALIDADOR_TCP_PORT"
            value = "42428"
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
