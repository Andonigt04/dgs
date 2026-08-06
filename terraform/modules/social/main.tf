# Módulo social — nodo social/cuenta (TCP:42430) (§3.8, P8). Refleja la configuración del plano
# social de P7: las zonas se SUSCRIBEN (SOCIAL_HOST/SOCIAL_TCP_PORT) para recibir bans/permisos.

terraform {
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.0" }
  }
}

resource "kubernetes_deployment" "social" {
  metadata {
    name      = "social"
    namespace = "dgs"
  }
  spec {
    replicas = 1
    selector {
      match_labels = { app = "social" }
    }
    template {
      metadata {
        labels = { app = "social" }
      }
      spec {
        container {
          name  = "social"
          image = "dgs-social-node:latest"
          image_pull_policy = "IfNotPresent"
          port {
            container_port = 42430
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
            name  = "SOCIAL_TCP_PORT"
            value = "42430"
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
