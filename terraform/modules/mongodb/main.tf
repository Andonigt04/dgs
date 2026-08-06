# Módulo mongodb — MongoDB managed (opción recomendada) o self-hosted en el clúster.
# El persistence_node y el nodo social escriben aquí (write-through, P6/P7).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

# Endpoint de un Mongo gestionado (AWS DocumentDB). En GCP/Azure se usa el equivalente; en
# instalaciones in-cluster (k3s) este módulo no se usa y Mongo corre como StatefulSet.
output "endpoint" {
  value = "mongodb://mongo.dgs.svc:27017/dgs"
}

# Recurso de ejemplo para un despliegue gestionado (DocumentDB):
# resource "aws_docdb_cluster" "dgs" {
#   cluster_identifier = "dgs-mongo"
#   engine_version     = "4.0.0"
#   master_username    = var.mongo_user
#   master_password    = var.mongo_pass
#   vpc_security_group_ids = [var.security_group_id]
# }
