# Módulo network — VPC, subredes y security groups para el clúster y MongoDB (§3.8, P8).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

provider "aws" {
  region = var.region
}

resource "aws_vpc" "dgs" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "dgs-vpc" }
}

resource "aws_subnet" "dgs" {
  count                   = 3
  vpc_id                  = aws_vpc.dgs.id
  cidr_block              = cidrsubnet(aws_vpc.dgs.cidr_block, 8, count.index)
  map_public_ip_on_launch = true

  tags = { Name = "dgs-subnet-${count.index}" }
}

resource "aws_internet_gateway" "dgs" {
  vpc_id = aws_vpc.dgs.id

  tags = { Name = "dgs-igw" }
}

resource "aws_route_table" "dgs" {
  vpc_id = aws_vpc.dgs.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.dgs.id
  }
}

resource "aws_route_table_association" "dgs" {
  count          = 3
  subnet_id      = aws_subnet.dgs[count.index].id
  route_table_id = aws_route_table.dgs.id
}

# Security group del clúster: UDP/TCP del DGS (42424-42430) + MongoDB 27017 + k8s API 443.
resource "aws_security_group" "dgs" {
  name   = "dgs-sg"
  vpc_id = aws_vpc.dgs.id

  ingress {
    from_port = 42424
    to_port   = 42430
    protocol  = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port = 42424
    to_port   = 42430
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 27017
    to_port     = 27017
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

output "vpc_id" {
  value = aws_vpc.dgs.id
}

output "subnet_ids" {
  value = aws_subnet.dgs[*].id
}

output "security_group_id" {
  value = aws_security_group.dgs.id
}
