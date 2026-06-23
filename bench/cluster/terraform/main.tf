################################################################################
# Lightstream cross-host benchmark infrastructure.
#
# Creates a VPC, an EKS cluster, a two-node managed node group and an ECR
# repository for the benchmark image. Both worker nodes are placed in the same
# Availability Zone to reduce network latency variance.
#
# `bench/cluster/run.sh` provisions the infrastructure, builds and pushes the
# image, runs the sender and receiver on separate nodes, collects the result and
# destroys the infrastructure.
#
# Resource names include a random suffix to isolate concurrent runs. The VPC
# uses public subnets without a NAT gateway.
################################################################################

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "random_id" "suffix" {
  byte_length = 3
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name = "ls-bench-${random_id.suffix.hex}"
  azs  = slice(data.aws_availability_zones.available.names, 0, 2)
  tags = {
    Project   = "lightstream-bench"
    Ephemeral = "true"
  }
}

################################################################################
# Networking
################################################################################

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = local.name
  cidr = "10.0.0.0/16"

  azs            = local.azs
  public_subnets = ["10.0.1.0/24", "10.0.2.0/24"]

  map_public_ip_on_launch = true
  enable_nat_gateway      = false
  enable_dns_hostnames    = true

  # Tag public subnets for Kubernetes load balancer discovery.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  tags = local.tags
}

################################################################################
# EKS cluster and managed node group
################################################################################

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.public_subnets

  eks_managed_node_groups = {
    bench = {
      instance_types = [var.instance_type]
      ami_type       = var.ami_type

      min_size     = 2
      max_size     = 2
      desired_size = 2

      # Place both nodes in the same Availability Zone. Pod anti-affinity places
      # the sender and receiver on separate nodes.
      subnet_ids = [module.vpc.public_subnets[0]]
    }
  }

  tags = local.tags
}

################################################################################
# Benchmark image repository
################################################################################

resource "aws_ecr_repository" "bench" {
  name                 = "${local.name}-image"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = local.tags
}

################################################################################
# Variables
################################################################################

variable "region" {
  description = "AWS region in which to create the benchmark infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "kubernetes_version" {
  description = "Kubernetes version for the EKS control plane."
  type        = string
  default     = "1.31"
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type for the worker nodes. The default, `c5n.large`, is a
    network-optimised x86_64 instance. Larger instance types provide more
    network bandwidth at a higher cost.
  EOT
  type        = string
  default     = "c5n.large"
}

variable "ami_type" {
  description = <<-EOT
    AMI type for the EKS managed node group. Use `AL2023_x86_64_STANDARD`
    for x86_64 instance types and `AL2023_ARM_64_STANDARD` for Graviton
    instance types.
  EOT
  type        = string
  default     = "AL2023_x86_64_STANDARD"
}

################################################################################
# Outputs used by `bench/cluster/run.sh`
################################################################################

output "cluster_name" {
  description = "EKS cluster name used to configure kubectl access."
  value       = module.eks.cluster_name
}

output "region" {
  description = "AWS region containing the EKS cluster."
  value       = var.region
}

output "ecr_repository_url" {
  description = "ECR repository URL for the benchmark image."
  value       = aws_ecr_repository.bench.repository_url
}
