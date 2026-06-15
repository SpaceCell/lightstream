################################################################################
# Lightstream cross-host bench - ephemeral EKS cluster.
#
# Stands up a throwaway VPC, an EKS cluster, a two-node managed node
# group pinned to one subnet, and an ECR repository for the bench image.
# Both nodes share an AZ for a real NIC hop with minimal latency variance.
#
# `bench/cluster/run.sh` drives the full lifecycle: apply, build and push
# the image, schedule the sender and receiver pods on separate nodes via
# pod anti-affinity, collect the receiver result, then destroy.
#
# Everything carries a random suffix so parallel runs stay isolated and
# each destroys cleanly. The VPC and node group use public subnets only
# with no NAT gateway, so apply and destroy stay fast.
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
# Networking - throwaway VPC, public subnets only
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

  # EKS discovers cluster subnets by tag.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  tags = local.tags
}

################################################################################
# EKS cluster + two-node group on a single subnet (same AZ)
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

      # Pin both nodes to the first subnet so they share an AZ. Pod
      # anti-affinity then lands the sender and receiver on the two
      # distinct nodes, and traffic crosses a real NIC.
      subnet_ids = [module.vpc.public_subnets[0]]
    }
  }

  tags = local.tags
}

################################################################################
# ECR repository for the bench image
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
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "kubernetes_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.31"
}

variable "instance_type" {
  description = <<-EOT
    Node instance type. Default `c5n.large` is network-optimised x86 in
    the class used for the published numbers. Larger sizes lift the
    network ceiling at higher cost.
  EOT
  type        = string
  default     = "c5n.large"
}

variable "ami_type" {
  description = <<-EOT
    EKS managed node group AMI type. Must match the instance
    architecture: `AL2023_x86_64_STANDARD` for x86 (c5n/c7i),
    `AL2023_ARM_64_STANDARD` for Graviton (c7gn/m7g).
  EOT
  type        = string
  default     = "AL2023_x86_64_STANDARD"
}

################################################################################
# Outputs - consumed by bench/cluster/run.sh
################################################################################

output "cluster_name" {
  description = "EKS cluster name for `aws eks update-kubeconfig`."
  value       = module.eks.cluster_name
}

output "region" {
  description = "Region the cluster runs in."
  value       = var.region
}

output "ecr_repository_url" {
  description = "ECR repository URL to tag and push the bench image to."
  value       = aws_ecr_repository.bench.repository_url
}
