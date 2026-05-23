################################################################################
# Lightstream AWS A-to-B benchmark - infrastructure module.
#
# Provisions two EC2 instances in the same Availability Zone, in a cluster
# placement group, on the AWS default VPC. The instances are bootstrapped
# minimally (chrony enabled, ulimits raised); the lightstream binaries are
# expected to be SCP'd in after `terraform apply` returns, then driven by
# `bench/aws/run.sh`. Outputs include the public IPs to SSH to and the
# sender's private IP for the receiver to connect to.
#
# The user supplies their own SSH public key via the `ssh_public_key_path`
# variable. Terraform creates an `aws_key_pair` from it; SSH access uses
# the matching private key the user already holds. No key material is
# bundled with this module.
################################################################################

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.region
}

################################################################################
# Defaults sourced from the AWS account
################################################################################

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Pick the alphabetically-first subnet in the default VPC. Both instances
# land in the same AZ via the placement group's cluster strategy, which
# requires same-AZ membership.
locals {
  subnet_id = sort(data.aws_subnets.default.ids)[0]
}

data "aws_subnet" "selected" {
  id = local.subnet_id
}

# Latest Amazon Linux 2023 AMI for the chosen architecture.
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-${var.architecture}"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

################################################################################
# Networking - security group only; VPC and subnet come from defaults
################################################################################

resource "aws_security_group" "bench" {
  name        = "lightstream-bench-${random_id.suffix.hex}"
  description = "lightstream A-to-B bench rig - SSH from operator, bench port between members"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from operator workstation"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allow_cidr]
  }

  egress {
    description = "all outbound (so apt/dnf and Docker pulls work during bootstrap)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Separate rule so the group can reference itself. The bench port is open
# from any instance in this SG to any other - matches the workflow where
# the receiver connects to the sender on a single TCP port.
resource "aws_security_group_rule" "bench_self_ingress" {
  type                     = "ingress"
  from_port                = var.bench_port
  to_port                  = var.bench_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "lightstream bench port between SG members"
}

resource "random_id" "suffix" {
  byte_length = 3
}

################################################################################
# SSH key pair (built from a user-supplied public key)
################################################################################

resource "aws_key_pair" "bench" {
  key_name   = "lightstream-bench-${random_id.suffix.hex}"
  public_key = file(var.ssh_public_key_path)
}

################################################################################
# Placement group + instances
################################################################################

resource "aws_placement_group" "bench" {
  name     = "lightstream-bench-${random_id.suffix.hex}"
  strategy = "cluster"
}

resource "aws_instance" "sender" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  key_name                    = aws_key_pair.bench.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  placement_group             = aws_placement_group.bench.name
  associate_public_ip_address = true

  user_data = local.bootstrap_user_data

  tags = {
    Name = "lightstream-bench-sender"
    Role = "sender"
  }
}

resource "aws_instance" "receiver" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  key_name                    = aws_key_pair.bench.key_name
  vpc_security_group_ids      = [aws_security_group.bench.id]
  placement_group             = aws_placement_group.bench.name
  associate_public_ip_address = true

  user_data = local.bootstrap_user_data

  tags = {
    Name = "lightstream-bench-receiver"
    Role = "receiver"
  }
}

# Minimal bootstrap: enable chrony for clock sync (the bench reports
# sub-millisecond timings) and raise nofile so the bench binary's
# sockets and buffers do not hit the 1024 default.
locals {
  bootstrap_user_data = <<-EOT
    #!/bin/bash
    set -eu
    systemctl enable --now chronyd
    echo '*  soft  nofile  65536' >> /etc/security/limits.conf
    echo '*  hard  nofile  65536' >> /etc/security/limits.conf
  EOT
}

################################################################################
# Variables
################################################################################

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type. Default `c7gn.large` is network-optimised Graviton in
    the same NIC class as the published numbers. For x86, use `c7i.large`.
    Larger sizes lift the network ceiling at higher cost.
  EOT
  type        = string
  default     = "c7gn.large"
}

variable "architecture" {
  description = <<-EOT
    AMI architecture: `arm64` for Graviton instance types (c7g/c7gn/m7g),
    `x86_64` for Intel/AMD types (c7i/m7i).
  EOT
  type        = string
  default     = "arm64"
  validation {
    condition     = contains(["arm64", "x86_64"], var.architecture)
    error_message = "architecture must be arm64 or x86_64."
  }
}

variable "ssh_public_key_path" {
  description = <<-EOT
    Filesystem path to the SSH public key Terraform should register on
    both instances. The user owns the matching private key; no key
    material is bundled with this module. Example:
    `~/.ssh/lightstream_bench.pub`.
  EOT
  type        = string
}

variable "ssh_allow_cidr" {
  description = <<-EOT
    CIDR block permitted to SSH into the instances. Set to the operator's
    public IP/32 - the default of 0.0.0.0/0 is convenient for one-shot
    runs but is not recommended for anything left running.
  EOT
  type        = string
  default     = "0.0.0.0/0"
}

variable "bench_port" {
  description = "TCP port the sender listens on and the receiver connects to."
  type        = number
  default     = 9001
}

################################################################################
# Outputs - feed these into `bench/aws/run.sh`
################################################################################

output "sender_public_ip" {
  description = "Public IP for SSH'ing to the sender host."
  value       = aws_instance.sender.public_ip
}

output "sender_private_ip" {
  description = "Private IP the receiver should connect to (pass as SENDER_PRIVATE_IP to run.sh)."
  value       = aws_instance.sender.private_ip
}

output "receiver_public_ip" {
  description = "Public IP for SSH'ing to the receiver."
  value       = aws_instance.receiver.public_ip
}

output "availability_zone" {
  description = "AZ the instances land in (same-AZ via the placement group)."
  value       = data.aws_subnet.selected.availability_zone
}

output "ssh_user" {
  description = "SSH user for Amazon Linux 2023."
  value       = "ec2-user"
}

output "run_sh_invocation" {
  description = "Ready-to-paste invocation of bench/aws/run.sh using these outputs."
  value = <<-EOT
    SENDER_HOST=ec2-user@${aws_instance.sender.public_ip} \
    RECEIVER_HOST=ec2-user@${aws_instance.receiver.public_ip} \
    SENDER_PRIVATE_IP=${aws_instance.sender.private_ip} \
    SSH_OPTS="-i <your-private-key.pem> -o StrictHostKeyChecking=accept-new" \
    bench/aws/run.sh
  EOT
}
