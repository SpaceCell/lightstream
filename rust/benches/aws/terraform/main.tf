################################################################################
# Lightstream AWS A-to-B benchmark infrastructure.
#
# Creates two EC2 instances in the same Availability Zone and cluster placement
# group within the default VPC.
#
# Each instance is configured with chrony and increased resource limits.
# Lightstream binaries must be copied to the instances after `terraform apply`,
# then the benchmark can be run with `benches/aws/run.sh`.
#
# Outputs provide the public IP addresses for SSH access and the sender's private
# IP address for the receiver connection.
#
# The `ssh_public_key_path` variable must reference the user's SSH public key.
# Terraform registers it as an AWS key pair. The corresponding private key
# remains with the user and is not included in this module.
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

# Latest Ubuntu 24.04 LTS AMI for the chosen architecture.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-${var.architecture == "x86_64" ? "amd64" : "arm64"}-server-*"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

################################################################################
# Networking - security group.
#
# Note: VPC and subnet come from defaults
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

# Separate rule so the group can reference itself. All traffic is open
# between SG members, covering the bench port, the Flight comparison
# ports and iperf3.
resource "aws_security_group_rule" "bench_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "all traffic between SG members"
}

resource "random_id" "suffix" {
  byte_length = 3
}

################################################################################
# SSH key pair.
#
# This is built from a user-supplied public key
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
  ami                         = data.aws_ami.ubuntu.id
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
  ami                         = data.aws_ami.ubuntu.id
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

# Enable chrony for accurate benchmark timing, install iperf3 for the
# line-rate baseline, and increase the open-file limit to support the
# benchmark's sockets and buffers.
locals {
  bootstrap_user_data = <<-EOT
    #!/bin/bash
    set -eu
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq chrony iperf3
    systemctl enable --now chrony
    echo '*  soft  nofile  65536' >> /etc/security/limits.conf
    echo '*  hard  nofile  65536' >> /etc/security/limits.conf
  EOT
}

################################################################################
# Variables
################################################################################

variable "region" {
  description = "AWS region in which to create the benchmark infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type. The default, `c7gn.large`, is a network-optimised
    Graviton instance. Use `c7i.large` for x86_64. Larger instance types
    provide more network bandwidth at a higher cost.
  EOT
  type        = string
  default     = "c7gn.large"
}

variable "architecture" {
  description = <<-EOT
    AMI architecture. Use `arm64` for Graviton instance types such as
    c7g, c7gn and m7g. Use `x86_64` for Intel or AMD instance types such
    as c7i and m7i.
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
    Path to the SSH public key to register with AWS for both instances.
    The corresponding private key remains with the user and is not included
    in this module.
  EOT
  type        = string
}

variable "ssh_allow_cidr" {
  description = <<-EOT
    CIDR block allowed to connect to the instances over SSH. Restrict this
    to the operator's public IP address using a /32 suffix where possible.
    The default permits SSH access from any address.
  EOT
  type        = string
  default     = "0.0.0.0/0"
}

variable "bench_port" {
  description = "TCP port used for the benchmark connection."
  type        = number
  default     = 9001
}

################################################################################
# Outputs used by `benches/aws/run.sh`.
################################################################################

output "sender_public_ip" {
  description = "Public IP address of the sender instance."
  value       = aws_instance.sender.public_ip
}

output "sender_private_ip" {
  description = "Private IP address used by the receiver to connect to the sender."
  value       = aws_instance.sender.private_ip
}

output "receiver_public_ip" {
  description = "Public IP address of the receiver instance."
  value       = aws_instance.receiver.public_ip
}

output "availability_zone" {
  description = "Availability Zone containing both benchmark instances."
  value       = data.aws_subnet.selected.availability_zone
}

output "ssh_user" {
  description = "SSH username for Amazon Linux 2023."
  value       = "ubuntu"
}

output "run_sh_invocation" {
  description = "Command for running the benchmark with the provisioned instances."
  value = <<-EOT
    SENDER_HOST=ubuntu@${aws_instance.sender.public_ip} \
    RECEIVER_HOST=ubuntu@${aws_instance.receiver.public_ip} \
    SENDER_PRIVATE_IP=${aws_instance.sender.private_ip} \
    SSH_OPTS="-i <your-private-key.pem> -o StrictHostKeyChecking=accept-new" \
    benches/aws/run.sh
  EOT
}

