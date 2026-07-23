################################################################################
# Lightstream cross-host benchmark infrastructure on Amazon ECS (EC2 launch
# type).
#
# Creates two EC2 container instances in the same Availability Zone and cluster
# placement group within the default VPC, an ECS cluster they join, an ECR
# repository for the benchmark image, and the source and sink ECS task
# definitions. `benches/ecs/run.sh` builds and pushes the image, then runs the
# source task on one instance and the sink task on the other, placing each with
# an attribute constraint on the `bench_role` container-instance attribute.
#
# The two instances are pinned to the same AZ and cluster placement group so the
# network path between them is short and near-native. Tasks use host networking,
# so the benchmark binaries bind the host's ports directly and the benchmark
# traffic flows between the instances' private IPs within the subnet. Public IPs
# are assigned only so the instances can pull the image from ECR.
#
# Resource names include a random suffix to isolate concurrent runs.
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

locals {
  name = "ls-ecs-bench-${random_id.suffix.hex}"
  tags = {
    Project   = "lightstream-bench"
    Ephemeral = "true"
  }
}

################################################################################
# Networking - default VPC, a single subnet, security group.
#
# Both instances land in the same AZ via the placement group's cluster
# strategy, which requires same-AZ membership. When `availability_zone` is
# set, the default VPC subnet in that zone is used, which moves the whole
# rig to a zone with instance capacity when the first choice runs dry.
# When it is empty, the alphabetically-first subnet in the default VPC
# selects the zone.
################################################################################

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  dynamic "filter" {
    for_each = var.availability_zone == "" ? [] : [var.availability_zone]
    content {
      name   = "availability-zone"
      values = [filter.value]
    }
  }
}

locals {
  subnet_id = sort(data.aws_subnets.default.ids)[0]
}

data "aws_subnet" "selected" {
  id = local.subnet_id
}

resource "aws_security_group" "bench" {
  name        = "${local.name}-sg"
  description = "lightstream ECS bench rig - benchmark ports between the two instances"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "all outbound (so instances can pull the image from ECR and reach CloudWatch)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

# Separate self-referencing rules so the group can reference itself. These open
# the Flight, latency echo and Lightstream ports between the two instances only.
resource "aws_security_group_rule" "bench_lightstream" {
  type                     = "ingress"
  from_port                = var.ls_port
  to_port                  = var.ls_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "Lightstream TCP between SG members"
}

resource "aws_security_group_rule" "bench_flight" {
  type                     = "ingress"
  from_port                = var.flight_port
  to_port                  = var.flight_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "Arrow Flight between SG members"
}

resource "aws_security_group_rule" "bench_echo" {
  type                     = "ingress"
  from_port                = var.echo_port
  to_port                  = var.echo_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "latency echo between SG members"
}

resource "aws_security_group_rule" "bench_ctrl" {
  type                     = "ingress"
  from_port                = var.ctrl_port
  to_port                  = var.ctrl_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.bench.id
  source_security_group_id = aws_security_group.bench.id
  description              = "sink-to-source phase control between SG members"
}

################################################################################
# ECS cluster, ECR repository and CloudWatch log groups
################################################################################

resource "aws_ecs_cluster" "bench" {
  name = local.name
  tags = local.tags
}

resource "aws_ecr_repository" "bench" {
  name                 = "${local.name}-image"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "source" {
  name              = "/ecs/${local.name}/source"
  retention_in_days = 7
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "sink" {
  name              = "/ecs/${local.name}/sink"
  retention_in_days = 7
  tags              = local.tags
}

################################################################################
# IAM - container-instance role and profile, task execution role
################################################################################

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${local.name}-instance"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = local.tags
}

# Lets the ECS agent on each instance register with the cluster and pull images.
resource "aws_iam_role_policy_attachment" "instance_ecs" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "instance" {
  name = "${local.name}-instance"
  role = aws_iam_role.instance.name
  tags = local.tags
}

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Task execution role. The ECS agent uses this to pull the image from ECR and
# write container logs to CloudWatch.
resource "aws_iam_role" "task_execution" {
  name               = "${local.name}-task-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

################################################################################
# ECS-optimised Amazon Linux 2023 AMI resolved via SSM
#
# The SSM parameter path is architecture-specific. `ami_ssm_parameter` defaults
# to the x86_64 recommended image to match the default x86_64 instance type;
# switch it to the arm64 path when using a Graviton instance type.
################################################################################

data "aws_ssm_parameter" "ecs_ami" {
  name = var.ami_ssm_parameter
}

################################################################################
# Placement group and container instances
################################################################################

resource "aws_placement_group" "bench" {
  name     = local.name
  strategy = "cluster"
  tags     = local.tags
}

# user_data joins each instance to the cluster, tags it with a bench_role
# container-instance attribute, and assembles the local NVMe instance-store
# devices into one filesystem at /mnt/nvme before the ECS agent needs it.
#
# The ECS-optimized AL2023 AMI does not auto-mount instance storage. The nvme
# instance-store devices are identified by their NVMe model string
# ('Amazon EC2 NVMe Instance Storage'), which excludes the EBS root volume.
# When more than one device is present they are combined with mdadm RAID0 so
# the aggregate read rate is available to the replay dataset; a single device
# is used directly. The filesystem is XFS, mounted at /mnt/nvme and made
# world-writable so the container's non-root user can write the dataset. The
# source and sink task definitions bind /mnt/nvme to /data in the container.
locals {
  nvme_setup = <<-EOT
    devices=$(lsblk -dno NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print "/dev/"$1}')
    device_count=$(printf '%s\n' $devices | grep -c .)
    target=""
    if [ "$device_count" -gt 1 ]; then
      command -v mdadm >/dev/null || dnf install -y mdadm
      mdadm --create /dev/md0 --level=0 --raid-devices="$device_count" $devices
      target=/dev/md0
    elif [ "$device_count" -eq 1 ]; then
      target=$devices
    fi
    if [ -n "$target" ]; then
      mkfs.xfs -f "$target"
      mkdir -p /mnt/nvme
      mount "$target" /mnt/nvme
      chmod 1777 /mnt/nvme
    fi
  EOT

  ecs_config = {
    source = <<-EOT
      #!/bin/bash
      ${local.nvme_setup}
      cat <<'CFG' >> /etc/ecs/ecs.config
      ECS_CLUSTER=${local.name}
      ECS_INSTANCE_ATTRIBUTES={"bench_role":"source"}
      CFG
    EOT
    sink = <<-EOT
      #!/bin/bash
      ${local.nvme_setup}
      cat <<'CFG' >> /etc/ecs/ecs.config
      ECS_CLUSTER=${local.name}
      ECS_INSTANCE_ATTRIBUTES={"bench_role":"sink"}
      CFG
    EOT
  }
}

resource "aws_instance" "source" {
  ami                         = data.aws_ssm_parameter.ecs_ami.value
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.bench.id]
  placement_group             = aws_placement_group.bench.name
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.instance.name

  user_data = local.ecs_config.source

  tags = merge(local.tags, {
    Name       = "${local.name}-source"
    bench_role = "source"
  })
}

resource "aws_instance" "sink" {
  ami                         = data.aws_ssm_parameter.ecs_ami.value
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.bench.id]
  placement_group             = aws_placement_group.bench.name
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.instance.name

  user_data = local.ecs_config.sink

  tags = merge(local.tags, {
    Name       = "${local.name}-sink"
    bench_role = "sink"
  })
}

################################################################################
# ECS task definitions
#
# Both tasks use the EC2 launch type with host networking, so the benchmark
# binaries bind the host's ports. The container `command` is overridden
# at run-task time by run.sh, which fills in the workload arguments and the peer
# instance's private IP. The memory value is the ECS scheduler reservation and
# the container's hard ceiling. Each instance runs a single task, so the task
# is handed most of the instance's RAM, and the ceiling covers the replay
# dataset: its file pages are charged to the container's cgroup, and the warm
# nvme passes serve from the page cache, so the default dataset size in
# run.sh is set to stay resident under it. Each task binds the instance's
# /mnt/nvme filesystem to /data, the source's default dataset directory.
################################################################################

resource "aws_ecs_task_definition" "source" {
  family                   = "${local.name}-source"
  requires_compatibilities = ["EC2"]
  network_mode             = "host"
  execution_role_arn       = aws_iam_role.task_execution.arn

  volume {
    name      = "nvme"
    host_path = "/mnt/nvme"
  }

  container_definitions = jsonencode([
    {
      name      = "source"
      image     = var.image_ref
      essential = true
      memory    = var.task_memory
      # This is a placeholder - run.sh overrides it with the full argument list.
      command = ["bench_ecs_source", "--help"]
      mountPoints = [
        {
          sourceVolume  = "nvme"
          containerPath = "/data"
          readOnly      = false
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.source.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "source"
        }
      }
    }
  ])

  tags = local.tags
}

resource "aws_ecs_task_definition" "sink" {
  family                   = "${local.name}-sink"
  requires_compatibilities = ["EC2"]
  network_mode             = "host"
  execution_role_arn       = aws_iam_role.task_execution.arn

  volume {
    name      = "nvme"
    host_path = "/mnt/nvme"
  }

  container_definitions = jsonencode([
    {
      name      = "sink"
      image     = var.image_ref
      essential = true
      memory    = var.task_memory
      # This is a placeholder - run.sh overrides it with the full argument list.
      command = ["bench_ecs_sink", "--help"]
      mountPoints = [
        {
          sourceVolume  = "nvme"
          containerPath = "/data"
          readOnly      = false
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.sink.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "sink"
        }
      }
    }
  ])

  tags = local.tags
}

################################################################################
# Variables
################################################################################

variable "region" {
  description = "AWS region in which to create the benchmark infrastructure."
  type        = string
  default     = "eu-west-2"
}

variable "availability_zone" {
  description = <<-EOT
    Availability zone for the benchmark subnet, for example `eu-west-2a`.
    Both instances follow the subnet into this zone. Leave empty to use the
    alphabetically-first subnet in the default VPC. Set it when the first
    choice reports InsufficientInstanceCapacity for the instance type, as
    the error message names the zones that currently have capacity.
  EOT
  type        = string
  default     = ""
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type for the two container instances. The default,
    `i3en.12xlarge`, is an x86_64 instance with:

      * At least 16 vCPUs, so a 16-stream cell runs each stream on its own
        core rather than becoming scheduler-bound. `i3en.12xlarge` has 48.
      * Guaranteed network bandwidth. `i3en.12xlarge` provides 50 Gbps
        (6.25 GB/s) sustained for the whole run, where smaller types offer a
        burst allowance that depletes under a continuous transfer. AWS caps
        each TCP flow at about 10 Gbps within a cluster placement group, so
        single-stream cells are flow-limited on every instance type and only
        the multi-stream cells aggregate toward the instance figure.
      * Local NVMe instance storage whose aggregate read rate exceeds the
        network rate, so the nvme data source replays off the device without
        the disk becoming the limit. `i3en.12xlarge` has 4 x 7500 GB NVMe
        reading around 8 GB/s in aggregate, above the 6.25 GB/s the network
        carries.

    The architecture matches the x86_64 image produced by a typical operator
    Docker build. Use a Graviton type together with the arm64 SSM AMI path in
    `ami_ssm_parameter` and an arm64 image (`i3en` is x86_64). Larger instance
    types provide more network bandwidth at a higher cost.
  EOT
  type        = string
  default     = "i3en.12xlarge"
}

variable "ami_ssm_parameter" {
  description = <<-EOT
    SSM parameter name for the ECS-optimised Amazon Linux 2023 AMI. The default
    is the x86_64 recommended image, matching the default x86_64 instance type.
    For Graviton instance types use
    `/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id`.
  EOT
  type        = string
  default     = "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
}

variable "image_ref" {
  description = <<-EOT
    Full image reference used by the task definitions. `run.sh` passes the
    tagged ECR image it builds and pushes (repository URL plus the git short
    SHA). When applying Terraform manually before an image exists, the default
    placeholder lets the plan complete; the tasks are only ever run through
    `run.sh`, which re-applies with the real reference.
  EOT
  type        = string
  default     = "public.ecr.aws/docker/library/busybox:latest"
}

variable "task_memory" {
  description = <<-EOT
    Hard memory limit in MiB for the benchmark container, which ECS also
    reserves on the instance at scheduling time. The default, 358400
    (350 GiB), hands the task most of an `i3en.12xlarge`'s 384 GiB and leaves
    headroom for the ECS agent and the OS. The limit covers the replay
    dataset: its file pages are charged to the container's cgroup, and the
    warm nvme passes serve from the page cache, so the default 350 GB dataset
    stays resident. Lower the value and the dataset size together for smaller
    instance types.
  EOT
  type        = number
  default     = 358400
}

variable "flight_port" {
  description = "TCP port on the source instance serving Arrow Flight."
  type        = number
  default     = 9101
}

variable "echo_port" {
  description = "TCP port on the source instance serving the latency echo."
  type        = number
  default     = 9102
}

variable "ls_port" {
  description = "TCP port on the sink instance serving the Lightstream reader."
  type        = number
  default     = 9103
}

variable "ctrl_port" {
  description = "TCP port on the source instance serving the sink's phase-control channel."
  type        = number
  default     = 9104
}

################################################################################
# Outputs used by `benches/ecs/run.sh`
################################################################################

output "region" {
  description = "AWS region containing the benchmark infrastructure."
  value       = var.region
}

output "cluster_name" {
  description = "ECS cluster name the container instances join."
  value       = aws_ecs_cluster.bench.name
}

output "ecr_repository_url" {
  description = "ECR repository URL for the benchmark image."
  value       = aws_ecr_repository.bench.repository_url
}

output "source_private_ip" {
  description = "Private IP of the source instance, used by the sink to connect."
  value       = aws_instance.source.private_ip
}

output "sink_private_ip" {
  description = "Private IP of the sink instance, used by the source to connect."
  value       = aws_instance.sink.private_ip
}

output "source_task_family" {
  description = "Family name of the source task definition."
  value       = aws_ecs_task_definition.source.family
}

output "sink_task_family" {
  description = "Family name of the sink task definition."
  value       = aws_ecs_task_definition.sink.family
}

output "source_log_group" {
  description = "CloudWatch log group receiving source container logs."
  value       = aws_cloudwatch_log_group.source.name
}

output "sink_log_group" {
  description = "CloudWatch log group receiving sink container logs."
  value       = aws_cloudwatch_log_group.sink.name
}

output "availability_zone" {
  description = "Availability Zone containing both container instances."
  value       = data.aws_subnet.selected.availability_zone
}
