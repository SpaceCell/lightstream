# AWS A-to-B benchmark infrastructure

This Terraform module creates two EC2 instances in the same Availability Zone and cluster placement group, using the default VPC.

The instances are configured with clock synchronisation and increased open-file limits. After applying the Terraform configuration, copy the Lightstream `bench_sender` and `bench_receiver` binaries to the instances and run the benchmark with `benches/aws/run.sh`.

## Prerequisites

* An AWS account with permission to manage EC2 and VPC resources.
* Terraform 1.6 or later.
* AWS credentials configured through the AWS CLI or environment variables.
* An SSH key pair.

Create an SSH key pair if required:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/lightstream_bench -N ""
```

Set `ssh_public_key_path` to the path of the public key. Terraform registers the public key as an EC2 key pair. The private key remains on the local machine and is not stored in this repository or managed by Terraform.

## Inputs

| Variable              |      Default | Description                                                                                                 |
| --------------------- | -----------: | ----------------------------------------------------------------------------------------------------------- |
| `region`              |  `us-east-1` | AWS region in which to create the benchmark infrastructure.                                                 |
| `instance_type`       | `c7gn.large` | EC2 instance type. Use `c7i.large` with `architecture = "x86_64"` for x86_64.                               |
| `architecture`        |      `arm64` | AMI architecture: `arm64` for Graviton or `x86_64` for Intel and AMD instances.                             |
| `ssh_public_key_path` |     Required | Path to the SSH public key, such as `~/.ssh/lightstream_bench.pub`.                                         |
| `ssh_allow_cidr`      |  `0.0.0.0/0` | CIDR block permitted to connect over SSH. Restrict this to the operator's public IP address where possible. |
| `bench_port`          |       `9001` | TCP port used for the benchmark connection.                                                                 |

## Provision the instances

```bash
cd benches/aws/terraform

terraform init

terraform apply \
    -var "ssh_public_key_path=$HOME/.ssh/lightstream_bench.pub" \
    -var "ssh_allow_cidr=$(curl -fsS https://checkip.amazonaws.com)/32"
```

Terraform returns the instance addresses and a command for running the benchmark:

```text
Outputs:

sender_public_ip   = "..."
sender_private_ip  = "..."
receiver_public_ip = "..."
availability_zone  = "us-east-1a"
ssh_user           = "ec2-user"
run_sh_invocation  = <<EOT
SENDER_HOST=ec2-user@... ...
EOT
```

## Build and copy the binaries

For the default ARM64 instance type:

```bash
cargo build --release --target aarch64-unknown-linux-gnu \
    --example bench_sender \
    --example bench_receiver \
    --features tcp
```

For x86_64 instances, omit `--target` or use `--target x86_64-unknown-linux-gnu`.

Copy the binaries to both instances:

```bash
SENDER_IP=$(terraform output -raw sender_public_ip)
RECEIVER_IP=$(terraform output -raw receiver_public_ip)
KEY=$HOME/.ssh/lightstream_bench

for host in "$SENDER_IP" "$RECEIVER_IP"; do
  scp -i "$KEY" -o StrictHostKeyChecking=accept-new \
      target/aarch64-unknown-linux-gnu/release/examples/bench_sender \
      target/aarch64-unknown-linux-gnu/release/examples/bench_receiver \
      "ec2-user@$host:/tmp/"

  ssh -i "$KEY" "ec2-user@$host" \
      "sudo mv /tmp/bench_{sender,receiver} /usr/local/bin/"
done
```

Update the binary paths when building for a different target.

## Run the benchmark

The `run_sh_invocation` output contains the instance addresses required by `benches/aws/run.sh`. Set `SSH_OPTS` to use the corresponding private key:

```bash
SENDER_HOST=ec2-user@$(terraform output -raw sender_public_ip) \
RECEIVER_HOST=ec2-user@$(terraform output -raw receiver_public_ip) \
SENDER_PRIVATE_IP=$(terraform output -raw sender_private_ip) \
SHAPE=mixed \
ROWS=100000 \
BATCHES=2000 \
SSH_OPTS="-i $HOME/.ssh/lightstream_bench -o StrictHostKeyChecking=accept-new" \
../run.sh
```

The receiver writes a result line to standard output:

```text
RESULT shape=... gib_per_s=...
```

The sender log remains on the sender instance at:

```text
/tmp/lightstream_bench_sender_<pid>.log
```

## Destroy the infrastructure

```bash
terraform destroy \
    -var "ssh_public_key_path=$HOME/.ssh/lightstream_bench.pub"
```

Destroy the infrastructure after the benchmark completes to avoid further EC2 charges.

## Limitations

* Both instances are created in the same Availability Zone. Cross-AZ and cross-region benchmarks require separate infrastructure and network configuration.
* Larger instance types may provide higher network bandwidth and may have additional networking requirements.
* The benchmark uses the sender's private IPv4 address. IPv6 is not configured.
* The sender and receiver use plaintext TCP. TLS, QUIC, WebTransport and HTTP/2 measurements are handled by `benches/transport/transport_bench_matrix.rs`.
