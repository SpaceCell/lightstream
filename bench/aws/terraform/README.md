# Terraform provisioning for the A-to-B rig

Stands up two EC2 instances in the same Availability Zone, in a cluster
placement group, on the AWS default VPC. The instances are bootstrapped
minimally; the lightstream `bench_sender` and `bench_receiver` binaries
are SCP'd in after `terraform apply` and then driven by `bench/aws/run.sh`.

## Prerequisites

- AWS account with EC2 + VPC permissions.
- Terraform `>= 1.6` installed locally.
- `aws` CLI configured with credentials (`aws configure` or environment
  variables).
- An SSH key pair the user already holds. Generate one if you have not:

  ```bash
  ssh-keygen -t ed25519 -f ~/.ssh/lightstream_bench -N ""
  ```

No SSH key material is committed in this repository. The public key path
is supplied through the `ssh_public_key_path` variable; Terraform reads
it from disk and registers it as an EC2 key pair tied to a one-shot name
prefixed with a random suffix. The matching private key stays on the
operator's workstation.

## Inputs

| Variable               | Default          | Notes                                                                              |
|------------------------|------------------|------------------------------------------------------------------------------------|
| `region`               | `us-east-1`      | Any region the AWS account can launch EC2 in.                                       |
| `instance_type`        | `c7gn.large`     | Network-optimised Graviton. For Intel/AMD, use `c7i.large` and switch `architecture`. |
| `architecture`         | `arm64`          | `arm64` for Graviton, `x86_64` for Intel/AMD types.                                |
| `ssh_public_key_path`  | _(required)_     | Filesystem path to your SSH public key, e.g. `~/.ssh/lightstream_bench.pub`.       |
| `ssh_allow_cidr`       | `0.0.0.0/0`      | CIDR allowed to SSH. Set to `<your-public-ip>/32` if leaving the rig running.       |
| `bench_port`           | `9001`           | TCP port the sender listens on and the receiver connects to.                       |

## Apply

```bash
cd bench/aws/terraform

terraform init

terraform apply \
    -var "ssh_public_key_path=$HOME/.ssh/lightstream_bench.pub" \
    -var "ssh_allow_cidr=$(curl -fsS https://checkip.amazonaws.com)/32"
```

Successful apply outputs:

```
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

## Push binaries onto the instances

Build the binaries locally - `arm64` if you kept the Graviton default:

```bash
cargo build --release --target aarch64-unknown-linux-gnu \
    --example bench_sender --example bench_receiver --features tcp
```

For x86 instances, drop the `--target` flag or use `x86_64-unknown-linux-gnu`.

Then SCP both binaries to both hosts (each side runs only one of the
two, but mirroring is the simplest setup):

```bash
SENDER_IP=$(terraform output -raw sender_public_ip)
RECEIVER_IP=$(terraform output -raw receiver_public_ip)
KEY=$HOME/.ssh/lightstream_bench

for host in $SENDER_IP $RECEIVER_IP; do
  scp -i "$KEY" -o StrictHostKeyChecking=accept-new \
      target/aarch64-unknown-linux-gnu/release/examples/bench_sender \
      target/aarch64-unknown-linux-gnu/release/examples/bench_receiver \
      ec2-user@$host:/tmp/
  ssh -i "$KEY" ec2-user@$host "sudo mv /tmp/bench_{sender,receiver} /usr/local/bin/"
done
```

## Run the bench

`terraform output run_sh_invocation` prints a ready-to-paste line that
plugs the host/IP outputs into `bench/aws/run.sh`. Set `SSH_OPTS` to
point at the matching private key:

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

The receiver prints one `RESULT shape=... gib_per_s=...` line on stdout;
the sender's log is left on the sender host at `/tmp/lightstream_bench_sender_<pid>.log`.

## Tear down

```bash
terraform destroy \
    -var "ssh_public_key_path=$HOME/.ssh/lightstream_bench.pub"
```

EC2 instances are billed by the second; the rig is intended to be
provisioned, measured, and destroyed in one session.

## What this module does not cover

- Cross-region or cross-AZ rigs. The cluster placement group requires
  same-AZ. For an inter-region measurement, fork this module into two
  per-region copies and add explicit peering.
- Larger network classes that require Elastic Network Adapter
  enablement (`*.metal` and several `n` types). For most measurements
  the `c7gn.large` default is enough; bump `instance_type` for higher
  ceilings, but verify ENA is on by default for your chosen size.
- IPv6. The bench connects via the IPv4 private IP exposed in the
  `sender_private_ip` output.
- TLS or QUIC variants. The sender/receiver pair speaks plaintext TCP;
  the matrix bench at `benches/transport_throughput.rs` is where to
  measure TLS / QUIC / WebTransport / HTTP/2.
