#!/usr/bin/env bash
# Orchestrate a lightstream A->B bench between two reachable hosts.
#
# Assumes the `bench_sender` and `bench_receiver` binaries (or the
# container image built from `bench/aws/Dockerfile`) are already
# present on both hosts. The script ssh's into the sender host to
# bind a TCP listener, then ssh's into the receiver host to connect
# and time the receive loop. Both sides report their own throughput;
# the receiver number is the headline figure.
#
# Configuration via environment:
#
#   SENDER_HOST       SSH target for the sender (e.g. ec2-user@1.2.3.4)
#   RECEIVER_HOST     SSH target for the receiver
#   SENDER_PRIVATE_IP Private IP of the sender that the receiver
#                     connects to (typically the VPC interface). If
#                     unset, defaults to SENDER_HOST stripped of any
#                     user@ prefix - useful when ssh'ing via the
#                     private IP directly.
#   PORT              TCP port (default 9001).
#   SHAPE             mixed | narrow_numeric | string_heavy | wide
#   ROWS              rows per batch
#   BATCHES           total batches to send
#   SSH_OPTS          extra ssh options (-i key.pem, -p port, ...)
#   BIN_DIR           remote directory holding the binaries
#                     (default /usr/local/bin)

set -euo pipefail

SENDER_HOST="${SENDER_HOST:?SENDER_HOST required}"
RECEIVER_HOST="${RECEIVER_HOST:?RECEIVER_HOST required}"
SENDER_PRIVATE_IP="${SENDER_PRIVATE_IP:-${SENDER_HOST#*@}}"
PORT="${PORT:-9001}"
SHAPE="${SHAPE:-mixed}"
ROWS="${ROWS:-100000}"
BATCHES="${BATCHES:-1000}"
SSH_OPTS="${SSH_OPTS:-}"
BIN_DIR="${BIN_DIR:-/usr/local/bin}"

echo "[run.sh] sender=$SENDER_HOST receiver=$RECEIVER_HOST"
echo "[run.sh] shape=$SHAPE rows=$ROWS batches=$BATCHES port=$PORT"
echo "[run.sh] receiver will connect to $SENDER_PRIVATE_IP:$PORT"

# Launch the sender, backgrounded, with output streamed to a log file
# on the sender host.
SENDER_LOG="/tmp/lightstream_bench_sender_$$.log"
ssh $SSH_OPTS "$SENDER_HOST" \
    "nohup $BIN_DIR/bench_sender \
        --bind 0.0.0.0:$PORT \
        --shape $SHAPE \
        --rows $ROWS \
        --batches $BATCHES \
        > $SENDER_LOG 2>&1 < /dev/null &"

# Give the sender a moment to bind before the receiver connects. The
# accept() side of bench_sender blocks until the client arrives, so
# the sleep is just to keep startup order legible in the logs.
sleep 1

echo "[run.sh] launching receiver"
ssh $SSH_OPTS "$RECEIVER_HOST" \
    "$BIN_DIR/bench_receiver \
        --connect $SENDER_PRIVATE_IP:$PORT \
        --shape $SHAPE \
        --rows $ROWS \
        --batches $BATCHES"

echo "[run.sh] receiver complete; sender log on $SENDER_HOST -> $SENDER_LOG"
