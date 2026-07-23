#!/usr/bin/env bash
# Runs the Lightstream A-to-B benchmark across two hosts.
#
# The `bench_sender` and `bench_receiver` binaries must already be installed on
# both hosts. The script starts the sender over SSH, then runs the receiver and
# waits for it to complete. The sender output is written to a remote log file.
#
# Configuration:
#
#   SENDER_HOST        SSH target for the sender, such as ec2-user@1.2.3.4.
#   RECEIVER_HOST      SSH target for the receiver.
#   SENDER_PRIVATE_IP  Address used by the receiver to connect to the sender.
#                      Defaults to SENDER_HOST without its user prefix.
#   PORT               TCP port. Defaults to 9001.
#   SHAPE              Data shape: mixed, narrow_numeric, string_heavy or wide.
#   ROWS               Number of rows per batch.
#   BATCHES            Number of batches to send.
#   SSH_OPTS           Additional SSH options, such as an identity file or port.
#   BIN_DIR            Remote binary directory. Defaults to /usr/local/bin.

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

# Start the sender and write its output to a log file on the remote host.
SENDER_LOG="/tmp/lightstream_bench_sender_$$.log"
ssh $SSH_OPTS "$SENDER_HOST" \
    "nohup $BIN_DIR/bench_sender \
        --bind 0.0.0.0:$PORT \
        --shape $SHAPE \
        --rows $ROWS \
        --batches $BATCHES \
        > $SENDER_LOG 2>&1 < /dev/null &"

# Allow the sender time to bind before starting the receiver.
sleep 1

echo "[run.sh] launching receiver"
ssh $SSH_OPTS "$RECEIVER_HOST" \
    "$BIN_DIR/bench_receiver \
        --connect $SENDER_PRIVATE_IP:$PORT \
        --shape $SHAPE \
        --rows $ROWS \
        --batches $BATCHES"

echo "[run.sh] receiver complete; sender log on $SENDER_HOST -> $SENDER_LOG"
