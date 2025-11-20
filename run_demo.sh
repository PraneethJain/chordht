#!/bin/bash

# Chord DHT Demo Script
# This script starts the monitor and multiple nodes, then opens the visualization

set -e

echo "Starting Chord DHT Demo..."

echo "Cleaning up any existing processes..."
pkill -f "chord_monitor" || true
pkill -f "chord_node" || true
sleep 1

echo "Building frontend..."
pushd chord_monitor/frontend > /dev/null
npm install
npm run build
popd > /dev/null

echo "Building project..."
cargo build --workspace

MONITOR_BIN="./target/debug/chord_monitor"
NODE_BIN="./target/debug/chord_node"


echo "Starting monitor..."
pushd chord_monitor > /dev/null
../target/debug/chord_monitor &
MONITOR_PID=$!
popd > /dev/null
sleep 2

echo "Starting node 1 (port 5000)..."
$NODE_BIN --port 5000 --monitor 127.0.0.1:50051 &
NODE1_PID=$!
sleep 2

echo "Starting node 2 (port 5001)..."
$NODE_BIN --port 5001 --join 127.0.0.1:5000 --monitor 127.0.0.1:50051 &
NODE2_PID=$!
sleep 2

echo "Starting node 3 (port 5002)..."
$NODE_BIN --port 5002 --join 127.0.0.1:5000 --monitor 127.0.0.1:50051 &
NODE3_PID=$!
sleep 2

echo "Starting node 4 (port 5003)..."
$NODE_BIN --port 5003 --join 127.0.0.1:5000 --monitor 127.0.0.1:50051 &
NODE4_PID=$!
sleep 2

echo ""
echo "All nodes started successfully!"
echo "Monitor PID: $MONITOR_PID"
echo "Node 1 PID: $NODE1_PID"
echo "Node 2 PID: $NODE2_PID"
echo "Node 3 PID: $NODE3_PID"
echo "Node 4 PID: $NODE4_PID"
echo ""
echo "Visit: http://localhost:3000"

echo ""
echo "Press Ctrl+C to stop all processes..."

trap "echo 'Stopping all processes...'; kill $MONITOR_PID $NODE1_PID $NODE2_PID $NODE3_PID $NODE4_PID 2>/dev/null; exit" INT
wait
