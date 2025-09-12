#!/bin/bash
# scripts/init_cluster.sh

set -e

echo "🚀 Setting up MapReduce development cluster..."

# Create directory structure
mkdir -p dev/ssh dev/scripts target/release

# Generate SSH keys for passwordless access between containers
if [ ! -f dev/ssh/id_rsa ]; then
    echo "📝 Generating SSH keys..."
    ssh-keygen -t rsa -b 2048 -f dev/ssh/id_rsa -N "" -C "mapreduce-dev"
fi

# Create SSH config for easy access
cat > dev/ssh/config <<EOF
Host master worker*
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    User root
    IdentityFile /root/.ssh/id_rsa
    LogLevel ERROR
EOF

# Create a helper script to build binaries if needed
cat > dev/scripts/build.sh <<'EOF'
#!/bin/bash
echo "🔨 Building MapReduce binaries..."
cargo build --release --bin master
cargo build --release --bin worker
EOF
chmod +x dev/scripts/build.sh

# Create deployment script
cat > dev/scripts/deploy.sh <<'EOF'
#!/bin/bash
set -e

WORKERS=${1:-3}

echo "🚀 Deploying cluster with $WORKERS workers..."

# Stop existing cluster
docker-compose down 2>/dev/null || true

# Generate dynamic docker-compose with specified number of workers
cat > docker-compose.dynamic.yml <<COMPOSE
version: '3.8'

services:
  master:
    build:
      context: .
      dockerfile: Dockerfile.node
    container_name: mapreduce-master
    hostname: master
    networks:
      mapreduce-net:
        ipv4_address: 172.28.1.10
    volumes:
      - ./target/release/master:/opt/mapreduce/bin/master:ro
      - ./target/release/worker:/opt/mapreduce/bin/worker:ro
      - ./dev/ssh/id_rsa.pub:/root/.ssh/authorized_keys:ro
      - ./dev/ssh/id_rsa:/root/.ssh/id_rsa:ro
      - ./dev/ssh/config:/root/.ssh/config:ro
      - mapreduce-data:/opt/mapreduce/data
    ports:
      - "50051:50051"
      - "2221:22"

COMPOSE

# Add workers dynamically
for i in $(seq 1 $WORKERS); do
    cat >> docker-compose.dynamic.yml <<WORKER

  worker$i:
    build:
      context: .
      dockerfile: Dockerfile.node
    container_name: mapreduce-worker$i
    hostname: worker$i
    networks:
      mapreduce-net:
        ipv4_address: 172.28.1.$((10 + i))
    volumes:
      - ./target/release/worker:/opt/mapreduce/bin/worker:ro
      - ./dev/ssh/id_rsa.pub:/root/.ssh/authorized_keys:ro
      - ./dev/ssh/id_rsa:/root/.ssh/id_rsa:ro
      - ./dev/ssh/config:/root/.ssh/config:ro
      - mapreduce-data:/opt/mapreduce/data
    depends_on:
      - master
WORKER
done

# Add networks and volumes
cat >> docker-compose.dynamic.yml <<FOOTER

networks:
  mapreduce-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16

volumes:
  mapreduce-data:
FOOTER

# Start the cluster
docker-compose -f docker-compose.dynamic.yml up -d

echo "⏳ Waiting for containers to be ready..."
sleep 3

echo "✅ Cluster is ready!"
echo ""
echo "📊 Cluster status:"
docker-compose -f docker-compose.dynamic.yml ps

echo ""
echo "🔗 Access points:"
echo "  - Master SSH: ssh -p 2221 root@localhost"
echo "  - Master RPC: localhost:50051"
echo ""
echo "📝 Useful commands:"
echo "  - View logs: docker-compose -f docker-compose.dynamic.yml logs -f"
echo "  - Stop cluster: docker-compose -f docker-compose.dynamic.yml down"
echo "  - SSH to master: docker exec -it mapreduce-master /bin/sh"
echo "  - SSH to worker: docker exec -it mapreduce-worker1 /bin/sh"
EOF
chmod +x dev/scripts/deploy.sh

# Create test orchestration script
cat > dev/scripts/test-orchestration.sh <<'EOF'
#!/bin/bash
# Test SSH orchestration from master to workers

echo "🧪 Testing orchestration..."

# SSH into master and run commands on workers
docker exec mapreduce-master sh -c '
    for i in 1 2 3; do
        echo "Testing connection to worker$i..."
        ssh worker$i "hostname && echo OK"
    done
'

# Test spawning a binary on all workers from master
docker exec mapreduce-master sh -c '
    for i in 1 2 3; do
        echo "Spawning test process on worker$i..."
        ssh worker$i "nohup /opt/mapreduce/bin/worker --test > /tmp/worker.log 2>&1 &"
    done
'
EOF
chmod +x dev/scripts/test-orchestration.sh

echo "✅ Development environment setup complete!"
echo ""
echo "📚 Quick start:"
echo "  1. Build your binaries: ./dev/scripts/build.sh"
echo "  2. Deploy cluster: ./dev/scripts/deploy.sh [num_workers]"
echo "  3. Test orchestration: ./dev/scripts/test-orchestration.sh"
