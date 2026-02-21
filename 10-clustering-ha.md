# Clustering and High Availability

## Why Clustering?

Single RabbitMQ instance is a single point of failure. Clustering provides:

- **High Availability**: Service continues if nodes fail
- **Load Distribution**: Spread traffic across nodes
- **Scalability**: Handle more messages by adding nodes
- **Fault Tolerance**: Tolerate node failures

```
                    ┌─────────────────┐
                    │  Load Balancer  │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ↓                    ↓                    ↓
    ┌────────┐           ┌────────┐          ┌────────┐
    │ Node 1 │───────────│ Node 2 │──────────│ Node 3 │
    │ Broker │           │ Broker │          │ Broker │
    └────────┘           └────────┘          └────────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                    (cluster network)
```

## Key Concepts

### Erlang Cookie

All nodes must share the same Erlang cookie to communicate.

```bash
# Check cookie location
cat ~/.erlang.cookie
cat /var/lib/rabbitmq/.erlang.cookie

# Set cookie (on all nodes - must be identical)
export RABBITMQ_ERLANG_COOKIE=my_secret_cookie_12345
```

### Cluster Formation

```
Node A starts first (seed node)
  ↓
Node B joins Node A (becomes cluster member)
  ↓
Node C joins Node A (becomes cluster member)
  ↓
All three form cluster and share state
```

## Setting Up a Cluster

### Step 1: Start First Node (Seed)

```bash
# On Node A (192.168.1.10)
docker run -d --name rabbitmq-1 \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_ERLANG_COOKIE=shared_secret \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3.12-management
```

### Step 2: Start Additional Nodes

```bash
# On Node B (192.168.1.11)
docker run -d --name rabbitmq-2 \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_ERLANG_COOKIE=shared_secret \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  --link rabbitmq-1 \
  rabbitmq:3.12-management

# On Node C (192.168.1.12)
docker run -d --name rabbitmq-3 \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_ERLANG_COOKIE=shared_secret \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  --link rabbitmq-1 \
  rabbitmq:3.12-management
```

### Step 3: Cluster Nodes

```bash
# SSH into Node B
docker exec -it rabbitmq-2 bash

# Stop app (don't shut down Erlang)
rabbitmqctl stop_app

# Join cluster (connect to Node A)
rabbitmqctl join_cluster rabbit@rabbitmq-1

# Start app
rabbitmqctl start_app

# Repeat for Node C
```

### Step 4: Verify Cluster

```bash
rabbitmqctl cluster_status

# Output:
# Cluster status of node rabbit@rabbitmq-1
# [{nodes,[{disc,[rabbit@rabbitmq-1,rabbit@rabbitmq-2,rabbit@rabbitmq-3]}]},
#  {running_nodes,[rabbit@rabbitmq-1,rabbit@rabbitmq-2,rabbit@rabbitmq-3]}]
```

## Queue Replication with Mirroring (Classic Queues)

Classic queues can be mirrored (replicated) across nodes:

```
Queue Master (Node A)
    ↓ ↓ ↓ (replicate)
    ↓   ↓
Replica(Node B, Node C)

If Node A fails, a replica becomes master
```

### Setting Up Queue Mirroring

```bash
# Set policy: mirror all queues to 2 nodes
rabbitmqctl set_policy ha-all ".*" \
  '{"ha-mode":"exactly", "ha-params":2, "ha-sync-mode":"automatic"}'

# Policies:
# ha-mode: all, nodes, exactly
# ha-params: number of replicas or node list
# ha-sync-mode: automatic or manual
```

### Policy Examples

```bash
# Mirror all queues to all nodes
rabbitmqctl set_policy ha-all ".*" \
  '{"ha-mode":"all", "ha-sync-mode":"automatic"}'

# Mirror to exactly 2 nodes
rabbitmqctl set_policy ha-2 "important-.*" \
  '{"ha-mode":"exactly", "ha-params":2}'

# Mirror to specific nodes
rabbitmqctl set_policy ha-nodes "critical.*" \
  '{"ha-mode":"nodes", "ha-params":["rabbit@node1","rabbit@node2"]}'

# Remove policy
rabbitmqctl clear_policy ha-all
```

## Classic vs Quorum Queues

### Classic Queues (Mirrored)

```
Master Queue (Node A)
    ↓ (sync via master)
Replica (Node B)
Replica (Node C)

If master dies → Replica promoted
BUT: unsync replicas may lose messages
```

**Pros:**
- Traditional approach
- Flexible replica count

**Cons:**
- Async replication can lose messages
- Complex failover

### Quorum Queues (Modern - RabbitMQ 3.8+)

```
Node A
  ↓ ↓ ↓ (Raft consensus)
Node B ↔ Node C

All 3 agree before message confirmed
Guaranteed no message loss
```

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare quorum queue (requires 3+ nodes)
channel.queue_declare(
    queue='quorum_queue',
    durable=True,
    arguments={
        'x-queue-type': 'quorum'  # Not classic (default)
    }
)

# Quorum queues auto-replicate across cluster
print("Quorum queue created - auto-replicated!")
connection.close()
```

**Pros:**
- Stronger consistency (Raft consensus)
- Guaranteed message delivery
- Simpler failover

**Cons:**
- Minimum 3 nodes required
- Slightly higher latency

## Load Balancer Configuration

Use HAProxy or Nginx to distribute connections:

### HAProxy Example

```
# /etc/haproxy/haproxy.cfg
global
    maxconn 4096

frontend rabbitmq_lb
    bind *:5672
    mode tcp
    default_backend rabbitmq_cluster

backend rabbitmq_cluster
    mode tcp
    balance roundrobin
    server rabbitmq1 192.168.1.10:5672 check
    server rabbitmq2 192.168.1.11:5672 check
    server rabbitmq3 192.168.1.12:5672 check
```

### Client Connection

```python
import pika

# Connect through load balancer
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='load-balancer.example.com',  # Points to all nodes
        port=5672,
        connection_attempts=3,
        retry_delay=2
    )
)
```

## Node Types

### Disc Node

```
- Stores queue/exchange definitions on disk
- Larger memory footprint
- Can start independently
```

```bash
rabbitmqctl change_cluster_node_type disc
```

### RAM Node

```
- Only in-memory storage of definitions
- Cannot survive restart
- Must connect to disc node on startup
```

```bash
rabbitmqctl change_cluster_node_type ram
```

**Typical Setup:**
- 1-2 disc nodes (master/backup)
- 1+ ram nodes (worker nodes)

## Monitoring Cluster Health

### Check Node Status

```bash
rabbitmqctl node_health_check

# Output: Health check passed
```

### List Cluster Members

```bash
rabbitmqctl list_cluster_nodes

# Output:
# rabbit@node1
# rabbit@node2
# rabbit@node3
```

### Check Queue Masters

```bash
rabbitmqctl list_queues name node

# Output:
# hello               rabbit@node1
# task_queue          rabbit@node2
# notifications       rabbit@node3
```

## Complete Clustering Example (Docker Compose)

```yaml
# docker-compose.yml
version: '3.8'

services:
  rabbitmq-1:
    image: rabbitmq:3.12-management
    container_name: rabbitmq-1
    environment:
      RABBITMQ_ERLANG_COOKIE: clustering_cookie_secret
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin123
      RABBITMQ_DEFAULT_VHOST: /
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq1_data:/var/lib/rabbitmq
    networks:
      - rabbit_cluster
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 30s
      timeout: 10s
      retries: 5

  rabbitmq-2:
    image: rabbitmq:3.12-management
    container_name: rabbitmq-2
    environment:
      RABBITMQ_ERLANG_COOKIE: clustering_cookie_secret
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin123
      RABBITMQ_DEFAULT_VHOST: /
    ports:
      - "5673:5672"
      - "15673:15672"
    volumes:
      - rabbitmq2_data:/var/lib/rabbitmq
    networks:
      - rabbit_cluster
    depends_on:
      - rabbitmq-1
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 30s
      timeout: 10s
      retries: 5

  rabbitmq-3:
    image: rabbitmq:3.12-management
    container_name: rabbitmq-3
    environment:
      RABBITMQ_ERLANG_COOKIE: clustering_cookie_secret
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin123
      RABBITMQ_DEFAULT_VHOST: /
    ports:
      - "5674:5672"
      - "15674:15672"
    volumes:
      - rabbitmq3_data:/var/lib/rabbitmq
    networks:
      - rabbit_cluster
    depends_on:
      - rabbitmq-1
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  rabbitmq1_data:
  rabbitmq2_data:
  rabbitmq3_data:

networks:
  rabbit_cluster:
    driver: bridge
```

**Start cluster:**
```bash
docker-compose up -d

# Join nodes
docker exec rabbitmq-2 rabbitmqctl stop_app
docker exec rabbitmq-2 rabbitmqctl join_cluster rabbit@rabbitmq-1
docker exec rabbitmq-2 rabbitmqctl start_app

docker exec rabbitmq-3 rabbitmqctl stop_app
docker exec rabbitmq-3 rabbitmqctl join_cluster rabbit@rabbitmq-1
docker exec rabbitmq-3 rabbitmqctl start_app

# Verify
docker exec rabbitmq-1 rabbitmqctl cluster_status
```

## Removing Nodes from Cluster

```bash
# Graceful removal (on the node to remove)
rabbitmqctl stop_app
rabbitmqctl reset
rabbitmqctl start_app

# Force remove from another node
rabbitmqctl forget_cluster_node rabbit@old-node
```

## Clustering Best Practices

1. **Use quorum queues** for new clusters (stronger guarantees)
2. **Minimum 3 nodes** for HA (odd number for Raft)
3. **Odd number of nodes** (1, 3, 5, 7, etc.)
4. **Separate load balancer** (HAProxy, Nginx)
5. **Monitor cluster status** continuously
6. **Regular backups** of definitions (definitions.json)
7. **Use disc nodes** as master/backup
8. **Sync queues** before replication-heavy operations
9. **Plan failover** scenarios in advance
10. **Document** your cluster topology
