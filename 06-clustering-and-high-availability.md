# 06 - Clustering & High Availability

In a production environment, you cannot rely on a single RabbitMQ node. If it goes down, your entire system stops.

---

## 1. What is a RabbitMQ Cluster?
A cluster is a group of RabbitMQ nodes that appear as a single logical broker. 

- **Metadata**: All nodes share the same users, vhosts, exchanges, and bindings.
- **Data (Queues)**: By default, a queue resides on **one** node (the node it was declared on).

---

## 2. Quorum Queues (The Modern HA)
**Quorum Queues** are the recommended way to achieve high availability in modern RabbitMQ (3.8+).

- **How they work**: Based on the **Raft consensus algorithm**. Data is replicated across a majority of nodes in the cluster.
- **Benefits**:
  - **Durable by Default**: They are always persistent.
  - **Fault Tolerant**: If one node fails, the queue remains available as long as a majority (quorum) of nodes are online.
  - **No Split-Brain**: Raft prevents data corruption during network partitions.

> [!TIP]
> Use Quorum Queues for all critical business data where reliability is more important than raw speed.

---

## 3. Mirrored Queues (Deprecated)
Before Quorum Queues, RabbitMQ used "Classic Mirrored Queues". 
- **Status**: These are being deprecated.
- **Problem**: They are prone to synchronization issues and performance bottlenecks during network hiccups. 
- **Action**: If you are starting a new project, **always use Quorum Queues** instead.

---

## 4. RabbitMQ Streams
Introduced in version 3.9, **Streams** allow for high-throughput, persistent messaging that can be "replayed".

- **Similar to Kafka**: Data is stored as an append-only log on disk.
- **Use Case**: When you need to process the same messages multiple times or handle extreme throughput that regular queues can't manage.

---

## 5. Cross-Cluster Communication

Sometimes you need to move messages between different data centers or cloud regions.

### 5.1 Federation
Allows an exchange or queue on one broker to receive messages published to an exchange or queue on another broker.
- **Use Case**: Sharing data between geographically separated clusters.

### 5.2 Shovel
A simpler, lower-level way to move messages. It literally consumes from one queue and publishes to another (potentially on a different broker).
- **Use Case**: Migrating data between different versions of RabbitMQ or bridging two isolated systems.

---

## 6. Load Balancing
You should never connect your application to a single node in the cluster. Use a **Load Balancer** (like HAProxy or AWS NLB) in front of the cluster to distribute connections.

---

## Next Steps
Scaling is great, but how do we know if it's working? Next, we'll cover **Performance Tuning & Monitoring**.
