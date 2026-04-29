# 05 - Advanced Routing & Patterns

RabbitMQ isn't just for point-to-point messaging. It supports several classic distributed system patterns.

---

## 1. Work Queues (Competing Consumers)
Used to distribute time-consuming tasks among multiple workers.

- **Pattern**: One Queue, Multiple Consumers.
- **Behavior**: RabbitMQ uses Round-Robin by default. Each message is delivered to exactly one consumer.
- **Goal**: Parallelize work. If tasks are piling up, just add more workers.

---

## 2. Publish/Subscribe
Delivering the same message to multiple consumers.

- **Pattern**: Fanout Exchange, Multiple Queues (one per consumer).
- **Goal**: One event (e.g., `user_registered`) triggers multiple independent actions (e.g., Send Welcome Email, Create Analytics Profile, Generate Invoice).

---

## 3. RPC (Remote Procedure Call)
Using RabbitMQ to wait for a response from a remote service.

- **How it works**:
  1. The client sends a request and sets a `reply_to` queue property.
  2. The server processes the request and sends the result to the `reply_to` queue.
  3. The client waits for the message on its private callback queue.
- **Correlation ID**: A unique ID per request to match responses to requests.

---

## 4. Priority Queues
Messages with higher priority are consumed before lower priority ones.

- **Requirement**: The queue must be declared with `x-max-priority`.
- **Message Property**: Set the `priority` property on each message.
- **Note**: This only works if consumers are slow or the queue is backed up. If consumers are fast, they just take whatever arrives first.

---

## 5. Consistent Hash Exchange (Plugin)
Used for **Sharding** messages across multiple queues.

- **Why?**: Standard exchanges can be a bottleneck at extreme scales (millions of msgs/sec). Consistent hashing ensures a specific "routing key" always goes to the same queue, which is useful for maintaining order or local caching.

---

## Summary of Patterns

| Pattern | Exchange Type | Best For... |
| :--- | :--- | :--- |
| **Work Queues** | Default/Direct | Task distribution |
| **Pub/Sub** | Fanout | Event-driven systems |
| **Routing** | Direct | Selective logging/filtering |
| **Topics** | Topic | Complex event routing |
| **RPC** | Default | Request-Response |

---

## Next Steps
As your system grows, a single RabbitMQ node becomes a single point of failure. Next, we'll learn about **Clustering & High Availability**.
