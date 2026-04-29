# 07 - Performance Tuning & Monitoring

A "slow" RabbitMQ is usually a result of misconfiguration or lack of monitoring. 

---

## 1. The Management Plugin
The built-in Web UI is your first line of defense.
- **Overview Tab**: Shows cluster-wide health.
- **Queues Tab**: Look for "Message Rates" and "Consumer Count".
- **Idle Queues**: Find queues that aren't being used and delete them.

---

## 2. Resource Alarms
RabbitMQ will stop accepting messages if it runs out of resources.

- **Memory Alarm**: By default, if RabbitMQ uses > 40% of available RAM, it blocks all producers.
- **Disk Alarm**: If free disk space falls below a threshold (default 50MB), it blocks all producers.

> [!CAUTION]
> If your producers are "blocked", your application will hang. Always monitor these thresholds!

---

## 3. Flow Control
When a producer sends messages faster than the broker can process or disk can write, RabbitMQ triggers **Flow Control**. 
- It slows down the TCP connection of the producer to allow the broker to catch up.

---

## 4. Performance Tuning Tips

### 4.1 Message Size
RabbitMQ is optimized for small messages (kilobytes). If you need to send large files (megabytes), store them in S3/Minio and only send the **URL** in the message.

### 4.2 Connection & Channel Management
- **Reuse Connections**: Opening a TCP connection is slow. Keep them open.
- **Don't Overuse Channels**: While channels are light, thousands of them can still consume significant memory.

### 4.3 Message Persistence
Persistent messages are written to disk, which is slower than memory-only messages. Only use persistence for data that **cannot** be lost.

### 4.4 Lazy Queues
Use **Lazy Queues** if you expect to have millions of messages sitting in a queue for a long time. They store messages on disk immediately, saving RAM.

---

## 5. Modern Monitoring
For production, use the **Prometheus** plugin.
- **Endpoint**: `/metrics`
- **Dashboard**: Use the official RabbitMQ Grafana dashboard to track:
  - Throughput (msgs/sec)
  - Queue Length
  - Unacknowledged Messages
  - Erlang Process Count

---

## Summary Checklist
- [ ] Monitor Memory and Disk usage.
- [ ] Use Quorum Queues for HA.
- [ ] Keep message payloads small.
- [ ] Use `rabbitmq-diagnostics` for CLI troubleshooting.

---

## Next Steps
Before we start our project, we need to talk about **Best Practices & Security**.
