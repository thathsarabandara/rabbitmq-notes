# 04 - Message Delivery & Reliability

In production, "sending" a message isn't enough. You need to ensure the message is received, processed, and not lost if the server restarts.

---

## 1. Consumer Acknowledgments (ACKs)

When RabbitMQ delivers a message to a consumer, it needs to know when it's safe to remove that message from the queue.

### Types of Acknowledgments:
- **Manual ACK**: The consumer sends a "success" signal back to the broker after processing. **(Recommended)**
- **Automatic ACK**: RabbitMQ considers the message delivered as soon as it's sent over the wire. (Risky if the consumer crashes mid-processing).
- **NACK / Reject**: The consumer tells the broker, "I couldn't process this." You can choose to **requeue** it or move it to a Dead Letter Exchange.

---

## 2. Durability and Persistence

To survive a RabbitMQ node restart, three things must be true:

1.  **Durable Exchange**: The exchange must be declared as `durable: true`.
2.  **Durable Queue**: The queue must be declared as `durable: true`.
3.  **Persistent Messages**: The producer must mark the message as `persistent` (Delivery Mode 2).

> [!IMPORTANT]
> If you have a durable queue but send non-persistent messages, those messages will be lost on restart!

---

## 3. Publisher Confirms

How does a Producer know if the message even reached the Exchange?
**Publisher Confirms** are the producer-side equivalent of ACKs. The broker sends a signal back to the producer once the message has been successfully handled by the exchange and (optionally) persisted to disk.

---

## 4. Dead Letter Exchanges (DLX)

What happens to messages that fail?
Messages can be "Dead Lettered" (moved to a special DLX) if:
- They are **Rejected** or **NACKed** with `requeue: false`.
- They expire due to **TTL** (Time To Live).
- The queue is full (**Length Limit**).

### Why use DLX?
It allows you to store "bad" messages for later inspection or retry logic without blocking the main queue.

---

## 5. Message TTL (Time To Live)

You can set an expiration time on a message or a whole queue.
- If a message isn't consumed within the TTL, it is deleted (or sent to a DLX).
- **Use Case**: Temporary tokens, price alerts, or time-sensitive notifications.

---

## 6. Prefetch Count (QoS)

The **Prefetch Count** tells RabbitMQ: "Don't give this consumer more than `N` unacknowledged messages at a time."

- **Why?**: Without this, RabbitMQ will push messages as fast as possible, potentially overwhelming one consumer while others sit idle.
- **Value of 1**: Ensures the consumer finishes one task before getting another (Fair Dispatch).

---

## Summary for Reliability
| Feature | Prevents Loss From... |
| :--- | :--- |
| **Durable Queues** | Broker Restarts |
| **Persistent Messages** | Broker Restarts |
| **Manual ACKs** | Consumer Crashes |
| **Publisher Confirms** | Network issues / Broker failures during send |
| **DLX** | Processing failures / Expirations |

---

## Next Steps
Now that we have reliable delivery, let's look at how to structure our applications using **Advanced Routing & Patterns**.
