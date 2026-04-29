# 02 - Core Concepts of RabbitMQ

Before writing code, it's crucial to understand the "vocabulary" of RabbitMQ. These concepts form the backbone of any messaging architecture.

---

## 1. The Basic Flow

In a simple world, a sender sends a message to a receiver. In RabbitMQ, the flow looks like this:

**Producer** -> **Exchange** -> **Queue** -> **Consumer**

### 1.1 Producer
An application that **sends** messages. It never sends a message directly to a queue; instead, it sends it to an **Exchange**.

### 1.2 Consumer
An application that **receives** messages. It connects to a **Queue** to subscribe to messages.

### 1.3 Message
The data being sent. It consists of:
- **Payload**: The actual data (JSON, String, Binary, etc.).
- **Attributes**: Metadata like content-type, expiration, or custom headers.

---

## 2. The Infrastructure

### 2.1 Exchange
The "routing engine". It receives messages from producers and pushes them to queues based on specific rules (bindings).

### 2.2 Queue
The "buffer" that stores messages. Messages stay here until they are consumed or expire. A queue is defined by its name and properties (durability, auto-delete, etc.).

### 2.3 Binding
The link between an Exchange and a Queue. It tells the exchange: "Send messages to this queue if they match certain criteria."

### 2.4 Routing Key
A virtual address that the producer attaches to a message. The exchange uses this key to decide which queue the message should go to.

---

## 3. Connections and Channels

When your application talks to RabbitMQ, it uses two main abstractions:

### Connection
A real TCP connection between your application and the RabbitMQ broker. Connections are expensive to open and close.

### Channel
A "virtual connection" inside a TCP connection. 
- You can have many channels inside a single connection.
- Channels are lightweight and should be used for most operations (declaring queues, publishing, consuming).
- **Rule of Thumb**: Use one connection per process and one channel per thread/task.

---

## 4. Visualizing the Components

| Component | Responsibility | Analogous To... |
| :--- | :--- | :--- |
| **Producer** | Sends the message | The person writing a letter |
| **Exchange** | Routes the message | The Mail Sorting Office |
| **Routing Key** | Specifies the target | The Address on the envelope |
| **Binding** | Rules for delivery | The mailman's delivery route |
| **Queue** | Holds the message | Your physical Mailbox |
| **Consumer** | Reads the message | You opening the mail |

---

## Summary Checklist
- [ ] A Producer sends a message to an Exchange.
- [ ] The Exchange routes it to one or more Queues via Bindings.
- [ ] A Consumer pulls (or is pushed) the message from the Queue.
- [ ] We use Channels to perform these actions over a single TCP Connection.

---

## Next Steps
Now that we know the parts, we need to learn how the **Exchange** decides where to send messages. We'll explore **Exchange Types** next.
