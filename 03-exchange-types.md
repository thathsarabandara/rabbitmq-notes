# 03 - RabbitMQ Exchange Types

The Exchange is the brain of RabbitMQ. Depending on its type, it will route messages in very different ways. There are four primary exchange types you must know.

---

## 1. Direct Exchange (The Postman)
A direct exchange delivers messages to queues based on an **exact match** of the routing key.

- **How it works**: A queue binds to the exchange with a routing key (e.g., `pdf_events`). When a message arrives with the routing key `pdf_events`, it goes to that queue.
- **Use Case**: Simple task distribution where you know exactly which queue should handle a specific message.

---

## 2. Fanout Exchange (The Broadcaster)
A fanout exchange ignores the routing key and sends the message to **all queues** bound to it.

- **How it works**: If 5 queues are bound to a fanout exchange, every message sent to that exchange is copied to all 5 queues.
- **Use Case**: Broadcasting configuration updates, real-time sports scores, or any "one-to-many" communication.

---

## 3. Topic Exchange (The Filter)
A topic exchange routes messages based on **wildcard matches** between the routing key and the binding pattern.

- **Routing Key Format**: A list of words delimited by dots (e.g., `stock.usd.nyse`).
- **Wildcards**:
  - `*` (star): Substitutes for exactly **one** word.
  - `#` (hash): Substitutes for **zero or more** words.

### Example:
- Binding: `stock.*.nyse` -> Matches `stock.usd.nyse`, `stock.eur.nyse`.
- Binding: `stock.#` -> Matches `stock.usd.nyse`, `stock.nyse`, `stock.aapl.nasdaq.extra`.

---

## 4. Headers Exchange (The Inspector)
A headers exchange uses message header attributes for routing instead of the routing key.

- **How it works**: You bind a queue using a set of key-value pairs and an `x-match` argument.
- **x-match**:
  - `all`: All headers must match.
  - `any`: At least one header must match.
- **Use Case**: When routing depends on multiple complex attributes that don't fit well into a string routing key.

---

## 5. The Default (Nameless) Exchange
When you send a message without specifying an exchange, RabbitMQ uses the **Default Exchange**.

- **Type**: It is a pre-declared **Direct** exchange.
- **Routing**: Every queue is automatically bound to it with a routing key equal to the **queue name**.
- **Usage**: Useful for simple applications where you just want to send a message directly to a specific queue name.

---

## Summary Comparison

| Exchange Type | Routing Logic | Key Attribute |
| :--- | :--- | :--- |
| **Direct** | Exact Match | Routing Key |
| **Fanout** | Broadcast | None (ignores keys) |
| **Topic** | Pattern Match | Routing Key (with `*` and `#`) |
| **Headers** | Attribute Match | Message Headers |

---

## Next Steps
Routing is great, but what happens if the broker crashes? Or if a consumer fails to process a message? In the next lesson, we'll cover **Message Delivery & Reliability**.
