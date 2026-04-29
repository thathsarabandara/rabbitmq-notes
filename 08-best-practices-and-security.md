# 08 - Best Practices & Security

To move from "Beginner" to "Expert," you need to know how to structure your RabbitMQ environment securely and efficiently.

---

## 1. Virtual Hosts (vhosts)
Vhosts are like "namespaces" or "mini-brokers" inside a single RabbitMQ instance.
- **Isolation**: Queues/Exchanges in `vhost_A` cannot see those in `vhost_B`.
- **Permissions**: You can grant a user access to only one vhost.
- **Use Case**: Use separate vhosts for `development`, `staging`, and `production`.

---

## 2. Security & Users

### 2.1 Principle of Least Privilege
- **Management User**: Only for admins.
- **Application User**: Should only have permissions to `read`, `write`, and `configure` specific queues/exchanges.

### 2.2 Disable 'guest' User
The default `guest/guest` account only works from `localhost`. For remote access, **always create a new user** and delete the guest account.

### 2.3 TLS/SSL
Always encrypt traffic between your application and RabbitMQ using TLS (Port 5671), especially if communicating over the public internet.

---

## 3. Queue Naming Conventions
Consistency is key for maintainability.

- **Bad**: `test_queue`, `q1`
- **Good**: `service_name.resource_type.action`
  - Example: `orders.invoice.generate`
  - Example: `analytics.user.signup`

---

## 4. Error Handling Best Practices

1.  **Don't Requeue Infinitely**: If a message causes a crash, requeuing it will cause an infinite loop (Poison Pill). Use a retry limit or move it to a **Dead Letter Queue**.
2.  **Idempotency**: Your consumers should be able to process the same message twice without causing issues (e.g., check if an order is already paid before processing payment).
3.  **Circuit Breakers**: If the broker is down, your application should fail gracefully or cache messages locally.

---

## 5. Anti-Patterns (What NOT to do)

- **One Queue per Message**: Don't create a new queue for every single message. Queues are meant to be long-lived.
- **Massive Queues**: Try to keep queues short. Large queues consume more memory and slow down management operations.
- **Using RabbitMQ as a Database**: Messages should be consumed and deleted. Don't use it for long-term storage (unless using Streams).

---

## Summary
| Do | Don't |
| :--- | :--- |
| Use Vhosts for isolation | Use `guest` user in production |
| Set Prefetch Count (QoS) | Send large file binaries in messages |
| Implement Manual ACKs | Let queues grow indefinitely |
| Use Quorum Queues for HA | Neglect Monitoring & Alarms |

---

## Final Step
You are now ready to put all this knowledge into practice. Proceed to the **Practice Project**.
