# Exchanges: The Message Router

## What is an Exchange?

An exchange is the component that receives messages from producers and routes them to one or more queues based on routing rules (bindings).

```
Producer → Exchange → (routing decision) → Queue(s)
```

## Key Properties

- **Name**: Unique identifier
- **Type**: How it routes messages (direct, topic, fanout, headers)
- **Durable**: Persists after broker restart
- **Auto-delete**: Removed when no longer in use

## Exchange Types

### 1. Direct Exchange

Routes messages to queues with an **exact matching** routing key.

**Use Case**: One-to-one communication with specific routing

```
Exchange: direct_logs

Binding 1: routing_key = "error"   → Queue: error_logs
Binding 2: routing_key = "info"    → Queue: info_logs
Binding 3: routing_key = "warning" → Queue: warning_logs

Message with routing_key "error" → Goes to error_logs queue only
Message with routing_key "info"  → Goes to info_logs queue only
```

**Example: Python**

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare direct exchange
channel.exchange_declare(exchange='logs', exchange_type='direct', durable=True)

# Declare queues
channel.queue_declare(queue='error_logs', durable=True)
channel.queue_declare(queue='info_logs', durable=True)

# Create bindings
channel.queue_bind(exchange='logs', queue='error_logs', routing_key='error')
channel.queue_bind(exchange='logs', queue='info_logs', routing_key='info')

# Publish messages
channel.basic_publish(
    exchange='logs',
    routing_key='error',
    body='Error: User not found'
)

channel.basic_publish(
    exchange='logs',
    routing_key='info',
    body='Info: User logged in'
)

connection.close()
```

### 2. Fanout Exchange

Routes messages to **all bound queues** regardless of routing key.

**Use Case**: Broadcasting (same message to multiple consumers)

```
Exchange: notifications (fanout)

┌─→ Queue: email_notifications
│
├─→ Queue: sms_notifications
│
└─→ Queue: push_notifications

Message published → All three queues receive it
```

**Example: Broadcasting Notifications**

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare fanout exchange
channel.exchange_declare(exchange='notifications', exchange_type='fanout', durable=True)

# Declare queues for different notification channels
channel.queue_declare(queue='email_queue', durable=True)
channel.queue_declare(queue='sms_queue', durable=True)
channel.queue_declare(queue='slack_queue', durable=True)

# Bind all queues to fanout (routing_key ignored)
channel.queue_bind(exchange='notifications', queue='email_queue')
channel.queue_bind(exchange='notifications', queue='sms_queue')
channel.queue_bind(exchange='notifications', queue='slack_queue')

# Publish once, goes to all queues
notification = "Server maintenance scheduled at 2:00 AM"
channel.basic_publish(
    exchange='notifications',
    routing_key='',  # Ignored in fanout
    body=notification
)

print("Notification sent to all channels!")
connection.close()
```

### 3. Topic Exchange

Routes messages based on **pattern matching** with routing keys.

**Use Case**: Multiple criteria routing, selective subscriptions

```
Routing Key Patterns:
- "*.log"        → Matches: user.log, payment.log, order.log
- "order.*"      → Matches: order.created, order.cancelled
- "order.#"      → Matches: order.created, order.payment.failed, etc.

Wildcard Rules:
- * (star)  : Matches exactly one word
- # (hash)  : Matches zero or more words
```

**Diagram**

```
Exchange: notifications (topic)

Binding 1: "user.*"          → user_events_queue
Binding 2: "order.*"         → order_events_queue
Binding 3: "payment.#"       → payment_queue
Binding 4: "#"               → audit_queue (all messages)

Message: "user.created"      → user_events_queue, audit_queue
Message: "order.cancelled"   → order_events_queue, audit_queue
Message: "payment.failed"    → payment_queue, audit_queue
Message: "payment.retry.attempt2" → payment_queue, audit_queue
```

**Example: Event Routing**

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare topic exchange
channel.exchange_declare(exchange='events', exchange_type='topic', durable=True)

# Declare queues
channel.queue_declare(queue='user_queue', durable=True)
channel.queue_declare(queue='order_queue', durable=True)
channel.queue_declare(queue='audit_queue', durable=True)

# Create bindings with patterns
channel.queue_bind(exchange='events', queue='user_queue', routing_key='user.*')
channel.queue_bind(exchange='events', queue='order_queue', routing_key='order.*')
channel.queue_bind(exchange='events', queue='audit_queue', routing_key='#')  # All events

# Publish events
channel.basic_publish(
    exchange='events',
    routing_key='user.created',
    body='{"id": 123, "name": "John"}'
)

channel.basic_publish(
    exchange='events',
    routing_key='order.shipped',
    body='{"order_id": 456, "status": "shipped"}'
)

print("Events published!")
connection.close()
```

### 4. Headers Exchange

Routes messages based on **message headers** instead of routing key.

**Use Case**: Complex matching criteria

```
Message Headers:
{
    'x-match': 'all',  // or 'any'
    'severity': 'high',
    'type': 'error'
}

Binding: x-match=all, severity=high, type=error
→ Only messages with ALL these headers route here

Binding: x-match=any, region=US, region=EU
→ Messages with ANY of these headers route here
```

**Example**

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare headers exchange
channel.exchange_declare(exchange='alerts', exchange_type='headers', durable=True)

# Declare queue
channel.queue_declare(queue='critical_alerts', durable=True)

# Bind with header matching (all headers must match)
channel.queue_bind(
    exchange='alerts',
    queue='critical_alerts',
    arguments={
        'x-match': 'all',
        'severity': 'critical',
        'service': 'payment'
    }
)

# Publish with headers
channel.basic_publish(
    exchange='alerts',
    routing_key='',
    body='Payment service down!',
    properties=pika.BasicProperties(
        headers={
            'severity': 'critical',
            'service': 'payment'
        }
    )
)

print("Alert published!")
connection.close()
```

## Default Exchange

RabbitMQ has a built-in default exchange with these properties:

- **Name**: Empty string ""
- **Type**: Direct
- **Behavior**: Routes to queue with name matching routing key

```python
# When you don't specify an exchange, uses default
channel.basic_publish(
    exchange='',           # Default exchange
    routing_key='my_queue',  # Must match queue name exactly
    body='Hello World'
)

# Equivalent to:
channel.queue_declare(queue='my_queue')
channel.basic_publish(
    exchange='',
    routing_key='my_queue',
    body='Hello World'
)
```

## Declaring Exchanges

### Properties

```python
channel.exchange_declare(
    exchange='my_exchange',
    exchange_type='direct',
    passive=False,      # Create if not exists (False = create)
    durable=True,       # Persist after broker restart
    auto_delete=False,  # Delete when no bindings
    internal=False,     # Can't publish directly to (advanced)
    arguments={}        # Extra arguments
)
```

### Idempotent Declaration

```python
# Safe to call multiple times - won't error if already exists
channel.exchange_declare(
    exchange='logs',
    exchange_type='direct',
    durable=True
)
```

## Exchange Comparison

| Feature | Direct | Fanout | Topic | Headers |
|---------|--------|--------|-------|---------|
| Routing Basis | Exact match | None | Pattern | Header values |
| Performance | Fastest | Fast | Slower | Slowest |
| Use Case | One-to-one | Broadcast | Selective | Complex rules |
| Complexity | Simple | Simple | Medium | Complex |

## Best Practices

1. **Always declare as durable** in production for persistence
2. **Use topic exchanges** for most applications (flexible)
3. **Match exchange type** to your routing needs
4. **Avoid fanout** unless truly broadcasting
5. **Name exchanges meaningfully**: `logs`, `notifications`, `events`
6. **Document your bindings** for team understanding
