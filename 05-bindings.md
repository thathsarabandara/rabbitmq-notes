# Bindings: Connecting Exchanges and Queues

## What is a Binding?

A binding is a relationship between an exchange and a queue that specifies how messages from the exchange should be routed to the queue.

```
Exchange ──binding──> Queue
         (routing rule)
```

Without bindings, exchanges don't know which queues to send messages to.

## Binding Components

1. **Exchange**: Source of messages
2. **Queue**: Destination for messages
3. **Routing Key**: The matching pattern/criteria
4. **Arguments**: Additional matching rules

## Creating Bindings

### Basic Binding

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# 1. Declare exchange
channel.exchange_declare(exchange='orders', exchange_type='direct')

# 2. Declare queue
channel.queue_declare(queue='order_processing')

# 3. Create binding
channel.queue_bind(
    exchange='orders',
    queue='order_processing',
    routing_key='order.created'
)

connection.close()
```

### Complete Binding Example

```python
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Setup infrastructure
channel.exchange_declare(exchange='events', exchange_type='topic', durable=True)
channel.queue_declare(queue='user_events', durable=True)
channel.queue_declare(queue='audit_log', durable=True)

# Create bindings
channel.queue_bind(
    exchange='events',
    queue='user_events',
    routing_key='user.*'
)

channel.queue_bind(
    exchange='events',
    queue='audit_log',
    routing_key='#'  # All events
)

# Publish events
events = [
    ('user.created', {'id': 1, 'name': 'Alice'}),
    ('user.deleted', {'id': 2, 'name': 'Bob'}),
    ('payment.processed', {'amount': 100})
]

for routing_key, data in events:
    channel.basic_publish(
        exchange='events',
        routing_key=routing_key,
        body=json.dumps(data),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/json'
        )
    )

print("Events published and routed!")
connection.close()
```

## Routing Key Matching

### Direct Exchange

**Exact match required**

```
Exchange: auth (direct)

Binding 1: Queue1 ← routing_key: "success"
Binding 2: Queue2 ← routing_key: "failure"

Message with key "success"  → Queue1 only
Message with key "failure"  → Queue2 only
Message with key "pending"  → No match, message discarded!
```

### Topic Exchange

**Pattern matching** using wildcards

```
Binding:
queue="notifications" ← routing_key: "order.*.email"

Matches:
✓ order.created.email
✓ order.shipped.email
✓ order.cancelled.email

Doesn't match:
✗ order.created.sms        (more than one level)
✗ user.created.email       (wrong service)
```

**Wildcard Rules:**
- `*` = exactly one word (between dots)
- `#` = zero or more words

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Setup
channel.exchange_declare(exchange='logs', exchange_type='topic')
channel.queue_declare(queue='error_analytics')
channel.queue_declare(queue='critical_alerts')
channel.queue_declare(queue='all_logs')

# Binding examples with topic patterns
# Errors from any service
channel.queue_bind(
    exchange='logs',
    queue='error_analytics',
    routing_key='*.error'  # e.g., payment.error, user.error
)

# Only critical errors
channel.queue_bind(
    exchange='logs',
    queue='critical_alerts',
    routing_key='payment.error.critical'  # Exact, no wildcards
)

# Everything
channel.queue_bind(
    exchange='logs',
    queue='all_logs',
    routing_key='#'
)

# Test routing
test_keys = [
    'payment.error',
    'user.error',
    'payment.error.critical',
    'user.warning'
]

for key in test_keys:
    channel.basic_publish(
        exchange='logs',
        routing_key=key,
        body=f'Log: {key}'
    )

print("Test messages routed!")
connection.close()
```

### Fanout Exchange

**No routing key used**

```
Exchange: broadcast (fanout)

Binding 1: Queue1 ← routing_key: "ignored"
Binding 2: Queue2 ← routing_key: "also_ignored"
Binding 3: Queue3 ← routing_key: ""

Published message → All three queues
Routing key irrelevant!
```

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Fanout setup
channel.exchange_declare(exchange='notifications', exchange_type='fanout')
channel.queue_declare(queue='email')
channel.queue_declare(queue='sms')
channel.queue_declare(queue='push')

# Bindings (routing_key ignored for fanout)
channel.queue_bind(exchange='notifications', queue='email', routing_key='')
channel.queue_bind(exchange='notifications', queue='sms', routing_key='')
channel.queue_bind(exchange='notifications', queue='push', routing_key='')

# Publish once
notification = "System maintenance at 2 AM"
channel.basic_publish(
    exchange='notifications',
    routing_key='irrelevant',
    body=notification
)

# Goes to all: email, sms, push
print("Broadcast sent to all channels!")
connection.close()
```

### Headers Exchange

**Match message headers**

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Headers exchange
channel.exchange_declare(exchange='requests', exchange_type='headers')
channel.queue_declare(queue='high_priority')
channel.queue_declare(queue='admin_only')

# Binding with multiple header conditions
channel.queue_bind(
    exchange='requests',
    queue='high_priority',
    routing_key='',
    arguments={
        'x-match': 'all',  # All conditions must match
        'priority': 'high',
        'type': 'request'
    }
)

# Binding requiring any one condition
channel.queue_bind(
    exchange='requests',
    queue='admin_only',
    routing_key='',
    arguments={
        'x-match': 'any',  # Any condition can match
        'role': 'admin',
        'role': 'superuser'
    }
)

# Publish with headers
channel.basic_publish(
    exchange='requests',
    routing_key='',
    body='Process high priority admin request',
    properties=pika.BasicProperties(
        headers={
            'priority': 'high',
            'type': 'request',
            'role': 'admin'
        }
    )
)

print("Request routed by headers!")
connection.close()
```

## Binding Arguments

All binding arguments start with `x-`:

```python
channel.queue_bind(
    exchange='events',
    queue='my_queue',
    routing_key='order.*',
    arguments={
        'x-match': 'all',  # For headers matching
        'custom-arg': 'custom-value'
    }
)
```

## Multiple Bindings

One queue can bind to same exchange multiple times with different routing keys:

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Setup
channel.exchange_declare(exchange='events', exchange_type='direct')
channel.queue_declare(queue='alerts')

# Multiple bindings = multiple routing keys to same queue
channel.queue_bind(exchange='events', queue='alerts', routing_key='error')
channel.queue_bind(exchange='events', queue='alerts', routing_key='critical')
channel.queue_bind(exchange='events', queue='alerts', routing_key='warning')

# All these messages go to alerts queue
channel.basic_publish(exchange='events', routing_key='error', body='Error occurred')
channel.basic_publish(exchange='events', routing_key='critical', body='Critical issue')
channel.basic_publish(exchange='events', routing_key='warning', body='Warning raised')

print("Multiple bindings created!")
connection.close()
```

## Queue to Multiple Exchanges

One queue can bind to multiple different exchanges:

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Setup
channel.exchange_declare(exchange='orders', exchange_type='direct')
channel.exchange_declare(exchange='payments', exchange_type='direct')
channel.queue_declare(queue='accounting')

# Same queue binds to two different exchanges
channel.queue_bind(exchange='orders', queue='accounting', routing_key='order.created')
channel.queue_bind(exchange='payments', queue='accounting', routing_key='payment.processed')

print("Queue bound to multiple exchanges!")
connection.close()
```

## Unbinding

Remove a binding:

```python
channel.queue_unbind(
    exchange='events',
    queue='my_queue',
    routing_key='user.*'
)
```

## Binding Visualization Tool

Use the Management UI to visualize bindings:

```
http://localhost:15672
→ Exchanges tab → Click exchange → See "Bindings" section
→ Shows all connected queues and routing keys
```

## Complete Real-World Example

```python
import pika
import json
from datetime import datetime

def setup_rabbitmq():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare exchanges
    channel.exchange_declare(exchange='ecommerce', exchange_type='topic', durable=True)

    # Declare queues
    queues = {
        'order_processing': 'order.#',        # All order events
        'payment_service': 'payment.*',       # Payment events
        'notifications': 'notification.*',    # User notifications
        'audit_log': '#'                      # Everything
    }

    for queue_name, routing_key in queues.items():
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            exchange='ecommerce',
            queue=queue_name,
            routing_key=routing_key
        )

    # Publish sample events
    events = [
        ('order.created', {'order_id': 1, 'total': 99.99}),
        ('order.shipped', {'order_id': 1, 'tracking': 'ABC123'}),
        ('payment.processed', {'order_id': 1, 'status': 'success'}),
        ('notification.email', {'message': 'Your order shipped!'}),
    ]

    for routing_key, event_data in events:
        channel.basic_publish(
            exchange='ecommerce',
            routing_key=routing_key,
            body=json.dumps({
                'timestamp': datetime.now().isoformat(),
                **event_data
            }),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type='application/json'
            )
        )

    print(f"Setup complete! {len(queues)} queues configured")
    connection.close()

if __name__ == '__main__':
    setup_rabbitmq()
```

## Binding Best Practices

1. **Plan your routing strategy** before binding
2. **Use topic exchanges** for most scenarios (flexible)
3. **Name routing keys meaningfully**: `service.event.level`
4. **Avoid over-binding** one queue to too many keys
5. **Document bindings** for team understanding
6. **Test routing** with sample messages
7. **Monitor binding performance** for busy exchanges
