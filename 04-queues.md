# Queues: Message Storage

## What is a Queue?

A queue is a buffer that stores messages until a consumer processes them. Messages wait in the queue in FIFO (First In, First Out) order.

```
Producer → Exchange → Queue (FIFO buffer) → Consumer
```

## Queue Characteristics

- **Name**: Unique identifier
- **Durable**: Persists after broker restart
- **Exclusive**: Only one consumer, deleted when consumer disconnects
- **Auto-delete**: Removed when no consumers
- **Priority**: Messages can have priority levels
- **TTL (Time-To-Live)**: Message expiration

## Declaring Queues

### Basic Declaration

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare a queue
channel.queue_declare(
    queue='my_queue',
    passive=False,      # Create if doesn't exist
    durable=True,       # Persist after restart
    exclusive=False,    # Can have multiple consumers
    auto_delete=False   # Don't auto-delete
)

connection.close()
```

### Queue Declaration Parameters

```python
channel.queue_declare(
    queue='task_queue',
    durable=True,           # Survives broker restart
    exclusive=False,        # Not exclusive to one connection
    auto_delete=False,      # Don't auto-delete when empty
    arguments={
        'x-max-length': 10000,  # Max messages
        'x-message-ttl': 86400000,  # 24 hours in ms
        'x-dead-letter-exchange': 'dlx'  # Dead letter exchange
    }
)
```

## Types of Queues

### 1. Standard Queue (Durable)

Persists messages to disk, survives broker restarts.

```python
# Production queue - must survive broker crashes
channel.queue_declare(
    queue='orders',
    durable=True
)

# Publish with persistence
channel.basic_publish(
    exchange='',
    routing_key='orders',
    body='Order #123',
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent
    )
)
```

### 2. Transient/Non-Durable Queue

Exists only in memory, lost if broker restarts.

```python
# Temporary queue - lost on restart
channel.queue_declare(
    queue='cache',
    durable=False
)
```

### 3. Exclusive Queue

Created by consumer, exists only while consumer is connected, deleted when disconnected.

```python
# Temporary queue for this connection only
channel.queue_declare(
    queue='response_queue',
    exclusive=True  # Unique to this connection
)
```

**Use Case**: RPC (Remote Procedure Call) patterns

```python
import pika
import uuid

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Create exclusive callback queue
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue  # Auto-generated name

# Send RPC request expecting response on callback_queue
channel.basic_publish(
    exchange='',
    routing_key='rpc_queue',
    body='2+2',
    properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=str(uuid.uuid4())
    )
)
```

### 4. Lazy Queue

Optimized for large backlog of messages. Writes to disk more aggressively.

```python
# Lazy queue - spills to disk quickly (good for large backlogs)
channel.queue_declare(
    queue='events_queue',
    durable=True,
    arguments={
        'x-queue-mode': 'lazy'  # Default is 'default'
    }
)
```

## Queue Features

### 1. Queue Length Limit

Limit maximum number of messages:

```python
channel.queue_declare(
    queue='limited_queue',
    durable=True,
    arguments={
        'x-max-length': 10000  # Max 10k messages
    }
)
```

### 2. Message TTL (Time-To-Live)

Messages expire after timeout:

```python
# Messages expire after 1 hour (3600000 ms)
channel.queue_declare(
    queue='temporary_queue',
    durable=True,
    arguments={
        'x-message-ttl': 3600000  # milliseconds
    }
)
```

### 3. Queue TTL

Empty queue deleted after timeout:

```python
# Queue deleted 30 minutes after becoming empty
channel.queue_declare(
    queue='temp_queue',
    arguments={
        'x-expires': 1800000  # 30 minutes in ms
    }
)
```

### 4. Priority Queue

Messages processed based on priority (0-10):

```python
# Declare priority queue
channel.queue_declare(
    queue='priority_tasks',
    durable=True,
    arguments={
        'x-max-priority': 10
    }
)

# High priority message
channel.basic_publish(
    exchange='',
    routing_key='priority_tasks',
    body='Urgent task',
    properties=pika.BasicProperties(
        priority=10  # Highest
    )
)

# Low priority message
channel.basic_publish(
    exchange='',
    routing_key='priority_tasks',
    body='Low priority task',
    properties=pika.BasicProperties(
        priority=1
    )
)
```

**Processing**: Consumed in priority order, not FIFO

## Queue Management

### List Queues (Management UI)

Access `http://localhost:15672` and view Queues tab

### Declare vs Passive

```python
# Declare: Create if doesn't exist, error if exists with different params
channel.queue_declare(
    queue='my_queue',
    durable=True
)

# Passive: Check existence only, no creation
try:
    channel.queue_declare(
        queue='my_queue',
        passive=True  # Just verify it exists
    )
except:
    print("Queue doesn't exist!")
```

### Purge Queue (Delete all messages)

```python
channel.queue_purge(queue='my_queue')
```

### Delete Queue

```python
channel.queue_delete(queue='my_queue')
```

## Durable vs Persistent

**Important**: Both needed for reliability!

```
Durable Queue + Persistent Message = Messages survive broker restart
Durable Queue + Non-persistent = Messages lost on restart
Non-durable Queue + Persistent = Queue lost, so message lost too
```

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# 1. Declare DURABLE queue
channel.queue_declare(
    queue='important_queue',
    durable=True
)

# 2. Publish PERSISTENT message
channel.basic_publish(
    exchange='',
    routing_key='important_queue',
    body='Important data that must survive',
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent  # Mode 2 = persistent
    )
)

print("Message stored durably!")
connection.close()
```

## Complete Queue Setup Example

```python
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare exchange
channel.exchange_declare(
    exchange='notifications',
    exchange_type='topic',
    durable=True
)

# Declare queue with advanced options
channel.queue_declare(
    queue='email_queue',
    durable=True,
    arguments={
        'x-max-priority': 10,           # Priority support
        'x-message-ttl': 3600000,       # 1 hour TTL
        'x-max-length': 100000,         # Max 100k messages
        'x-dead-letter-exchange': 'dlx' # Failed messages here
    }
)

# Bind queue to exchange
channel.queue_bind(
    exchange='notifications',
    queue='email_queue',
    routing_key='notification.*'
)

# Publish a message
message = {'to': 'user@example.com', 'subject': 'Hello!'}
channel.basic_publish(
    exchange='notifications',
    routing_key='notification.email',
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        priority=5,
        content_type='application/json'
    )
)

print("Message queued!")
connection.close()
```

## Queue Best Practices

1. **Always make queues durable** in production
2. **Combine durable + persistent** for critical data
3. **Use priority queues** for SLA requirements
4. **Set appropriate TTL** for time-sensitive data
5. **Monitor queue lengths** to detect slowdowns
6. **Use lazy queues** for large backlogs
7. **Set dead-letter exchange** for error handling
8. **Document queue purposes** and retention policies
