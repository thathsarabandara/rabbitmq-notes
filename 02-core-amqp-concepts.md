# Core AMQP Concepts

## AMQP (Advanced Message Queuing Protocol)

AMQP is the protocol that RabbitMQ implements. It defines how messages are formatted and transmitted between applications.

### AMQP Model

The AMQP model consists of:

1. **Publisher (Producer)**: Sends messages
2. **Exchange**: Receives messages from publishers
3. **Queue**: Stores messages
4. **Subscriber (Consumer)**: Receives messages from queues

## Connections and Channels

### Connection

A physical connection between your application and RabbitMQ broker.

```
Application ──TCP Connection──→ RabbitMQ Broker
```

**Characteristics:**
- Expensive to create
- Should be reused
- Manages authentication and encryption

### Channel

A virtual connection inside a physical connection.

```
Connection {
  ├─ Channel 1 (for messages)
  ├─ Channel 2 (for configuration)
  └─ Channel 3 (for admin tasks)
}
```

**Characteristics:**
- Lightweight, quick to create/destroy
- Multiple channels on single connection
- Each channel can have its own configuration

### Example: Python Code

```python
import pika

# Create connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)

# Create channel
channel = connection.channel()

# Use channel to declare queues, exchanges, etc.
channel.queue_declare(queue='my_queue')

# Close when done
connection.close()
```

## Messages

A message is the actual data being transmitted through RabbitMQ.

### Message Structure

```
┌──────────────────────────────────┐
│      Message Envelope            │
├──────────────────────────────────┤
│  Properties (metadata)           │
│  - Content-type                  │
│  - Delivery mode (persistent)    │
│  - Priority                      │
│  - Correlation ID                │
│  - Reply-to                      │
├──────────────────────────────────┤
│  Body (actual data)              │
│  - String, JSON, bytes, etc.     │
└──────────────────────────────────┘
```

### Message Properties

```python
channel.basic_publish(
    exchange='my_exchange',
    routing_key='my_routing_key',
    body='Hello World',
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # Survive broker restart
        content_type='text/plain',
        priority=5,
        correlation_id='12345',
        reply_to='reply_queue'
    )
)
```

### Content Types

Common content types:
- `text/plain`: Text messages
- `application/json`: JSON data
- `application/xml`: XML data
- `application/octet-stream`: Binary data

## Message Lifecycle

```
1. PUBLISH
   Producer sends message to Exchange
   
2. ROUTING
   Exchange examines routing key and message properties
   Decides which queue(s) to route to
   
3. QUEUE STORAGE
   Message stored in queue (on disk if persistent)
   
4. DELIVERY
   Consumer receives message from queue
   
5. ACKNOWLEDGMENT
   Consumer processes and acknowledges receipt
   
6. DELETION
   Message removed from queue
```

### Example: Message with Metadata

```python
import pika
import json
from datetime import datetime

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

# Declare exchange and queue
channel.exchange_declare(exchange='logs', exchange_type='direct')
channel.queue_declare(queue='error_logs')
channel.queue_bind(exchange='logs', queue='error_logs', routing_key='error')

# Create message
message = {
    'level': 'ERROR',
    'service': 'user-service',
    'message': 'Database connection failed',
    'timestamp': datetime.now().isoformat()
}

# Publish with properties
channel.basic_publish(
    exchange='logs',
    routing_key='error',
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        content_type='application/json',
        priority=9  # High priority for errors
    )
)

print("Error logged!")
connection.close()
```

## Virtual Hosts (vhosts)

Virtual hosts create isolated environments within a single RabbitMQ instance.

```
RabbitMQ Broker
├─ /production
│  ├─ exchanges
│  ├─ queues
│  └─ users
├─ /staging
│  ├─ exchanges
│  ├─ queues
│  └─ users
└─ /development
   ├─ exchanges
   ├─ queues
   └─ users
```

### Benefits

- **Isolation**: Separate environments for different apps/environments
- **Multi-tenancy**: Different customers/tenants
- **Security**: Different users per vhost

### Example: Connecting to vhost

```python
credentials = pika.PlainCredentials('user', 'password')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        virtual_host='/production',
        credentials=credentials
    )
)
```

## Summary

- **Connection**: Physical link to broker
- **Channel**: Virtual connection for operations
- **Message**: Data with metadata being exchanged
- **vhost**: Isolated environment within broker
- AMQP protocol ensures consistency across implementations
