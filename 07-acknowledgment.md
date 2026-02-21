# Message Acknowledgment

## What is Acknowledgment?

Acknowledgment (ACK) is a signal from the consumer to RabbitMQ saying "I have successfully processed this message, you can remove it from the queue."

```
Message Flow:
1. Message in Queue
2. Consumer receives message
3. Consumer processes message
4. Consumer sends ACK
5. RabbitMQ removes message from queue
```

## Types of Acknowledgment

### 1. Automatic Acknowledgment (auto_ack=True)

RabbitMQ removes message immediately after delivery, before consumer processes it.

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='messages')

def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")
    # If crash here, message is already deleted!

channel.basic_consume(
    queue='messages',
    on_message_callback=callback,
    auto_ack=True  # Automatic acknowledgment
)

channel.start_consuming()
```

**Risks:**
- Message lost if consumer crashes during processing
- No retry mechanism
- **Use only for non-critical messages**

### 2. Manual Acknowledgment (auto_ack=False)

Consumer explicitly acknowledges after successful processing.

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)

def callback(ch, method, properties, body):
    try:
        print(f"Processing: {body.decode()}")
        # Do work
        process_data(body)
        print("✓ Success")
        
        # Acknowledge only after successful processing
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"✗ Error: {e}")
        # Handle error (see below)

channel.basic_consume(
    queue='tasks',
    on_message_callback=callback,
    auto_ack=False  # Manual acknowledgment required
)

channel.start_consuming()
```

**Advantages:**
- Message preserved if consumer crashes
- Only deleted after confirmed processing
- Can be retried on failure
- **Use for critical messages**

## Handling Failed Messages

### Acknowledge (Success)

```python
ch.basic_ack(delivery_tag=method.delivery_tag)
```

Message is deleted from queue.

### Reject with Requeue (Retry)

```python
ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
```

Message goes back to queue for another attempt.

```python
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='retry_queue', durable=True)

attempt = {}

def callback(ch, method, properties, body):
    msg_id = properties.correlation_id or str(method.delivery_tag)
    attempt[msg_id] = attempt.get(msg_id, 0) + 1
    
    try:
        print(f"Attempt {attempt[msg_id]}: {body.decode()}")
        
        # Simulate failure on attempt 1-2
        if attempt[msg_id] < 3:
            raise Exception("Temporary failure")
        
        print("✓ Success!")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"✗ {e}")
        
        if attempt[msg_id] < 3:
            # Requeue for retry
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(1)  # Delay before retry
        else:
            # Max retries reached, acknowledge to prevent infinite loop
            print("Max retries exceeded, discarding")
            ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='retry_queue',
    on_message_callback=callback,
    auto_ack=False
)

channel.start_consuming()
```

### Reject without Requeue (Dead Letter)

```python
ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

Message goes to dead letter exchange (if configured) or is discarded.

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Setup dead letter exchange
channel.exchange_declare(exchange='dlx', exchange_type='direct', durable=True)
channel.queue_declare(queue='failed_messages', durable=True)
channel.queue_bind(exchange='dlx', queue='failed_messages', routing_key='failed')

# Main queue with dead letter configured
channel.queue_declare(
    queue='main_queue',
    durable=True,
    arguments={'x-dead-letter-exchange': 'dlx'}
)

def callback(ch, method, properties, body):
    try:
        print(f"Processing: {body.decode()}")
        # Simulate permanent failure
        raise Exception("Unrecoverable error")
        
    except Exception as e:
        print(f"✗ Fatal error: {e}")
        # Reject without requeue → goes to DLX
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(
    queue='main_queue',
    on_message_callback=callback,
    auto_ack=False
)

channel.start_consuming()
```

## ACK Timeout

If consumer doesn't acknowledge within timeout, RabbitMQ assumes it crashed and redelivers.

**Default**: 30 minutes

**Configure** in RabbitMQ config:
```
consumer_timeout = 1800000  # 30 min in milliseconds
```

Or set per connection:
```python
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        channel_max=2048,
        frame_max=131072,
        heartbeat=600  # 10 minutes
    )
)
```

## Complete Real-World Example

```python
import pika
import json
import time
from datetime import datetime

def setup_error_handling():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # 1. Setup Dead Letter Exchange
    channel.exchange_declare(
        exchange='dlx',
        exchange_type='direct',
        durable=True
    )
    channel.queue_declare(queue='failed_messages', durable=True)
    channel.queue_bind(
        exchange='dlx',
        queue='failed_messages',
        routing_key='permanent_failure'
    )

    # 2. Setup Retry Exchange
    channel.exchange_declare(
        exchange='retry_exchange',
        exchange_type='direct',
        durable=True
    )
    channel.queue_declare(
        queue='retry_queue',
        durable=True,
        arguments={
            'x-message-ttl': 5000,  # 5 seconds
            'x-dead-letter-exchange': 'work_exchange'  # Retry back to main
        }
    )
    channel.queue_bind(
        exchange='retry_exchange',
        queue='retry_queue',
        routing_key='retry'
    )

    # 3. Setup Main Work Queue
    channel.exchange_declare(
        exchange='work_exchange',
        exchange_type='direct',
        durable=True
    )
    channel.queue_declare(
        queue='work_queue',
        durable=True,
        arguments={'x-dead-letter-exchange': 'dlx'}
    )
    channel.queue_bind(
        exchange='work_exchange',
        queue='work_queue',
        routing_key='task'
    )

    channel.basic_qos(prefetch_count=1)

    # Counter for retry attempts
    attempts = {}

    def process_work(ch, method, properties, body):
        msg_id = properties.correlation_id or str(method.delivery_tag)
        
        # Track attempts
        attempts[msg_id] = attempts.get(msg_id, 0) + 1
        attempt_num = attempts[msg_id]

        try:
            data = json.loads(body)
            print(f"\n[Attempt {attempt_num}] Processing: {data}")

            # Simulate work with potential failure
            import random
            if random.random() < 0.6 and attempt_num < 3:  # 60% failure rate
                raise Exception("Processing failed")

            # Success
            print(f"✓ Task {msg_id} completed")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            del attempts[msg_id]

        except Exception as e:
            print(f"✗ {e}")

            if attempt_num < 3:
                # Retry: Send to retry queue with delay
                print(f"  ↻ Scheduling retry (attempt {attempt_num + 1}/3)...")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
                # Republish to retry queue
                channel.basic_publish(
                    exchange='retry_exchange',
                    routing_key='retry',
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                        correlation_id=msg_id
                    )
                )
            else:
                # Max retries: Send to dead letter
                print(f"  ✗ Max retries ({attempt_num}) exceeded!")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # Start consumer
    channel.basic_consume(
        queue='work_queue',
        on_message_callback=process_work,
        auto_ack=False
    )

    print("Error handling setup complete. Listening for tasks...")
    channel.start_consuming()

if __name__ == '__main__':
    setup_error_handling()
```

## Acknowledgment Best Practices

1. **Use manual acknowledgment** for important data
2. **Acknowledge only after successful processing**
3. **Implement proper error handling** (retry vs discard)
4. **Set reasonable retry limits** (prevent infinite loops)
5. **Use correlation IDs** to track retry attempts
6. **Monitor failed messages** in dead letter queue
7. **Configure appropriate timeouts** for slow processing
8. **Log ACK/NACK** decisions for debugging

## Acknowledgment Summary Table

| Setting | Auto-ACK | Manual ACK |
|---------|----------|-----------|
| When deleted | Immediately | After processing |
| Message loss risk | High | Low |
| Retry capability | No | Yes |
| Complexity | Low | Medium |
| Use case | Non-critical | Critical/Important |
| Failure recovery | No | Yes |
