# Producers and Consumers

## Producers (Publishers)

A producer is an application that sends messages to RabbitMQ.

### Basic Producer

```python
import pika

def simple_producer():
    # Connect
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Declare exchange
    channel.exchange_declare(exchange='logs', exchange_type='direct', durable=True)

    # Publish message
    channel.basic_publish(
        exchange='logs',
        routing_key='info',
        body='Application started'
    )

    print("Message published!")
    connection.close()

if __name__ == '__main__':
    simple_producer()
```

### Producer with Confirmations

Wait for broker confirmation that message was received:

```python
import pika

def producer_with_confirmation():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Enable publisher confirmations
    channel.confirm_delivery()

    channel.exchange_declare(exchange='orders', exchange_type='direct', durable=True)

    try:
        # Publish with confirmation
        channel.basic_publish(
            exchange='orders',
            routing_key='order.create',
            body='Order #123',
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        print("✓ Message confirmed by broker")
    except pika.exceptions.UnroutableError:
        print("✗ Message could not be routed!")
    
    connection.close()

if __name__ == '__main__':
    producer_with_confirmation()
```

### Batch Producer

Publish multiple messages efficiently:

```python
import pika
import json
from datetime import datetime

def batch_producer(num_messages=1000):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='metrics', exchange_type='topic', durable=True)
    channel.confirm_delivery()  # Enable confirmations for batch

    for i in range(num_messages):
        message = {
            'id': i,
            'value': i * 10,
            'timestamp': datetime.now().isoformat()
        }

        channel.basic_publish(
            exchange='metrics',
            routing_key='system.cpu',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type='application/json'
            )
        )

        if (i + 1) % 100 == 0:
            print(f"Published {i + 1} messages...")

    print("✓ All messages confirmed!")
    connection.close()

if __name__ == '__main__':
    batch_producer(1000)
```

## Consumers (Subscribers)

A consumer is an application that receives and processes messages from RabbitMQ.

### Basic Consumer

```python
import pika

def simple_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Declare queue
    channel.queue_declare(queue='hello', durable=True)

    # Define callback
    def callback(ch, method, properties, body):
        print(f"Received: {body.decode()}")

    # Set up consumer
    channel.basic_consume(
        queue='hello',
        on_message_callback=callback,
        auto_ack=True  # Automatically acknowledge
    )

    print("Waiting for messages...")
    channel.start_consuming()

if __name__ == '__main__':
    simple_consumer()
```

### Consumer with Manual Acknowledgment

Acknowledge only after successful processing:

```python
import pika
import time

def consumer_with_ack():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='tasks', durable=True)

    def process_message(ch, method, properties, body):
        try:
            print(f"Processing: {body.decode()}")
            # Simulate processing
            time.sleep(2)
            print("✓ Task completed")
            
            # Acknowledge after successful processing
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"✗ Error: {e}")
            # Reject and requeue for retry
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    # auto_ack=False = manual acknowledgment required
    channel.basic_consume(
        queue='tasks',
        on_message_callback=process_message,
        auto_ack=False
    )

    print("Consumer started (manual ack)...")
    channel.start_consuming()

if __name__ == '__main__':
    consumer_with_ack()
```

### QoS (Quality of Service)

Control how many messages a consumer receives at once:

```python
import pika

def consumer_with_qos():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='work', durable=True)

    # Set QoS: Process only 1 message at a time
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        print(f"Processing: {body.decode()}")
        # Simulate slow processing
        import time
        time.sleep(5)
        print("Done!")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue='work',
        on_message_callback=callback,
        auto_ack=False
    )

    print("Worker started (QoS=1)...")
    channel.start_consuming()

if __name__ == '__main__':
    consumer_with_qos()
```

**QoS Options:**
- `prefetch_count=1`: Process 1 message at a time (safe)
- `prefetch_count=10`: Process up to 10 messages (faster but risky)
- `prefetch_size=0`: No byte limit (count only)

### Multiple Consumers (Fair Dispatch)

Scale processing with multiple workers:

```python
import pika
import time

def worker(worker_id):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='jobs', durable=True)
    channel.basic_qos(prefetch_count=1)  # Fair dispatch

    def process_job(ch, method, properties, body):
        print(f"[Worker {worker_id}] Got job: {body.decode()}")
        time.sleep((worker_id % 3) + 1)  # Variable processing time
        print(f"[Worker {worker_id}] ✓ Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue='jobs',
        on_message_callback=process_job,
        auto_ack=False
    )

    print(f"[Worker {worker_id}] Started...")
    channel.start_consuming()

# Run multiple workers
if __name__ == '__main__':
    import threading
    
    for i in range(3):
        t = threading.Thread(target=worker, args=(i+1,), daemon=True)
        t.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
```

## Producer-Consumer Pattern

### Simple Echo Pattern

```python
# producer.py
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='echo', durable=True)

# Send message
channel.basic_publish(
    exchange='',
    routing_key='echo',
    body='Hello Echo!',
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent
    )
)

print("Message sent!")
connection.close()

# consumer.py
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='echo', durable=True)

def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='echo',
    on_message_callback=callback,
    auto_ack=False
)

print("Listening...")
channel.start_consuming()
```

## Real-World Example: Order Processing

```python
# order_producer.py
import pika
import json
from datetime import datetime

def publish_order(order_data):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Setup
    channel.exchange_declare(exchange='orders', exchange_type='topic', durable=True)

    # Publish
    channel.basic_publish(
        exchange='orders',
        routing_key='order.created',
        body=json.dumps({
            'timestamp': datetime.now().isoformat(),
            **order_data
        }),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/json'
        )
    )

    print(f"Order {order_data['order_id']} published!")
    connection.close()

# Example usage
if __name__ == '__main__':
    publish_order({
        'order_id': 'ORD-001',
        'customer': 'Alice',
        'total': 99.99
    })

# ============================================
# order_consumer.py
import pika
import json
import time

def start_order_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Setup
    channel.exchange_declare(exchange='orders', exchange_type='topic', durable=True)
    channel.queue_declare(queue='order_processor', durable=True)
    channel.queue_bind(
        exchange='orders',
        queue='order_processor',
        routing_key='order.#'
    )

    channel.basic_qos(prefetch_count=1)

    def process_order(ch, method, properties, body):
        order = json.loads(body)
        print(f"\n[Processing] Order {order['order_id']} from {order['customer']}")
        
        try:
            # Simulate processing
            time.sleep(2)
            
            # Charge payment
            print(f"  💳 Charged ${order['total']}")
            
            # Send confirmation
            print(f"  📧 Confirmation sent")
            
            # Acknowledge success
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"✓ Order {order['order_id']} complete")
        except Exception as e:
            print(f"✗ Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(
        queue='order_processor',
        on_message_callback=process_order,
        auto_ack=False
    )

    print("Order processor ready...")
    channel.start_consuming()

if __name__ == '__main__':
    start_order_consumer()
```

## Consumer Best Practices

1. **Use manual acknowledgments** for critical messages
2. **Set appropriate QoS** (prefetch_count=1 for fairness)
3. **Implement idempotent processing** (handle duplicates)
4. **Add error handling** for failed processing
5. **Use requeue sparingly** (prevents infinite loops with poison pills)
6. **Monitor consumer lag** using management UI
7. **Graceful shutdown**: Stop consuming before closing connection

## Producer Best Practices

1. **Use delivery mode persistent** for important messages
2. **Enable publisher confirmations** when critical
3. **Handle errors** (unroutable, connection lost)
4. **Batch publish** for performance
5. **Set properties** (content-type, correlation ID)
6. **Implement circuit breaker** for degraded broker
7. **Monitor publishing** success rates
