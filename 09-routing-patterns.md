# Advanced Routing Patterns

## RPC (Remote Procedure Call) Pattern

RPC allows a client to call a remote method and wait for the response.

```
Client (Publisher)
  ↓
  Sends request → RPC Queue
  ↓
Server (Consumer) receives request
  ↓
Server processes → Response Queue (created just for this request)
  ↓
Client receives response
```

### RPC Components

1. **Request Queue**: Client sends request
2. **Response Queue**: Unique per request, client listens here
3. **Correlation ID**: Matches request with response
4. **Reply-to**: Queue where response should go

### RPC Example: Mathematical Operations

```python
# rpc_server.py
import pika
import json

def start_rpc_server():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare RPC queue
    channel.queue_declare(queue='rpc_queue', durable=True)
    channel.basic_qos(prefetch_count=1)

    def handle_rpc_request(ch, method, properties, body):
        try:
            request = json.loads(body)
            print(f"\n[RPC] Received: {request}")
            
            # Process request
            a = request['a']
            b = request['b']
            operation = request['operation']
            
            if operation == 'add':
                result = a + b
            elif operation == 'multiply':
                result = a * b
            elif operation == 'divide':
                if b == 0:
                    raise ValueError("Division by zero")
                result = a / b
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            response = {'result': result, 'status': 'success'}
            print(f"  → Result: {result}")
            
            # Send response
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=json.dumps(response),
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id
                )
            )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"✗ Error: {e}")
            response = {'error': str(e), 'status': 'failure'}
            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=json.dumps(response),
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id
                )
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue='rpc_queue',
        on_message_callback=handle_rpc_request,
        auto_ack=False
    )

    print("RPC Server ready, waiting for requests...")
    channel.start_consuming()

if __name__ == '__main__':
    start_rpc_server()

# ============================================
# rpc_client.py
import pika
import json
import uuid

class RPCClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()
        
        # Create exclusive callback queue
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        # Set up consumer for responses
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, properties, body):
        # Check if response matches our request
        if properties.correlation_id == self.corr_id:
            self.response = json.loads(body)

    def call(self, a, b, operation):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        
        request = {
            'a': a,
            'b': b,
            'operation': operation
        }
        
        # Send request
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            body=json.dumps(request),
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            )
        )
        
        print(f"[RPC Client] Calling {operation}({a}, {b})...")
        
        # Wait for response
        while self.response is None:
            self.connection.process_data_events()
        
        return self.response

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    client = RPCClient()
    
    # Make RPC calls
    results = [
        client.call(5, 3, 'add'),
        client.call(5, 3, 'multiply'),
        client.call(10, 5, 'divide'),
    ]
    
    for result in results:
        print(f"  ✓ {result}")
    
    client.close()
```

## Work Queue Pattern

Distribute tasks among multiple workers.

```
Producer → Queue ← Worker 1
           ↓      ← Worker 2
           ← Worker 3

Fair distribution: Each worker processes one task at a time
```

### Work Queue Example: Image Processing

```python
# task_producer.py
import pika
import json

def publish_image_task(image_id, image_url):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare durable queue
    channel.queue_declare(queue='image_tasks', durable=True)

    task = {
        'image_id': image_id,
        'image_url': image_url,
        'operations': ['resize', 'watermark', 'compress']
    }

    channel.basic_publish(
        exchange='',
        routing_key='image_tasks',
        body=json.dumps(task),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/json'
        )
    )

    print(f"✓ Task published: Image {image_id}")
    connection.close()

if __name__ == '__main__':
    for i in range(1, 11):
        publish_image_task(f'IMG-{i:03d}', f'http://example.com/img{i}.jpg')

# ============================================
# worker.py
import pika
import json
import time
import sys

def start_worker(worker_id):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='image_tasks', durable=True)
    channel.basic_qos(prefetch_count=1)  # Fair dispatch

    def process_image(ch, method, properties, body):
        task = json.loads(body)
        print(f"\n[Worker {worker_id}] Processing: {task['image_id']}")
        
        try:
            for operation in task['operations']:
                print(f"  ⚙️  {operation}...")
                time.sleep(2)  # Simulate processing
            
            print(f"  ✓ Complete!")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(
        queue='image_tasks',
        on_message_callback=process_image,
        auto_ack=False
    )

    print(f"[Worker {worker_id}] Started")
    channel.start_consuming()

if __name__ == '__main__':
    worker_id = sys.argv[1] if len(sys.argv) > 1 else 'default'
    start_worker(worker_id)
```

## Publish-Subscribe Pattern

Same message delivered to multiple interested subscribers.

```
Publisher → Exchange (fanout)
            ├→ Queue 1 → Subscriber 1
            ├→ Queue 2 → Subscriber 2
            └→ Queue 3 → Subscriber 3

All get same message
```

### Publish-Subscribe Example: System Events

```python
# publisher.py
import pika
import json
from datetime import datetime

def publish_event(event_type, event_data):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Fanout exchange
    channel.exchange_declare(exchange='system_events', exchange_type='fanout', durable=True)

    event = {
        'type': event_type,
        'timestamp': datetime.now().isoformat(),
        'data': event_data
    }

    channel.basic_publish(
        exchange='system_events',
        routing_key='',
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/json'
        )
    )

    print(f"✓ Event published: {event_type}")
    connection.close()

if __name__ == '__main__':
    publish_event('server.startup', {'server': 'api-1', 'port': 8080})
    publish_event('database.connected', {'db': 'production'})
    publish_event('cache.cleared', {'keys': 1000})

# ============================================
# subscriber_email.py
import pika
import json

def email_subscriber():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='system_events', exchange_type='fanout', durable=True)
    
    # Exclusive queue for this subscriber
    result = channel.queue_declare(queue='', exclusive=True)
    queue = result.method.queue
    
    channel.queue_bind(exchange='system_events', queue=queue)

    def handle_event(ch, method, properties, body):
        event = json.loads(body)
        print(f"[Email] Sending alert for: {event['type']}")
        print(f"  📧 admin@example.com")

    channel.basic_consume(queue=queue, on_message_callback=handle_event, auto_ack=True)

    print("[Email Subscriber] Ready")
    channel.start_consuming()

if __name__ == '__main__':
    email_subscriber()

# ============================================
# subscriber_logging.py
import pika
import json

def logging_subscriber():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='system_events', exchange_type='fanout', durable=True)
    
    result = channel.queue_declare(queue='', exclusive=True)
    queue = result.method.queue
    
    channel.queue_bind(exchange='system_events', queue=queue)

    def handle_event(ch, method, properties, body):
        event = json.loads(body)
        print(f"[Logger] {event['timestamp']} - {event['type']}: {event['data']}")

    channel.basic_consume(queue=queue, on_message_callback=handle_event, auto_ack=True)

    print("[Logging Subscriber] Ready")
    channel.start_consuming()

if __name__ == '__main__':
    logging_subscriber()
```

## Selective Consumer Pattern

Subscribers specify which messages they want using routing keys.

```
Publisher → Exchange (topic)
            ├→ order.* → Order Service
            ├→ payment.* → Payment Service
            └→ # → Audit Log

Each service gets only its messages
```

### Selective Consumer Example

```python
# core_service.py
import pika
import json

def setup_selective_consumer(service_name, routing_pattern):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Topic exchange
    channel.exchange_declare(exchange='domain_events', exchange_type='topic', durable=True)
    
    # Service-specific queue
    queue_name = f'{service_name}_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Bind with pattern
    channel.queue_bind(
        exchange='domain_events',
        queue=queue_name,
        routing_key=routing_pattern
    )

    def process_event(ch, method, properties, body):
        event = json.loads(body)
        print(f"\n[{service_name}] Processing: {method.routing_key}")
        print(f"  Event: {event}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=process_event,
        auto_ack=False
    )

    print(f"[{service_name}] Listening for: {routing_pattern}")
    channel.start_consuming()

if __name__ == '__main__':
    import threading
    
    # Different services subscribe to different events
    services = [
        ('OrderService', 'order.*'),
        ('PaymentService', 'payment.*'),
        ('NotificationService', '#'),  # All events
    ]
    
    for service_name, pattern in services:
        t = threading.Thread(
            target=setup_selective_consumer,
            args=(service_name, pattern),
            daemon=True
        )
        t.start()
    
    # Keep running
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
```

## Schema/Topic-based Routing

Route based on message schema or type.

```python
import pika
import json

def setup_schema_routing():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Declare schema-based queues
    channel.queue_declare(queue='user_events', durable=True)
    channel.queue_declare(queue='order_events', durable=True)
    channel.queue_declare(queue='payment_events', durable=True)
    
    # Define schema mapping
    schema_routing = {
        'UserCreated': 'user_events',
        'UserUpdated': 'user_events',
        'OrderPlaced': 'order_events',
        'OrderShipped': 'order_events',
        'PaymentProcessed': 'payment_events',
    }
    
    # In producer, use schema name as routing key
    for schema, queue in schema_routing.items():
        channel.queue_bind(
            exchange='events',
            queue=queue,
            routing_key=schema
        )

# Advanced pattern matching
def advanced_routing_rules():
    """
    Route by multiple criteria:
    - Message type (schema)
    - Priority
    - Region/Source
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Topic pattern examples
    patterns = {
        'critical.*': 'critical_alerts',      # All critical
        'user.*.us': 'us_user_queue',        # US users only
        'order.#.premium': 'premium_orders',  # Premium orders
        '*.*.europe': 'europe_queue',        # All Europe events
    }

    for pattern, queue in patterns.items():
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(
            exchange='events',
            queue=queue,
            routing_key=pattern
        )
```

## Routing Pattern Comparison

| Pattern | Best For | Exchange | Scalability |
|---------|----------|----------|-------------|
| **RPC** | Synchronous requests | Direct | Low |
| **Work Queue** | Load balancing tasks | Direct/Default | High |
| **Pub-Sub** | Broadcasting | Fanout | Medium |
| **Selective** | Custom subscriptions | Topic | High |
| **Schema-based** | Type-specific routing | Direct/Topic | Medium |

