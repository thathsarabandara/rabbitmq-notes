# Performance and Optimization

## Performance Metrics

Key metrics to monitor:

### Throughput
- Messages per second (msg/s)
- Bytes per second (MB/s)

### Latency
- Message publish latency
- End-to-end latency (publish → consumer receives)

### Queue Characteristics
- Queue length (backlog)
- Consumer lag
- Unacknowledged messages

## Optimization Techniques

### 1. Message Batching

Publish multiple messages per connection round trip:

```python
import pika
import json
import time

def batch_publishing(batch_size=1000):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='metrics', exchange_type='direct')
    
    # Enable publisher confirmations for batches
    channel.confirm_delivery()

    messages = []
    start_time = time.time()

    for i in range(10000):
        message = json.dumps({'id': i, 'value': i * 10})
        messages.append(message)

        if len(messages) >= batch_size:
            # Publish batch
            for msg in messages:
                channel.basic_publish(
                    exchange='metrics',
                    routing_key='data',
                    body=msg
                )
            messages = []

            elapsed = time.time() - start_time
            rate = (i / elapsed) if elapsed > 0 else 0
            print(f"Published {i} messages ({rate:.0f} msg/s)")

    # Publish remaining messages
    for msg in messages:
        channel.basic_publish(exchange='metrics', routing_key='data', body=msg)

    total_time = time.time() - start_time
    print(f"✓ 10000 messages in {total_time:.2f}s ({10000/total_time:.0f} msg/s)")
    connection.close()

if __name__ == '__main__':
    batch_publishing(batch_size=500)
```

### 2. Channel Reuse

Reuse channels instead of creating new ones:

```python
# ❌ BAD: New channel per message
def publish_bad(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()  # Expensive!
    channel.basic_publish(exchange='', routing_key='queue', body=message)
    connection.close()

# ✓ GOOD: Reuse channel
class MessagePublisher:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()

    def publish(self, message):
        self.channel.basic_publish(
            exchange='', 
            routing_key='queue', 
            body=message
        )

    def close(self):
        self.connection.close()

publisher = MessagePublisher()
for msg in messages:
    publisher.publish(msg)
publisher.close()
```

### 3. Connection Pooling

Manage multiple connections:

```python
from queue import Queue
import pika
from threading import Lock

class ConnectionPool:
    def __init__(self, host='localhost', pool_size=10):
        self.pool = Queue(maxsize=pool_size)
        self.host = host
        self.pool_size = pool_size
        self.lock = Lock()
        
        # Pre-create connections
        for _ in range(pool_size):
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.pool.put(conn)

    def get_connection(self):
        return self.pool.get()

    def return_connection(self, conn):
        self.pool.put(conn)

    def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get()
            conn.close()

# Usage
pool = ConnectionPool(pool_size=20)
conn = pool.get_connection()
try:
    channel = conn.channel()
    channel.basic_publish(exchange='', routing_key='queue', body='msg')
finally:
    pool.return_connection(conn)
```

### 4. Message Compression

Compress message body to reduce network traffic:

```python
import pika
import json
import zlib
import base64

def publish_compressed(data):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Compress message
    message_json = json.dumps(data).encode()
    compressed = zlib.compress(message_json)
    
    # Encode as base64 for transmission
    encoded = base64.b64encode(compressed)

    channel.basic_publish(
        exchange='',
        routing_key='compressed_queue',
        body=encoded,
        properties=pika.BasicProperties(
            content_encoding='gzip'
        )
    )

    print(f"Original: {len(message_json)} bytes")
    print(f"Compressed: {len(encoded)} bytes")
    print(f"Ratio: {len(encoded)/len(message_json):.1%}")
    connection.close()

def consume_compressed():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    def callback(ch, method, properties, body):
        # Decompress
        decoded = base64.b64decode(body)
        decompressed = zlib.decompress(decoded)
        data = json.loads(decompressed)
        print(f"Received: {data}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue='compressed_queue',
        on_message_callback=callback,
        auto_ack=False
    )

    channel.start_consuming()
```

### 5. Appropriate QoS (Prefetch Count)

Balance fairness and throughput:

```python
import pika

def worker_with_optimal_qos():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='tasks', durable=True)

    # QoS optimization
    # prefetch_count = 1: Fair distribution, slow
    # prefetch_count = 10-100: Balanced
    # prefetch_count = 1000+: Fast, potential unbalanced load
    
    prefetch = 10  # Balanced for most cases
    channel.basic_qos(prefetch_count=prefetch)

    def process_task(ch, method, properties, body):
        print(f"Processing: {body}")
        # Do work
        time.sleep(1)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue='tasks',
        on_message_callback=process_task,
        auto_ack=False
    )

    print(f"Worker started (QoS prefetch={prefetch})")
    channel.start_consuming()
```

### 6. Lazy Queue Mode

For large queues, use lazy mode to spool to disk faster:

```python
import pika

def declare_lazy_queue():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Lazy queue - writes to disk aggressively
    channel.queue_declare(
        queue='large_queue',
        durable=True,
        arguments={
            'x-queue-mode': 'lazy'  # vs 'default'
        }
    )

    print("Lazy queue declared")
    connection.close()
```

**Lazy vs Default:**
- **Default**: Keeps messages in memory (faster, limited capacity)
- **Lazy**: Uses disk heavily (slower, unlimited capacity)

### 7. Use Transactions Sparingly

Transactions reduce throughput significantly:

```python
# ❌ Slow: With transaction
def publish_with_transaction():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.tx_select()  # Begin transaction
    channel.basic_publish(exchange='', routing_key='queue', body='msg')
    channel.tx_commit()  # Confirm - SLOW!
    connection.close()

# ✓ Fast: Use publisher confirmations instead
def publish_with_confirmations():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.confirm_delivery()  # Enable confirmations
    channel.basic_publish(exchange='', routing_key='queue', body='msg')
    # Much faster than transactions!
    connection.close()
```

### 8. Message TTL Configuration

Set appropriate TTL to prevent queue buildup:

```python
import pika

def setup_ttl_queue():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Messages expire after 1 hour
    channel.queue_declare(
        queue='temp_messages',
        durable=True,
        arguments={
            'x-message-ttl': 3600000  # 1 hour in ms
        }
    )

    # Queue itself expires after 30 min of inactivity
    channel.queue_declare(
        queue='transient_queue',
        arguments={
            'x-expires': 1800000  # 30 minutes
        }
    )

    print("TTL queues created")
    connection.close()
```

## Performance Monitoring

### Consumer Lag

Monitor how far behind consumers are:

```python
import pika

def check_consumer_lag():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # Get queue stats
    queue_stats = channel.queue_declare(
        queue='my_queue',
        passive=True  # Don't create, just check
    )

    message_count = queue_stats.method.message_count
    consumer_count = queue_stats.method.consumer_count

    print(f"Queue: my_queue")
    print(f"  Messages waiting: {message_count}")
    print(f"  Active consumers: {consumer_count}")
    print(f"  Lag per consumer: {message_count / max(consumer_count, 1):.0f}")

    connection.close()
```

### Using Management API

```python
import requests
import json

def get_queue_stats(queue_name, vhost='/'):
    # Connect to management API
    response = requests.get(
        f'http://localhost:15672/api/queues/{vhost}/{queue_name}',
        auth=('guest', 'guest')
    )
    
    stats = response.json()
    
    print(f"Queue: {queue_name}")
    print(f"  Messages: {stats['messages']}")
    print(f"  Messages ready: {stats['messages_ready']}")
    print(f"  Messages unacked: {stats['messages_unacked']}")
    print(f"  Consumers: {stats['consumers']}")
    print(f"  Idle since: {stats['idle_since']}")

get_queue_stats('my_queue')
```

## Benchmarking

Simple performance test:

```python
import pika
import time
import json
from concurrent.futures import ThreadPoolExecutor

def benchmark_throughput(num_messages=10000, num_workers=4):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='benchmark', durable=True)
    channel.basic_qos(prefetch_count=10)

    start_time = time.time()
    processed = [0]

    def worker():
        def callback(ch, method, properties, body):
            processed[0] += 1
            if processed[0] % 1000 == 0:
                elapsed = time.time() - start_time
                rate = processed[0] / elapsed
                print(f"Processed: {processed[0]} ({rate:.0f} msg/s)")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(
            queue='benchmark',
            on_message_callback=callback,
            auto_ack=False
        )

        while processed[0] < num_messages:
            channel.connection.process_data_events(time_limit=1)

    # Publish messages
    for i in range(num_messages):
        channel.basic_publish(
            exchange='',
            routing_key='benchmark',
            body=json.dumps({'id': i}),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )

    # Start workers
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker) for _ in range(num_workers)]
        for future in futures:
            future.result()

    total_time = time.time() - start_time
    print(f"\n✓ Benchmark complete:")
    print(f"  Messages: {num_messages}")
    print(f"  Time: {total_time:.2f}s")
    print(f"  Throughput: {num_messages/total_time:.0f} msg/s")

if __name__ == '__main__':
    benchmark_throughput(num_messages=10000, num_workers=4)
```

## Performance Best Practices

1. **Batch messages** when possible (500-1000 per batch)
2. **Reuse connections and channels**
3. **Use connection pooling** (not unlimited)
4. **Set appropriate QoS** (prefetch_count=10-50)
5. **Use publisher confirmations** instead of transactions
6. **Compress large messages** (>10KB)
7. **Monitor queue length** and consumer lag
8. **Use lazy queues** for large backlogs
9. **Set message TTL** to prevent stale data
10. **Profile regularly** with benchmarks

## Why Performance Matters

```
10M messages/day at 100 msg/s:
  Queue length ok ✓

10M messages/day at 10 msg/s:
  Queue builds up 1000s of messages
  Latency goes up
  System becomes unreliable ✗
```

Always match throughput to producer rate!
