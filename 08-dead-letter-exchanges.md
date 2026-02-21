# Dead Letter Exchanges (DLX)

## What is a Dead Letter?

A dead letter is a message that cannot be delivered to the intended consumer. Instead of being lost, it's routed to a Dead Letter Exchange (DLX) for special handling.

```
┌─────────────────────────────────────────┐
│ Main Queue                               │
│ - Max retries exceeded                   │
│ - TTL expired                            │
│ - Rejected by consumer                   │
└──────────┬────────────────────────────────┘
           │
           ↓
  ┌─────────────────────┐
  │  Dead Letter        │
  │  Exchange (DLX)     │
  └────────┬────────────┘
           │
           ↓
  ┌─────────────────────┐
  │ Dead Letter Queue   │
  │ (for investigation) │
  └─────────────────────┘
```

## When Messages Become Dead Letters

1. **Consumer Rejection**: Message rejected with `requeue=false`
2. **TTL Expiration**: Message expires before processing
3. **Queue Overflow**: Max-length exceeded, oldest messages removed
4. **Negative Acknowledgment Too Many Times**: Max retries exceeded

## Setting Up Dead Letter Exchange

### Basic Setup

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# 1. Declare DLX
channel.exchange_declare(
    exchange='dlx_exchange',
    exchange_type='direct',
    durable=True
)

# 2. Declare DLQ (Dead Letter Queue)
channel.queue_declare(
    queue='dead_letter_queue',
    durable=True
)

# 3. Bind DLQ to DLX
channel.queue_bind(
    exchange='dlx_exchange',
    queue='dead_letter_queue',
    routing_key='failed'
)

# 4. Declare main queue with DLX reference
channel.queue_declare(
    queue='main_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'dlx_exchange',
        'x-dead-letter-routing-key': 'failed'
    }
)

# 5. Declare work exchange
channel.exchange_declare(
    exchange='work_exchange',
    exchange_type='direct',
    durable=True
)

# 6. Bind main queue to work exchange
channel.queue_bind(
    exchange='work_exchange',
    queue='main_queue',
    routing_key='task'
)

print("DLX setup complete!")
connection.close()
```

## Handling Dead Letters

### Monitoring Dead Letters

```python
import pika
import json

def monitor_dead_letters():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    channel.queue_declare(queue='dead_letter_queue', durable=True)

    def handle_dead_letter(ch, method, properties, body):
        try:
            message = json.loads(body)
            print(f"\n[DEAD LETTER] {message}")
            print(f"  Original exchange: {properties.headers.get('x-death', [{}])[0].get('exchange')}")
            print(f"  Original queue: {properties.headers.get('x-death', [{}])[0].get('queue')}")
            print(f"  Failure count: {properties.headers.get('x-death', [{}])[0].get('count')}")
            print(f"  Reason: {properties.headers.get('x-death', [{}])[0].get('reason')}")
            
            # Log to database or alert system
            log_dead_letter(message, properties)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing dead letter: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(
        queue='dead_letter_queue',
        on_message_callback=handle_dead_letter,
        auto_ack=False
    )

    print("Monitoring dead letters...")
    channel.start_consuming()

def log_dead_letter(message, properties):
    # TODO: Log to database, send alert, etc.
    pass

if __name__ == '__main__':
    monitor_dead_letters()
```

## Real-World Example: Payment Processing with DLX

```python
import pika
import json
import time
from datetime import datetime

def setup_payment_system():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    # ============ DLX Setup ============
    channel.exchange_declare(
        exchange='payment_dlx',
        exchange_type='direct',
        durable=True
    )
    channel.queue_declare(queue='failed_payments', durable=True)
    channel.queue_bind(
        exchange='payment_dlx',
        queue='failed_payments',
        routing_key='payment.failed'
    )

    # ============ Main Setup ============
    channel.exchange_declare(
        exchange='payments',
        exchange_type='direct',
        durable=True
    )
    channel.queue_declare(
        queue='payment_queue',
        durable=True,
        arguments={
            'x-dead-letter-exchange': 'payment_dlx',
            'x-dead-letter-routing-key': 'payment.failed'
        }
    )
    channel.queue_bind(
        exchange='payments',
        queue='payment_queue',
        routing_key='payment.process'
    )

    channel.basic_qos(prefetch_count=1)

    # Payment processor
    def process_payment(ch, method, properties, body):
        try:
            payment = json.loads(body)
            order_id = payment['order_id']
            
            print(f"\n[Processing] Payment for Order {order_id}")
            print(f"  Amount: ${payment['amount']}")
            
            # Simulate payment processing
            if payment['amount'] <= 0:
                raise ValueError("Invalid amount")
            
            # Simulate occasional failures
            import random
            if random.random() < 0.3:  # 30% failure rate
                raise Exception("Payment gateway timeout")
            
            # Success
            print(f"✓ Payment processed!")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Publish payment.completed event
            channel.basic_publish(
                exchange='',
                routing_key='payment.completed',
                body=json.dumps({**payment, 'status': 'completed'}),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type='application/json'
                )
            )
            
        except Exception as e:
            print(f"✗ Error: {e}")
            # Reject without requeue → goes to DLX
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(
        queue='payment_queue',
        on_message_callback=process_payment,
        auto_ack=False
    )

    # ============ Dead Letter Handler ============
    def handle_failed_payment(ch, method, properties, body):
        try:
            payment = json.loads(body)
            
            print(f"\n[FAILED PAYMENT] Order {payment['order_id']}")
            print(f"  Amount: ${payment['amount']}")
            
            # Extract failure info
            death_info = properties.headers.get('x-death', [{}])[0]
            print(f"  Reason: {death_info.get('reason')}")
            print(f"  Failure count: {death_info.get('count')}")
            
            # Log to database
            log_failed_payment(payment)
            
            # Send alert to finance team
            send_alert(f"Payment failed for order {payment['order_id']}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error handling dead letter: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # Start listening to DLQ
    channel.queue_declare(queue='failed_payments', durable=True)
    channel.basic_consume(
        queue='failed_payments',
        on_message_callback=handle_failed_payment,
        auto_ack=False
    )

    print("Payment system ready!")
    print("- Processing payments from 'payment_queue'")
    print("- Handling failures at 'failed_payments'")
    channel.start_consuming()

def log_failed_payment(payment):
    # TODO: Log to database with timestamp and reason
    print(f"  💾 Logged to database")

def send_alert(message):
    # TODO: Send email/Slack/SMS alert
    print(f"  🚨 Alert sent: {message}")

if __name__ == '__main__':
    setup_payment_system()
```

## Publisher: Sending Payments

```python
# payment_producer.py
import pika
import json

def submit_payment(order_id, amount):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost')
    )
    channel = connection.channel()

    payment = {
        'order_id': order_id,
        'amount': amount,
        'timestamp': datetime.now().isoformat()
    }

    channel.basic_publish(
        exchange='payments',
        routing_key='payment.process',
        body=json.dumps(payment),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type='application/json',
            correlation_id=order_id
        )
    )

    print(f"✓ Payment submitted: Order {order_id} (${amount})")
    connection.close()

if __name__ == '__main__':
    from datetime import datetime
    
    # Submit test payments
    submit_payment('ORD-001', 99.99)
    submit_payment('ORD-002', 50.00)
    submit_payment('ORD-003', -10.00)  # Invalid - will fail
```

## Message Headers in DLX

When a message becomes a dead letter, RabbitMQ adds headers:

```python
headers = {
    'x-death': [
        {
            'reason': 'rejected',  # or 'expired', 'maxlen'
            'queue': 'payment_queue',
            'time': <timestamp>,
            'exchange': 'payments',
            'routing-keys': ['payment.process'],
            'count': 1  # Number of times redelivered
        }
    ]
}
```

**Access in handler:**
```python
def handle_dead_letter(ch, method, properties, body):
    death_info = properties.headers.get('x-death', [{}])[0]
    reason = death_info['reason']      # 'rejected', 'expired', etc.
    count = death_info['count']         # Retry count
    queue = death_info['queue']         # Original queue name
```

## DLX Routing Key Strategies

### Strategy 1: Single DLX Route

All dead letters go to same queue:

```python
'x-dead-letter-exchange': 'dlx',
'x-dead-letter-routing-key': 'failed'
```

### Strategy 2: Exchange-based Routing

Route by original exchange:

```python
'x-dead-letter-exchange': 'dlx',
'x-dead-letter-routing-key': 'original_exchange'
```

Then DLX uses topic bindings:
```python
# From payments exchange
channel.queue_bind(exchange='dlx', queue='failed_payments', routing_key='payments')

# From orders exchange  
channel.queue_bind(exchange='dlx', queue='failed_orders', routing_key='orders')
```

### Strategy 3: Reason-based Routing

```python
# This would need custom code - RabbitMQ doesn't auto-set
# but you can add custom headers before rejecting:

message_headers = {
    'reason': 'duplicate_payment',
    'context': 'fraud_check'
}

ch.basic_publish(
    exchange='dlx',
    routing_key='duplicate',
    body=body,
    properties=pika.BasicProperties(headers=message_headers)
)
```

## DLX Best Practices

1. **Always configure DLX** for important queues
2. **Separate DLX from main exchanges** (prevent loops)
3. **Monitor DLQ regularly** for stuck messages
4. **Log all dead letters** with reason and context
5. **Set DLX routing key explicitly** (don't rely on defaults)
6. **Archive old dead letters** periodically
7. **Alert on DLQ growth** (indicates systemic issue)
8. **Implement DLX processing** - don't ignore dead letters!

## Anti-Patterns to Avoid

### ❌ Don't Create Infinite Loops
```python
# BAD: DLX publishes back to same queue
channel.queue_bind(exchange='dlx', queue='main_queue', routing_key='retry')
# This will loop infinitely!
```

### ❌ Don't Forget to Process DLQ
```python
# BAD: Messages accumulate in dead letter queue forever
# Good: Monitor and process them
```

### ❌ Don't Lose Context
```python
# BAD: Strip message in error handling
original_msg_lost = True

# Good: Preserve original message and add metadata
```
