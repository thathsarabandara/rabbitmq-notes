# Complete Real-World Project: E-Commerce Order Processing

This example ties together all RabbitMQ concepts in a realistic e-commerce scenario.

## System Architecture

```
Customer →  API Server  → Order Queue
               ↓
           Order Service ─→ Order Exchange
               ↓
         (routing by status)
               ├→ Payment Queue  → Payment Service
               ├→ Inventory Queue → Inventory Service
               ├→ Notification Queue → Email/SMS Service
               └→ Audit Queue → Analytics Service
```

## Complete Implementation

### 1. Setup Infrastructure

```python
# infrastructure.py
import pika
import json

class RabbitMQInfrastructure:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()

    def setup(self):
        """Setup entire message infrastructure"""
        
        # ==================== Exchanges ====================
        
        # Main order exchange (topic)
        self.channel.exchange_declare(
            exchange='orders',
            exchange_type='topic',
            durable=True
        )

        # DLX for failed messages
        self.channel.exchange_declare(
            exchange='orders_dlx',
            exchange_type='direct',
            durable=True
        )

        # Retry exchange
        self.channel.exchange_declare(
            exchange='orders_retry',
            exchange_type='direct',
            durable=True
        )

        # ==================== Queues ====================

        # Order ingestion queue
        self.channel.queue_declare(
            queue='order_ingestion',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'orders_dlx',
                'x-max-length': 100000
            }
        )

        # Payment processing queue
        self.channel.queue_declare(
            queue='payment_queue',
            durable=True,
            arguments={
                'x-max-priority': 10,
                'x-message-ttl': 3600000,  # 1 hour
                'x-dead-letter-exchange': 'orders_dlx'
            }
        )

        # Inventory management queue
        self.channel.queue_declare(
            queue='inventory_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'orders_dlx'
            }
        )

        # Notifications queue
        self.channel.queue_declare(
            queue='notifications_queue',
            durable=True
        )

        # Audit/Analytics queue
        self.channel.queue_declare(
            queue='audit_queue',
            durable=True,
            arguments={
                'x-queue-mode': 'lazy'  # Large queue, disk-heavy
            }
        )

        # Dead letter queue
        self.channel.queue_declare(
            queue='failed_orders_queue',
            durable=True
        )

        # Retry queue (with TTL to redeliver)
        self.channel.queue_declare(
            queue='retry_queue',
            durable=True,
            arguments={
                'x-message-ttl': 10000,  # 10 seconds
                'x-dead-letter-exchange': 'orders'  # Redeliver
            }
        )

        # ==================== Bindings ====================

        # Bind ingestion queue to orders exchange
        self.channel.queue_bind(
            exchange='orders',
            queue='order_ingestion',
            routing_key='order.created'
        )

        # Bind service queues to order exchange
        self.channel.queue_bind(
            exchange='orders',
            queue='payment_queue',
            routing_key='order.payment*'
        )

        self.channel.queue_bind(
            exchange='orders',
            queue='inventory_queue',
            routing_key='order.inventory*'
        )

        self.channel.queue_bind(
            exchange='orders',
            queue='notifications_queue',
            routing_key='order.*'
        )

        self.channel.queue_bind(
            exchange='orders',
            queue='audit_queue',
            routing_key='#'  # All events
        )

        # Bind DLX
        self.channel.queue_bind(
            exchange='orders_dlx',
            queue='failed_orders_queue',
            routing_key='failed'
        )

        # Bind retry queue to redeliver
        self.channel.queue_bind(
            exchange='orders_retry',
            queue='retry_queue',
            routing_key='retry'
        )

        print("✓ Infrastructure setup complete!")

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    infra = RabbitMQInfrastructure()
    infra.setup()
    infra.close()
```

### 2. Order Producer (API Server)

```python
# order_producer.py
import pika
import json
import uuid
from datetime import datetime

class OrderProducer:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def create_order(self, customer_id, items, total_amount):
        """Publish new order to system"""
        
        order_id = str(uuid.uuid4())
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'items': items,
            'total_amount': total_amount,
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        try:
            self.channel.basic_publish(
                exchange='orders',
                routing_key='order.created',
                body=json.dumps(order),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type='application/json',
                    correlation_id=order_id,
                    timestamp=int(datetime.now().timestamp())
                )
            )
            
            print(f"✓ Order created: {order_id}")
            return order_id
            
        except pika.exceptions.UnroutableError:
            print(f"✗ No queue for order {order_id}")
            return None

    def update_order_status(self, order_id, new_status):
        """Publish order status update"""
        
        update = {
            'order_id': order_id,
            'status': new_status,
            'updated_at': datetime.now().isoformat()
        }

        routing_key = f'order.{new_status}'
        
        self.channel.basic_publish(
            exchange='orders',
            routing_key=routing_key,
            body=json.dumps(update),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type='application/json',
                correlation_id=order_id
            )
        )

        print(f"Status updated: {order_id} → {new_status}")

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    producer = OrderProducer()
    
    # Create sample orders
    orders = [
        {
            'customer_id': 'CUST001',
            'items': [
                {'sku': 'PROD001', 'quantity': 2, 'price': 29.99},
                {'sku': 'PROD002', 'quantity': 1, 'price': 49.99}
            ],
            'total_amount': 109.97
        },
        {
            'customer_id': 'CUST002',
            'items': [
                {'sku': 'PROD003', 'quantity': 3, 'price': 19.99}
            ],
            'total_amount': 59.97
        }
    ]
    
    for order_data in orders:
        producer.create_order(**order_data)
    
    producer.close()
```

### 3. Payment Service

```python
# payment_service.py
import pika
import json
import time
import random

class PaymentService:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='payment_queue', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        
        # For publishing status updates
        self.channel.exchange_declare(
            exchange='orders',
            exchange_type='topic',
            durable=True
        )

    def process_payments(self):
        """Consume and process payment requests"""
        
        def handle_payment(ch, method, properties, body):
            order = json.loads(body)
            order_id = order['order_id']
            
            try:
                print(f"\n[Payment Service] Processing payment for {order_id}")
                print(f"  Amount: ${order['total_amount']}")
                
                # Simulate payment processing
                time.sleep(2)
                
                # Simulate occasional failures
                if random.random() < 0.1:  # 10% failure rate
                    raise Exception("Payment gateway timeout")
                
                # Update status
                self.channel.basic_publish(
                    exchange='orders',
                    routing_key='order.payment_completed',
                    body=json.dumps({
                        'order_id': order_id,
                        'status': 'payment_completed',
                        'transaction_id': f'TXN-{order_id[:8]}',
                        'amount': order['total_amount']
                    }),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                        content_type='application/json',
                        correlation_id=order_id
                    )
                )
                
                print(f"  ✓ Payment processed!")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"  ✗ Payment failed: {e}")
                
                attempt = int(properties.headers.get('x-attempt', [0])[0] or 0) + 1
                
                if attempt < 3:
                    # Retry
                    print(f"  ↻ Retry {attempt}/3")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
                    # Republish to retry queue with delay
                    retry_headers = {
                        'x-attempt': [attempt],
                        'x-original-error': [str(e)]
                    }
                    self.channel.basic_publish(
                        exchange='orders_retry',
                        routing_key='retry',
                        body=body,
                        properties=pika.BasicProperties(
                            delivery_mode=pika.DeliveryMode.Persistent,
                            headers=retry_headers,
                            correlation_id=order_id
                        )
                    )
                else:
                    # Max retries exceeded
                    print(f"  ✗ Max retries exceeded, moving to DLX")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue='payment_queue',
            on_message_callback=handle_payment,
            auto_ack=False
        )

        print("[Payment Service] Ready to process payments")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    service = PaymentService()
    service.process_payments()
```

### 4. Inventory Service

```python
# inventory_service.py
import pika
import json
import time

class InventoryService:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='inventory_queue', durable=True)
        self.channel.basic_qos(prefetch_count=2)
        
        # Inventory data (in real app, use database)
        self.inventory = {
            'PROD001': 100,
            'PROD002': 50,
            'PROD003': 200
        }
        
        self.channel.exchange_declare(
            exchange='orders',
            exchange_type='topic',
            durable=True
        )

    def process_inventory(self):
        """Check and reserve inventory"""
        
        def handle_inventory(ch, method, properties, body):
            order = json.loads(body)
            order_id = order['order_id']
            
            try:
                print(f"\n[Inventory Service] Reserving items for {order_id}")
                
                # Check availability
                for item in order['items']:
                    sku = item['sku']
                    qty = item['quantity']
                    available = self.inventory.get(sku, 0)
                    
                    print(f"  {sku}: need {qty}, have {available}")
                    
                    if available < qty:
                        raise Exception(f"Out of stock: {sku}")
                
                # Reserve items
                for item in order['items']:
                    sku = item['sku']
                    qty = item['quantity']
                    self.inventory[sku] -= qty
                
                print(f"  ✓ Inventory reserved!")
                
                # Publish success
                self.channel.basic_publish(
                    exchange='orders',
                    routing_key='order.inventory_reserved',
                    body=json.dumps({
                        'order_id': order_id,
                        'status': 'inventory_reserved',
                        'items': order['items']
                    }),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                        correlation_id=order_id
                    )
                )
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"  ✗ {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue='inventory_queue',
            on_message_callback=handle_inventory,
            auto_ack=False
        )

        print("[Inventory Service] Ready to process inventory")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    service = InventoryService()
    service.process_inventory()
```

### 5. Notification Service

```python
# notification_service.py
import pika
import json

class NotificationService:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='notifications_queue', durable=True)

    def send_notifications(self):
        """Send customer notifications"""
        
        def handle_notification(ch, method, properties, body):
            order = json.loads(body)
            order_id = order['order_id']
            status = order.get('status', 'unknown')
            
            # Extract customer email (would come from order data in real app)
            customer_id = order.get('customer_id', 'unknown')
            
            print(f"\n[Notification Service] Sending notification for {order_id}")
            
            # Determine notification type
            if status == 'pending':
                subject = "Order Received"
                body_text = f"Order {order_id} has been received"
            elif status == 'payment_completed':
                subject = "Payment Confirmed"
                body_text = f"Payment for order {order_id} has been processed"
            elif status == 'inventory_reserved':
                subject = "Order Confirmed"
                body_text = f"Order {order_id} is being prepared"
            else:
                subject = f"Order Update: {status}"
                body_text = f"Order {order_id} status: {status}"
            
            # Send email/SMS/push (simulated)
            print(f"  📧 Email to {customer_id}: {subject}")
            print(f"     {body_text}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue='notifications_queue',
            on_message_callback=handle_notification,
            auto_ack=False
        )

        print("[Notification Service] Ready to send notifications")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    service = NotificationService()
    service.send_notifications()
```

### 6. Audit Service

```python
# audit_service.py
import pika
import json
from datetime import datetime

class AuditService:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='audit_queue', durable=True)

    def log_events(self):
        """Log all events for analytics"""
        
        events_log = []
        
        def handle_audit(ch, method, properties, body):
            event = json.loads(body)
            
            # Add metadata
            audit_record = {
                'timestamp': datetime.now().isoformat(),
                'routing_key': method.routing_key,
                'correlation_id': properties.correlation_id,
                'event': event
            }
            
            events_log.append(audit_record)
            
            # Log to file/database
            print(f"\n[Audit] {method.routing_key}: {event.get('order_id', '?')}")
            
            # Store to database in real app
            # db.save_audit_log(audit_record)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue='audit_queue',
            on_message_callback=handle_audit,
            auto_ack=False
        )

        print("[Audit Service] Ready to log events")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    service = AuditService()
    service.log_events()
```

### 7. Dead Letter Handler

```python
# dead_letter_handler.py
import pika
import json

class DeadLetterHandler:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='failed_orders_queue', durable=True)

    def handle_dead_letters(self):
        """Handle failed messages"""
        
        def process_dead_letter(ch, method, properties, body):
            try:
                message = json.loads(body)
                
                print(f"\n[Dead Letter Handler] Failed message for order {message.get('order_id')}")
                
                # Extract failure info
                death_header = properties.headers.get('x-death', [{}])[0]
                print(f"  Reason: {death_header.get('reason')}")
                print(f"  Original queue: {death_header.get('queue')}")
                print(f"  Failure count: {death_header.get('count')}")
                
                # Send alert
                print(f"  🚨 Alert sent to ops team")
                
                # Log for investigation
                print(f"  💾 Logged to investigation database")
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"Error processing dead letter: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue='failed_orders_queue',
            on_message_callback=process_dead_letter,
            auto_ack=False
        )

        print("[Dead Letter Handler] Ready to handle failures")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    handler = DeadLetterHandler()
    handler.handle_dead_letters()
```

## Running the Complete System

### Step 1: Setup Infrastructure

```bash
python infrastructure.py
# Output: ✓ Infrastructure setup complete!
```

### Step 2: Start Services (in separate terminals)

```bash
# Terminal 1: Payment Service
python payment_service.py

# Terminal 2: Inventory Service
python inventory_service.py

# Terminal 3: Notification Service
python notification_service.py

# Terminal 4: Audit Service
python audit_service.py

# Terminal 5: Dead Letter Handler
python dead_letter_handler.py
```

### Step 3: Send Orders

```bash
python order_producer.py

# Output example:
# ✓ Order created: a1b2c3d4-e5f6-4g7h-8i9j-0k1l2m3n4o5p
# ✓ Order created: b2c3d4e5-f6g7-4h8i-9j0k-1l2m3n4o5p6q
```

### Expected Output

```
[Payment Service] Processing payment for a1b2c3d4-...
  Amount: $109.97
  ✓ Payment processed!

[Inventory Service] Reserving items for a1b2c3d4-...
  PROD001: need 2, have 100
  PROD002: need 1, have 50
  ✓ Inventory reserved!

[Notification Service] Sending notification for a1b2c3d4-...
  📧 Email to CUST001: Order Confirmed
     Order a1b2c3d4-... is being prepared

[Audit] order.created: a1b2c3d4-...
[Audit] order.payment_completed: a1b2c3d4-...
[Audit] order.inventory_reserved: a1b2c3d4-...
```

## Key Takeaways

1. **Decoupling**: Services don't communicate directly
2. **Scalability**: Easy to add more payment processors, inventory managers
3. **Reliability**: Failed orders go to DLX for handling
4. **Auditability**: All events logged for analytics
5. **Extensibility**: New services can subscribe to events
6. **Fault Tolerance**: Retry logic with max attempts
7. **Visibility**: Each service logs its activities

This architecture scales from small to enterprise systems!
