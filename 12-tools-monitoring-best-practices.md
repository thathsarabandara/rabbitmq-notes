# Tools, Monitoring, and Best Practices

## Management UI

Built-in web interface for administration.

### Accessing Management UI

```
Browser: http://localhost:15672
Username: guest
Password: guest
```

### Tabs Overview

#### Connections
- Active client connections
- Memory per connection
- Data rate

#### Channels
- Virtual connections per application
- Message rate
- Consumers per channel

#### Queues
- Queue list with message counts
- Consumer count
- Message workrate

#### Exchanges
- Exchange types and bindings
- Traffic metrics

#### Admin
- Add users and permissions
- Virtual hosts (vhosts)
- Server policies

## Command Line Tools

### RabbitMQ Control CLI

```bash
# User management
rabbitmqctl add_user username password
rabbitmqctl set_permissions -p / username ".*" ".*" ".*"
rabbitmqctl list_users

# Queue management
rabbitmqctl list_queues name messages consumers
rabbitmqctl queue_length queue_name
rabbitmqctl purge_queue queue_name

# Cluster management
rabbitmqctl cluster_status
rabbitmqctl join_cluster rabbit@other-node
rabbitmqctl forget_cluster_node rabbit@old-node

# Diagnostics
rabbitmqctl ping
rabbitmqctl node_health_check
rabbitmqctl status
```

### RabbitMQ Admin CLI (New)

```bash
# More intuitive command structure
rabbitmq-ctl user add alice secret
rabbitmq-ctl permission grant alice
rabbitmq-ctl queue info my_queue
```

## Monitoring Solutions

### 1. Prometheus Integration

RabbitMQ exposes Prometheus metrics:

```bash
# Enable management plugin (for metrics)
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_management

# Access metrics endpoint
curl http://localhost:15672/metrics
```

### Prometheus Config

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'rabbitmq'
    static_configs:
      - targets: ['localhost:15672']
    metrics_path: '/metrics'
    basic_auth:
      username: 'guest'
      password: 'guest'
```

### Key Metrics to Monitor

```
# Queue depth
rabbitmq_queue_messages_ready
rabbitmq_queue_messages_unacked

# Consumer lag
(queue_messages_ready / consumers) = lag_per_consumer

# Publish rate
rate(rabbitmq_channel_messages_published[1m])

# Consumer ack rate
rate(rabbitmq_channel_messages_acknowledged[1m])

# Connection count
rabbitmq_connections

# Memory usage
rabbitmq_memory_used_bytes
```

### Alert Rules

```yaml
# promethe_alerts.yml
groups:
  - name: rabbitmq
    rules:
      - alert: QueueBacklog
        expr: rabbitmq_queue_messages_ready > 10000
        for: 5m
        annotations:
          summary: "Queue {{ $labels.queue }} has {{ $value }} messages"

      - alert: HighMemory
        expr: rabbitmq_memory_used_bytes / rabbitmq_max_available_memory > 0.9
        for: 5m
        annotations:
          summary: "RabbitMQ memory at {{ $value }}%"

      - alert: NoConsumers
        expr: rabbitmq_queue_consumers == 0 and rabbitmq_queue_messages_ready > 0
        for: 10m
        annotations:
          summary: "Queue {{ $labels.queue }} has no consumers"
```

### 2. Grafana Dashboards

Pre-built dashboards available on Grafana Labs:

```
Search Grafana marketplace for "RabbitMQ"
Popular: Dashboard #4369 by Grafana
```

Dashboard shows:
- Queue depths over time
- Consumer lag trends
- Message rates
- Memory/CPU usage
- Connection count

### 3. ELK Stack Integration

Send RabbitMQ logs to Elasticsearch:

```json
// logstash config
input {
  file {
    path => "/var/log/rabbitmq/*.log"
    type => "rabbitmq"
  }
}

filter {
  if [type] == "rabbitmq" {
    grok {
      match => { "message" => "%{TIMESTAMP_ISO8601:timestamp} \[%{DATA:level}\] %{DATA:module}: %{GREEDYDATA:message}" }
    }
  }
}

output {
  elasticsearch {
    hosts => ["localhost:9200"]
    index => "rabbitmq-%{+YYYY.MM.dd}"
  }
}
```

## Health Checks

### Liveness Check

```python
import pika

def health_check_liveness():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                connection_attempts=1,
                retry_delay=0,
                socket_timeout=2
            )
        )
        connection.close()
        return {'status': 'healthy'}
    except Exception as e:
        return {'status': 'unhealthy', 'error': str(e)}
```

### Readiness Check

```python
def health_check_readiness():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        channel = connection.channel()
        
        # Try to declare queue (verifies write access)
        channel.queue_declare(queue='healthcheck', passive=False)
        
        connection.close()
        return {'status': 'ready'}
    except Exception as e:
        return {'status': 'not_ready', 'error': str(e)}
```

### Kubernetes Health Probes

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: rabbitmq
    image: rabbitmq:3.12-management
    livenessProbe:
      exec:
        command:
        - rabbitmq-diagnostics
        - ping
      initialDelaySeconds: 60
      periodSeconds: 30
      
    readinessProbe:
      exec:
        command:
        - rabbitmq-diagnostics
        - is_ready
      initialDelaySeconds: 20
      periodSeconds: 10
```

## Debugging Tools

### 1. Message Tracing

Enable tracing to see message routing:

```bash
# Enable trace logging
rabbitmqctl trace_on

# Trace specific queue
rabbitmqctl list_trace_on -p /
```

### 2. Monitor Plugin

Detailed connection/channel metrics:

```bash
rabbitmq-plugins enable rabbitmq_web_mqtt
rabbitmqctl list_connections
rabbitmqctl list_channels
```

### 3. Log Analysis

Check RabbitMQ logs:

```bash
# Docker logs
docker logs rabbitmq

# System logs
tail -f /var/log/rabbitmq/rabbit@hostname.log
tail -f /var/log/rabbitmq/rabbit@hostname-sasl.log
```

## Best Practices

### Architecture

1. **Decouple Producers/Consumers**
   - Producers don't wait for consumers
   - Consumers can be added/removed dynamically

2. **Use Appropriate Exchange Types**
   - Direct: One-to-one routing
   - Topic: Pattern-based routing
   - Fanout: Broadcasting

3. **Plan Your Queue Strategy**
   - Separate queues by service/purpose
   - Use durable queues for critical data
   - Set appropriate TTL

### Reliability

1. **Always Use Durable + Persistent**
   ```python
   # Durable queue
   channel.queue_declare(queue='important', durable=True)
   
   # Persistent message
   channel.basic_publish(
       body=message,
       properties=pika.BasicProperties(
           delivery_mode=pika.DeliveryMode.Persistent
       )
   )
   ```

2. **Manual Acknowledgment**
   ```python
   channel.basic_consume(
       queue='important_queue',
       on_message_callback=callback,
       auto_ack=False  # Manual ACK required
   )
   ```

3. **Handle Failures**
   - Implement retry logic
   - Use dead letter exchanges
   - Monitor failed messages

### Scalability

1. **Multiple Consumers**
   - Add consumers to handle load
   - Use QoS for fair distribution

2. **Connection Pooling**
   - Don't create connection per message
   - Reuse connections/channels

3. **Message Batching**
   - Batch publish operations
   - Reduces network roundtrips

### Security

1. **Authentication & Authorization**
   ```bash
   rabbitmqctl add_user appuser password123
   rabbitmqctl set_permissions -p /myapp appuser ".*" ".*" ".*"
   ```

2. **Use Virtual Hosts**
   ```bash
   rabbitmqctl add_vhost /production
   rabbitmqctl add_user prod_app prod_pass
   rabbitmqctl set_permissions -p /production prod_app ".*" ".*" ".*"
   ```

3. **Enable SSL/TLS**
   ```python
   import ssl
   context = ssl.create_default_context()
   context.cert_reqs = ssl.CERT_REQUIRED
   
   connection = pika.BlockingConnection(
       pika.ConnectionParameters(
           host='rabbitmq.example.com',
           port=5671,
           ssl_options=pika.SSLOptions(context)
       )
   )
   ```

### Operations

1. **Version Control Definitions**
   - Export queue/exchange definitions
   - Keep in git repository

2. **Backup Configurations**
   ```bash
   rabbitmqctl export_definitions /tmp/definitions.json
   rabbitmqctl import_definitions /tmp/definitions.json
   ```

3. **Monitoring & Alerts**
   - Set up Prometheus + Grafana
   - Alert on queue depth, lag
   - Monitor memory/CPU

4. **Regular Maintenance**
   - Purge old dead letters
   - Clean up unused queues
   - Update RabbitMQ regularly

### Development

1. **Use Local Docker**
   ```bash
   docker run -d --name rabbitmq \
     -p 5672:5672 \
     -p 15672:15672 \
     rabbitmq:3.12-management
   ```

2. **Idempotent Consumer**
   ```python
   # Handle duplicate messages gracefully
   def process_message(message):
       if is_duplicate(message['id']):
           return  # Already processed
       
       do_work(message)
       mark_processed(message['id'])
   ```

3. **Log Correlation IDs**
   ```python
   properties=pika.BasicProperties(
       correlation_id='req-123-abc',
       trace_id='trace-xyz'
   )
   
   # Consumer can use same IDs for logging
   ```

4. **Test Both Paths**
   - Success scenarios
   - Failure/retry scenarios
   - Dead letter queue handling

## Common Pitfalls

### ❌ Don't Do This

1. **Creating connection per message**
   ```python
   # BAD
   for msg in messages:
       conn = pika.BlockingConnection(...)
       conn.channel().basic_publish(...)
       conn.close()
   ```

2. **Using auto_ack for critical data**
   ```python
   # BAD - Messages can be lost
   channel.basic_consume(queue='important', auto_ack=True)
   ```

3. **Requeuing indefinitely**
   ```python
   # BAD - Causes infinite loops
   ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
   ```

4. **No error handling**
   ```python
   # BAD - Crashes silently
   def callback(ch, method, properties, body):
       process_message(body)  # No try/except
       ch.basic_ack(...)
   ```

5. **Ignoring dead letter queue**
   ```python
   # BAD - Messages accumulate, no insight
   # Good - Process and alert on DLQ messages
   ```

### ✓ Do This Instead

1. **Connection pooling**
2. **Manual acknowledgment for critical data**
3. **Max retry limit with DLX**
4. **Comprehensive error handling**
5. **Monitor and process dead letters**

## Troubleshooting Checklist

1. **Can't connect?**
   - Check host/port
   - Verify firewall rules
   - Check credentials

2. **Messages not being consumed?**
   - Check consumer is running
   - Verify queue/exchange bindings
   - Check consumer ACK status

3. **Queue growing unbounded?**
   - Consumer is too slow
   - Add more consumers
   - Check for processing errors

4. **High memory usage?**
   - Too many messages in memory
   - Use lazy queue mode
   - Increase consumers

5. **Connection drops?**
   - Network issues
   - Broker restart
   - Resource exhaustion

## Summary of Key Points

| Aspect | Best Practice |
|--------|----------------|
| Queue | Always durable for critical data |
| Message | Use persistent delivery mode |
| Acknowledgment | Manual for important, auto for non-critical |
| Error Handling | Implement retry + DLX strategy |
| Monitoring | Prometheus + Grafana |
| Scaling | Multiple consumers, batching |
| Security | vhosts + restricted users |
| Reliability | Cluster with replication |

