"""
Kafka Producer for Real-Time Tiling Demo

This script produces customer events to Kafka in real-time.
"""

import json
import time
from datetime import datetime, timezone
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def wait_for_kafka(bootstrap_servers='localhost:9092', max_retries=30):
    """Wait for Kafka to be ready"""
    print("🔄 Waiting for Kafka to be ready...")
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            print("✅ Kafka is ready!")
            return True
        except NoBrokersAvailable:
            if i < max_retries - 1:
                print(f"   Attempt {i+1}/{max_retries}: Kafka not ready, waiting...")
                time.sleep(2)
            else:
                print("❌ Kafka is not available after maximum retries")
                return False
    return False

def create_producer(bootstrap_servers='localhost:9092'):
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def generate_customer_event():
    """Generate a realistic customer event"""
    customer_id = f"customer_{random.randint(1, 100)}"
    
    # Generate realistic values
    # 60% chance of purchase with amounts between $10-$500
    if random.random() < 0.6:
        purchase_amount = round(random.uniform(10, 500), 2)
    else:
        purchase_amount = 0.0
    
    page_views = random.randint(1, 20)
    session_duration = round(random.expovariate(1/180), 2)  # Avg 3 minutes
    
    event = {
        "customer_id": customer_id,
        "purchase_amount": purchase_amount,
        "page_views": page_views,
        "session_duration": session_duration,
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    return event

def produce_events(topic='customer_events', num_events=100, delay=0.5):
    """
    Produce customer events to Kafka
    
    Args:
        topic: Kafka topic name
        num_events: Number of events to produce (0 for infinite)
        delay: Delay between events in seconds
    """
    if not wait_for_kafka():
        return
    
    producer = create_producer()
    
    print(f"\n🚀 Producing events to topic '{topic}'")
    print(f"📊 Events: {num_events if num_events > 0 else 'infinite'}")
    print(f"⏱️  Delay: {delay}s between events")
    print("\nPress Ctrl+C to stop\n")
    
    event_count = 0
    try:
        while num_events == 0 or event_count < num_events:
            # Generate event
            event = generate_customer_event()
            
            # Send to Kafka
            future = producer.send(topic, value=event)
            
            # Wait for confirmation
            try:
                record_metadata = future.get(timeout=10)
                event_count += 1
                
                # Print every 10 events
                if event_count % 10 == 0:
                    print(f"✅ Produced {event_count} events")
                    print(f"   Latest: customer={event['customer_id']}, "
                          f"purchase=${event['purchase_amount']:.2f}, "
                          f"views={event['page_views']}")
                
            except Exception as e:
                print(f"❌ Error sending event: {e}")
            
            # Delay before next event
            time.sleep(delay)
            
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Stopped. Produced {event_count} events total.")
    
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer closed")

def produce_batch_events(topic='customer_events', batch_size=1000):
    """Produce a batch of events quickly for testing"""
    if not wait_for_kafka():
        return
    
    producer = create_producer()
    
    print(f"\n🚀 Producing batch of {batch_size} events to '{topic}'")
    
    start_time = time.time()
    for i in range(batch_size):
        event = generate_customer_event()
        producer.send(topic, value=event)
        
        if (i + 1) % 100 == 0:
            print(f"✅ Produced {i + 1}/{batch_size} events")
    
    producer.flush()
    producer.close()
    
    elapsed = time.time() - start_time
    print(f"\n✅ Produced {batch_size} events in {elapsed:.2f}s")
    print(f"   Rate: {batch_size / elapsed:.0f} events/sec")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "batch":
            batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
            produce_batch_events(batch_size=batch_size)
        elif sys.argv[1] == "continuous":
            delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
            produce_events(num_events=0, delay=delay)
        else:
            num_events = int(sys.argv[1])
            delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
            produce_events(num_events=num_events, delay=delay)
    else:
        # Default: produce 100 events
        produce_events(num_events=100, delay=0.5)

