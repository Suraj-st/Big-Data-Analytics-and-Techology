# scripts/kafka_traffic_producer_lst.py
import json
import time
from datetime import datetime
from confluent_kafka import Producer
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'iot-traffic-producer'
        })
        self.topic = 'traffic_volume_data'
    
    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def load_traffic_data(self):
        """Load traffic volume data from JSONL file"""
        try:
            messages = []
            with open('data/kafka_traffic_volume_messages.jsonl', 'r') as f:
                for line in f:
                    if line.strip():
                        messages.append(json.loads(line.strip()))
            
            logger.info(f"Loaded {len(messages)} real traffic volume records")
            return messages
        except FileNotFoundError:
            logger.error("Kafka traffic volume messages file not found. Run download script first.")
            return []
    
    def enhance_with_current_timestamp(self, message):
        """Update the message with current timestamp for real-time streaming"""
        enhanced_message = message.copy()
        enhanced_message['kafka_timestamp'] = datetime.now().isoformat()
        return enhanced_message
    
    def simulate_real_time_streaming(self, interval=5):
        """Simulate real-time streaming of actual traffic volume data"""
        messages = self.load_traffic_data()
        
        if not messages:
            logger.error("No traffic volume messages to stream")
            return
        
        logger.info(f"Starting real-time simulation with {len(messages)} actual traffic records")
        logger.info(f"Streaming interval: {interval} seconds")
        
        sequence_id = 0
        
        try:
            while True:  # Continuous streaming
                for message in messages:
                    sequence_id += 1
                    
                    # Enhance message with current timestamp
                    current_message = self.enhance_with_current_timestamp(message)
                    current_message['sequence_id'] = sequence_id
                    
                    # Use intersection_name + direction as key for consistent partitioning
                    key = f"{current_message.get('intersection_name', 'unknown')}_{current_message.get('direction', 'unknown')}"
                    
                    try:
                        # Send to Kafka
                        self.producer.produce(
                            topic=self.topic,
                            key=key,
                            value=json.dumps(current_message),
                            callback=self.delivery_report
                        )
                        
                        # Trigger any available delivery report callbacks
                        self.producer.poll(0)
                        
                        # Log the actual traffic data
                        logger.info(f"Message {sequence_id}: "
                                   f"{current_message['intersection_name']} - "
                                   f"{current_message['direction']} {current_message['movement']} - "
                                   f"Volume: {current_message['volume']} vehicles - "
                                   f"Speed: {current_message['speed_average']} mph - "
                                   f"Occupancy: {current_message.get('seconds_in_zone_average', 0):.1f}s")
                        
                    except Exception as e:
                        logger.error(f"Failed to send message {sequence_id}: {e}")
                    
                    # Wait before sending next message
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        finally:
            self.producer.flush()
    
    def stream_historical_data_sequentially(self, interval=2):
        """Stream historical data in sequence (one-time streaming)"""
        messages = self.load_traffic_data()
        
        if not messages:
            logger.error("No traffic volume messages to stream")
            return
        
        logger.info(f"Starting sequential streaming of {len(messages)} historical traffic records")
        logger.info(f"Streaming interval: {interval} seconds")
        
        # Sort messages by read_date to maintain temporal order
        messages_sorted = sorted(messages, key=lambda x: x.get('read_date', ''))
        
        for sequence_id, message in enumerate(messages_sorted, 1):
            # Enhance message with current timestamp
            current_message = self.enhance_with_current_timestamp(message)
            current_message['sequence_id'] = sequence_id
            
            # Use intersection_name + direction as key for consistent partitioning
            key = f"{current_message.get('intersection_name', 'unknown')}_{current_message.get('direction', 'unknown')}"
            
            try:
                # Send to Kafka
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=json.dumps(current_message),
                    callback=self.delivery_report
                )
                
                # Trigger any available delivery report callbacks
                self.producer.poll(0)
                
                # Log the actual traffic data
                logger.info(f"Message {sequence_id}/{len(messages_sorted)}: "
                           f"{current_message['intersection_name']} - "
                           f"{current_message['direction']} {current_message['movement']} - "
                           f"Volume: {current_message['volume']} vehicles - "
                           f"Speed: {current_message['speed_average']} mph - "
                           f"Time: {current_message.get('read_date', 'N/A')}")
                
            except Exception as e:
                logger.error(f"Failed to send message {sequence_id}: {e}")
            
            # Wait before sending next message
            time.sleep(interval)
        
        # Wait for any outstanding messages
        self.producer.flush()
        logger.info("Finished streaming all historical traffic records")

    def close(self):
        """Close the producer connection"""
        self.producer.flush()

if __name__ == "__main__":
    producer = TrafficDataProducer()
    
    try:
        print("Choose streaming mode:")
        print("1. Continuous real-time simulation")
        print("2. One-time historical data streaming")
        
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "2":
            # Stream historical data once
            producer.stream_historical_data_sequentially(interval=2)
        else:
            # Continuous real-time simulation (default)
            producer.simulate_real_time_streaming(interval=5)
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.close()