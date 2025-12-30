# scripts/kafka_streams_consumer.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime
from kafka_streams_processor import KafkaStreamsProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamsDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.processor = KafkaStreamsProcessor(bootstrap_servers)
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'streams-processor-group',
            'auto.offset.reset': 'earliest'
        })
        
        # Start background cleanup task
        self.processor.start_cleanup_task()
    
    def process_stream(self):
        """Main method to process the stream of traffic data"""
        self.consumer.subscribe(['traffic_volume_data'])
        
        logger.info("Starting Kafka Streams processing...")
        logger.info("Computing: Hourly averages, Daily peaks, Sensor availability")
        
        message_count = 0
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                message_count += 1
                
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    
                    # Process the message through our streams processor
                    self.processor.process_traffic_data(message_value)
                    
                    # Log progress
                    if message_count % 50 == 0:
                        logger.info(f"Processed {message_count} messages")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping streams processor...")
        finally:
            self.consumer.close()
            logger.info("Streams processor stopped")

if __name__ == "__main__":
    streams_consumer = StreamsDataConsumer()
    streams_consumer.process_stream()