# scripts/kafka_traffic_consumer_lst.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrafficDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', group_id='traffic-consumer-group'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })
        self.message_count = 0
        self.total_volume = 0
        self.total_speed = 0.0
    
    def consume_traffic_data(self):
        """Consume and process traffic volume data from Kafka"""
        self.consumer.subscribe(['traffic_volume_data'])
        
        logger.info("Starting to consume traffic volume data...")
        logger.info("Press Ctrl+C to stop the consumer")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                # Parse the message
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    self.message_count += 1
                    
                    self.process_message(msg, message_value)
                    
                    # Print summary every 10 messages
                    if self.message_count % 10 == 0:
                        self.print_summary()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.print_final_summary()
        finally:
            self.close()
    
    def process_message(self, msg, message_value):
        """Process and log individual traffic volume message"""
        # Extract key
        key = msg.key().decode('utf-8') if msg.key() else 'no-key'
        
        # Update statistics
        volume = message_value.get('volume', 0)
        speed = message_value.get('speed_average', 0)
        self.total_volume += volume
        self.total_speed += speed
        
        # Log the message details
        logger.info("=" * 70)
        logger.info(f"Message #{self.message_count}")
        logger.info(f"Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")
        logger.info(f"Key: {key}")
        
        # Display intersection information
        logger.info("-" * 40)
        logger.info("INTERSECTION INFORMATION:")
        logger.info(f"  Intersection: {message_value.get('intersection_name', 'N/A')}")
        logger.info(f"  Direction: {message_value.get('direction', 'N/A')}")
        logger.info(f"  Movement: {message_value.get('movement', 'N/A')}")
        logger.info(f"  Device ID: {message_value.get('atd_device_id', 'N/A')}")
        logger.info(f"  Record ID: {message_value.get('record_id', 'N/A')}")
        
        # Display traffic measurements
        logger.info("-" * 40)
        logger.info("TRAFFIC MEASUREMENTS:")
        logger.info(f"  Vehicle Volume: {volume} vehicles")
        logger.info(f"  Average Speed: {speed:.2f} mph")
        logger.info(f"  Speed Std Dev: {message_value.get('speed_stddev', 0):.2f} mph")
        
        # Display time in zone (occupancy proxy)
        seconds_avg = message_value.get('seconds_in_zone_average', 0)
        seconds_std = message_value.get('seconds_in_zone_stddev', 0)
        logger.info(f"  Time in Zone: {seconds_avg:.2f} ± {seconds_std:.2f} seconds")
        
        # Display heavy vehicle information
        heavy_vehicle = message_value.get('heavy_vehicle', False)
        logger.info(f"  Heavy Vehicles: {'Yes' if heavy_vehicle else 'No'}")
        
        # Display time information
        logger.info("-" * 40)
        logger.info("TIME INFORMATION:")
        read_date = message_value.get('read_date', 'N/A')
        logger.info(f"  Measurement Time: {self.format_timestamp(read_date)}")
        
        kafka_ts = message_value.get('kafka_timestamp')
        if kafka_ts:
            logger.info(f"  Kafka Time: {self.format_timestamp(kafka_ts)}")
        
        # Display bin information
        logger.info(f"  Time Bin: {message_value.get('hour', 0):02d}:{message_value.get('minute', 0):02d}")
        logger.info(f"  Bin Duration: {message_value.get('bin_duration', 0)} seconds")
        logger.info(f"  Date: {message_value.get('month', 0)}/{message_value.get('day', 0)}/{message_value.get('year', 0)}")
        logger.info(f"  Day of Week: {self.get_day_name(message_value.get('day_of_week', 0))}")
        
        logger.info("=" * 70)
    
    def format_timestamp(self, timestamp_str):
        """Format timestamp for better readability"""
        try:
            if 'T' in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return timestamp_str
        except:
            return timestamp_str
    
    def get_day_name(self, day_of_week):
        """Convert day of week number to name"""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return days[day_of_week] if 0 <= day_of_week < len(days) else f'Day {day_of_week}'
    
    def print_summary(self):
        """Print periodic summary statistics"""
        if self.message_count > 0:
            avg_speed = self.total_speed / self.message_count
            logger.info("\n" + ">" * 50)
            logger.info(f"INTERIM SUMMARY (Last {self.message_count} messages):")
            logger.info(f"Total Vehicles: {self.total_volume}")
            logger.info(f"Average Speed: {avg_speed:.2f} mph")
            logger.info(f"Messages Processed: {self.message_count}")
            logger.info(">" * 50 + "\n")
    
    def print_final_summary(self):
        """Print final summary when consumer stops"""
        if self.message_count > 0:
            avg_speed = self.total_speed / self.message_count
            logger.info("\n" + "=" * 60)
            logger.info("FINAL CONSUMER SUMMARY:")
            logger.info(f"Total Messages Processed: {self.message_count}")
            logger.info(f"Total Vehicle Volume: {self.total_volume}")
            logger.info(f"Overall Average Speed: {avg_speed:.2f} mph")
            logger.info(f"Average Volume per Message: {self.total_volume / self.message_count:.1f} vehicles")
            logger.info("=" * 60)
    
    def close(self):
        """Close the consumer connection"""
        self.consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    consumer = TrafficDataConsumer()
    consumer.consume_traffic_data()