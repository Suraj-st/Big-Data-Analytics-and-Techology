# scripts/kafka_persistent_consumer.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersistentTrafficConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'traffic-persistent-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })
        self.consumed_data = []
        self.output_file = 'data/consumed_traffic_volume_data.csv'
        
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
    
    def consume_and_save(self):
        """Consume traffic volume data and periodically save to file"""
        self.consumer.subscribe(['traffic_volume_data'])
        
        logger.info("Starting persistent traffic volume consumer...")
        logger.info(f"Data will be saved to: {self.output_file}")
        
        message_count = 0
        total_volume = 0
        
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
                
                # Process message
                message_value = json.loads(msg.value().decode('utf-8'))
                message_count += 1
                
                # Add consumption metadata
                message_value['kafka_consumed_timestamp'] = datetime.now().isoformat()
                message_value['kafka_topic'] = msg.topic()
                message_value['kafka_partition'] = msg.partition()
                message_value['kafka_offset'] = msg.offset()
                
                self.consumed_data.append(message_value)
                
                # Update statistics
                volume = message_value.get('volume', 0)
                total_volume += volume
                
                # Log message with actual traffic data
                logger.info(f"Consumed #{message_count}: "
                           f"{message_value.get('intersection_name')} - "
                           f"{message_value.get('direction')} - "
                           f"Volume: {volume} - "
                           f"Speed: {message_value.get('speed_average', 0):.1f} mph")
                
                # Save to file every 20 messages and print statistics
                if len(self.consumed_data) % 20 == 0:
                    self.save_to_csv()
                    avg_volume = total_volume / message_count
                    logger.info(f"📊 Statistics: {message_count} messages, {total_volume} total vehicles, {avg_volume:.1f} avg/msg")
                    
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            # Save remaining data
            if self.consumed_data:
                self.save_to_csv()
            
            # Print final statistics
            if message_count > 0:
                avg_volume = total_volume / message_count
                logger.info(f"🎯 FINAL: {message_count} messages, {total_volume} total vehicles, {avg_volume:.1f} avg/msg")
        finally:
            self.consumer.close()
    
    def save_to_csv(self):
        """Save consumed data to CSV file with proper formatting"""
        if not self.consumed_data:
            logger.warning("No data to save")
            return
        
        try:
            df = pd.DataFrame(self.consumed_data)
            
            # Ensure proper data types for numeric columns
            numeric_columns = ['volume', 'speed_average', 'speed_stddev', 
                             'seconds_in_zone_average', 'seconds_in_zone_stddev',
                             'month', 'day', 'year', 'hour', 'minute', 'day_of_week', 'bin_duration']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert boolean column
            if 'heavy_vehicle' in df.columns:
                df['heavy_vehicle'] = df['heavy_vehicle'].astype(bool)
            
            # Reorder columns for better readability (put key metrics first)
            preferred_order = [
                'record_id', 'atd_device_id', 'intersection_name', 'direction', 'movement',
                'volume', 'speed_average', 'speed_stddev', 'seconds_in_zone_average',
                'heavy_vehicle', 'read_date', 'year', 'month', 'day', 'hour', 'minute',
                'day_of_week', 'bin_duration', 'data_type', 'kafka_timestamp',
                'kafka_consumed_timestamp', 'kafka_topic', 'kafka_partition', 'kafka_offset'
            ]
            
            # Get existing columns in preferred order, then remaining columns
            existing_columns = [col for col in preferred_order if col in df.columns]
            remaining_columns = [col for col in df.columns if col not in existing_columns]
            final_columns = existing_columns + remaining_columns
            
            df = df[final_columns]
            
            # Save to CSV
            df.to_csv(self.output_file, index=False)
            logger.info(f"💾 Saved {len(df)} traffic volume records to {self.output_file}")
            
            # Print file summary
            self.print_file_summary(df)
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")
    
    def print_file_summary(self, df):
        """Print summary statistics of the saved data"""
        try:
            total_volume = df['volume'].sum() if 'volume' in df.columns else 0
            avg_speed = df['speed_average'].mean() if 'speed_average' in df.columns else 0
            unique_intersections = df['intersection_name'].nunique() if 'intersection_name' in df.columns else 0
            time_range = ""
            
            if 'read_date' in df.columns and not df['read_date'].isnull().all():
                df['read_date_clean'] = pd.to_datetime(df['read_date'], errors='coerce')
                min_time = df['read_date_clean'].min()
                max_time = df['read_date_clean'].max()
                if pd.notna(min_time) and pd.notna(max_time):
                    time_range = f"{min_time.strftime('%Y-%m-%d %H:%M')} to {max_time.strftime('%Y-%m-%d %H:%M')}"
            
            logger.info("📈 File Summary:")
            logger.info(f"   Total Vehicle Volume: {total_volume}")
            logger.info(f"   Average Speed: {avg_speed:.2f} mph")
            logger.info(f"   Unique Intersections: {unique_intersections}")
            logger.info(f"   Time Range: {time_range}")
            logger.info(f"   Data Points: {len(df)}")
            
        except Exception as e:
            logger.error(f"Error generating file summary: {e}")
    
    def analyze_saved_data(self):
        """Analyze the previously saved data"""
        if not os.path.exists(self.output_file):
            logger.warning(f"No data file found at {self.output_file}")
            return
        
        try:
            df = pd.read_csv(self.output_file)
            logger.info(f"📊 Analysis of saved data ({len(df)} records):")
            
            # Basic statistics
            if 'volume' in df.columns:
                logger.info(f"   Volume Statistics:")
                logger.info(f"     Total: {df['volume'].sum()} vehicles")
                logger.info(f"     Average: {df['volume'].mean():.1f} vehicles/record")
                logger.info(f"     Maximum: {df['volume'].max()} vehicles")
                logger.info(f"     Minimum: {df['volume'].min()} vehicles")
            
            if 'speed_average' in df.columns:
                logger.info(f"   Speed Statistics:")
                logger.info(f"     Average: {df['speed_average'].mean():.1f} mph")
                logger.info(f"     Maximum: {df['speed_average'].max():.1f} mph")
                logger.info(f"     Minimum: {df['speed_average'].min():.1f} mph")
            
            if 'intersection_name' in df.columns:
                top_intersections = df['intersection_name'].value_counts().head(5)
                logger.info(f"   Top 5 Intersections:")
                for intersection, count in top_intersections.items():
                    logger.info(f"     {intersection}: {count} records")
            
            if 'direction' in df.columns:
                direction_counts = df['direction'].value_counts()
                logger.info(f"   Direction Distribution:")
                for direction, count in direction_counts.items():
                    logger.info(f"     {direction}: {count} records")
                    
        except Exception as e:
            logger.error(f"Error analyzing saved data: {e}")

if __name__ == "__main__":
    consumer = PersistentTrafficConsumer()
    
    # Check if user wants to analyze existing data or consume new data
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "analyze":
        consumer.analyze_saved_data()
    else:
        consumer.consume_and_save()