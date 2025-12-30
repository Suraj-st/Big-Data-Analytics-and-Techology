# scripts/processed_data_consumer.py
from confluent_kafka import Consumer, KafkaError
import json
import logging
import pandas as pd
from datetime import datetime
import sqlite3
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessedDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'processed-data-consumer',
            'auto.offset.reset': 'earliest'
        })
        
        # Initialize database
        self.setup_database()
    
    def setup_database(self):
        """Setup SQLite database for persistence"""
        os.makedirs('data', exist_ok=True)
        self.db_path = 'data/traffic_analytics.db'
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hourly_averages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT,
                intersection_name TEXT,
                hour_key TEXT,
                hourly_avg_volume REAL,
                total_volume INTEGER,
                record_count INTEGER,
                computation_timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_peaks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                peak_volume INTEGER,
                peak_intersection TEXT,
                computation_timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_availability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT,
                date TEXT,
                availability_percentage REAL,
                received_windows INTEGER,
                total_windows INTEGER,
                computation_timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS traffic_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT,
                sensor_id TEXT,
                intersection TEXT,
                volume INTEGER,
                threshold REAL,
                recent_average REAL,
                timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database setup completed")
    
    def consume_processed_data(self):
        """Consume processed data and persist to database"""
        self.consumer.subscribe(['iot-processed-data', 'traffic-alerts'])
        
        logger.info("Starting processed data consumer...")
        logger.info("Persisting analytics to database")
        
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
                
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    metric_type = message_value.get('metric_type', 'alert')
                    
                    # Persist based on metric type
                    if msg.topic() == 'iot-processed-data':
                        self.persist_analytics_data(message_value, metric_type)
                    elif msg.topic() == 'traffic-alerts':
                        self.persist_alert_data(message_value)
                    
                    # Log the processed data
                    self.log_processed_message(message_value, metric_type, msg.topic())
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping processed data consumer...")
        finally:
            self.consumer.close()
    
    def persist_analytics_data(self, data, metric_type):
        """Persist analytics data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            if metric_type == 'hourly_average':
                cursor.execute('''
                    INSERT INTO hourly_averages 
                    (sensor_id, intersection_name, hour_key, hourly_avg_volume, 
                     total_volume, record_count, computation_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['sensor_id'],
                    data['intersection_name'],
                    data['hour_key'],
                    data['hourly_avg_volume'],
                    data['total_volume'],
                    data['record_count'],
                    data['computation_timestamp']
                ))
                
            elif metric_type == 'daily_peak':
                cursor.execute('''
                    INSERT INTO daily_peaks 
                    (date, peak_volume, peak_intersection, computation_timestamp)
                    VALUES (?, ?, ?, ?)
                ''', (
                    data['date'],
                    data['peak_volume'],
                    data['peak_intersection'],
                    data['computation_timestamp']
                ))
                
            elif metric_type == 'sensor_availability':
                cursor.execute('''
                    INSERT INTO sensor_availability 
                    (sensor_id, date, availability_percentage, received_windows, 
                     total_windows, computation_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    data['sensor_id'],
                    data['date'],
                    data['availability_percentage'],
                    data['received_windows'],
                    data['total_windows'],
                    data['computation_timestamp']
                ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error persisting data to database: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def persist_alert_data(self, alert_data):
        """Persist alert data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO traffic_alerts 
                (alert_type, sensor_id, intersection, volume, threshold, 
                 recent_average, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                alert_data['type'],
                alert_data['sensor_id'],
                alert_data.get('intersection', ''),
                alert_data['volume'],
                alert_data.get('threshold'),
                alert_data.get('recent_average'),
                alert_data['timestamp']
            ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error persisting alert to database: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def log_processed_message(self, data, metric_type, topic):
        """Log processed messages in a readable format"""
        if metric_type == 'hourly_average':
            logger.info(f"📊 HOURLY AVG: {data['intersection_name']} - "
                       f"Avg: {data['hourly_avg_volume']} vehicles")
        
        elif metric_type == 'daily_peak':
            logger.info(f"🏆 DAILY PEAK: {data['date']} - "
                       f"{data['peak_volume']} vehicles at {data['peak_intersection']}")
        
        elif metric_type == 'sensor_availability':
            logger.info(f"📡 AVAILABILITY: {data['sensor_id']} - "
                       f"{data['availability_percentage']}% ({data['received_windows']}/{data['total_windows']})")
        
        elif topic == 'traffic-alerts':
            logger.warning(f"🚨 ALERT: {data['type']} - "
                          f"{data.get('intersection', data['sensor_id'])} - "
                          f"Volume: {data['volume']}")

if __name__ == "__main__":
    processed_consumer = ProcessedDataConsumer()
    processed_consumer.consume_processed_data()