# scripts/kafka_streams_processor.py
import json
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaStreamsProcessor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        
        # Data structures for real-time computations
        self.hourly_metrics = {}  # {sensor_id: {hour: {volume_sum, count}}}
        self.daily_peak_volume = {}  # {date: peak_volume}
        self.sensor_availability = {}  # {sensor_id: {date: {total_windows, received_windows}}}
        
        # Initialize topics
        self.setup_topics()
    
    def setup_topics(self):
        """Create required Kafka topics if they don't exist"""
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        
        topics = [
            NewTopic("iot-processed-data", num_partitions=3, replication_factor=1),
            NewTopic("traffic-alerts", num_partitions=1, replication_factor=1)
        ]
        
        try:
            fs = admin_client.create_topics(topics)
            for topic, f in fs.items():
                f.result()  # Wait for operation to complete
                logger.info(f"Topic {topic} created successfully")
        except Exception as e:
            logger.info(f"Topics may already exist: {e}")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def process_traffic_data(self, message_value):
        """Process incoming traffic data and compute real-time metrics"""
        try:
            sensor_id = message_value.get('atd_device_id')
            intersection = message_value.get('intersection_name')
            volume = message_value.get('volume', 0)
            timestamp_str = message_value.get('read_date')
            hour = message_value.get('hour', 0)
            day = message_value.get('day', 0)
            month = message_value.get('month', 0)
            year = message_value.get('year', 0)
            
            if not all([sensor_id, timestamp_str]):
                return
            
            # Parse timestamp
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                date_key = f"{year}-{month:02d}-{day:02d}"
                hour_key = f"{date_key}-{hour:02d}"
            except:
                timestamp = datetime.now()
                date_key = timestamp.strftime("%Y-%m-%d")
                hour_key = f"{date_key}-{hour:02d}"
            
            # Update hourly averages
            self.update_hourly_metrics(sensor_id, intersection, hour_key, volume, timestamp)
            
            # Update daily peak volume
            self.update_daily_peak_volume(date_key, volume, intersection)
            
            # Update sensor availability
            self.update_sensor_availability(sensor_id, date_key, hour)
            
            # Generate alerts for unusual patterns
            self.check_for_alerts(sensor_id, intersection, volume, timestamp)
            
        except Exception as e:
            logger.error(f"Error processing traffic data: {e}")
    
    def update_hourly_metrics(self, sensor_id, intersection, hour_key, volume, timestamp):
        """Compute hourly average vehicle count per sensor"""
        if sensor_id not in self.hourly_metrics:
            self.hourly_metrics[sensor_id] = {}
        
        if hour_key not in self.hourly_metrics[sensor_id]:
            self.hourly_metrics[sensor_id][hour_key] = {
                'volume_sum': 0,
                'count': 0,
                'intersection': intersection,
                'last_updated': timestamp
            }
        
        # Update metrics
        self.hourly_metrics[sensor_id][hour_key]['volume_sum'] += volume
        self.hourly_metrics[sensor_id][hour_key]['count'] += 1
        self.hourly_metrics[sensor_id][hour_key]['last_updated'] = timestamp
        
        # Calculate average
        count = self.hourly_metrics[sensor_id][hour_key]['count']
        if count > 0:
            avg_volume = self.hourly_metrics[sensor_id][hour_key]['volume_sum'] / count
            
            # Emit hourly average to processed data topic
            hourly_avg_message = {
                'sensor_id': sensor_id,
                'intersection_name': intersection,
                'hour_key': hour_key,
                'hourly_avg_volume': round(avg_volume, 2),
                'total_volume': self.hourly_metrics[sensor_id][hour_key]['volume_sum'],
                'record_count': count,
                'computation_timestamp': datetime.now().isoformat(),
                'metric_type': 'hourly_average'
            }
            
            self.producer.produce(
                'iot-processed-data',
                key=sensor_id.encode('utf-8'),
                value=json.dumps(hourly_avg_message),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            
            logger.info(f"Hourly average for {intersection} ({hour_key}): {avg_volume:.2f} vehicles")
    
    def update_daily_peak_volume(self, date_key, volume, intersection):
        """Track daily peak traffic volume across all sensors"""
        if date_key not in self.daily_peak_volume:
            self.daily_peak_volume[date_key] = {
                'peak_volume': 0,
                'peak_intersection': '',
                'last_updated': datetime.now()
            }
        
        if volume > self.daily_peak_volume[date_key]['peak_volume']:
            self.daily_peak_volume[date_key]['peak_volume'] = volume
            self.daily_peak_volume[date_key]['peak_intersection'] = intersection
            self.daily_peak_volume[date_key]['last_updated'] = datetime.now()
            
            # Emit peak volume update
            peak_message = {
                'date': date_key,
                'peak_volume': volume,
                'peak_intersection': intersection,
                'computation_timestamp': datetime.now().isoformat(),
                'metric_type': 'daily_peak'
            }
            
            self.producer.produce(
                'iot-processed-data',
                key=date_key.encode('utf-8'),
                value=json.dumps(peak_message),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            
            logger.info(f"New daily peak for {date_key}: {volume} vehicles at {intersection}")
    
    def update_sensor_availability(self, sensor_id, date_key, hour):
        """Calculate daily sensor availability based on data presence"""
        if sensor_id not in self.sensor_availability:
            self.sensor_availability[sensor_id] = {}
        
        if date_key not in self.sensor_availability[sensor_id]:
            self.sensor_availability[sensor_id][date_key] = {
                'total_windows': 96,  # 96 * 15-minute windows in a day
                'received_windows': set(),
                'last_updated': datetime.now()
            }
        
        # Track this time window as received
        window_key = hour  # Using hour as window identifier
        self.sensor_availability[sensor_id][date_key]['received_windows'].add(window_key)
        
        # Calculate availability percentage
        received_count = len(self.sensor_availability[sensor_id][date_key]['received_windows'])
        total_windows = self.sensor_availability[sensor_id][date_key]['total_windows']
        availability_pct = (received_count / total_windows) * 100
        
        # Emit availability update (every 10 windows to reduce noise)
        if received_count % 10 == 0:
            availability_message = {
                'sensor_id': sensor_id,
                'date': date_key,
                'availability_percentage': round(availability_pct, 2),
                'received_windows': received_count,
                'total_windows': total_windows,
                'computation_timestamp': datetime.now().isoformat(),
                'metric_type': 'sensor_availability'
            }
            
            self.producer.produce(
                'iot-processed-data',
                key=sensor_id.encode('utf-8'),
                value=json.dumps(availability_message),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            
            logger.info(f"Availability for {sensor_id} on {date_key}: {availability_pct:.1f}%")
    
    def check_for_alerts(self, sensor_id, intersection, volume, timestamp):
        """Generate alerts for unusual traffic patterns"""
        alerts = []
        
        # High volume alert
        if volume > 100:  # Threshold for high volume
            alerts.append({
                'type': 'HIGH_VOLUME',
                'sensor_id': sensor_id,
                'intersection': intersection,
                'volume': volume,
                'threshold': 100,
                'timestamp': timestamp.isoformat()
            })
        
        # Sudden drop alert (if we have historical data)
        if sensor_id in self.hourly_metrics:
            recent_avg = self.get_recent_average(sensor_id)
            if recent_avg and volume < (recent_avg * 0.3):  # 70% drop
                alerts.append({
                    'type': 'VOLUME_DROP',
                    'sensor_id': sensor_id,
                    'intersection': intersection,
                    'volume': volume,
                    'recent_average': recent_avg,
                    'timestamp': timestamp.isoformat()
                })
        
        # Send alerts
        for alert in alerts:
            self.producer.produce(
                'traffic-alerts',
                key=sensor_id.encode('utf-8'),
                value=json.dumps(alert),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            
            logger.warning(f"ALERT: {alert['type']} at {intersection} - Volume: {volume}")
    
    def get_recent_average(self, sensor_id):
        """Get recent average volume for a sensor"""
        if sensor_id not in self.hourly_metrics:
            return None
        
        # Get averages from last 3 hours
        recent_hours = list(self.hourly_metrics[sensor_id].keys())[-3:]
        if not recent_hours:
            return None
        
        total_volume = 0
        total_count = 0
        
        for hour in recent_hours:
            metrics = self.hourly_metrics[sensor_id][hour]
            if metrics['count'] > 0:
                total_volume += metrics['volume_sum'] / metrics['count']
                total_count += 1
        
        return total_volume / total_count if total_count > 0 else None
    
    def cleanup_old_data(self):
        """Clean up old data to prevent memory leaks"""
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(days=2)  # Keep 2 days of data
        
        # Clean hourly metrics
        for sensor_id in list(self.hourly_metrics.keys()):
            for hour_key in list(self.hourly_metrics[sensor_id].keys()):
                last_updated = self.hourly_metrics[sensor_id][hour_key]['last_updated']
                if last_updated < cutoff_time:
                    del self.hourly_metrics[sensor_id][hour_key]
            
            # Remove empty sensor entries
            if not self.hourly_metrics[sensor_id]:
                del self.hourly_metrics[sensor_id]
        
        logger.info("Cleaned up old metrics data")
    
    def start_cleanup_task(self):
        """Start background task for cleaning old data"""
        def cleanup_loop():
            while True:
                time.sleep(3600)  # Run every hour
                self.cleanup_old_data()
        
        cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        cleanup_thread.start()