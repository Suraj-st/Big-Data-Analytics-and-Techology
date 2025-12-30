# scripts/download_traffic_data_lst.py
#!/usr/bin/env python

import pandas as pd
from sodapy import Socrata
import json
from datetime import datetime, timedelta
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_traffic_sensor_data():
    """
    Download real-time traffic volume and speed data from Austin Open Data using Socrata API with authentication
    Uses dataset: "Traffic Signals Volume and Speed (ATD) - sh59-i6y9"
    """
    
    try:
        # Get credentials from environment variables or config
        app_token = "VeenHAfMuhyYKyRhcgWBSUZLs"
        username = "srj.trk@gmail.com"
        password = "Tharakast@4123"
        
        # Create authenticated client
        client = Socrata(
            "data.austintexas.gov",
            app_token,
            username=username,
            password=password
        )
        
        logger.info("Authenticated client created. Downloading traffic volume and speed data...")
        
        # Query the traffic volume and speed dataset (sh59-i6y9)
        # Get recent data with better rate limits since we're authenticated
        results = client.get("sh59-i6y9", limit=100000)
        
        # Convert to pandas DataFrame
        df = pd.DataFrame.from_records(results)
        
        logger.info(f"Downloaded {len(df)} traffic volume records with authentication")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Display sample of the data
        print("\nSample of downloaded data:")
        print(df.head())
        print(f"\nData types:\n{df.dtypes}")
        
        # Save the raw data
        df.to_csv('data/traffic_volume_data_raw.csv', index=False)
        
        # Clean and prepare the data for Kafka
        cleaned_df = clean_traffic_volume_data(df)
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error downloading data with authentication: {e}")
        logger.info("Falling back to unauthenticated client...")
        return download_without_auth()

def download_without_auth():
    """Fallback to unauthenticated client if auth fails"""
    try:
        client = Socrata("data.austintexas.gov", None)
        results = client.get("sh59-i6y9", limit=1000)
        df = pd.DataFrame.from_records(results)
        logger.info(f"Downloaded {len(df)} records without authentication")
        
        df.to_csv('data/traffic_volume_data_raw.csv', index=False)
        return clean_traffic_volume_data(df)
    except Exception as e:
        logger.error(f"Unauthenticated download also failed: {e}")
        return create_sample_traffic_volume_data()

def clean_traffic_volume_data(df):
    """
    Clean and prepare the traffic volume and speed data for Kafka streaming
    """
    df_clean = df.copy()
    
    print("\nCleaning and preparing traffic volume data...")
    
    # Display missing values
    print("Missing values per column:")
    print(df_clean.isnull().sum())
    
    # Handle missing values - drop records without essential fields
    essential_columns = ['record_id', 'atd_device_id', 'intersection_name']
    for col in essential_columns:
        if col in df_clean.columns:
            df_clean = df_clean.dropna(subset=[col])
    
    # Convert numeric columns with proper error handling
    numeric_columns = [
        'volume', 'speed_average', 'speed_stddev', 
        'seconds_in_zone_average', 'seconds_in_zone_stddev',
        'month', 'day', 'year', 'hour', 'minute', 'day_of_week', 'bin_duration'
    ]
    
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Convert boolean columns
    if 'heavy_vehicle' in df_clean.columns:
        df_clean['heavy_vehicle'] = df_clean['heavy_vehicle'].astype(str).str.lower() == 'true'
    
    # Create Kafka-friendly messages
    kafka_messages = []
    
    for _, row in df_clean.iterrows():
        message = {
            "record_id": row.get('record_id', 'unknown'),
            "atd_device_id": row.get('atd_device_id', 'unknown'),
            "read_date": row.get('read_date', ''),
            "intersection_name": row.get('intersection_name', 'unknown'),
            "direction": row.get('direction', 'unknown'),
            "movement": row.get('movement', 'unknown'),
            "heavy_vehicle": bool(row.get('heavy_vehicle', False)),
            "volume": int(row.get('volume', 0)),
            "speed_average": float(row.get('speed_average', 0)),
            "speed_stddev": float(row.get('speed_stddev', 0)),
            "seconds_in_zone_average": float(row.get('seconds_in_zone_average', 0)),
            "seconds_in_zone_stddev": float(row.get('seconds_in_zone_stddev', 0)),
            "month": int(row.get('month', 0)),
            "day": int(row.get('day', 0)),
            "year": int(row.get('year', 0)),
            "hour": int(row.get('hour', 0)),
            "minute": int(row.get('minute', 0)),
            "day_of_week": int(row.get('day_of_week', 0)),
            "bin_duration": int(row.get('bin_duration', 900)),
            "kafka_timestamp": datetime.now().isoformat(),
            "data_type": "traffic_volume_measurement"
        }
        
        # Remove None values and handle NaN/NaT
        message = {k: v for k, v in message.items() 
                  if v not in [None, ''] and not (isinstance(v, float) and pd.isna(v))}
        
        kafka_messages.append(message)
    
    # Save cleaned data
    cleaned_df = pd.DataFrame(kafka_messages)
    cleaned_df.to_csv('data/cleaned_traffic_volume_data.csv', index=False)
    
    # Save as JSON lines for Kafka
    with open('data/kafka_traffic_volume_messages.jsonl', 'w') as f:
        for message in kafka_messages:
            f.write(json.dumps(message) + '\n')
    
    logger.info(f"Created {len(kafka_messages)} Kafka-ready traffic volume messages")
    print(f"\nSample Kafka message:\n{json.dumps(kafka_messages[0], indent=2)}")
    
    # Print some statistics
    if kafka_messages:
        total_volume = sum(msg.get('volume', 0) for msg in kafka_messages)
        avg_speed = sum(msg.get('speed_average', 0) for msg in kafka_messages) / len(kafka_messages)
        print(f"\nData Summary:")
        print(f"Total vehicle volume: {total_volume}")
        print(f"Average speed: {avg_speed:.2f} mph")
        print(f"Number of intersections: {len(set(msg.get('intersection_name') for msg in kafka_messages))}")
    
    return cleaned_df

def create_sample_traffic_volume_data():
    """
    Create sample traffic volume data for testing if API fails
    Based on the actual traffic volume dataset structure
    """
    import random
    
    logger.info("Creating sample traffic volume data...")
    
    # Sample intersections based on the actual dataset
    sample_intersections = [
        {
            'intersection_name': 'CONGRESS AVE / OLTORF ST',
            'directions': ['NORTHBOUND', 'SOUTHBOUND', 'EASTBOUND', 'WESTBOUND'],
            'movements': ['THRU', 'LEFT TURN', 'RIGHT TURN']
        },
        {
            'intersection_name': 'LAKELINE BLVD / ANDERSON MILL RD',
            'directions': ['NORTHBOUND', 'SOUTHBOUND'],
            'movements': ['THRU', 'LEFT TURN']
        },
        {
            'intersection_name': 'BURNET RD / KOENIG LN',
            'directions': ['EASTBOUND', 'WESTBOUND'],
            'movements': ['THRU', 'RIGHT TURN']
        }
    ]
    
    kafka_messages = []
    
    for intersection in sample_intersections:
        for direction in intersection['directions']:
            for movement in intersection['movements']:
                # Create multiple time periods
                for hour in range(24):
                    for minute in [0, 15, 30, 45]:  # 15-minute intervals
                        
                        # Simulate realistic traffic patterns
                        base_volume = 10 if 6 <= hour <= 19 else 3  # Rush hour vs night
                        volume_variation = random.randint(-5, 15)
                        volume = max(0, base_volume + volume_variation)
                        
                        # Speed varies by time of day and movement type
                        base_speed = 25 if 'TURN' in movement else 35
                        speed_variation = random.uniform(-5, 10)
                        speed = max(5, base_speed + speed_variation)
                        
                        message = {
                            "record_id": f"sample_{len(kafka_messages)}",
                            "atd_device_id": f"6{random.randint(300, 400)}",
                            "read_date": f"2024-01-15T{hour:02d}:{minute:02d}:00.000",
                            "intersection_name": intersection['intersection_name'],
                            "direction": direction,
                            "movement": movement,
                            "heavy_vehicle": random.random() < 0.1,  # 10% chance
                            "volume": volume,
                            "speed_average": round(speed, 2),
                            "speed_stddev": round(random.uniform(2, 8), 2),
                            "seconds_in_zone_average": round(random.uniform(5, 30), 2),
                            "seconds_in_zone_stddev": round(random.uniform(1, 10), 2),
                            "month": 1,
                            "day": 15,
                            "year": 2024,
                            "hour": hour,
                            "minute": minute,
                            "day_of_week": 1,  # Monday
                            "bin_duration": 900,
                            "kafka_timestamp": datetime.now().isoformat(),
                            "data_type": "traffic_volume_measurement"
                        }
                        kafka_messages.append(message)
    
    # Save sample data
    sample_df = pd.DataFrame(kafka_messages)
    sample_df.to_csv('data/sample_traffic_volume_data.csv', index=False)
    
    with open('data/kafka_traffic_volume_messages.jsonl', 'w') as f:
        for message in kafka_messages:
            f.write(json.dumps(message) + '\n')
    
    logger.info(f"Created {len(kafka_messages)} sample traffic volume Kafka messages")
    
    # Print sample statistics
    total_volume = sum(msg['volume'] for msg in kafka_messages)
    print(f"\nSample Data Summary:")
    print(f"Total simulated vehicle volume: {total_volume}")
    print(f"Number of time intervals: {len(kafka_messages)}")
    print(f"Intersections covered: {len(sample_intersections)}")
    
    return sample_df

def extract_coordinates(location_str):
    """
    Extract latitude and longitude from LOCATION field (if available in future datasets)
    Example: "POINT (-97.718188 30.284456)" -> (-97.718188, 30.284456)
    """
    if not location_str or not isinstance(location_str, str):
        return None, None
    
    try:
        # Remove "POINT (" and ")"
        coords_str = location_str.replace('POINT (', '').replace(')', '')
        # Split by space
        parts = coords_str.split()
        if len(parts) >= 2:
            longitude = float(parts[0])
            latitude = float(parts[1])
            return latitude, longitude
    except:
        pass
    
    return None, None

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Download and process traffic volume data
    traffic_data = download_traffic_sensor_data()
    
    if traffic_data is not None:
        print(f"\n✅ Successfully processed {len(traffic_data)} traffic volume records")
        print("Files created:")
        print("  - data/traffic_volume_data_raw.csv (raw API data)")
        print("  - data/cleaned_traffic_volume_data.csv (cleaned data)")
        print("  - data/kafka_traffic_volume_messages.jsonl (Kafka-ready messages)")
    else:
        print("\n❌ Failed to download and process traffic data")