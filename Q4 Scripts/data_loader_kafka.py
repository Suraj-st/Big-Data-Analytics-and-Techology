#!/usr/bin/env python3
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import pandas as pd
import json
import time
from datetime import datetime
import csv

class SocialMediaKafkaLoader:
    def __init__(self, bootstrap_servers='flink-kafka-1:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.admin_config = {'bootstrap.servers': bootstrap_servers}
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'social-media-loader'
        }
    
    def create_topics(self):
        """Create Kafka topics for social media data"""
        admin_client = AdminClient(self.admin_config)
        
        topics = [
            NewTopic("twitter-posts1", num_partitions=1, replication_factor=1),
            NewTopic("facebook-posts-1", num_partitions=1, replication_factor=1)
        ]
        
        fs = admin_client.create_topics(topics)
        
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Topic {topic} created successfully")
            except Exception as e:
                print(f"ℹ️ Topic {topic}: {e}")
    
    def clean_csv_data(self, df):
        """Clean the CSV data by removing empty rows and handling missing values"""
        # Remove completely empty rows
        df_clean = df.dropna(how='all')
        
        # Fill NaN values with appropriate defaults
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].fillna('')
            else:
                df_clean[col] = df_clean[col].fillna(0)
        
        return df_clean
    
    def process_twitter_csv(self, csv_file):
        """Process Twitter CSV file and convert to JSON"""
        print(f"📊 Processing Twitter CSV: {csv_file}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Clean the data
            df_clean = self.clean_csv_data(df)
            
            print(f"📈 Found {len(df_clean)} valid Twitter records")
            
            # Convert to list of dictionaries (JSON-like)
            records = []
            for idx, row in df_clean.iterrows():
                record = row.to_dict()
                
                # Add metadata
                record['_processed_timestamp'] = datetime.now().isoformat()
                record['_source'] = 'twitter'
                record['_record_id'] = idx
                
                # Clean up timestamp format
                if 'date_posted' in record and record['date_posted']:
                    # Remove extra quotes from timestamp
                    record['date_posted'] = str(record['date_posted']).replace('"', '')
                
                records.append(record)
            
            return records
            
        except Exception as e:
            print(f"❌ Error processing Twitter CSV: {e}")
            return []
    
    def process_facebook_csv(self, csv_file):
        """Process Facebook CSV file and convert to JSON"""
        print(f"📊 Processing Facebook CSV: {csv_file}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Clean the data
            df_clean = self.clean_csv_data(df)
            
            print(f"📈 Found {len(df_clean)} valid Facebook records")
            
            # Convert to list of dictionaries (JSON-like)
            records = []
            for idx, row in df_clean.iterrows():
                record = row.to_dict()
                
                # Add metadata
                record['_processed_timestamp'] = datetime.now().isoformat()
                record['_source'] = 'facebook'
                record['_record_id'] = idx
                
                # Clean up timestamp format
                if 'date_created' in record and record['date_created']:
                    # Remove extra quotes from timestamp
                    record['date_created'] = str(record['date_created']).replace('"', '')
                
                records.append(record)
            
            return records
            
        except Exception as e:
            print(f"❌ Error processing Facebook CSV: {e}")
            return []
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
        else:
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def produce_to_kafka(self, topic, records):
        """Produce records to Kafka topic"""
        producer = Producer(self.producer_config)
        
        print(f"📤 Producing {len(records)} records to {topic}...")
        
        for i, record in enumerate(records):
            try:
                # Convert record to JSON string
                message_value = json.dumps(record, ensure_ascii=False)
                
                # Create a key for the message
                if 'id' in record and record['id']:
                    message_key = str(record['id'])
                elif 'post_id' in record and record['post_id']:
                    message_key = str(record['post_id'])
                else:
                    message_key = f"record_{i}"
                
                # Send to Kafka
                producer.produce(
                    topic=topic,
                    key=message_key.encode('utf-8'),
                    value=message_value.encode('utf-8'),
                    callback=self.delivery_report
                )
                
                # Poll to handle callbacks
                producer.poll(0)
                
                # Small delay to simulate streaming
                time.sleep(0.05)
                
                if (i + 1) % 5 == 0:
                    print(f"  Sent {i + 1} records to {topic}...")
                    
            except Exception as e:
                print(f"❌ Error sending record {i}: {e}")
        
        # Wait for all messages to be delivered
        producer.flush()
        print(f"✅ Completed producing to {topic}")
    
    def run_complete_workflow(self):
        """Run the complete CSV to Kafka workflow"""
        print("🚀 Starting Social Media CSV to Kafka Workflow...")
        
        # Step 1: Create Kafka topics
        print("\n📝 Step 1: Creating Kafka topics...")
        self.create_topics()
        time.sleep(2)  # Wait for topics to be created
        
        # Step 2: Process Twitter data
        print("\n🐦 Step 2: Processing Twitter data...")
        twitter_records = self.process_twitter_csv('Twitter-datasets.csv')
        
        # Step 3: Process Facebook data
        print("\n📘 Step 3: Processing Facebook data...")
        facebook_records = self.process_facebook_csv('Facebook-datasets.csv')
        
        # Step 4: Send Twitter data to Kafka
        print("\n📤 Step 4: Sending Twitter data to Kafka...")
        if twitter_records:
            self.produce_to_kafka('twitter-posts1', twitter_records)
        else:
            print("❌ No Twitter records to send")
        
        # Step 5: Send Facebook data to Kafka
        print("\n📤 Step 5: Sending Facebook data to Kafka...")
        if facebook_records:
            self.produce_to_kafka('facebook-posts-1', facebook_records)
        else:
            print("❌ No Facebook records to send")
        
        print(f"\n🎉 Workflow completed!")
        print(f"   Twitter: {len(twitter_records)} records sent")
        print(f"   Facebook: {len(facebook_records)} records sent")

# Run the workflow
if __name__ == "__main__":
    loader = SocialMediaKafkaLoader()
    loader.run_complete_workflow()