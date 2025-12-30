from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time
import json
from datetime import datetime

class TweetTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            date_str = data.get('date_posted', '')
            if not date_str:
                return record_timestamp
            # Parse the ISO format timestamp
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)  # Convert to milliseconds
        except Exception as e:
            print(f"Timestamp error: {e}, using current time")
            return record_timestamp if record_timestamp else 0

def create_ultra_simple_counter_with_watermarks():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Ultra Simple Hashtag Counter WITH Watermarks ===")
    
    # Add Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Target hashtags
    target_hashtags = {"ufctampa", "ufc", "espn", "mma"}
    print(f"Tracking hashtags: {list(target_hashtags)}")
    
    # Kafka configuration
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'ultra-simple-watermark-group'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        'twitter',
        SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Create watermark strategy with 5 seconds out-of-order allowance
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(TweetTimestampAssigner())
    
    # Create the stream WITH watermarks
    stream = env.add_source(kafka_consumer) \
        .assign_timestamps_and_watermarks(watermark_strategy)
    
    print("✓ Watermarks enabled with 5-second out-of-order allowance")
    
    # Simple filter function
    def has_target_hashtag(tweet_json):
        try:
            data = json.loads(tweet_json)
            hashtags = data.get('hashtags', '[]')
            
            if isinstance(hashtags, str):
                hashtags = json.loads(hashtags)
            
            for tag in hashtags:
                if isinstance(tag, str) and tag.lower() in target_hashtags:
                    return True
            return False
        except:
            return False
    
    # Filter tweets with target hashtags
    filtered_tweets = stream.filter(has_target_hashtag)
    
    # Print when we find matching tweets
    filtered_tweets.map(
        lambda x: "🎯 Found tweet with target hashtag!",
        output_type=Types.STRING()
    ).print()
    
    # Event-time windowed count (5-second windows)
    windowed_count = filtered_tweets \
        .map(lambda x: ("hashtag_tweets", 1), 
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    windowed_count.map(
        lambda x: f"🕐 Tweets with target hashtags (5s Event Time): {x[1]}",
        output_type=Types.STRING()
    ).print()
    
    # Simple incremental counter (no windows - for comparison)
    incremental_count = filtered_tweets \
        .map(lambda x: ("total_hashtag_tweets", 1),
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    incremental_count.map(
        lambda x: f"📊 TOTAL tweets with target hashtags: {x[1]}",
        output_type=Types.STRING()
    ).print()
    
    # Also show tweet details for monitoring
    filtered_tweets.map(
        lambda x: process_tweet_info(x),
        output_type=Types.STRING()
    ).print()
    
    return env

def process_tweet_info(tweet_json):
    try:
        data = json.loads(tweet_json)
        user = data.get('user_posted', 'unknown')
        text = data.get('description', '')[:80] + "..." if len(data.get('description', '')) > 80 else data.get('description', '')
        date = data.get('date_posted', '')[:19]
        hashtags = json.loads(data.get('hashtags', '[]'))
        return f"🐦 {user} | {date} | Hashtags: {hashtags} | {text}"
    except Exception as e:
        return f"❌ ERROR: {e}"

if __name__ == '__main__':
    job_env = create_ultra_simple_counter_with_watermarks()
    print("=== Starting Ultra Simple Counter WITH Watermarks ===")
    print("This job now uses:")
    print("1. EVENT TIME based on tweet creation dates")
    print("2. Watermarks for 5-second out-of-order events")
    print("3. 5-second event time windows for counting")
    print("4. Still maintains simple incremental counting")
    job_env.execute("Ultra Simple Counter with Watermarks")