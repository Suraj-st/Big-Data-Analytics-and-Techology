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
            print(f"Timestamp error: {e}, using record timestamp")
            return record_timestamp if record_timestamp else 0

def create_scaling_experiment_with_watermarks():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Scaling Experiment - Twitter Counter WITH Watermarks ===")
    print("Configuration:")
    print("- Kafka topic: twitter-posts1 (2 partitions)")
    print("- Flink parallelism: 1")
    print("- EVENT TIME with watermarks enabled")
    print("- 5-second out-of-order allowance")
    print("- Event time windows for accurate counting")
    
    # Add Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Target hashtags
    target_hashtags = {"ufctampa", "ufc", "espn", "mma"}
    print(f"Tracking hashtags: {list(target_hashtags)}")
    
    # Kafka configuration for partitioned topic
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'scaling-experiment-watermark-group'
    }
    
    # Create Kafka consumer for partitioned topic
    kafka_consumer = FlinkKafkaConsumer(
        'twitter-posts1',  # Using 2-partition topic
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
    print("✓ Using 2-partition Kafka topic: twitter-posts1")
    print("✓ Event time processing based on tweet creation dates")
    
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
    
    # Print when we find matching tweets (with timestamp info)
    def print_tweet_with_time(tweet_json):
        try:
            data = json.loads(tweet_json)
            user = data.get('user_posted', 'unknown')
            date = data.get('date_posted', '')[:19]
            hashtags = json.loads(data.get('hashtags', '[]'))
            return f"🎯 [{date}] {user} - Hashtags: {hashtags}"
        except:
            return "🎯 Found tweet with target hashtag"
    
    filtered_tweets.map(print_tweet_with_time, Types.STRING()).print()
    
    # EVENT-TIME windowed count (5-second windows)
    windowed_count = filtered_tweets \
        .map(lambda x: ("hashtag_tweets_windowed", 1), 
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    windowed_count.map(
        lambda x: f"🕐 [Event-Time Window] Tweets with target hashtags (5s): {x[1]}",
        output_type=Types.STRING()
    ).print()
    
    # Count per hashtag with event-time windows
    def extract_hashtags(tweet_json):
        try:
            data = json.loads(tweet_json)
            hashtags_field = data.get('hashtags', '[]')
            
            if isinstance(hashtags_field, str):
                hashtags = json.loads(hashtags_field)
            else:
                hashtags = hashtags_field
            
            matching_hashtags = []
            for tag in hashtags:
                if isinstance(tag, str) and tag.lower() in target_hashtags:
                    matching_hashtags.append(tag.lower())
            return matching_hashtags
        except:
            return []
    
    def extract_hashtags_flat_map(tweet_json):
        hashtags = extract_hashtags(tweet_json)
        for hashtag in hashtags:
            yield (hashtag, 1)
    
    hashtag_stream = filtered_tweets.flat_map(
        extract_hashtags_flat_map,
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )
    
    # Windowed count per hashtag (event time)
    hashtag_windowed_counts = hashtag_stream \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    hashtag_windowed_counts.map(
        lambda x: f"📊 [Event-Time] #{x[0].upper()}: {x[1]} tweets/5s",
        output_type=Types.STRING()
    ).print()
    
    # Also include incremental count for comparison
    incremental_count = filtered_tweets \
        .map(lambda x: ("total_hashtag_tweets", 1),
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    incremental_count.map(
        lambda x: f"📈 [Incremental] TOTAL tweets with target hashtags: {x[1]}",
        output_type=Types.STRING()
    ).print()
    
    # Performance monitoring
    throughput = filtered_tweets \
        .map(lambda x: ("throughput", 1), 
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    throughput.map(
        lambda x: f"🚀 [Performance] Processing rate: {x[1]/10.0:.1f} tweets/sec",
        output_type=Types.STRING()
    ).print()
    
    return env

if __name__ == '__main__':
    job_env = create_scaling_experiment_with_watermarks()
    print("=== Starting Scaling Experiment WITH Watermarks ===")
    print("SCALING EXPERIMENT FEATURES:")
    print("• Kafka Topic: twitter-posts1 (2 partitions)")
    print("• Watermarking: ENABLED (Bounded Out-of-Orderness)")
    print("• Out-of-Order Allowance: 5 seconds")
    print("• Event Time Windows: 5 seconds")
    print("• Timestamp Source: tweet 'date_posted' field")
    print("")
    print("NOTE: This version uses event time for accurate time-based processing")
    print("but may use more memory than the processing-time version")
    job_env.execute("Scaling Experiment - Twitter Counter with Watermarks")