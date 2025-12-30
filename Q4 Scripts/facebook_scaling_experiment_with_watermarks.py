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

class FacebookTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            date_str = data.get('date_created', '')
            if not date_str:
                return record_timestamp
            # Parse the ISO format timestamp
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)  # Convert to milliseconds
        except Exception as e:
            print(f"Timestamp error: {e}, using record timestamp")
            return record_timestamp if record_timestamp else 0

def create_facebook_scaling_experiment_with_watermarks():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Scaling Experiment - Facebook Counter WITH Watermarks ===")
    print("Configuration:")
    print("- Kafka topic: facebook-posts1 (2 partitions)")
    print("- Flink parallelism: 1")
    print("- EVENT TIME with watermarks enabled")
    print("- 5-second out-of-order allowance")
    print("- Event time windows for accurate counting")
    
    # Add Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Kafka configuration for partitioned topic
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'facebook-scaling-watermark-group'
    }
    
    # Create Kafka consumer for partitioned topic
    kafka_consumer = FlinkKafkaConsumer(
        'facebook-posts-1',  # Using 2-partition topic
        SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Create watermark strategy with 5 seconds out-of-order allowance
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(FacebookTimestampAssigner())
    
    # Create the stream WITH watermarks
    stream = env.add_source(kafka_consumer) \
        .assign_timestamps_and_watermarks(watermark_strategy)
    
    print("✓ Watermarks enabled with 5-second out-of-order allowance")
    print("✓ Using 2-partition Kafka topic: facebook-posts1")
    print("✓ Event time processing based on comment creation dates")
    
    # Process each Facebook comment with timestamp info
    def process_facebook_comment(comment_json):
        try:
            data = json.loads(comment_json)
            user_name = data.get('user_name', 'Unknown')
            comment_text = data.get('comment_text', '')[:60] + "..." if len(data.get('comment_text', '')) > 60 else data.get('comment_text', '')
            date_created = data.get('date_created', '')[:19]
            num_likes = data.get('num_likes', 0)
            num_replies = data.get('num_replies', 0)
            
            return f"👤 {user_name} | 📅 {date_created} | 👍 {num_likes} | 💬 {num_replies} | {comment_text}"
        except Exception as e:
            return f"❌ ERROR: {e}"
    
    # Print comment details
    stream.map(process_facebook_comment, Types.STRING()).print()
    
    # Comments per minute using EVENT TIME windows
    comments_per_minute = stream \
        .map(lambda x: ("comments_per_minute", 1), 
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    comments_per_minute.map(
        lambda x: f"🕐 [Event-Time] COMMENTS PER MINUTE: {x[1]} comments",
        Types.STRING()
    ).print()
    
    # Engagement analysis with event time windows
    def analyze_engagement(comment_json):
        try:
            data = json.loads(comment_json)
            num_likes = data.get('num_likes', 0)
            num_replies = data.get('num_replies', 0)
            total_engagement = num_likes + num_replies
            
            if total_engagement >= 10:
                return "high_engagement"
            elif total_engagement >= 5:
                return "medium_engagement"
            elif total_engagement >= 1:
                return "low_engagement"
            else:
                return "zero_engagement"
        except:
            return "error"
    
    engagement_counter = stream \
        .map(lambda x: (analyze_engagement(x), 1),
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    engagement_counter.map(
        lambda x: f"📈 [Event-Time] {x[0].upper()}: {x[1]} comments/30s",
        Types.STRING()
    ).print()
    
    # Also include incremental count for comparison
    total_counter = stream \
        .map(lambda x: ("total_comments", 1),
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    total_counter.map(
        lambda x: f"📊 [Incremental] TOTAL COMMENTS: {x[1]}",
        Types.STRING()
    ).print()
    
    return env

if __name__ == '__main__':
    job_env = create_facebook_scaling_experiment_with_watermarks()
    print("=== Starting Facebook Scaling Experiment WITH Watermarks ===")
    print("SCALING EXPERIMENT FEATURES:")
    print("• Kafka Topic: facebook-posts1 (2 partitions)")
    print("• Watermarking: ENABLED (Bounded Out-of-Orderness)")
    print("• Out-of-Order Allowance: 5 seconds")
    print("• Event Time Windows: 30 seconds & 1 minute")
    print("• Timestamp Source: comment 'date_created' field")
    print("")
    print("NOTE: This version uses event time for accurate time-based processing")
    job_env.execute("Scaling Experiment - Facebook Counter with Watermarks")