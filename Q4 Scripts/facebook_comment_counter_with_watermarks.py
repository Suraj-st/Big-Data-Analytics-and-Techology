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
            print(f"Timestamp error: {e}, using current time")
            return record_timestamp if record_timestamp else 0

def create_facebook_comment_counter_with_watermarks():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Facebook Comment Counter WITH Watermarks ===")
    
    # Add Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Kafka configuration
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'facebook-watermark-group'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        'facebook',
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
    
    # Process each Facebook comment
    def process_facebook_comment(comment_json):
        try:
            data = json.loads(comment_json)
            user_name = data.get('user_name', 'Unknown')
            comment_text = data.get('comment_text', '')[:100] + "..." if len(data.get('comment_text', '')) > 100 else data.get('comment_text', '')
            date_created = data.get('date_created', '')[:19]
            num_likes = data.get('num_likes', 0)
            num_replies = data.get('num_replies', 0)
            
            return f"👤 {user_name} | 📅 {date_created} | 👍 {num_likes} | 💬 {num_replies} | Text: {comment_text}"
        except Exception as e:
            return f"❌ ERROR processing comment: {e}"
    
    # Print comment details
    stream.map(process_facebook_comment, Types.STRING()).print()
    
    # Count comments per minute using EVENT TIME windows
    comments_per_minute = stream \
        .map(lambda x: ("comments_per_minute", 1), 
             output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    comments_per_minute.map(
        lambda x: f"🕐 COMMENTS PER MINUTE (Event Time): {x[1]} comments",
        Types.STRING()
    ).print()
    
    # Count popular comments (with likes) per 30 seconds
    popular_comments = stream.filter(
        lambda x: json.loads(x).get('num_likes', 0) > 0
    ).map(
        lambda x: ("popular_comments", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    ).key_by(lambda x: x[0]) \
     .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
     .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    popular_comments.map(
        lambda x: f"❤️  POPULAR COMMENTS (30s Event Time): {x[1]} comments",
        Types.STRING()
    ).print()
    
    # Simple incremental counter (no windows)
    total_counter = stream.map(
        lambda x: ("total_comments", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    ).key_by(lambda x: x[0]) \
     .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    total_counter.map(
        lambda x: f"📊 TOTAL COMMENTS: {x[1]}",
        Types.STRING()
    ).print()
    
    return env

if __name__ == '__main__':
    job_env = create_facebook_comment_counter_with_watermarks()
    print("Starting Facebook Comment Counter WITH Watermarks...")
    print("This job uses EVENT TIME based on comment creation dates")
    print("Watermarks handle events up to 5 seconds out-of-order")
    job_env.execute("Facebook Comment Counter with Watermarks")