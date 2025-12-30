from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

def create_facebook_comment_counter():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Facebook Comment Counter ===")
    
    # Add Kafka connector
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Kafka configuration
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'facebook-comment-group'
    }
    
    # Create Kafka consumer for facebook-post topic
    kafka_consumer = FlinkKafkaConsumer(
        'facebook',
        SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(kafka_consumer)
    
    # Process each Facebook comment
    def process_facebook_comment(comment_json):
        try:
            data = json.loads(comment_json)
            user_name = data.get('user_name', 'Unknown')
            comment_text = data.get('comment_text', '')[:100] + "..." if len(data.get('comment_text', '')) > 100 else data.get('comment_text', '')
            date_created = data.get('date_created', '')[:19]  # Truncate to seconds
            num_likes = data.get('num_likes', 0)
            num_replies = data.get('num_replies', 0)
            
            return f"👤 {user_name} | 📅 {date_created} | 👍 {num_likes} | 💬 {num_replies} | Text: {comment_text}"
        except Exception as e:
            return f"❌ ERROR processing comment: {e}"
    
    # Print comment details
    stream.map(process_facebook_comment, Types.STRING()).print()
    
    # Count total comments
    comment_counter = stream.map(
        lambda x: ("total_comments", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    ).key_by(lambda x: x[0]) \
     .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    comment_counter.map(
        lambda x: f"📊 TOTAL COMMENTS PROCESSED: {x[1]}",
        Types.STRING()
    ).print()
    
    # Count comments with likes
    liked_comments_counter = stream.filter(
        lambda x: json.loads(x).get('num_likes', 0) > 0
    ).map(
        lambda x: ("comments_with_likes", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    ).key_by(lambda x: x[0]) \
     .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    liked_comments_counter.map(
        lambda x: f"❤️  COMMENTS WITH LIKES: {x[1]}",
        Types.STRING()
    ).print()
    
    return env

if __name__ == '__main__':
    job_env = create_facebook_comment_counter()
    print("Starting Facebook Comment Counter...")
    job_env.execute("Facebook Comment Counter")