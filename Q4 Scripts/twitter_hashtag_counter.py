from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

def create_ultra_simple_counter():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("=== Ultra Simple Hashtag Counter ===")
    
    # Add only the essential JAR
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
    
    # Target hashtags
    target_hashtags = {"ufctampa", "ufc", "espn", "mma"}
    
    # Minimal Kafka config
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'ultra-simple-group'
    }
    
    # Create consumer
    consumer = FlinkKafkaConsumer(
        'twitter',
        SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(consumer)
    
    # Simple processing - just filter and count
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
    filtered = stream.filter(has_target_hashtag)
    
    # Print when we find matching tweets
    filtered.map(
        lambda x: "🎯 Found tweet with target hashtag!",
        output_type=Types.STRING()
    ).print()
    
    # Simple incremental count
    filtered.map(
        lambda x: ("count", 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    ).key_by(lambda x: x[0]) \
     .reduce(lambda a, b: (a[0], a[1] + b[1])) \
     .map(lambda x: f"Total matching tweets: {x[1]}", Types.STRING()) \
     .print()
    
    return env

if __name__ == '__main__':
    job_env = create_ultra_simple_counter()
    print("Starting ultra simple counter...")
    job_env.execute("Ultra Simple Counter")