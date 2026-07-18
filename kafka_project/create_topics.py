"""Creates the two survey topics with 3 partitions each.

Run this once before producer.py / consumer.py. Without it, Kafka would
auto-create the topics on first use with a single partition, which works but
never actually demonstrates partitioning, one of the concepts worth
practicing here.
"""
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["survey-student-submissions", "survey-observer-submissions"]


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS]
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as e:
            # Already exists from a prior run is fine and expected on reruns.
            print(f"Topic '{topic}' not created (likely already exists): {e}")


if __name__ == "__main__":
    main()
