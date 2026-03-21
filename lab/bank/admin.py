from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

topic_list = []

new_topic = NewTopic(name="bank_branch", num_partitions=2, replication_factor=1)

topic_list.append(new_topic)

admin_client.create_topics(new_topics=topic_list)

configs = admin_client.describe_configs(
    config_resources=[ConfigResource(ConfigResourceType.TOPIC, "bank_branch")])
