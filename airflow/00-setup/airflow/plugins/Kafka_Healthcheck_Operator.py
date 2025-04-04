from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError
import time


class KafkaHealthCheckOperator(BaseOperator):
    def __init__(self, kafka_servers, topic, consumer_group, sasl_username=None, sasl_password=None, retries=3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.retries = retries

    def execute(self, context):
        attempt = 0
        while attempt < self.retries:
            try:
                self.log.info("Checking Kafka broker connectivity...")
                admin_client = KafkaAdminClient(
                    bootstrap_servers=self.kafka_servers,
                    security_protocol="SASL_PLAINTEXT",
                    sasl_mechanism="PLAIN",
                    sasl_plain_username=self.sasl_username,
                    sasl_plain_password=self.sasl_password
                )
                admin_client.list_topics()
                self.log.info("Kafka broker is reachable")

                self.log.info(f"Verifying topic '{self.topic}' availability...")
                topics = admin_client.list_topics()
                if self.topic not in topics:
                    raise ValueError(f"Topic '{self.topic}' not found")
                self.log.info(f"Topic '{self.topic}' exists")

                self.log.info(f"Checking consumer group '{self.consumer_group}' status...")
                consumer = KafkaConsumer(
                    group_id=self.consumer_group,
                    bootstrap_servers=self.kafka_servers,
                    security_protocol="SASL_PLAINTEXT",
                    sasl_mechanism="PLAIN",
                    sasl_plain_username=self.sasl_username,
                    sasl_plain_password=self.sasl_password
                )
                consumer_metrics = consumer.metrics()
                self.log.info(f"Consumer group metrics: {consumer_metrics}")

                self.log.info("Kafka health check passed")
                return
            except KafkaError as e:
                self.log.error(f"Kafka health check failed: {e}")
                attempt += 1
                time.sleep(5)
        raise Exception("Kafka health check failed after multiple attempts")


class KafkaDataFlowMonitoringOperator(BaseOperator):
    def __init__(self, kafka_servers, topic, consumer_group, sasl_username=None, sasl_password=None, threshold_lag=100, retries=3, **kwargs):
        super().__init__(**kwargs)
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.threshold_lag = threshold_lag
        self.retries = retries

    def execute(self, context):
        attempt = 0
        while attempt < self.retries:
            try:
                self.log.info("Monitoring Kafka data flow...")

                consumer = KafkaConsumer(
                    self.topic,
                    group_id=self.consumer_group,
                    bootstrap_servers=self.kafka_servers,
                    security_protocol="SASL_PLAINTEXT",
                    sasl_mechanism="PLAIN",
                    sasl_plain_username=self.sasl_username,
                    sasl_plain_password=self.sasl_password,
                )

                end_offsets = consumer.end_offsets(consumer.assignment())
                consumer_lag = {tp: end_offsets[tp] - consumer.position(tp) for tp in consumer.assignment()}

                for tp, lag in consumer_lag.items():
                    self.log.info(f"Partition {tp.partition}: Lag = {lag}")
                    if lag > self.threshold_lag:
                        self.log.warning(f"High consumer lag detected: {lag} messages")

                self.log.info("Kafka data flow monitoring completed")
                return
            except KafkaError as e:
                self.log.error(f"Kafka data flow monitoring failed: {e}")
                attempt += 1
                time.sleep(5)

        raise Exception("Kafka data flow monitoring failed after multiple attempts")
