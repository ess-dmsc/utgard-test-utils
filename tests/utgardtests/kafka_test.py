import concurrent.futures
import queue
import confluent_kafka.admin
import pytest
import utgardtests.kafka


class PartitionMetadataStub:
    pass


class TopicMetadataStub:
    def __init__(self, topic):
        self.topic = topic


class ClusterMetadataStub:
    topics = {
        "test-topic": TopicMetadataStub("test-topic"),
        "performance-topic": TopicMetadataStub("performance-topic"),
    }


class KafkaAdminStub:
    @staticmethod
    def list_topics():
        return ClusterMetadataStub()

    @staticmethod
    def create_topics(new_topics):
        topic_futures = {}
        for new_topic in new_topics:
            f = concurrent.futures.Future()
            if new_topic.topic == "good-topic":
                f.set_result(0)
            else:
                f.set_result(1)
                f.set_exception(Exception("An exception happened"))
            topic_futures[new_topic.topic] = f

        return topic_futures

    @staticmethod
    def delete_topics(topics):
        topic_futures = {}
        for topic in topics:
            f = concurrent.futures.Future()
            if topic == "good-topic":
                f.set_result(0)
            else:
                f.set_result(1)
                f.set_exception(Exception("An exception happened"))
            topic_futures[topic] = f

        return topic_futures


class TestKafkaAdmin:
    def get_mock_kafka_admin(self, *args, **kwargs):
        return KafkaAdminStub()

    @pytest.fixture
    def kafka_admin_client(self, monkeypatch):
        monkeypatch.setattr(
            confluent_kafka.admin, "AdminClient", self.get_mock_kafka_admin
        )
        kafka_admin_client = utgardtests.kafka.KafkaAdmin(
            "localhost:9092", queue.Queue()
        )
        return kafka_admin_client

    # def test_topic_exists(self, kafka_admin_client):
    #     assert kafka_admin_client.topic_exists("test-topic")
    #     assert kafka_admin_client.topic_exists("performance-topic")
    #     assert not kafka_admin_client.topic_exists("nonexistent-topic")

    def test_create_kafka_topic_exception(self, kafka_admin_client):
        replica_assignment = [[1, 2], [3, 5]]
        kafka_admin_client.create_kafka_topic("good-topic", replica_assignment)
        with pytest.raises(Exception):
            kafka_admin_client.create_kafka_topic(
                "bad-topic", replica_assignment
            )

    def test_delete_kafka_topic_exception(self, kafka_admin_client):
        kafka_admin_client.delete_kafka_topic("good-topic")
        with pytest.raises(Exception):
            kafka_admin_client.delete_kafka_topic("bad-topic")

    def test_log_length(self, kafka_admin_client):
        assert kafka_admin_client.log.empty()

        replica_assignment = [[1, 2], [3, 5]]
        kafka_admin_client.create_kafka_topic("good-topic", replica_assignment)
        with pytest.raises(Exception):
            kafka_admin_client.create_kafka_topic(
                "bad-topic", replica_assignment
            )
        kafka_admin_client.delete_kafka_topic("good-topic")

        n = 0
        while not kafka_admin_client.log.empty():
            kafka_admin_client.log.get()
            n = n + 1

        assert n == 3
