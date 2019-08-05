import concurrent.futures
import time
import confluent_kafka.admin


class KafkaAdmin:
    def __init__(self, bootstrap_servers_admin, log):
        self.kafka_admin_client = confluent_kafka.admin.AdminClient(
            {"bootstrap.servers": bootstrap_servers_admin}
        )
        self.log = log

    def topic_exists(self, topic_name):
        cluster_metadata = self.kafka_admin_client.list_topics()
        return topic_name in cluster_metadata.topics

    def create_kafka_topic(self, topic_name, replica_assignment):
        num_partitions = len(replica_assignment)
        new_topic = confluent_kafka.admin.NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replica_assignment=replica_assignment,
        )
        r = self.kafka_admin_client.create_topics([new_topic])
        _ = concurrent.futures.wait(
            r.values(), return_when=concurrent.futures.FIRST_EXCEPTION
        )
        e = r[new_topic.topic].exception()
        if e is not None:
            self._add_to_log(
                "Exception '{}' when trying to create topic '{}'".format(
                    e.args[0], topic_name
                )
            )
            raise e
        self._add_to_log(
            "Created topic '{}' with replica assignment '{}'".format(
                topic_name, replica_assignment
            ),
            True,
        )

    def delete_kafka_topic(self, topic_name):
        r = self.kafka_admin_client.delete_topics([topic_name])
        _ = concurrent.futures.wait(
            r.values(), return_when=concurrent.futures.FIRST_EXCEPTION
        )
        e = r[topic_name].exception()
        if e is not None:
            self._add_to_log(
                "Exception '{}' when trying to delete topic '{}'".format(
                    e.args[0], topic_name
                )
            )
            raise e
        self._add_to_log("Deleted topic '{}'".format(topic_name), True)

    def _add_to_log(self, message, print_message=False):
        t = round(time.time())
        self.log.put("{}: {}".format(t, message))
        if print_message:
            print(message)
