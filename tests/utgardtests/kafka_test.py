import utgardtests.kafka


class TestKafka():

    def test_value(self):
        kafka = utgardtests.kafka.Kafka()
        assert kafka.value == 1
