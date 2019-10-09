import pytest
from utgardtests.filewriter import statusprocessor


class MessageProcessorStub:
    def __init__(self, timestamp=None):
        self._timestamp = timestamp

    def process_msg(self, msgs):
        pass

    def get_latest_timestamp(self):
        return self._timestamp

    def get_metrics(self):
        return {}


class TestStatusProcessorStartAndStop:
    @pytest.fixture
    def sp(self, mocker):
        self.consumer = mocker.Mock()
        return statusprocessor.StatusProcessor(
            status_consumer=self.consumer, msg_processor=MessageProcessorStub()
        )

    def test_start(self, sp):
        sp.start()
        assert self.consumer.start.called

    def test_stop(self, sp):
        sp.start()
        sp.stop()
        assert self.consumer.stop.called


class TimeFunctionStub:
    def __init__(self, time):
        self._time = time

    def time(self):
        t = self._time
        self._time = self._time + 10.0
        return t


class TestStatusProcessorUpdate:
    def test_is_not_writing(self, mocker):
        consumer = mocker.Mock()
        consumer.get_messages.return_value = [1, 2, 3]
        sp = statusprocessor.StatusProcessor(
            status_consumer=consumer, msg_processor=MessageProcessorStub()
        )
        assert not sp.is_writing()
        sp.update_status()
        assert not sp.is_writing()

    def test_is_writing_then_stops(self, mocker):
        consumer = mocker.Mock()
        consumer.get_messages.return_value = [1, 2, 3]
        time_function_stub = TimeFunctionStub(2.0)
        sp = statusprocessor.StatusProcessor(
            status_consumer=consumer,
            msg_processor=MessageProcessorStub(1.0),
            time_function=time_function_stub.time,
        )
        assert not sp.is_writing()
        sp.update_status()
        assert sp.is_writing()
        sp.update_status()
        assert not sp.is_writing()


class TestStatusProcessorGetMetrics:
    @pytest.fixture
    def sp(self, mocker):
        consumer = mocker.Mock()
        consumer.get_messages.return_value = [1, 2, 3]
        time_function_stub = TimeFunctionStub(2.0)
        sp = statusprocessor.StatusProcessor(
            status_consumer=consumer,
            msg_processor=MessageProcessorStub(1.0),
            time_function=time_function_stub.time,
        )
        return sp

    def test_get_metrics(self, sp):
        metrics = sp.get_metrics()
        assert metrics is not None
        sp.update_status()
        sp.update_status()
        metrics = sp.get_metrics()
        assert metrics is not None

    def test_get_metrics_while_running_exception(self, sp):
        sp.update_status()
        assert sp.is_writing()
        with pytest.raises(RuntimeError):
            sp.get_metrics()
