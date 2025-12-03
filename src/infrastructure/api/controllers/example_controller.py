from typing import Dict, Optional

from globals.consts.const_strings import ConstStrings
from globals.enums.response_status import ResponseStatus
from infrastructure.interfaces.iexample_controller import IExampleController
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from model.data_classes.zmq_response import Response
from infrastructure.factories.logger_factory import LoggerFactory
from globals.consts.logger_messages import LoggerMessages


class ExampleController(IExampleController):
    def __init__(self, kafka_manager: IKafkaManager) -> None:
        self._kafka_manager = kafka_manager
        self._logger = LoggerFactory.get_logger_manager()

    def example_function(self, data: Optional[Dict] = None) -> Response:
        data = data or {}

        topic = data.get("topic", ConstStrings.EXAMPLE_TOPIC)
        message = data.get("message")

        if message is None:
            return Response(
                status=ResponseStatus.ERROR,
                data={ConstStrings.ERROR_MESSAGE: "missing 'message' field in data"}
            )

        processed_message = f"[ZMQ] {message}"

        self._kafka_manager.send_message(topic, processed_message)

        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            f"Forwarded ZMQ message to Kafka: topic={topic}, message={processed_message}"
        )

        return Response(
            status=ResponseStatus.SUCCESS,
            data={
                "sent": True,
                "topic": topic,
                "message": processed_message,
            }
        )
