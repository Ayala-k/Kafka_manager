import time
import threading
import json

from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from globals.consts.const_strings import ConstStrings
from globals.consts.consts import Consts
from globals.consts.logger_messages import LoggerMessages
from globals.consts.const_colors import ConstColors
from infrastructure.factories.logger_factory import LoggerFactory


class ExampleManager(IExampleManager):
    def __init__(self, config_manager: IConfigManager, kafka_manager: IKafkaManager) -> None:
        super().__init__()
        self._config_manager = config_manager
        self._kafka_manager = kafka_manager
        self._topic1 = ConstStrings.EXAMPLE_TOPIC
        self._topic2 = ConstStrings.ANOTHER_TOPIC

        self._logger = LoggerFactory.get_logger_manager()
        self._init_threading()
        self._init_consumers()

    def do_something(self) -> None:
        self._kafka_manager.send_message(
            self._topic1, "Manual do_something message")

    def _init_threading(self) -> None:
        self._producer_thread_1 = threading.Thread(
            target=self._produce_topic1_messages
        )
        self._producer_thread_1.start()

        self._producer_thread_2 = threading.Thread(
            target=self._produce_topic2_messages
        )
        self._producer_thread_2.start()

    def _init_consumers(self) -> None:

        self._kafka_manager.start_consuming(
            self._topic1,
            self._print_consumer
        )

        self._kafka_manager.start_consuming(
            self._topic2,
            self._print_consumer
        )

    def _produce_topic1_messages(self) -> None:
        while (True):
            time.sleep(Consts.SEND_MESSAGE_DURATION)
            self._kafka_manager.send_message(
                self._topic1,
                ConstStrings.EXAMPLE_MESSAGE
            )

    def _produce_topic2_messages(self) -> None:
        while (True):
            time.sleep(Consts.SEND_MESSAGE_DURATION * 2)
            self._kafka_manager.send_message(
                self._topic2,
                ConstStrings.ANOTHER_MESSAGE
            )

    def _print_consumer(self, topic: str, msg: str) -> None:
        colored_topic = f"{ConstColors.CYAN}[{topic}]{ConstColors.RESET}"
        colored_msg = f"{ConstColors.GREEN}{LoggerMessages.EXAMPLE_PRINT_CONSUMER_MSG.format(msg)}{ConstColors.RESET}"

        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            f"{colored_topic} {colored_msg}"
        )
