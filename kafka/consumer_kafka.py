from confluent_kafka import Consumer, KafkaError
from typing import List, Callable, Optional
import logging



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MultiConsumer:
    """
    Простой учебный Kafka consumer для нескольких топиков.
    """

    def __init__(
        self,
        topics: List[str],
        handler: Callable[[str, str], None],
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "group",
        auto_commit: bool = False,
    ):
        if not topics:
            raise ValueError("Список топиков не может быть пустым")

        self._topics = topics
        self._handler = handler
        self._running = False

        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": auto_commit,
        }

        self._consumer: Optional[Consumer] = None
        self._auto_commit = auto_commit

    def connect(self) -> None:
        """Создание consumer и подписка на топики."""
        self._consumer = Consumer(self._config)
        self._consumer.subscribe(self._topics)

        logger.info("Подписка на топики: %s", self._topics)

    def start(self) -> None:
        """Основной цикл чтения сообщений."""
        if not self._consumer:
            self.connect()

        self._running = True
        logger.info("Consumer запущен")

        try:
            while self._running:
                msg = self._consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    self._handle_error(msg.error())
                    continue

                self._process_message(msg)

                if not self._auto_commit:
                    self._consumer.commit(message=msg, asynchronous=False)

        except KeyboardInterrupt:
            logger.info("Остановка по Ctrl+C")
        finally:
            self.close()

    def stop(self) -> None:
        """Остановка consumer."""
        self._running = False

    def _process_message(self, msg) -> None:
        """Обработка сообщения."""
        topic = msg.topic()

        try:
            value = msg.value().decode("utf-8", errors="replace")
            self._handler(topic, value)

            logger.info("Обработано сообщение из [%s]", topic)

        except Exception as e:
            logger.error("Ошибка обработки сообщения: %s", e)

    def _handle_error(self, error: KafkaError) -> None:
        """Обработка ошибок Kafka."""
        if error.code() == KafkaError._PARTITION_EOF:
            return

        logger.error("Kafka ошибка: %s", error)
        self.stop()

    def close(self) -> None:
        """Закрытие consumer."""
        if self._consumer:
            logger.info("Закрытие consumer...")
            self._consumer.close()
            logger.info("Consumer закрыт")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def handle_message(topic: str, value: str):
    logger.info("[%s] %s", topic, value)

if __name__ == "__main__":

    consumer = MultiConsumer(
        topics=["orders", "payments"],
        handler=handle_message,
        group_id="order-service-consumers",
        auto_commit=False
    )

    consumer.start()

# if __name__ == "__main__":
#
#     consumer_A = MultiConsumer(
#         topics=["user-actions"],
#         handler=handle_message,
#         group_id="groupA",
#         auto_commit=False
#     )
#     consumer_A.start()

# if __name__ == "__main__":
#
#     consumer_B = MultiConsumer(
#         topics=["user-actions"],
#         handler=handle_message,
#         group_id="groupB",
#         auto_commit=False
#     )
#
#     consumer_B.start()
