import logging
from confluent_kafka import Consumer, KafkaError
from typing import List, Optional, Any

logger = logging.getLogger(__name__)


class MultiTopicConsumer:
    """Потребитель для чтения сообщений из нескольких топиков Kafka."""

    def __init__(
            self,
            topics: List[str],
            bootstrap_servers: str = "localhost:9092",
            group_id: str = "py-consumer-group"
    ):
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
        self._consumer: Optional[Consumer] = None
        self._running: bool = True

    def connect_and_subscribe(self) -> None:
        """Инициализирует потребитель и подписывается на топики."""
        self._consumer = Consumer(self._config)
        self._consumer.subscribe(self._topics)
        logger.info("Потребитель подключён: сервер=%s, топики=%s",
                    self._bootstrap_servers, self._topics)

    def start_consuming(self) -> None:
        """Запускает цикл чтения сообщений."""
        if not self._consumer:
            self.connect_and_subscribe()

        logger.info("Запуск чтения сообщений из топиков: %s", ", ".join(self._topics))

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Ошибка Kafka: %s", msg.error())
                    self._running = False
                    continue

                topic = msg.topic()
                value = msg.value().decode("utf-8", errors="replace")
                logger.info("Получено: [%s] %s", topic.upper(), value)

        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
        finally:
            self.close()

    def close(self) -> None:
        """Закрывает соединение с Kafka."""
        if self._consumer:
            logger.info("Закрытие соединения с Kafka...")
            self._consumer.close()
            logger.info("Ресурсы потребителя освобождены")

    def __enter__(self) -> "MultiTopicConsumer":
        self.connect_and_subscribe()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.close()
        return False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

    logger.info("Запуск потребителя для топиков orders и payments")

    with MultiTopicConsumer(
            topics=["orders", "payments"],
            group_id="student-consumer-group"
    ) as consumer:
        try:
            consumer.start_consuming()
        except KeyboardInterrupt:
            logger.info("Остановка по запросу пользователя")