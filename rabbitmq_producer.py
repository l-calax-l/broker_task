import logging
import pika
from typing import Optional, Any

logger = logging.getLogger(__name__)


class RabbitMQProducer:
    """Продюсер для отправки сообщений в очередь."""

    def __init__(self, host: str = "localhost", queue: str = "task_queue"):
        self.host = host
        self.queue = queue
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect(self) -> None:
        """Устанавливает соединение и объявляет очередь."""
        if self._connection and self._connection.is_open:
            return

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.queue, durable=True)
        logger.info("Продюсер подключён: host=%s, queue=%s", self.host, self.queue)

    def publish(self, message: str) -> None:
        """Отправляет сообщение. При необходимости вызывает connect()."""
        if not self._channel:
            self.connect()

        self._channel.basic_publish(
            exchange="",
            routing_key=self.queue,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        logger.info("Сообщение отправлено: %s", message)

    def close(self) -> None:
        """Корректно закрывает канал и соединение."""
        if self._channel and self._channel.is_open:
            self._channel.close()
        if self._connection and self._connection.is_open:
            self._connection.close()
        logger.info("Ресурсы продюсера закрыты")

    def __enter__(self) -> "RabbitMQProducer":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.close()
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)

    print("Отправка сообщений...")
    producer = RabbitMQProducer(queue="task_queue")
    producer.connect()
    producer.publish("1.Привет,это первое сообщение")
    producer.close()

    with RabbitMQProducer(queue="task_queue") as producer:
        producer.publish("2.Привет,это второе сообщение")
        producer.publish("3.Привет,это третье сообщение")

