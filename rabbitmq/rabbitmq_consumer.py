import logging
import pika
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)

class RabbitMQConsumer:
    """Потребитель для чтения сообщений из очереди."""

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
        logger.info("Потребитель подключён: host=%s, queue=%s", self.host, self.queue)

    def consume(self, callback: Callable) -> None:
        """Запускает цикл чтения сообщений."""
        if not self._channel:
            self.connect()

        logger.info("Запуск потребления из очереди: %s", self.queue)
        self._channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=False)
        self._channel.start_consuming()

    def close(self) -> None:
        """Останавливает потребление и закрывает ресурсы."""
        if self._channel and self._channel.is_open:
            try:
                self._channel.stop_consuming()
            except Exception:
                pass
            self._channel.close()
        if self._connection and self._connection.is_open:
            self._connection.close()
        logger.info("Ресурсы потребителя закрыты")

    def __enter__(self) -> "RabbitMQConsumer":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.close()
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)

    print("\n Чтение сообщений (нажмите Ctrl+C для остановки)...")


    def handle_message(ch, method, properties, body):
        print(f"Получено: {body.decode('utf-8')}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        with RabbitMQConsumer(queue="task_queue") as consumer:
            consumer.consume(handle_message)
    except KeyboardInterrupt:
        print("\n Остановка по запросу пользователя...")
