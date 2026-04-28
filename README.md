# 🐇 Задание по блоку M: RabbitMQ

**Исполнитель:** Салихов Ильяс  
**Стек:** Python 3.10, pika, RabbitMQ 4.2.5

---

## Задание 1

### 1.1 Создание очереди `task_queue`

<details>
<summary>📸 Скриншот 1.1: Успешное создание очереди</summary>

![Скриншот создания очереди](./screenshots/img_1_1.png)

</details>

> *Очередь `task_queue` создана, режим `durable: true`*

---

### 1.2 Отправка сообщения (Producer)

<details>
<summary>📸 Скриншот 1.2: Успешная отправка сообщений</summary>

![Скриншот отправки сообщений](./screenshots/img_1_2_1.png)
![Скриншот отправки сообщений](./screenshots/img_1_2_2.png)

</details>

---

### 1.3 Получение сообщения (Consumer)

<details>
<summary>📸 Скриншот 1.3: Успешное получение сообщений</summary>

![Скриншот получения сообщений](./screenshots/img_1_3.png)

</details>

---

## Задание 2: Exchange и маршрутизация

### 2.1 Создание Exchange `logs`

<details>
<summary>📸 Скриншот 2.1: Exchange создан</summary>

![Скриншот создания exchange](./screenshots/img_2_1.png)

</details>

---

### 2.2 Создание очередей `error_logs` и `info_logs`

<details>
<summary>📸 Скриншот 2.2: Очереди созданы</summary>

![Скриншот создания очередей](./screenshots/img_2_2.png)

</details>

---

### 2.3 Привязка очередей (Binding)

<details>
<summary>📸 Скриншот 2.3: Привязки настроены</summary>

![Скриншот биндингов](./screenshots/img_2_3.png)

</details>

---

### 2.4 Отправка сообщений через Exchange `logs`

<details>
<summary>📸 Скриншот 2.4: Сообщения опубликованы</summary>

![Скриншот отправки через exchange](./screenshots/img_2_4.png)

</details>

---

### 2.5 Чтение сообщений

<details>
<summary>📸 Скриншот 2.5.1: Получение error-сообщений</summary>

![Скриншот consumer error](./screenshots/img_2_5_1.png)

</details>

---

<details>
<summary>📸 Скриншот 2.5.2: Получение info-сообщений</summary>

![Скриншот consumer info](./screenshots/img_2_5_2.png)

</details>

---

> **Примечание:** Все скриншоты находятся в папке `./screenshots/`. Окружение: Windows 11, Python 3.10, RabbitMQ 4.2.5