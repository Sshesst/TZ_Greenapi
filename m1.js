const express = require('express');
const bodyParser = require('body-parser');
const amqp = require('amqplib/callback_api');

const app = express();
app.use(bodyParser.json());

const QUEUE_NAME = 'tasks_queue';

app.all('/process', async (req, res) => {
    const requestData = req.body;

    if (!requestData) {
        res.status(400).json({ message: 'Ошибка запроса: Нет данных' });
        return;
    }

    const httpMethod = req.method;
    console.log('Получен', httpMethod, 'запрос:', requestData);

    amqp.connect('amqp://localhost', (error, connection) => {
        if (error) {
            console.error('Ошибка подключения к RabbitMQ', error);
            res.status(500).json({ message: 'Внутренняя ошибка сервера' });
            return;
        }

        connection.createChannel((error, channel) => {
            if (error) {
                console.error('Ошибка создания канала', error);
                res.status(500).json({ message: 'Внутренняя ошибка сервера' });
                return;
            }

            channel.assertQueue(QUEUE_NAME, { durable: true });

            channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(requestData)), {
                persistent: true,
            });

            console.log('Задание отправлено в RabbitMQ:', requestData);
            res.status(200).json({ message: 'Задание отправлено на обработку' });
        });
    });
});

app.listen(3000, () => {
    console.log('M1 слушает порт 3000');
});
