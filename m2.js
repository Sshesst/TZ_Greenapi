const amqp = require('amqplib/callback_api');
const winston = require('winston');

const QUEUE_NAME = 'tasks_queue';

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.simple(),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'logs.log' }),
    ],
});

function processTask(taskData) {
    const processingTime = Math.floor(Math.random() * 5000);

    setTimeout(() => {
        const result = { result: `Processed: ${taskData}`, processingTime };
        logger.info('Задание обработано:', result);
        sendResultToRabbitMQ(result);
    }, processingTime);
}

function sendResultToRabbitMQ(result) {
    amqp.connect('amqp://localhost', (error, connection) => {
        if (error) {
            console.error('Ошибка подключения к RabbitMQ', error);
            return;
        }

        connection.createChannel((error, channel) => {
            if (error) {
                console.error('Ошибка создания канала', error);
                return;
            }

            const exchange = '';
            const routingKey = result.correlationId;
            const options = {
                correlationId: result.correlationId,
                persistent: true,
            };

            channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(result)), options);
            logger.info('Результат отправлен в RabbitMQ:', result);
        });
    });
}

amqp.connect('amqp://localhost', (error, connection) => {
    if (error) {
        console.error('Ошибка подключения к RabbitMQ', error);
        return;
    }

    connection.createChannel((error, channel) => {
        if (error) {
            console.error('Ошибка создания канала', error);
            return;
        }

        channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log('M2 ожидает задания...');
        channel.consume(QUEUE_NAME, (msg) => {
            const taskData = JSON.parse(msg.content.toString());
            logger.info('Получено задание из RabbitMQ:', taskData);

            processTask(taskData);

            channel.ack(msg);
        });
    });
});
