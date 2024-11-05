package com.my.topics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        // 创建多个临时队列
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, EXCHANGE_NAME, "#");

        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, EXCHANGE_NAME, "kern.*");

        String queueName3 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName3, EXCHANGE_NAME, "*.critical");

        String queueName4 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName4, EXCHANGE_NAME, "kern.*");
        channel.queueBind(queueName4, EXCHANGE_NAME, "*.critical");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName1 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName2 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        DeliverCallback deliverCallback3 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName3 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        DeliverCallback deliverCallback4 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [" + queueName4 +"] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        channel.basicConsume(queueName1, true, deliverCallback1, consumerTag -> { });
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
        channel.basicConsume(queueName3, true, deliverCallback3, consumerTag -> { });
        channel.basicConsume(queueName4, true, deliverCallback4, consumerTag -> { });
    }
}
