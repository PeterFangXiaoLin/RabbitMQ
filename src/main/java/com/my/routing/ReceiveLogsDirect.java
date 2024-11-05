package com.my.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        // 创建两个临时队列
        String queueName1 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName1, EXCHANGE_NAME, "error");

        String queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, EXCHANGE_NAME, "error");
        channel.queueBind(queueName2, EXCHANGE_NAME, "warn");
        channel.queueBind(queueName2, EXCHANGE_NAME, "info");

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

        channel.basicConsume(queueName1, true, deliverCallback1, consumerTag -> { });
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> { });
    }
}
