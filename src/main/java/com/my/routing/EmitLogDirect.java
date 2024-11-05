package com.my.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String[] split = scanner.nextLine().split(" ");
                String severity = split[0];
                String message = split[1];

                channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
            }
        }
    }
}
