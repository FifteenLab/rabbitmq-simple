package com.kemo.simple;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 备份交换器
 * 交换器无法找到符合条件的队列时，消息转发至备份交换器（转发时会带着routingKey）
 */
public class AlternateExchange {

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明普通交换器 并指定备份交换器
        Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("alternate-exchange", "ae.exchange");
        channel.exchangeDeclare("normal.exchange", BuiltinExchangeType.DIRECT, true, false, arguments);
        // 声明队列
        channel.queueDeclare("normal.queue", true, false, false, null);
        // 绑定
        channel.queueBind("normal.queue", "normal.exchange", "normal");

        // 声明备份交换器  【建议使用广播模式，避免routingKey影响】
        channel.exchangeDeclare("ae.exchange", BuiltinExchangeType.FANOUT, true, false, null);
        channel.queueDeclare("unrouted.queue", true, false, false, null);
        channel.queueBind("unrouted.queue", "ae.exchange", "");

        channel.basicPublish("normal.exchange", "normal-1", MessageProperties.PERSISTENT_TEXT_PLAIN, "test message.".getBytes());

        channel.close();
        connection.close();
    }
}
