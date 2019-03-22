package com.kemo.simple;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 *
 * @author Jack
 * @since 2019-03-20
 */
public class RabbitProducer {

    private static final String EXCHANGE_NAME = "exchange_demo";
    private static final String ROUTING_NAME = "routingkey_demo";
    private static final String QUEUE_NAME = "queue_demo";
    private static final String IP_ADDRESS = "192.168.2.84";
    private static final int PORT = 5672;

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(IP_ADDRESS);
        connectionFactory.setPort(PORT);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // type：类型。
        // durable：持久化。
        // autoDelete：自动删除。
        // 创建一个 type="direct"、持久化的、非自动删除的交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null);

        // durable：持久化。
        // exclusive：排他。
        // autoDelete：自动删除。
        // 创建一个持久化、非排他的、非自动删除的队列
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        // RoutingKey 实际是BindingKey
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_NAME);

        // 创建第二个队列 测试多个队列的场景  exchange 会把符合路由规则的消息发送到多个队列中
//        channel.queueDeclare("queue_demo_1", true, false, false, null);
//        channel.queueBind("queue_demo_1", EXCHANGE_NAME, ROUTING_NAME);

        String message = "Hello World!";
        // deliveryMode
        channel.basicPublish(EXCHANGE_NAME, ROUTING_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        channel.close();
        connection.close();
    }
}
