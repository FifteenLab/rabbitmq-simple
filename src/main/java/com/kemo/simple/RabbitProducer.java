package com.kemo.simple;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

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
        // arguments:
        //           alternate-exchange  设置备份交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true, false, null);

        // durable：持久化。
        // exclusive：排他。
        // autoDelete：自动删除。
        // 创建一个持久化、非排他的、非自动删除的队列
        // arguments:
        //           x-message-ttl  设置消息过期时间（单位：毫秒）。队列所有消息过期时间一致
        //           x-expires      控制队列在自动删除前处于未使用状态的时间
        //           x-dead-letter-exchange  为队列增加DLX
        // 延迟队列：DLX+TTL实现
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        // RoutingKey 实际是BindingKey
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_NAME);

        // 创建第二个队列 测试多个队列的场景  exchange 会把符合路由规则的消息发送到多个队列中
//        channel.queueDeclare("queue_demo_1", true, false, false, null);
//        channel.queueBind("queue_demo_1", EXCHANGE_NAME, ROUTING_NAME);

        String message = "Hello World!";
        // deliveryMode
        // mandatory 设为true时，交换器无法根据自身的类型和路由键找到一个符合条件的队列，通过Basic.Return命令将消息返回给生产者。
        //           默认false。遇到上述情景，则消息直接被丢弃。
        AMQP.BasicProperties.Builder builder = MessageProperties.PERSISTENT_TEXT_PLAIN.builder();
        // 消息增加过期时间
        // 不同于x-message-ttl。x-message-ttl消息过期后直接从队列中抹去。当前这种方法，即使过期，也不会马上从队列中抹去。
        builder.expiration("60000");// 单位毫秒
        channel.basicPublish(EXCHANGE_NAME, ROUTING_NAME, false, builder.build(), message.getBytes());

        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // 接收被返回的消息
                System.out.println("return msg: " +  new String(body));
            }
        });
        channel.close();
        connection.close();
    }
}
