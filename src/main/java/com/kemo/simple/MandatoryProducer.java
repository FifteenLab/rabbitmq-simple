package com.kemo.simple;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * mandatory 参数。
 * 设为true时，交换器无法找到符合条件的队列时，使用Basic.Return 将消息返回给生产者
 */
public class MandatoryProducer {

    private static final String EXCHANGE = "exchange_demo";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Channel channel = factory.newConnection().createChannel();

        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);

        String msg = "test msg01 ";
        // mandatory 默认 FALSE。
        channel.basicPublish(EXCHANGE, "mandatory_demo", true, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());

        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText, String exchange,
                                     String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(exchange + " : " + routingKey + " >>> " + new String(body));
            }
        });
    }
}
