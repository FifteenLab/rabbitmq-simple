package com.kemo.simple;

import com.alibaba.fastjson.JSONObject;
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
 * 延迟队列
 */
public class DelayMessageProducer {

    private static final String EXCHANGE = "delay.exchange";
    private static final String QUEUE = "delay.queue";
    private static final String ROUTING_KEY = "delay.test";
    private static final String DLX_EXCHANGE = "delay.dlx.exchange";
    private static final String DLX_QUEUE = "delay.dlx.queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明交换器
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);
        // 声明队列并指定消息过期时间
        Map<String, Object> queueArgs = new HashMap<String, Object>();
        queueArgs.put("x-message-ttl", 120000);// 单位毫秒
        // 添加DLX
        queueArgs.put("x-dead-letter-exchange", DLX_EXCHANGE);
        // 指定新路由键。默认使用原路由键
        // queueArgs.put("x-dead-letter-routing-key", "dlx-routing-key");
        channel.queueDeclare(QUEUE, true, false, false, queueArgs);
        channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);

        // 声明死信队列
        channel.exchangeDeclare(DLX_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);
        channel.queueDeclare(DLX_QUEUE, true, false, false, null);
        channel.queueBind(DLX_QUEUE, DLX_EXCHANGE, ROUTING_KEY);

        JSONObject message = new JSONObject();
        message.put("time", System.currentTimeMillis());
        message.put("content", "hello world!!!");
        channel.basicPublish(EXCHANGE, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.toJSONString().getBytes());

        channel.close();
        connection.close();
    }

}
