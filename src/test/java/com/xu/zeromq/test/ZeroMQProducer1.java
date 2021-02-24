package com.xu.zeromq.test;

import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.producer.ZeroMQProducer;
import org.apache.commons.lang3.StringUtils;


public class ZeroMQProducer1 {

    public static void main(String[] args) throws InterruptedException {
        ZeroMQProducer producer = new ZeroMQProducer("127.0.0.1:18888", "ZeroMQ-Topic-1");
        producer.setClusterId("zeromq cluster");
        producer.start();

        System.out.println(StringUtils.center("zeromq producer1 消息发送开始", 50, "*"));

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            String str = "hello zeromq from producer1[" + i + "]";
            message.setBody(str.getBytes());
            ProducerAckMessage result = producer.deliver(message);
            if (result.getStatus() == (ProducerAckMessage.SUCCESS)) {
                System.out.printf("zeromq producer1 发送消息编号:%s\n", result.getMsgId());
            }

            Thread.sleep(100);
        }

        producer.shutdown();
        System.out.println(StringUtils.center("zeromq producer1 消息发送完毕", 50, "*"));
    }

}
