package com.xu.zeromq.test;

import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.producer.AvatarMQProducer;
import org.apache.commons.lang3.StringUtils;


public class AvatarMQProducer1 {

    public static void main(String[] args) throws InterruptedException {
        AvatarMQProducer producer = new AvatarMQProducer("127.0.0.1:18888", "AvatarMQ-Topic-1");
        producer.setClusterId("AvatarMQCluster");
        producer.init();
        producer.start();

        System.out.println(StringUtils.center("AvatarMQProducer1 消息发送开始", 50, "*"));

        for (int i = 0; i < 1; i++) {
            Message message = new Message();
            String str = "Hello AvatarMQ From Producer1[" + i + "]";
            message.setBody(str.getBytes());
            ProducerAckMessage result = producer.delivery(message);
            if (result.getStatus() == (ProducerAckMessage.SUCCESS)) {
                System.out.printf("AvatarMQProducer1 发送消息编号:%s\n", result.getMsgId());
            }

            Thread.sleep(100);
        }

        producer.shutdown();
        System.out.println(StringUtils.center("AvatarMQProducer1 消息发送完毕", 50, "*"));
    }
}
