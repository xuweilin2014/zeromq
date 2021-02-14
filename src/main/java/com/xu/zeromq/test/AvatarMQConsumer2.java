package com.xu.zeromq.test;

import com.xu.zeromq.consumer.AvatarMQConsumer;
import com.xu.zeromq.consumer.ProducerMessageHook;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public class AvatarMQConsumer2 {

    private static ProducerMessageHook hook = new ProducerMessageHook() {
        public ConsumerAckMessage hookMessage(Message message) {
            System.out.printf("AvatarMQConsumer2 收到消息编号:%s,消息内容:%s\n", message.getMsgId(), new String(message.getBody()));
            ConsumerAckMessage result = new ConsumerAckMessage();
            result.setStatus(ConsumerAckMessage.SUCCESS);
            return result;
        }
    };

    public static void main(String[] args) {
        AvatarMQConsumer consumer = new AvatarMQConsumer("127.0.0.1:18888", "AvatarMQ-Topic-2", hook);
        consumer.init();
        consumer.setClusterId("AvatarMQCluster2");
        consumer.receiveMode();
        consumer.start();
    }
}
