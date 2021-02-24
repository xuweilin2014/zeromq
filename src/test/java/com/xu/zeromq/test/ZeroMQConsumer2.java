package com.xu.zeromq.test;

import com.xu.zeromq.consumer.ZeroMQConsumer;
import com.xu.zeromq.consumer.MessageConsumeHook;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public class ZeroMQConsumer2 {

    private static MessageConsumeHook hook = new MessageConsumeHook() {
        public ConsumerAckMessage consumeMessage(Message message) {
            System.out.printf("ZeroMQConsumer2 收到消息编号:%s,消息内容:%s\n", message.getMsgId(), new String(message.getBody()));
            ConsumerAckMessage result = new ConsumerAckMessage();
            result.setStatus(ConsumerAckMessage.SUCCESS);
            return result;
        }
    };

    public static void main(String[] args) {
        ZeroMQConsumer consumer = new ZeroMQConsumer("127.0.0.1:18888", "ZeroMQ-Topic-2", hook);
        consumer.setClusterId("ZeroMQCluster2");
        consumer.start();
    }
}
