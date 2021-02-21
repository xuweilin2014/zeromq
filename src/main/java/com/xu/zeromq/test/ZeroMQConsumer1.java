
package com.xu.zeromq.test;

import com.xu.zeromq.consumer.ZeroMQConsumer;
import com.xu.zeromq.consumer.ProducerMessageHook;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public class ZeroMQConsumer1 {

    // 由用户自定义，对 producer 发送过来的消息进行处理
    private static ProducerMessageHook hook = new ProducerMessageHook() {
        public ConsumerAckMessage hookMessage(Message message) {
            System.out.printf("AvatarMQConsumer1 收到消息编号:%s,消息内容:%s\n", message.getMsgId(), new String(message.getBody()));
            // 返回 ConsumerAckMessage 消息给 broker 服务端
            ConsumerAckMessage result = new ConsumerAckMessage();
            // 设置消息消费结果为 SUCCESS
            result.setStatus(ConsumerAckMessage.SUCCESS);
            return result;
        }
    };

    public static void main(String[] args) {
        ZeroMQConsumer consumer = new ZeroMQConsumer("127.0.0.1:18888", "AvatarMQ-Topic-1", hook);
        consumer.setClusterId("AvatarMQCluster");
        consumer.start();
    }
}
