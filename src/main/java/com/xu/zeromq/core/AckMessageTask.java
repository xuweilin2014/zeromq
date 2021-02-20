package com.xu.zeromq.core;

import com.xu.zeromq.msg.ProducerAckMessage;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.base.Splitter;


public class AckMessageTask implements Callable<Long> {

    CyclicBarrier barrier;
    String[] messages;
    private final AtomicLong count = new AtomicLong(0);

    public AckMessageTask(CyclicBarrier barrier, String[] messages) {
        this.barrier = barrier;
        this.messages = messages;
    }

    public Long call() throws Exception {
        // messages 是 producer 发送消息的标识，也就是 msgId + @ + msgId
        for (int i = 0; i < messages.length; i++) {
            boolean error = false;
            ProducerAckMessage ack = new ProducerAckMessage();
            Object[] msg = Splitter.on(MessageSystemConfig.MessageDelimiter).trimResults().splitToList(messages[i]).toArray();
            if (msg.length == 2) {
                // 在这里 ProducerAckMessage 的 ack 属性和 msgId 属性都被设置成为 msgId
                ack.setAck((String) msg[0]);
                ack.setMsgId((String) msg[1]);

                if (error) {
                    ack.setStatus(ProducerAckMessage.FAIL);
                } else {
                    // ProducerAckMessage 的状态设置为 SUCCESS
                    ack.setStatus(ProducerAckMessage.SUCCESS);
                    count.incrementAndGet();
                }

                // 将 ProducerAckMessage 保存到 AckTaskQueue 中
                AckTaskQueue.pushAck(ack);
                SemaphoreCache.release(MessageSystemConfig.AckTaskSemaphoreValue);
            }
        }

        barrier.await();
        return count.get();
    }

}
