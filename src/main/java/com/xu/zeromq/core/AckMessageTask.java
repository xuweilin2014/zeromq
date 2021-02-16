package com.xu.zeromq.core;

import com.xu.zeromq.msg.ProducerAckMessage;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.base.Splitter;


public class AckMessageTask implements Callable<Long> {

    CyclicBarrier barrier = null;
    String[] messages = null;
    private final AtomicLong count = new AtomicLong(0);

    public AckMessageTask(CyclicBarrier barrier, String[] messages) {
        this.barrier = barrier;
        this.messages = messages;
    }

    public Long call() throws Exception {
        for (int i = 0; i < messages.length; i++) {
            boolean error = false;
            ProducerAckMessage ack = new ProducerAckMessage();
            Object[] msg = Splitter.on(MessageSystemConfig.MessageDelimiter).trimResults().splitToList(messages[i]).toArray();
            if (msg.length == 2) {
                ack.setAck((String) msg[0]);
                ack.setMsgId((String) msg[1]);

                if (error) {
                    ack.setStatus(ProducerAckMessage.FAIL);
                } else {
                    ack.setStatus(ProducerAckMessage.SUCCESS);
                    count.incrementAndGet();
                }

                AckTaskQueue.pushAck(ack);
                SemaphoreCache.release(MessageSystemConfig.AckTaskSemaphoreValue);
            }
        }

        barrier.await();
        return count.get();
    }
}
