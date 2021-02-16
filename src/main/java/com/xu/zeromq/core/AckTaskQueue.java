package com.xu.zeromq.core;

import com.xu.zeromq.msg.ProducerAckMessage;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AckTaskQueue {

    private static ConcurrentLinkedQueue<ProducerAckMessage> ackQueue = new ConcurrentLinkedQueue<ProducerAckMessage>();

    public static boolean pushAck(ProducerAckMessage ack) {
        return ackQueue.offer(ack);
    }

    public static ProducerAckMessage getAck() {
        return ackQueue.poll();
    }
}
