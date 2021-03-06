package com.xu.zeromq.broker;

import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.core.AckTaskQueue;
import com.xu.zeromq.core.ChannelCache;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.core.SemaphoreCache;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.NettyUtil;
import io.netty.channel.Channel;
import java.util.concurrent.Callable;

public class SendAckController implements Callable<Void> {

    private volatile boolean stopped = false;

    public void stop() {
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }

    public Void call() {
        while (!stopped) {
            SemaphoreCache.acquire(MessageSystemConfig.AckTaskSemaphoreValue);

            // 从 AckTaskQueue 中获取保存的 ProducerAckMessage
            ProducerAckMessage ack = AckTaskQueue.getAck();
            String requestId = ack.getAck();
            Channel channel = ChannelCache.findChannel(requestId);

            // 将 ProducerAckMessage 保存到 ResponseMessage，然后把 ResponseMessage 发送给 producer 端
            if (NettyUtil.validateChannel(channel)) {
                ResponseMessage response = new ResponseMessage();
                response.setMsgId(requestId);
                response.setMsgType(MessageType.ProducerAck);
                response.setMsgParams(ack);

                channel.writeAndFlush(response);
            }
        }
        return null;
    }
}
