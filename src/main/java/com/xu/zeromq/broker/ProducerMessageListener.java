package com.xu.zeromq.broker;

import com.xu.zeromq.msg.Message;
import io.netty.channel.Channel;

public interface ProducerMessageListener {

    void hookProducerMessage(Message msg, String requestId, Channel channel);
}
