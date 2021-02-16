package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.broker.ConsumerMessageListener;
import com.xu.zeromq.broker.ProducerMessageListener;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;

public interface BrokerStrategy {

    void messageDispatch(RequestMessage request, ResponseMessage response);

    void setHookProducer(ProducerMessageListener hookProducer);

    void setHookConsumer(ConsumerMessageListener hookConsumer);

    void setChannelHandler(ChannelHandlerContext channelHandler);
}
