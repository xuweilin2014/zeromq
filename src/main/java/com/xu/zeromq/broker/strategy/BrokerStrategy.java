package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;

public interface BrokerStrategy {

    void messageDispatch(RequestMessage request, ResponseMessage response);

    void setChannelHandler(ChannelHandlerContext channelHandler);
}
