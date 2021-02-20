package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.UnSubscribeMessage;
import io.netty.channel.ChannelHandlerContext;

public class BrokerUnsubscribeStrategy implements BrokerStrategy {

    public BrokerUnsubscribeStrategy() {
    }

    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        UnSubscribeMessage msgUnSubscribe = (UnSubscribeMessage) request.getMsgParams();
        // 从 broker 服务器端删除掉这个 consumer 的订阅内容
        ConsumerContext.unLoad(msgUnSubscribe.getConsumerId());
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
    }
}
