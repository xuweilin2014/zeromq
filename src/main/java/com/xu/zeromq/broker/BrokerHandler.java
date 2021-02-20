package com.xu.zeromq.broker;

import com.xu.zeromq.broker.strategy.BrokerStrategyContext;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.AbstractHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicReference;

@ChannelHandler.Sharable
public class BrokerHandler extends AbstractHandler<Object> {

    private AtomicReference<RequestMessage> message = new AtomicReference<RequestMessage>();

    public BrokerHandler() {
        super.setWrapper(this);
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        RequestMessage request = message.get();
        ResponseMessage response = new ResponseMessage();
        response.setMsgId(request.getMsgId());

        BrokerStrategyContext strategy = new BrokerStrategyContext(request, response, ctx);
        strategy.invoke();
    }

    public void beforeMessage(Object msg) {
        message.set((RequestMessage) msg);
    }

}
