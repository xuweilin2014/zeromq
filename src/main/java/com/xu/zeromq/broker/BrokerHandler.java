package com.xu.zeromq.broker;

import com.xu.zeromq.broker.strategy.StrategyFacade;
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
        // 创建返回给 consumer 和 producer 端的响应
        ResponseMessage response = new ResponseMessage();
        response.setMsgId(request.getMsgId());
        // 根据发送到 broker 的消息类型，分别采用不同的方式进行处理
        // 发送到 broker 的消息类型有四种：Subscribe、UnSubscribe、Message、ConsumerAck
        StrategyFacade strategy = new StrategyFacade(request, response, ctx);
        strategy.invoke();
    }

    public void beforeMessage(Object msg) {
        message.set((RequestMessage) msg);
    }

}
