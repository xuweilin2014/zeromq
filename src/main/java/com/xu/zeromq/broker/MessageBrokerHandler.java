package com.xu.zeromq.broker;

import com.xu.zeromq.broker.strategy.BrokerStrategyContext;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.netty.ShareMessageEventWrapper;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicReference;

public class MessageBrokerHandler extends ShareMessageEventWrapper<Object> {

    private AtomicReference<ProducerMessageListener> hookProducer;
    private AtomicReference<ConsumerMessageListener> hookConsumer;
    private AtomicReference<RequestMessage> message = new AtomicReference<RequestMessage>();

    public MessageBrokerHandler() {
        super.setWrapper(this);
    }

    public MessageBrokerHandler buildProducerHook(ProducerMessageListener hookProducer) {
        this.hookProducer = new AtomicReference<ProducerMessageListener>(hookProducer);
        return this;
    }

    public MessageBrokerHandler buildConsumerHook(ConsumerMessageListener hookConsumer) {
        this.hookConsumer = new AtomicReference<ConsumerMessageListener>(hookConsumer);
        return this;
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        RequestMessage request = message.get();
        ResponseMessage response = new ResponseMessage();
        response.setMsgId(request.getMsgId());
        response.setMsgSource(MessageSource.AvatarMQBroker);

        BrokerStrategyContext strategy = new BrokerStrategyContext(request, response, ctx);
        strategy.setHookConsumer(hookConsumer.get());
        strategy.setHookProducer(hookProducer.get());
        strategy.invoke();
    }

    public void beforeMessage(Object msg) {
        message.set((RequestMessage) msg);
    }
}
