package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.msg.Message;
import com.xu.zeromq.broker.ConsumerMessageListener;
import com.xu.zeromq.broker.ProducerMessageListener;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;

public class BrokerProducerMessageStrategy implements BrokerStrategy {

    private ProducerMessageListener hookProducer;

    private ChannelHandlerContext channelHandler;

    public BrokerProducerMessageStrategy() {
    }

    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        Message message = (Message) request.getMsgParams();
        // 如果有消费者对发送的消息进行订阅，那么就将消息推送给这些 consumer
        hookProducer.hookProducerMessage(message, request.getMsgId(), channelHandler.channel());
    }

    public void setHookProducer(ProducerMessageListener hookProducer) {
        this.hookProducer = hookProducer;
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
        this.channelHandler = channelHandler;
    }

    public void setHookConsumer(ConsumerMessageListener hookConsumer) {
    }

}
