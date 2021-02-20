package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.map.TypedMap;

public class BrokerStrategyContext {

    public final static int AvatarMQProducerMessageStrategy = 1;
    public final static int AvatarMQConsumerMessageStrategy = 2;
    public final static int AvatarMQSubscribeStrategy = 3;
    public final static int AvatarMQUnsubscribeStrategy = 4;

    private RequestMessage request;
    private ResponseMessage response;
    private ChannelHandlerContext channelHandler;
    private BrokerStrategy strategy;

    private static Map strategyMap = TypedMap.decorate(new HashMap(), Integer.class, BrokerStrategy.class);

    static {
        // 有 hookProducer，没有 hookConsumer
        strategyMap.put(AvatarMQProducerMessageStrategy, new BrokerProducerMessageStrategy());
        // 没有 hookProducer，没有 hookConsumer
        strategyMap.put(AvatarMQConsumerMessageStrategy, new BrokerConsumerMessageStrategy());
        // 没有 hookProducer，有 hookConsumer
        strategyMap.put(AvatarMQSubscribeStrategy, new BrokerSubscribeStrategy());
        // 没有 hookProducer，没有 hookConsumer
        strategyMap.put(AvatarMQUnsubscribeStrategy, new BrokerUnsubscribeStrategy());
    }

    public BrokerStrategyContext(RequestMessage request, ResponseMessage response, ChannelHandlerContext channelHandler) {
        this.request = request;
        this.response = response;
        this.channelHandler = channelHandler;
    }

    public void invoke() {
        switch (request.getMsgType()) {
            // producer 发送到 broker 端的消息
            case Message:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQProducerMessageStrategy);
                break;
            // consumer 接收到 producer 发送的消息之后，返回的确认 ack 消息
            case ConsumerAck:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQConsumerMessageStrategy);
                break;
            // 消费者订阅某主题的消息
            case Subscribe:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQSubscribeStrategy);
                break;
            // 消费者取消订阅某主题的消息
            case Unsubscribe:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQUnsubscribeStrategy);
                break;
            default:
                break;
        }

        strategy.setChannelHandler(channelHandler);
        strategy.messageDispatch(request, response);
    }
}
