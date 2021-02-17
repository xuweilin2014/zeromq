package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.broker.ConsumerMessageListener;
import com.xu.zeromq.broker.ProducerMessageListener;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.MessageSource;
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
    private ProducerMessageListener hookProducer;
    private ConsumerMessageListener hookConsumer;
    private BrokerStrategy strategy;

    private static Map strategyMap = TypedMap.decorate(new HashMap(), Integer.class, BrokerStrategy.class);

    static {
        strategyMap.put(AvatarMQProducerMessageStrategy, new BrokerProducerMessageStrategy());
        strategyMap.put(AvatarMQConsumerMessageStrategy, new BrokerConsumerMessageStrategy());
        strategyMap.put(AvatarMQSubscribeStrategy, new BrokerSubscribeStrategy());
        strategyMap.put(AvatarMQUnsubscribeStrategy, new BrokerUnsubscribeStrategy());
    }

    public BrokerStrategyContext(RequestMessage request, ResponseMessage response, ChannelHandlerContext channelHandler) {
        this.request = request;
        this.response = response;
        this.channelHandler = channelHandler;
    }

    public void setHookProducer(ProducerMessageListener hookProducer) {
        this.hookProducer = hookProducer;
    }

    public void setHookConsumer(ConsumerMessageListener hookConsumer) {
        this.hookConsumer = hookConsumer;
    }

    public void invoke() {
        switch (request.getMsgType()) {
            // producer 或者 consumer 发送的消息
            // source 为 AvatarMQConsumer 的消息是 consumer 接收到 producer 发送的消息并且进行处理之后，
            // 返回的 ack 消息结果
            // source 为 AvatarMQProducer 的消息是 producer 发送给 broker 的消息
            case AvatarMQMessage:
                strategy = (BrokerStrategy) strategyMap.get(request.getMsgSource() == MessageSource.AvatarMQProducer ? AvatarMQProducerMessageStrategy : AvatarMQConsumerMessageStrategy);
                break;
            // 消费者订阅某主题的消息
            case AvatarMQSubscribe:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQSubscribeStrategy);
                break;
            // 消费者取消订阅某主题的消息
            case AvatarMQUnsubscribe:
                strategy = (BrokerStrategy) strategyMap.get(AvatarMQUnsubscribeStrategy);
                break;
            default:
                break;
        }

        strategy.setChannelHandler(channelHandler);
        strategy.setHookConsumer(hookConsumer);
        strategy.setHookProducer(hookProducer);
        strategy.messageDispatch(request, response);
    }
}
