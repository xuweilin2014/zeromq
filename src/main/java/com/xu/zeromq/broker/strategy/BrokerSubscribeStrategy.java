package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.*;
import com.xu.zeromq.msg.SubscribeMessage;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerSubscribeStrategy implements BrokerStrategy {

    public static final Logger logger = LoggerFactory.getLogger(BrokerSubscribeStrategy.class);

    private ChannelHandlerContext channelHandler;

    public BrokerSubscribeStrategy() {
    }

    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        SubscribeMessage subscription = (SubscribeMessage) request.getMsgParams();
        String clientKey = subscription.getConsumerId();
        RemoteChannelData channel = new RemoteChannelData(channelHandler.channel(), clientKey);
        // 将对应 RemoteChannelData 和 SubscribeMessage 信息保存在 broker 服务器端
        handleSubscribeMessage(subscription, channel);
        // 接收到 consumer 发送过来的 subscribe 消息之后，返回 consumer ack 消息
        response.setMsgType(MessageType.SubscribeAck);
        channelHandler.writeAndFlush(response);
    }

    public void handleSubscribeMessage(SubscribeMessage request, RemoteChannelData channel) {
        logger.info("receive subscription info groupId:" + request.getClusterId() + " topic:" + request.getTopic() + " clientId:" + channel.getClientId());

        SubscriptionData subscription = new SubscriptionData();
        subscription.setTopic(request.getTopic());
        // 将 SubscriptionData 保存到 RemoteChannelData 中，RemoteChannelData 可以看成是一个 <channel, clientId, subscriptionData>
        // 三个对象的封装
        channel.setSubscript(subscription);
        // 将 <clientId, RemoteChannelData> 保存在 channelMap 中
        // 将 RemoteChannelData 保存在 channelList 中
        // 将 <topic, SubscriptionData> 保存到 subMap 中
        ConsumerContext.addClusters(request.getClusterId(), channel);
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
        this.channelHandler = channelHandler;
    }

}
