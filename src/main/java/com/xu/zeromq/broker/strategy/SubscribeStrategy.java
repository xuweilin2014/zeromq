package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.*;
import com.xu.zeromq.msg.SubscribeAckMessage;
import com.xu.zeromq.msg.SubscribeMessage;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscribeStrategy implements Strategy {

    public static final Logger logger = LoggerFactory.getLogger(SubscribeStrategy.class);

    private ChannelHandlerContext channelHandler;

    public SubscribeStrategy() {
    }

    // 对 consumer 发送过来的订阅消息进行处理
    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        SubscribeMessage subMsg = (SubscribeMessage) request.getMsgParams();

        // 获取消费者的标识
        String consumerId = subMsg.getConsumerId();
        SubscriptionData subData = new SubscriptionData();
        subData.setTopic(subMsg.getTopic());

        // 将 SubscriptionData 保存到 RemoteChannelData 中
        // RemoteChannelData 可以看成是一个 <channel, consumerId, subscriptionData> 三个对象的封装
        RemoteChannelData channel = new RemoteChannelData(channelHandler.channel(), consumerId, subData);
        logger.info("receive subscription info groupId:" + subMsg.getClusterId() + " topic:" + subMsg.getTopic() + " clientId:" +
                channel.getConsumerId());

        // 将对应 RemoteChannelData 和 SubscribeMessage 信息保存在 broker 服务器端
        // 将 <consumerId, RemoteChannelData> 保存在 channelMap 中
        // 将 RemoteChannelData 保存在 channelList 中
        // 将 <topic, SubscriptionData> 保存到 subMap 中
        SubscribeAckMessage subscribeAck = ConsumerContext.addClusters(subMsg.getClusterId(), channel, request.getMsgId());

        response.setMsgParams(subscribeAck);
        // 接收到 consumer 发送过来的 subscribe 消息之后，返回 SubscribeAck 消息
        response.setMsgType(MessageType.SubscribeAck);
        channelHandler.writeAndFlush(response);
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
        this.channelHandler = channelHandler;
    }

}
