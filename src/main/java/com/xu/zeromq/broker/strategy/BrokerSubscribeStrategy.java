package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.broker.ConsumerMessageListener;
import com.xu.zeromq.broker.ProducerMessageListener;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.msg.SubscribeMessage;
import io.netty.channel.ChannelHandlerContext;

public class BrokerSubscribeStrategy implements BrokerStrategy {

    private ConsumerMessageListener hookConsumer;
    private ChannelHandlerContext channelHandler;

    public BrokerSubscribeStrategy() {
    }

    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        SubscribeMessage subscription = (SubscribeMessage) request.getMsgParams();
        String clientKey = subscription.getConsumerId();
        RemoteChannelData channel = new RemoteChannelData(channelHandler.channel(), clientKey);
        // 将对应 RemoteChannelData 和 SubscribeMessage 信息保存在 broker 服务器端
        hookConsumer.hookConsumerMessage(subscription, channel);
        // 接收到 consumer 发送过来的 subscribe 消息之后，返回 consumer ack 消息
        response.setMsgType(MessageType.AvatarMQConsumerAck);
        channelHandler.writeAndFlush(response);
    }

    public void setHookConsumer(ConsumerMessageListener hookConsumer) {
        this.hookConsumer = hookConsumer;
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
        this.channelHandler = channelHandler;
    }

    public void setHookProducer(ProducerMessageListener hookProducer) {
    }

}
