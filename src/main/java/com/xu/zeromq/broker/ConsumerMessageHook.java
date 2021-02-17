package com.xu.zeromq.broker;

import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.msg.SubscribeMessage;
import com.xu.zeromq.model.SubscriptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMessageHook implements ConsumerMessageListener {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerMessageHook.class);

    public ConsumerMessageHook() {
    }

    public void hookConsumerMessage(SubscribeMessage request, RemoteChannelData channel) {
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
}
