package com.xu.zeromq.model;

import io.netty.channel.Channel;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * RemoteChannelData 是对连接 channel 和 客户端 id 的一个封装
 * 这里的客户端 id 是消费者的 id，也就是: clusterId + @ + topic + @ + msgId
 */
public class RemoteChannelData {

    private Channel channel;

    private String consumerId;

    private SubscriptionData subscription;

    public SubscriptionData getSubscript() {
        return subscription;
    }

    public void setSubscript(SubscriptionData subscript) {
        this.subscription = subscript;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public RemoteChannelData(Channel channel, String consumerId, SubscriptionData subscriptionData) {
        this.channel = channel;
        this.consumerId = consumerId;
        this.subscription = subscriptionData;
    }

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj != null && RemoteChannelData.class.isAssignableFrom(obj.getClass())) {
            RemoteChannelData info = (RemoteChannelData) obj;
            result = new EqualsBuilder().append(consumerId, info.getConsumerId()).isEquals();
        }
        return result;
    }

}
