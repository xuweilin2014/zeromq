package com.xu.zeromq.model;

import io.netty.channel.Channel;
import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * RemoteChannelData 是对连接 channel 和 客户端 id 的一个封装
 */
public class RemoteChannelData {

    private Channel channel;

    private String clientId;

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

    public String getClientId() {
        return clientId;
    }

    public RemoteChannelData(Channel channel, String clientId) {
        this.channel = channel;
        this.clientId = clientId;
    }

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj != null && RemoteChannelData.class.isAssignableFrom(obj.getClass())) {
            RemoteChannelData info = (RemoteChannelData) obj;
            result = new EqualsBuilder().append(clientId, info.getClientId()).isEquals();
        }
        return result;
    }

}
