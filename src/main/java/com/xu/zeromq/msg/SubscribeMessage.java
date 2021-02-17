package com.xu.zeromq.msg;

import java.io.Serializable;

public class SubscribeMessage extends BaseMessage implements Serializable {

    private String clusterId;
    private String topic;
    private String consumerId;

    public SubscribeMessage() {
        super();
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
