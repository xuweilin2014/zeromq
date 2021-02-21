package com.xu.zeromq.msg;

import java.io.Serializable;

public class UnSubscribeMessage extends BaseMessage implements Serializable {

    private String consumerId;

    private String clusterId;

    public UnSubscribeMessage(String consumerId, String clusterId) {
        this.consumerId = consumerId;
        this.clusterId = clusterId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
