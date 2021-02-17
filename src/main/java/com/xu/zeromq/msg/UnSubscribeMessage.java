package com.xu.zeromq.msg;

import java.io.Serializable;

public class UnSubscribeMessage extends BaseMessage implements Serializable {

    private String consumerId;

    public UnSubscribeMessage(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
