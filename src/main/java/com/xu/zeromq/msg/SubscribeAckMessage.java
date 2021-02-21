package com.xu.zeromq.msg;

import java.io.Serializable;

public class SubscribeAckMessage extends BaseMessage implements Serializable {

    private String ack;

    private int status;

    private String msgId;

    public String getAck() {
        return ack;
    }

    public void setAck(String ack) {
        this.ack = ack;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}
