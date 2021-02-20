package com.xu.zeromq.model;

import com.xu.zeromq.msg.BaseMessage;
import java.io.Serializable;

public abstract class BusinessMessage implements Serializable {

    public final static int SUCCESS = 0;

    public final static int FAIL = 1;

    protected String msgId;

    protected BaseMessage msgParams;

    protected MessageType msgType;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public BaseMessage getMsgParams() {
        return msgParams;
    }

    public void setMsgParams(BaseMessage msgParams) {
        this.msgParams = msgParams;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(MessageType msgType) {
        this.msgType = msgType;
    }
}
