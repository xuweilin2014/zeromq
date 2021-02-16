package com.xu.zeromq.msg;

import java.io.Serializable;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Message extends BaseMessage implements Serializable {
    // 消息 id
    private String msgId;
    // 消息的主题
    private String topic;
    // 消息体
    private byte[] body;
    // 时间戳
    private long timeStamp;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String toString() {
        ReflectionToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        return ReflectionToStringBuilder.toStringExclude(this, "body");
    }

    public int hashCode() {
        return new HashCodeBuilder(11, 23).append(msgId).toHashCode();
    }

    public boolean equals(Object obj) {
        boolean result = false;
        if (obj != null && Message.class.isAssignableFrom(obj.getClass())) {
            Message msg = (Message) obj;
            result = new EqualsBuilder().append(topic, msg.getTopic()).append(body, msg.getBody())
                    .isEquals();
        }
        return result;
    }
}
