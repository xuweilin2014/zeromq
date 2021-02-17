package com.xu.zeromq.consumer;

import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.BaseMessage;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public class ConsumerHookMessageEvent extends HookMessageEvent<Object> {

    private ProducerMessageHook hook;

    public ConsumerHookMessageEvent(ProducerMessageHook hook) {
        this.hook = hook;
    }

    public Object callBackMessage(Object obj) {
        ResponseMessage response = (ResponseMessage) obj;
        if (response.getMsgParams() instanceof Message) {
            // 调用 hook 的 hookMessage（由用户自己定义），来对消息进行处理
            ConsumerAckMessage result = hook.hookMessage((Message) response.getMsgParams());
            // 设置 msgId 到 ConsumerAckMessage 中，然后返回
            result.setMsgId(((Message) response.getMsgParams()).getMsgId());
            return result;
        } else {
            return null;
        }
    }
}
