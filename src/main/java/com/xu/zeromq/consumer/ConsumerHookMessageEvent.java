package com.xu.zeromq.consumer;

import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public class ConsumerHookMessageEvent extends HookMessageEvent<Object> {

    private MessageConsumeHook hook;

    public ConsumerHookMessageEvent(MessageConsumeHook hook) {
        this.hook = hook;
    }

    public Object callBackMessage(Object obj) {
        if (obj instanceof Message) {
            // 调用 hook 的 hookMessage（由用户自己定义），来对消息进行处理
            ConsumerAckMessage result = hook.consumeMessage((Message) obj);
            // 设置 msgId 到 ConsumerAckMessage 中，然后返回
            result.setMsgId(((Message) obj).getMsgId());
            return result;
        } else {
            return null;
        }
    }
}
