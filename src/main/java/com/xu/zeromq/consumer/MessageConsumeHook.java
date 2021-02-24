package com.xu.zeromq.consumer;

import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public interface MessageConsumeHook {

    ConsumerAckMessage consumeMessage(Message paramMessage);
    
}
