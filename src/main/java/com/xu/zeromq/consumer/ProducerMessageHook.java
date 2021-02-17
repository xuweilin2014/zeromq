package com.xu.zeromq.consumer;

import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;

public interface ProducerMessageHook {

    ConsumerAckMessage hookMessage(Message paramMessage);
}
