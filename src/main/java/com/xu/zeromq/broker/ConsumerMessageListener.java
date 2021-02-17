package com.xu.zeromq.broker;

import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.msg.SubscribeMessage;

public interface ConsumerMessageListener {

    void hookConsumerMessage(SubscribeMessage msg, RemoteChannelData channel);

}
