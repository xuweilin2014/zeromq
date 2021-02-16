package com.xu.zeromq.core;

import com.xu.zeromq.msg.ProducerAckMessage;

public interface NotifyCallback {

    void onSuccess(ProducerAckMessage result);

    void onException(ProducerAckMessage result, Throwable reason);
}
