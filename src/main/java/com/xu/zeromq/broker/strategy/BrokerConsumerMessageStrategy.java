package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.broker.ConsumerMessageListener;
import com.xu.zeromq.broker.ProducerMessageListener;
import com.xu.zeromq.broker.SendMessageLauncher;
import com.xu.zeromq.core.CallBackInvoker;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;

public class BrokerConsumerMessageStrategy implements BrokerStrategy {

    public BrokerConsumerMessageStrategy() {
    }

    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        String key = response.getMsgId();

        // 在 SendMessageLauncher 发送消息到 consumer 端时，会创建一个 CallBackInvoker，然后阻塞
        if (SendMessageLauncher.getInstance().trace(key)) {
            CallBackInvoker<Object> future = SendMessageLauncher.getInstance().detach(key);
            if (future != null) {
                future.setMessageResult(request);
            }
        }
    }

    public void setHookProducer(ProducerMessageListener hookProducer) {
    }

    public void setHookConsumer(ConsumerMessageListener hookConsumer) {
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
    }

}
