package com.xu.zeromq.broker.strategy;

import com.xu.zeromq.broker.SendMessageLauncher;
import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;

public class ConsumerStrategy implements Strategy {

    public ConsumerStrategy() {
    }

    // broker 处理 consumer 消费完消息之后返回的 ConsumerAck 消息
    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        String key = response.getMsgId();

        // 在 SendMessageLauncher 发送消息到 consumer 端时，会创建一个 CallBackFuture，然后阻塞
        // 当 consumer 接收到发送过去的消息，并且进行了处理之后，就会返回一个 ConsumerAckMessage，然后
        // 在这里被接收到，然后唤醒前面阻塞在 CallBackFuture 中的线程
        if (SendMessageLauncher.getInstance().trace(key)) {
            CallBackFuture<Object> future = SendMessageLauncher.getInstance().detach(key);
            if (future != null) {
                // 唤醒阻塞的 CallBackFuture 线程
                future.setMessageResult(request);
            }
        }
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
    }

}
