package com.xu.zeromq.producer;

import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.AbstractHandler;
import com.xu.zeromq.netty.MessageProcessor;
import io.netty.channel.ChannelHandlerContext;

// MessageProducerHandler 是 ChannelInboundHandlerAdapter 的子类，也就是只会对入站的消息进行处理
// 也就是会对 broker 发送过来的 ack 响应进行处理
public class ProducerHandler extends AbstractHandler<String> {

    private String key;

    public ProducerHandler(MessageProcessor processor) {
        this(processor, null);
        super.setWrapper(this);
    }

    public ProducerHandler(MessageProcessor processor, HookMessageEvent<String> hook) {
        super(processor, hook);
        super.setWrapper(this);
    }

    public void beforeMessage(Object msg) {
        key = ((ResponseMessage) msg).getMsgId();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        // 当接收到对方发送过来的响应时，先检查 CallBackFuture 这个 map 中是否存在对应的 CallBackFuture，
        // producer 每发送一次 msg 都会生成一个特定的 msgId，以及一个对应的 CallBackFuture，
        // 并且把这个 <msgId, CallBackFuture> 映射保存到 MessageConnectFactory 中
        if (!connection.traceInvoker(key)) {
            return;
        }

        CallBackFuture<Object> future = connection.detachInvoker(key);

        if (future == null) {
            return;
        }

        // 如果发生了异常，那么就把这个异常保存到 CallBackFuture 中
        if (this.getCause() != null) {
            future.setReason(getCause());
        // 把 broker 返回的 ack 消息保存到 CallBackFuture 中，唤醒阻塞等待响应的 producer 线程
        } else {
            future.setMessageResult(msg);
        }
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}
