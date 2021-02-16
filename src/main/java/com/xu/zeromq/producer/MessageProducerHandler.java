package com.xu.zeromq.producer;

import com.xu.zeromq.core.CallBackInvoker;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.MessageEventWrapper;
import com.xu.zeromq.netty.MessageProcessor;
import io.netty.channel.ChannelHandlerContext;

// MessageProducerHandler 是 ChannelInboundHandlerAdapter 的子类，也就是只会对入站的消息进行处理
// 也就是会对 broker 发送过来的 ack 响应进行处理
public class MessageProducerHandler extends MessageEventWrapper<String> {

    private String key;

    public MessageProducerHandler(MessageProcessor processor) {
        this(processor, null);
        super.setWrapper(this);
    }

    public MessageProducerHandler(MessageProcessor processor, HookMessageEvent<String> hook) {
        super(processor, hook);
        super.setWrapper(this);
    }

    public void beforeMessage(Object msg) {
        key = ((ResponseMessage) msg).getMsgId();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        // 当接收到对方发送过来的响应时，先检查 CallBack 这个 map 中是否存在 CallBackInvoker，
        // producer 每发送一次 msg 都会生成一个特定的 msgId，以及一个对应的 CallBackInvoker，
        // 并且把这个 <msgId, CallBackInvoker> 映射保存到 MessageConnectFactory 中
        if (!factory.traceInvoker(key)) {
            return;
        }

        CallBackInvoker<Object> invoker = factory.detachInvoker(key);

        if (invoker == null) {
            return;
        }

        // 如果发生了异常，那么就把这个异常保存到 CallBackInvoker 中
        if (this.getCause() != null) {
            invoker.setReason(getCause());
        // 把 broker 返回的 ack 消息保存到 CallBackInvoker 中
        } else {
            invoker.setMessageResult(msg);
        }
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (hook != null) {
            hook.disconnect(ctx.channel().remoteAddress().toString());
        }
        super.channelInactive(ctx);
    }
}
