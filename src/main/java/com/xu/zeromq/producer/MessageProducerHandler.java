package com.xu.zeromq.producer;

import com.xu.zeromq.core.CallBackInvoker;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.MessageEventWrapper;
import com.xu.zeromq.netty.MessageProcessor;
import io.netty.channel.ChannelHandlerContext;

public class MessageProducerHandler extends MessageEventWrapper<String> {

    private String key;

    public MessageProducerHandler(MessageProcessor processor) {
        this(processor, null);
        super.setWrapper(this);
    }

    public MessageProducerHandler(MessageProcessor processor, HookMessageEvent hook) {
        super(processor, hook);
        super.setWrapper(this);
    }

    public void beforeMessage(Object msg) {
        key = ((ResponseMessage) msg).getMsgId();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        if (!factory.traceInvoker(key)) {
            return;
        }

        CallBackInvoker<Object> invoker = factory.detachInvoker(key);

        if (invoker == null) {
            return;
        }

        if (this.getCause() != null) {
            invoker.setReason(getCause());
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
