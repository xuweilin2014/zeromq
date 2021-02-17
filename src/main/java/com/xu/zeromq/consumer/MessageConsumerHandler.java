package com.xu.zeromq.consumer;

import io.netty.channel.ChannelHandlerContext;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.netty.MessageEventWrapper;
import com.xu.zeromq.netty.MessageProcessor;

public class MessageConsumerHandler extends MessageEventWrapper<Object> {

    private String key;

    public MessageConsumerHandler(MessageProcessor processor) {
        this(processor, null);
        super.setWrapper(this);
    }

    public MessageConsumerHandler(MessageProcessor processor, HookMessageEvent<Object> hook) {
        super(processor, hook);
        super.setWrapper(this);
    }

    public void beforeMessage(Object msg) {
        key = ((ResponseMessage) msg).getMsgId();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        // factory 中包含 key 对应的 CallBackInvoker，并且用户定义的 hook 不为 null
        if (!factory.traceInvoker(key) && hook != null) {
            ResponseMessage message = (ResponseMessage) msg;
            // 获取用户定义的 hook 来对 message 进行处理，并且获取到处理的结果 ConsumerAckMessage
            ConsumerAckMessage result = (ConsumerAckMessage) hook.callBackMessage(message);
            // 获取到处理结果之后再发送给 broker 服务器
            if (result != null) {
                RequestMessage request = new RequestMessage();
                request.setMsgId(message.getMsgId());
                request.setMsgSource(MessageSource.AvatarMQConsumer);
                request.setMsgType(MessageType.AvatarMQMessage);
                request.setMsgParams(result);

                ctx.writeAndFlush(request);
            }
        }
    }
}
