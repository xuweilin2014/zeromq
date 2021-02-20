package com.xu.zeromq.consumer;

import io.netty.channel.ChannelHandlerContext;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.netty.AbstractHandler;
import com.xu.zeromq.netty.MessageProcessor;

@SuppressWarnings("GrazieInspection")
public class ConsumerHandler extends AbstractHandler<Object> {

    private String key;

    public ConsumerHandler(MessageProcessor processor) {
        this(processor, null);
        super.setWrapper(this);
    }

    public ConsumerHandler(MessageProcessor processor, HookMessageEvent<Object> hook) {
        super(processor, hook);
        super.setWrapper(this);
    }

    public void beforeMessage(Object msg) {
        key = ((ResponseMessage) msg).getMsgId();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
        // factory 中不包含 key 对应的 callBackFuture，并且用户定义的 hook 不为 null，才会使用 hook 来对 message 进行处理
        // 这是因为，consumer 发送 subscribe message 到 broker 之后，会在 factory 中生成一个 <msgId, CallBackFuture>
        // 映射关系，而 broker 在接收到 subscribe message 之后会返回一个 SubscribeAck，这个 ack 会直接被 consumer 忽略掉。
        //
        // 当 broker 发送 consumer 所订阅主题的消息过来时，才会进入 if 语句进行处理
        if (!factory.traceInvoker(key) && hook != null) {
            ResponseMessage message = (ResponseMessage) msg;
            // 获取用户定义的 hook 来对 message 进行处理，并且获取到处理的结果 ConsumerAckMessage
            ConsumerAckMessage result = (ConsumerAckMessage) hook.callBackMessage(message);
            // 获取到处理结果之后再发送给 broker 服务器
            if (result != null) {
                RequestMessage request = new RequestMessage();
                request.setMsgId(message.getMsgId());
                request.setMsgType(MessageType.ConsumerAck);
                request.setMsgParams(result);

                ctx.writeAndFlush(request);
            }
        }
    }
}
