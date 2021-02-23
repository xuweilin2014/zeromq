package com.xu.zeromq.producer;

import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.BaseMessage;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.msg.SubscribeAckMessage;
import com.xu.zeromq.netty.AbstractHandler;
import com.xu.zeromq.netty.MessageProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// MessageProducerHandler 是 ChannelInboundHandlerAdapter 的子类，也就是只会对入站的消息进行处理
// 也就是会对 broker 发送过来的 ack 响应进行处理
public class ProducerHandler extends AbstractHandler<String> {

    private String key;

    public static final Logger logger = LoggerFactory.getLogger(ProducerHandler.class);

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
        if (!connection.traceFuture(key)) {
            return;
        }

        CallBackFuture<Object> future = connection.detachInvoker(key);

        if (future == null) {
            return;
        }

        BaseMessage message = ((ResponseMessage) msg).getMsgParams();

        // ProducerHandler 只对 ProducerAckMessage 类型的消息进行处理，
        if (message instanceof ProducerAckMessage){
            ProducerAckMessage ackMessage = (ProducerAckMessage) message;
            // 如果发生了异常，那么就把这个异常保存到 ProducerAckMessage 中
            if (this.getCause() != null) {
                logger.warn("error occurs and message is " + this.getCause().getMessage());
                ackMessage.setException(this.getCause());
                ackMessage.setStatus(SubscribeAckMessage.FAIL);
            // 把 broker 返回的 ack 属性包装到一个异常对象中，最后写入到 ProducerAckMessage 唤醒阻塞等待响应的 producer 线程
            } else if (ackMessage.getStatus() == ProducerAckMessage.FAIL){
                if (ackMessage.getAck() != null && ackMessage.getAck().length() != 0)
                    logger.warn(ackMessage.getAck());
                ackMessage.setException(new Throwable(ackMessage.getAck()));
            // 如果成功返回 ProducerAckMessage，那么直接显示日志里的信息，然后直接返回
            } else {
                logger.info(ackMessage.getAck());
                ackMessage.setStatus(ProducerAckMessage.SUCCESS);
            }

            future.setMessageResult(ackMessage);
            return;
        }

        throw new RuntimeException("wrong type message sent to producer, message type " + message.getClass().getName());
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}
