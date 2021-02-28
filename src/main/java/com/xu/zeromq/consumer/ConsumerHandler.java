package com.xu.zeromq.consumer;

import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.msg.BaseMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.SubscribeAckMessage;
import com.xu.zeromq.netty.Connection;
import io.netty.channel.ChannelHandlerContext;
import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.core.HookMessageEvent;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.netty.AbstractHandler;
import com.xu.zeromq.netty.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ConsumerHandler extends AbstractHandler<Object> {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerHandler.class);

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

    public void handleMessage(final ChannelHandlerContext ctx, final Object msg) {

        final Throwable cause = this.getCause();
        // 将对消息的具体处理放入到线程池中去处理，这样就不会阻塞掉 netty 的 io 线程
        executor.submit(new Runnable() {
            @Override
            public void run() {
                BaseMessage message = ((ResponseMessage) msg).getMsgParams();

                // 如果是 broker 发送过来的 Message
                if (message instanceof Message && hook != null) {
                    // 获取用户定义的 hook 来对 message 进行处理，并且获取到处理的结果 ConsumerAckMessage
                    ConsumerAckMessage result = (ConsumerAckMessage) hook.callBackMessage(message);
                    // 获取到处理结果之后再发送给 broker 服务器
                    if (result != null) {
                        RequestMessage request = new RequestMessage();
                        request.setMsgId(((Message) message).getMsgId());
                        request.setMsgType(MessageType.ConsumerAck);
                        request.setMsgParams(result);

                        ctx.writeAndFlush(request);
                    }
                }

                // 如果是 broker 对 consumer 订阅消息的响应
                if (message instanceof SubscribeAckMessage){
                    SubscribeAckMessage ackMessage = (SubscribeAckMessage) message;
                    Connection connection = processor.getConnection();
                    String msgId = ackMessage.getMsgId();
                    // 获取到阻塞的 CallBackFuture 对象
                    CallBackFuture<Object> future = connection.getFutureMap().get(msgId);

                    if (future == null){
                        logger.warn("request " + msgId + " is removed from the futureMap");
                        return;
                    }

                    if (cause != null){
                        logger.warn("error occurs and message is " + cause.getMessage());
                        ackMessage.setException(cause);
                        ackMessage.setStatus(SubscribeAckMessage.FAIL);
                        // 如果订阅不成功的话，就打印相关信息，并且将异常设置到 future 中，唤醒阻塞的线程
                    } else if (ackMessage.getStatus() == SubscribeAckMessage.FAIL){
                        logger.warn(ackMessage.getAck());
                        Throwable t = new Throwable(ackMessage.getAck());
                        ackMessage.setException(t);
                        // 如果订阅成功的话，打印信息，同样将结果设置到 future 中，唤醒阻塞的线程
                    }else {
                        logger.info(ackMessage.getAck());
                        ackMessage.setStatus(SubscribeAckMessage.SUCCESS);
                    }

                    future.setMessageResult(ackMessage);
                }
            }
        });
    }


}
