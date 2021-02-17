package com.xu.zeromq.netty;

import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.core.CallBackInvoker;
import com.xu.zeromq.core.CallBackListener;
import com.xu.zeromq.core.NotifyCallback;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MessageProcessor {

    public static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private MessageConnectFactory factory = null;

    private MessageConnectPool pool = null;

    public MessageProcessor(String serverAddress) {
        MessageConnectPool.setServerAddress(serverAddress);
        // pool 是一个对象池，用来保存和复用 MessageConnectFactory 对象，MessageConnectFactory 可以用来创建到 broker 服务器的连接
        pool = MessageConnectPool.getMessageConnectPoolInstance();
        // 从对象池中获取一个 MessageConnectFactory 对象
        this.factory = pool.borrow();
    }

    public void closeMessageConnectFactory() {
        pool.restore();
    }

    public MessageConnectFactory getMessageConnectFactory() {
        return factory;
    }

    public void sendAsyncMessage(RequestMessage request, final NotifyCallback listener) {
        Channel channel = factory.getMessageChannel();
        if (channel == null) {
            return;
        }

        Map<String, CallBackInvoker<Object>> callBackMap = factory.getCallBackMap();
        final CallBackInvoker<Object> invoker = new CallBackInvoker<Object>();
        callBackMap.put(request.getMsgId(), invoker);
        invoker.setRequestId(request.getMsgId());

        // 将 CallBackListener 保存到 CallBackInvoker 中的 listeners 集合中，等到有结果之后，就会对 listeners
        // 中的 listener 进行回调
        invoker.join(new CallBackListener<Object>() {
            public void onCallBack(Object t, Throwable reason) {
                ResponseMessage response = (ResponseMessage) t;
                // 如果 reason 不为 null，就表明发生了异常，需要进行处理
                if (reason != null){
                    listener.onException((ProducerAckMessage) response.getMsgParams(), reason);
                // 否则进行正常处理
                }else {
                    // response#getMsgParams 获取到的是 ProducerAckMessage，然后进行回调处理
                    listener.onSuccess((ProducerAckMessage) response.getMsgParams());
                }
            }
        });

        ChannelFuture channelFuture = channel.writeAndFlush(request);
        channelFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    invoker.setReason(future.cause());
                }
            }
        });

    }

    // 异步发送消息
    public Object sendAsyncMessage(RequestMessage request) {
        // 从 MessageConnectFactory 中获取到和 broker 的连接 channel
        Channel channel = factory.getMessageChannel();

        if (channel == null) {
            return null;
        }

        Map<String, CallBackInvoker<Object>> callBackMap = factory.getCallBackMap();
        final CallBackInvoker<Object> invoker = new CallBackInvoker<>();
        // request 中的 msgId 其实就是消息 id
        // 消息 id 和 CallBackInvoker 一一对应，一起保存在 MessageConnectFactory 的 callBackMap 中
        callBackMap.put(request.getMsgId(), invoker);
        invoker.setRequestId(request.getMsgId());

        ChannelFuture channelFuture = channel.writeAndFlush(request);
        channelFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    invoker.setReason(future.cause());
                }
            }
        });

        try {
            Object result = invoker.getMessageResult(factory.getTimeOut(), TimeUnit.MILLISECONDS);
            callBackMap.remove(request.getMsgId());
            return result;
        } catch (RuntimeException e) {
            logger.warn(e.getMessage());
            return null;
        }
    }

    // 同步发送消息
    public void sendSyncMessage(RequestMessage request) {
        Channel channel = factory.getMessageChannel();

        if (channel == null) {
            return;
        }

        Map<String, CallBackInvoker<Object>> callBackMap = factory.getCallBackMap();
        final CallBackInvoker<Object> invoker = new CallBackInvoker<Object>();
        callBackMap.put(request.getMsgId(), invoker);
        invoker.setRequestId(request.getMsgId());

        ChannelFuture channelFuture;
        try {
            channelFuture = channel.writeAndFlush(request).sync();
            channelFuture.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) {
                    if (!future.isSuccess()) {
                        invoker.setReason(future.cause());
                    }
                }
            });
        } catch (InterruptedException ex) {
            logger.warn(ex.getMessage());
        }
    }
}
