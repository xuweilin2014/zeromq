package com.xu.zeromq.netty;

import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.core.CallBackListener;
import com.xu.zeromq.core.NotifyCallback;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.netty.pool.MessageConnectionPool;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MessageProcessor {

    public static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private Connection connection;

    private String address;

    public MessageProcessor(String serverAddress) {
        this.address = serverAddress;
        // pool 是一个对象池，用来保存和复用 MessageConnectFactory 对象，MessageConnectFactory 可以用来创建到 broker 服务器的连接
        // 从对象池中获取一个 MessageConnectFactory 对象
        this.connection = MessageConnectionPool.borrowConnection(serverAddress);
    }

    public void returnConnection() {
        MessageConnectionPool.returnConnection(address, connection);
    }

    public Connection getConnection() {
        return connection;
    }

    public void sendAsyncMessage(RequestMessage request, final NotifyCallback listener) {
        Channel channel = connection.getMessageChannel();
        if (channel == null) {
            return;
        }

        Map<String, CallBackFuture<Object>> futureMap = connection.getFutureMap();
        final CallBackFuture<Object> callBackFuture = new CallBackFuture<Object>();
        futureMap.put(request.getMsgId(), callBackFuture);
        callBackFuture.setRequestId(request.getMsgId());

        // 将 CallBackListener 保存到 CallBackFuture 中的 listeners 集合中，等到 broker 返回结果之后，就会对 listeners
        // 中的 listener 进行回调
        callBackFuture.addListener(new CallBackListener<Object>() {
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
                    callBackFuture.setReason(future.cause());
                }
            }
        });

    }

    // 同步发送消息
    public Object sendSyncMessage(RequestMessage request) {
        // 从 MessageConnectFactory 中获取到和 broker 的连接 channel
        Channel channel = connection.getMessageChannel();

        if (channel == null) {
            return null;
        }

        Map<String, CallBackFuture<Object>> futureMap = connection.getFutureMap();
        final CallBackFuture<Object> callBackFuture = new CallBackFuture<>();
        // request 中的 msgId 其实就是消息 id
        // 消息 id 和 callBackFuture 一一对应，一起保存在 MessageConnectFactory 的 futureMap 中
        futureMap.put(request.getMsgId(), callBackFuture);
        callBackFuture.setRequestId(request.getMsgId());

        ChannelFuture channelFuture = channel.writeAndFlush(request);
        channelFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
                if (!future.isSuccess()) {
                    callBackFuture.setReason(future.cause());
                }
            }
        });

        try {
            // 阻塞等待 broker 的响应
            Object result = callBackFuture.getMessageResult(connection.getTimeOut(), TimeUnit.MILLISECONDS);
            futureMap.remove(request.getMsgId());
            return result;
        } catch (RuntimeException e) {
            logger.warn(e.getMessage());
            return null;
        }
    }
}
