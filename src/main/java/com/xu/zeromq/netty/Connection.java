package com.xu.zeromq.netty;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xu.zeromq.core.CallBackFuture;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.serialize.KryoCodecUtil;
import com.xu.zeromq.serialize.KryoPoolFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

// 表示从 consumer 或者 producer 到 broker 服务器建立的一条连接
public class Connection {

    public static final Logger logger = LoggerFactory.getLogger(Connection.class);
    // broker 服务器的地址
    private SocketAddress remoteAddr = null;

    private ChannelInboundHandlerAdapter messageHandler = null;
    // <msgId, CallBackFuture>，producer 和 consumer 在这条连接上发送的请求，这些请求还没有得到响应
    private Map<String, CallBackFuture<Object>> futureMap = new ConcurrentHashMap<>();

    private Bootstrap bootstrap = null;

    private long timeout = 10 * 1000;

    private volatile boolean closed = false;

    private EventLoopGroup eventLoopGroup = null;

    private static KryoCodecUtil util = new KryoCodecUtil(KryoPoolFactory.getKryoPoolInstance());

    private Channel messageChannel = null;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private NettyClustersConfig nettyClustersConfig = new NettyClustersConfig();

    private ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("MessageConnectFactory-%d")
            .setDaemon(true)
            .build();

    public Connection(String serverAddress) {
        // 初始化远程地址，也就是 Broker 服务器的地址
        String[] ipAddr = serverAddress.split(MessageSystemConfig.IpV4AddressDelimiter);
        if (ipAddr.length == 2) {
            remoteAddr = NettyUtil.string2SocketAddress(serverAddress);
        }
    }

    public void setMessageHandler(ChannelInboundHandlerAdapter messageHandler) {
        // 这里的 messageHandler 既可以是 MessageProducerHandler，也可以是 MessageConsumerHandler
        this.messageHandler = messageHandler;
    }

    public void init() {
        try {
            defaultEventExecutorGroup = new DefaultEventExecutorGroup(NettyClustersConfig.getWorkerThreads(), threadFactory);
            eventLoopGroup = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        public void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(defaultEventExecutorGroup);
                            channel.pipeline().addLast(new MessageObjectEncoder(util));
                            channel.pipeline().addLast(new MessageObjectDecoder(util));
                            channel.pipeline().addLast(messageHandler);
                        }
                    })
                    .option(ChannelOption.SO_SNDBUF, nettyClustersConfig.getClientSocketSndBufSize())
                    .option(ChannelOption.SO_RCVBUF, nettyClustersConfig.getClientSocketRcvBufSize())
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false);
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
        }
    }

    public void connect() {
        Preconditions.checkNotNull(messageHandler, "Message's Handler is Null!");

        try {
            // 对 netty 进行初始化设置
            init();
            // 尝试进行连接到 broker 服务器
            ChannelFuture channelFuture = bootstrap.connect(this.remoteAddr).sync();
            channelFuture.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) throws Exception {
                    messageChannel = future.channel();
                }
            });

            logger.info("ip address:" + this.remoteAddr.toString());
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    public void close() {
        if (closed)
            return;

        if (messageChannel != null) {
            try {
                messageChannel.close().sync();
                eventLoopGroup.shutdownGracefully();
                defaultEventExecutorGroup.shutdownGracefully();
                closed = true;
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }
    }

    public boolean isConnected() {
        if (isClosed() || !messageChannel.isActive() || !messageChannel.isOpen())
            return false;
        return true;
    }

    public boolean traceFuture(String key) {
        if (key == null || key.length() == 0) {
            return false;
        }
        return getFutureMap().containsKey(key);
    }

    public CallBackFuture<Object> detachInvoker(String key) {
        if (traceFuture(key)) {
            return getFutureMap().remove(key);
        } else {
            return null;
        }
    }

    public void setTimeOut(long timeout) {
        this.timeout = timeout;
    }

    public long getTimeOut() {
        return this.timeout;
    }

    public Channel getMessageChannel() {
        return messageChannel;
    }

    public Map<String, CallBackFuture<Object>> getFutureMap() {
        return futureMap;
    }

    public void setFutureMap(Map<String, CallBackFuture<Object>> callBackMap) {
        this.futureMap = callBackMap;
    }

    public boolean isClosed(){
        return closed;
    }

}
