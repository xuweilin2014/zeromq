package com.xu.zeromq.netty;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xu.zeromq.core.CallBackInvoker;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.serialize.KryoCodecUtil;
import com.xu.zeromq.serialize.KryoPoolFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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

public class MessageConnectFactory {

    public static final Logger logger = LoggerFactory.getLogger(MessageConnectFactory.class);
    // broker 服务器的地址
    private SocketAddress remoteAddr = null;

    private ChannelInboundHandlerAdapter messageHandler = null;
    // <msgId, CallBackInvoker>
    private Map<String, CallBackInvoker<Object>> callBackMap = new ConcurrentHashMap<String, CallBackInvoker<Object>>();

    private Bootstrap bootstrap = null;

    private long timeout = 10 * 1000;

    private boolean connected = false;

    private EventLoopGroup eventLoopGroup = null;

    private static KryoCodecUtil util = new KryoCodecUtil(KryoPoolFactory.getKryoPoolInstance());

    private Channel messageChannel = null;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private NettyClustersConfig nettyClustersConfig = new NettyClustersConfig();

    private ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("MessageConnectFactory-%d")
            .setDaemon(true)
            .build();

    public MessageConnectFactory(String serverAddress) {
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
                        public void initChannel(SocketChannel channel) throws Exception {
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
            connected = true;
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    public void close() {
        if (messageChannel != null) {
            try {
                messageChannel.close().sync();
                eventLoopGroup.shutdownGracefully();
                defaultEventExecutorGroup.shutdownGracefully();
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    // 查找在
    public boolean traceInvoker(String key) {
        if (key == null) {
            return false;
        }
        return getCallBackMap().containsKey(key);
    }

    public CallBackInvoker<Object> detachInvoker(String key) {
        if (traceInvoker(key)) {
            return getCallBackMap().remove(key);
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

    public Map<String, CallBackInvoker<Object>> getCallBackMap() {
        return callBackMap;
    }

    public void setCallBackMap(Map<String, CallBackInvoker<Object>> callBackMap) {
        this.callBackMap = callBackMap;
    }
}
