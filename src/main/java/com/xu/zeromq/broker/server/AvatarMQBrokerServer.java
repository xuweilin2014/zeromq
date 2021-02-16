package com.xu.zeromq.broker.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xu.zeromq.broker.ConsumerMessageHook;
import com.xu.zeromq.broker.MessageBrokerHandler;
import com.xu.zeromq.broker.ProducerMessageHook;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.netty.MessageObjectDecoder;
import com.xu.zeromq.netty.MessageObjectEncoder;
import com.xu.zeromq.netty.NettyClustersConfig;
import com.xu.zeromq.netty.NettyUtil;
import com.xu.zeromq.serialize.KryoCodecUtil;
import com.xu.zeromq.serialize.KryoPoolFactory;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvatarMQBrokerServer extends BrokerParallelServer implements RemotingServer {

    public static final Logger logger = LoggerFactory.getLogger(AvatarMQBrokerServer.class);

    private ThreadFactory threadBossFactory = new ThreadFactoryBuilder()
            .setNameFormat("ZeroMQBroker[BossSelector]-%d")
            .setDaemon(true)
            .build();

    private ThreadFactory threadWorkerFactory = new ThreadFactoryBuilder()
            .setNameFormat("ZeroMQBroker[WorkerSelector]-%d")
            .setDaemon(true)
            .build();

    private int brokerServerPort = 0;
    private ServerBootstrap bootstrap;
    private MessageBrokerHandler handler;
    private SocketAddress serverIpAddr;
    private NettyClustersConfig nettyClustersConfig = new NettyClustersConfig();
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private EventLoopGroup boss;
    private EventLoopGroup workers;

    public AvatarMQBrokerServer(String serverAddress) {
        String[] ipAddr = serverAddress.split(MessageSystemConfig.IpV4AddressDelimiter);

        if (ipAddr.length == 2) {
            serverIpAddr = NettyUtil.string2SocketAddress(serverAddress);
        }
    }

    public void init() {
        try {
            handler = new MessageBrokerHandler().buildConsumerHook(new ConsumerMessageHook()).buildProducerHook(new ProducerMessageHook());

            boss = new NioEventLoopGroup(1, threadBossFactory);
            workers = new NioEventLoopGroup(parallel, threadWorkerFactory, NettyUtil.getNioSelectorProvider());

            final KryoCodecUtil util = new KryoCodecUtil(KryoPoolFactory.getKryoPoolInstance());

            bootstrap = new ServerBootstrap();

            bootstrap.group(boss, workers).channel(NioServerSocketChannel.class)
                     .option(ChannelOption.SO_BACKLOG, 1024)
                     .option(ChannelOption.SO_REUSEADDR, true)
                     .option(ChannelOption.SO_KEEPALIVE, false)
                     .childOption(ChannelOption.TCP_NODELAY, true)
                     .option(ChannelOption.SO_SNDBUF, nettyClustersConfig.getClientSocketSndBufSize())
                     .option(ChannelOption.SO_RCVBUF, nettyClustersConfig.getClientSocketRcvBufSize())
                     .handler(new LoggingHandler(LogLevel.INFO))
                     .localAddress(serverIpAddr)
                     .childHandler(new ChannelInitializer<SocketChannel>() {
                         public void initChannel(SocketChannel ch) {
                             ch.pipeline().addLast(
                                    defaultEventExecutorGroup,
                                    new MessageObjectEncoder(util),
                                    new MessageObjectDecoder(util),
                                    handler);
                         }
                     });
            // 创建 executorService
            super.init();
        } catch (IOException ex) {
            logger.warn(ex.getMessage());
        }
    }

    public int localListenPort() {
        return brokerServerPort;
    }

    public void shutdown() {
        try {
            super.shutdown();
            boss.shutdownGracefully();
            workers.shutdownGracefully();
            defaultEventExecutorGroup.shutdownGracefully();
        } catch (Exception e) {
            logger.warn("ZeroMQ broker server shutdown exception!");
            logger.warn(e.getMessage());
        }
    }

    public void start() {
        try {
            String ipAddress = NettyUtil.socketAddress2String(serverIpAddr);
            logger.info("broker server ip:[" + ipAddress + "]");
            ChannelFuture sync = this.bootstrap.bind().sync();
            super.start();
            sync.channel().closeFuture().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            brokerServerPort = addr.getPort();
        } catch (InterruptedException ex) {
            logger.warn(ex.getMessage());
        }
    }
}
