package com.xu.zeromq.broker.server;

import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.xu.zeromq.broker.AckPullMessageController;
import com.xu.zeromq.broker.AckPushMessageController;
import com.xu.zeromq.broker.SendMessageController;
import com.xu.zeromq.netty.NettyClustersConfig;

public class BrokerParallelServer implements RemotingServer {

    // parallel 是核心线程数，也就是处理器的数目 * 2
    protected int parallel = NettyClustersConfig.getWorkerThreads();

    protected ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parallel));

    protected ExecutorCompletionService<Void> executorService;

    public BrokerParallelServer() {
    }

    public void init() {
        executorService = new ExecutorCompletionService<>(executor);
    }

    public void start() {
        for (int i = 0; i < parallel; i++) {
            executorService.submit(new SendMessageController());
            executorService.submit(new AckPullMessageController());
            executorService.submit(new AckPushMessageController());
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
