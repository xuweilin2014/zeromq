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
            // SendMessageController 不断循环从 MessageTaskQueue 中取出 MessageDispatchTask，然后
            // 将其转发给客户端 consumer，并且阻塞等待客户端返回 ConsumerAckMessage
            executorService.submit(new SendMessageController());
            // 从 AckTaskQueue 中获取保存的 ProducerAckMessage，并且将其发送到 producer 端，表示 broker 已经收到消息
            executorService.submit(new AckPullMessageController());
            // 在 ProducerMessageHook 中会将 producer 发送过来的消息的 msgId 生成标识，保存到 AckMessageCache 中，另外如果
            // producer 发送过来的消息如果没有消费者进行订阅，就会跳过这一步，直接生成一个 ProducerAckMessage，保存到 AckTaskQueue
            // 中。因此，这里 AckPushMessageController 的任务就是根据 AckMessageCache 保存的消息的标识，创建
            // ProducerAckMessage，然后将其并发地将其分发到 AckTaskQueue 中，等待 AckPullMessageController 将其发送到
            // producer 端
            executorService.submit(new AckPushMessageController());
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
