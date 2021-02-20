package com.xu.zeromq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckMessageCache extends MessageCache<String> {

    private CyclicBarrier barrier = null;

    public static final Logger logger = LoggerFactory.getLogger(AckMessageCache.class);

    private long successTaskCount = 0;

    private AckMessageCache() {
    }

    public long getSuccessTaskCount() {
        return successTaskCount;
    }

    private static class AckMessageCacheHolder {
        public static AckMessageCache cache = new AckMessageCache();
    }

    public static AckMessageCache getAckMessageCache() {
        return AckMessageCacheHolder.cache;
    }

    public void parallelDispatch(LinkedList<String> list) {
        List<Callable<Long>> tasks = new ArrayList<>();
        List<Future<Long>> futureList = new ArrayList<>();
        int startPosition = 0;
        Pair<Integer, Integer> pair = calculateBlocks(list.size(), list.size());
        int numberOfThreads = pair.getRight();
        int blocks = pair.getLeft();

        // 可以实现让一组线程等待至某个状态之后再全部同时执行。叫做回环是因为当所有等待线程都被释放以后，CyclicBarrier 可以被重用。
        // 我们暂且把这个状态就叫做 barrier，当调用 await 方法之后，线程就处于 barrier 了
        barrier = new CyclicBarrier(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            String[] task = new String[blocks];
            // 这里的 list 其实就是 AckMessageCache，其中缓存了 broker 收到 producer 发送过来的消息的标识，
            // 这个标识等于 msgId + @ + msgId，这里会为每一个收到的消息生成一个 ProducerAckMessage，保存到 AckTaskQueue
            // 中，然后在 AckPullMessageController 中将 ProducerAckMessage 转发给 producer，表示收到 producer 发送过来的对应消息
            System.arraycopy(list.toArray(), startPosition, task, 0, blocks);
            tasks.add(new AckMessageTask(barrier, task));
            startPosition += blocks;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        try {
            // 并发运行多个 AckMessageTask
            futureList = executor.invokeAll(tasks);
        } catch (InterruptedException ex) {
            logger.warn(ex.getMessage());
        }

        for (Future<Long> longFuture : futureList) {
            try {
                successTaskCount += longFuture.get();
            } catch (InterruptedException | ExecutionException ex) {
                logger.warn(ex.getMessage());
            }
        }
    }

}
