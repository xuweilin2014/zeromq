package com.xu.zeromq.core;

import com.xu.zeromq.model.MessageDispatchTask;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import org.apache.commons.lang3.tuple.Pair;

public class SendMessageCache extends MessageCache<MessageDispatchTask> {

    private Phaser phaser = new Phaser(0);

    private SendMessageCache() {

    }

    private static SendMessageCache cache;

    public synchronized static SendMessageCache getInstance() {
        if (cache == null) {
            cache = new SendMessageCache();
        }
        return cache;
    }

    // 并发地对消息进行派发
    public void parallelDispatch(LinkedList<MessageDispatchTask> list) {

        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        int startPosition = 0;
        Pair<Integer, Integer> pair = calculateBlocks(list.size(), list.size());
        // 并发所需要的线程数
        int numberOfThreads = pair.getRight();
        // 每一个线程负责的消息条数
        int blocks = pair.getLeft();

        for (int i = 0; i < numberOfThreads; i++) {
            MessageDispatchTask[] task = new MessageDispatchTask[blocks];
            phaser.register();
            // 将 list 数组中从 startPosition 开始的 blocks 个 MessageDispatchTask 对象拷贝到
            // task 数组中（从 0 开始，并且 task 数组的大小恰好是 blocks 个 MessageDispatchTask 对象）
            System.arraycopy(list.toArray(), startPosition, task, 0, blocks);
            // 一个 SendMessageTask 会负责 blocks 条消息的发送
            tasks.add(new SendMessageTask(phaser, task));
            startPosition += blocks;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        for (Callable<Void> element : tasks) {
            executor.submit(element);
        }

    }
}
