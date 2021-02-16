package com.xu.zeromq.core;

import com.xu.zeromq.model.MessageDispatchTask;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageTaskQueue {

    private static AtomicBoolean isInit = new AtomicBoolean(false);

    private static ConcurrentLinkedQueue<MessageDispatchTask> taskQueue = null;

    private volatile static MessageTaskQueue task = null;

    private MessageTaskQueue() {
    }

    public static MessageTaskQueue getInstance() {
        if (isInit.compareAndSet(false, true)) {
            taskQueue = new ConcurrentLinkedQueue<MessageDispatchTask>();
            task = new MessageTaskQueue();
        }
        return task;
    }

    public boolean pushTask(List<MessageDispatchTask> tasks) {
        return taskQueue.addAll(tasks);
    }

    public MessageDispatchTask getTask() {
        return taskQueue.poll();
    }
}
