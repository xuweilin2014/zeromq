package com.xu.zeromq.core;

import java.util.concurrent.Semaphore;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MessageCache<T> {

    private ConcurrentLinkedQueue<T> cache = new ConcurrentLinkedQueue<T>();

    private Semaphore semaphore = new Semaphore(0);

    public void appendMessage(T id) {
        cache.add(id);
        semaphore.release();
    }

    public void parallelDispatch(LinkedList<T> list) {
    }

    public void commit(ConcurrentLinkedQueue<T> tasks) {
        commitMessage(tasks);
    }

    public void commit() {
        commitMessage(cache);
    }

    @SuppressWarnings("CollectionAddAllCanBeReplacedWithConstructor")
    private void commitMessage(ConcurrentLinkedQueue<T> messages) {
        LinkedList<T> list = new LinkedList<T>();

        // 将 queue 中的累积的多条消息都添加到 list 中，然后进行派发
        list.addAll(messages);
        cache.clear();

        if (list.size() > 0) {
            parallelDispatch(list);
            list.clear();
        }

    }

    public boolean hold(long timeout) {
        try {
            return semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Logger.getLogger(MessageCache.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    protected Pair<Integer, Integer> calculateBlocks(int parallel, int sizeOfTasks) {
        int numberOfThreads = Math.min(parallel, sizeOfTasks);
        Pair<Integer, Integer> pair = new MutablePair<>(sizeOfTasks / numberOfThreads, numberOfThreads);
        return pair;
    }
}
