package com.xu.zeromq.core;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.xu.zeromq.netty.NettyClustersConfig;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SemaphoreCache {

    private final static int hookTime = MessageSystemConfig.SemaphoreCacheHookTimeValue;

    private static final LoadingCache<String, Semaphore> cache = CacheBuilder.newBuilder().
            concurrencyLevel(NettyClustersConfig.getWorkerThreads()).
            build(new CacheLoader<String, Semaphore>() {
                public Semaphore load(String input) throws Exception {
                    return new Semaphore(0);
                }
            });

    public static int getAvailablePermits(String key) {
        try {
            return cache.get(key).availablePermits();
        } catch (ExecutionException ex) {
            Logger.getLogger(SemaphoreCache.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
    }

    public static void release(String key) {
        try {
            cache.get(key).release();
            TimeUnit.MILLISECONDS.sleep(hookTime);
        } catch (ExecutionException ex) {
            Logger.getLogger(SemaphoreCache.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(SemaphoreCache.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void acquire(String key) {
        try {
            cache.get(key).acquire();
            TimeUnit.MILLISECONDS.sleep(hookTime);
        } catch (InterruptedException ex) {
            Logger.getLogger(SemaphoreCache.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(SemaphoreCache.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
