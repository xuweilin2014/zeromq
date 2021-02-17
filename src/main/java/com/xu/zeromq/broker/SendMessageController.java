package com.xu.zeromq.broker;

import com.xu.zeromq.core.SemaphoreCache;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.core.MessageTaskQueue;
import com.xu.zeromq.core.SendMessageCache;
import com.xu.zeromq.model.MessageDispatchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 生产者消息的分派，主要是由 com.xu.zeromq.broker 包下面的 SendMessageController 派发模块进行任务的分派，
 * 其中消息分派支持两种策略，一种是内存缓冲消息区里面只要一有消息就通知消费者；还有一种是对消息进行缓冲处理，
 * 累计到一定的数量之后进行派发，这个是根据：MessageSystemConfig 类中的核心参数：SystemPropertySendMessageControllerTaskCommitValue
 * （com.xu.zeromq.system.send.taskcommit）决定的，默认是 1。即一有消息就派发，如果改成大于 1 的数值，表示消息缓冲的数量。
 */
public class SendMessageController implements Callable<Void> {

    public static final Logger logger = LoggerFactory.getLogger(SendMessageController.class);

    private volatile boolean stopped = false;

    private AtomicBoolean flushTask = new AtomicBoolean(false);

    private ThreadLocal<ConcurrentLinkedQueue<MessageDispatchTask>> requestCacheList = new ThreadLocal<ConcurrentLinkedQueue<MessageDispatchTask>>() {
        protected ConcurrentLinkedQueue<MessageDispatchTask> initialValue() {
            return new ConcurrentLinkedQueue<>();
        }
    };

    private final Timer timer = new Timer("SendMessageTaskMonitor", true);

    public void stop() {
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }

    public Void call() {
        // 每间隔 period 时间将 flushTask 设置为 true，也就是控制消息派发的速率
        int period = MessageSystemConfig.SendMessageControllerPeriodTimeValue;
        // 决定消息是否到达之后立即进行派发，还是累积到一定程度之后再进行派发
        // 如果 commitNumber 为 1 的话，消息到达之后立即进行派发；如果大于 1 的话，表示先对消息进行缓冲，
        // 累积到一定数目之后再进行派发
        int commitNumber = MessageSystemConfig.SendMessageControllerTaskCommitValue;
        // 当没有消息到达时，会睡眠 sleepTime 时间，然后再去检查消息
        int sleepTime = MessageSystemConfig.SendMessageControllerTaskSleepTimeValue;

        ConcurrentLinkedQueue<MessageDispatchTask> queue = requestCacheList.get();
        SendMessageCache ref = SendMessageCache.getInstance();

        while (!stopped) {
            SemaphoreCache.acquire(MessageSystemConfig.NotifyTaskSemaphoreValue);
            // 消息是以 MessageDispatchTask 为载体的，MessageDispatchTask 代表了消息的主题 topic，message，消息集群的 id
            MessageDispatchTask task = MessageTaskQueue.getInstance().getTask();
            queue.add(task);

            if (queue.size() == 0) {
                try {
                    Thread.sleep(sleepTime);
                    continue;
                } catch (InterruptedException ex) {
                    logger.warn(ex.getMessage());
                }
            }

            // 在以下 3 个条件达到时，开始进行消息派发：
            // 1.消息到达时立即进行派发，并且有消息存在于队列中
            // 2.消息累积到一定数目之后才进行派发，并且消息累积数目达到要求
            // 3.消息还没有累积达到要求，但是 flushTask 变为了 true，也就是必须要进行派发
            if (queue.size() > 0 && (queue.size() % commitNumber == 0 || flushTask.get())) {
                // queue 中可能累积了多条消息，会一起进行派发
                ref.commit(queue);
                queue.clear();
                flushTask.compareAndSet(true, false);
            }

            // flushTask 用来控制进行消息派发的速率，只要 flushTask 为 true，并且有消息，那么就一定会进行消息派发
            // 每隔 period，会将 flushTask 设置成 true
            timer.scheduleAtFixedRate(new TimerTask() {
                public void run() {
                    try {
                        flushTask.compareAndSet(false, true);
                    } catch (Exception e) {
                        logger.warn("SendMessageTaskMonitor happen exception");
                    }
                }
            }, 1000, period);
        }

        return null;
    }
}
