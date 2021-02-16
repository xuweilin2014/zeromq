package com.xu.zeromq.broker;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.List;

import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.core.SemaphoreCache;
import com.xu.zeromq.consumer.ConsumerClusters;
import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.core.AckMessageCache;
import com.xu.zeromq.core.AckTaskQueue;
import com.xu.zeromq.core.MessageTaskQueue;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.model.MessageDispatchTask;
import com.xu.zeromq.core.ChannelCache;
import org.apache.commons.collections.Closure;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.ClosureUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.AnyPredicate;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMessageHook implements ProducerMessageListener {

    public static final Logger logger = LoggerFactory.getLogger(ProducerMessageHook.class);

    private List<ConsumerClusters> clustersSet = new ArrayList<ConsumerClusters>();

    private List<ConsumerClusters> focusTopicGroup = null;

    private void filterByTopic(final String topic) {
        // 谓词，用来判断某一个消费者集群是否关注了对应的 topic
        Predicate focusAllPredicate = new Predicate() {
            public boolean evaluate(Object object) {
                ConsumerClusters clusters = (ConsumerClusters) object;
                return clusters.findSubscriptionData(topic) != null;
            }
        };

        // AnyPredicate 实现了 Predicate 接口，在调用其 evaluate 方法时，会遍历 iPredicates 数组中的
        // 每一个 iPredicate 类对象，分别调用其 evaluate 方法，只要有一个返回 true，那么最后就会直接返回 true
        AnyPredicate any = new AnyPredicate(new Predicate[]{focusAllPredicate});

        Closure trueClosure = new Closure() {
            public void execute(Object input) {
                if (input instanceof ConsumerClusters) {
                    ConsumerClusters clusters = (ConsumerClusters) input;
                    clustersSet.add(clusters);
                }
            }
        };

        Closure falseClosure = new Closure() {
            public void execute(Object input) {
            }
        };

        Closure ifClosure = ClosureUtils.ifClosure(any, trueClosure, falseClosure);
        // 遍历 focusTopicGroup 中的每一个消费者集群，如果这个消费者集群关注了 topic 主题的话，就将其添加到 clustersSet 中
        CollectionUtils.forAllDo(focusTopicGroup, ifClosure);
    }

    private boolean checkClustersSet(Message msg, String requestId) {
        // 如果 clustersSet 的 size 为 0，那么说明没有消费者订阅对应的主题 topic，并且创建一个 ack 对象，
        // 并且将其保存到 AckTaskQueue 中，最后返回 false
        if (clustersSet.size() == 0) {
            logger.info("ZeroMQ does not have matched clusters!");
            ProducerAckMessage ack = new ProducerAckMessage();
            ack.setMsgId(msg.getMsgId());
            ack.setAck(requestId);
            ack.setStatus(ProducerAckMessage.SUCCESS);
            AckTaskQueue.pushAck(ack);
            SemaphoreCache.release(MessageSystemConfig.AckTaskSemaphoreValue);
            return false;
        } else {
            return true;
        }
    }

    private void dispatchTask(Message msg, String topic) {
        List<MessageDispatchTask> tasks = new ArrayList<MessageDispatchTask>();
        // 遍历 clustersSet 中每一个订阅了主题 topic 的消费者集群，并且创建 MessageDispatchTask，
        // 并且将其保存到 MessageTaskQueue 中
        for (int i = 0; i < clustersSet.size(); i++) {
            MessageDispatchTask task = new MessageDispatchTask();
            task.setClusters(clustersSet.get(i).getClustersId());
            task.setTopic(topic);
            task.setMessage(msg);
            tasks.add(task);
        }

        MessageTaskQueue.getInstance().pushTask(tasks);

        for (int i = 0; i < tasks.size(); i++) {
            SemaphoreCache.release(MessageSystemConfig.NotifyTaskSemaphoreValue);
        }
    }

    private void taskAck(Message msg, String requestId) {
        try {
            Joiner joiner = Joiner.on(MessageSystemConfig.MessageDelimiter).skipNulls();
            // key = requestId + @ + msgId
            String key = joiner.join(requestId, msg.getMsgId());
            // 将 key 保存到缓存队列中
            AckMessageCache.getAckMessageCache().appendMessage(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void hookProducerMessage(Message msg, String requestId, Channel channel) {

        ChannelCache.pushRequest(requestId, channel);

        String topic = msg.getTopic();
        // 获取到关注这个主题 topic 的消费者集群
        focusTopicGroup = ConsumerContext.selectByTopic(topic);
        // 再进行一次过滤，筛选出关注了主题 topic 的消费者集群
        filterByTopic(topic);

        if (checkClustersSet(msg, requestId)) {
            dispatchTask(msg, topic);
            taskAck(msg, requestId);
            clustersSet.clear();
        }
    }
}
