package com.xu.zeromq.broker.strategy;

import com.google.common.base.Joiner;
import com.xu.zeromq.consumer.ConsumerCluster;
import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.core.*;
import com.xu.zeromq.model.MessageDispatchTask;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.collections.Closure;
import org.apache.commons.collections.ClosureUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.functors.AnyPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ProducerStrategy implements Strategy {

    private ChannelHandlerContext channelHandler;

    public static final Logger logger = LoggerFactory.getLogger(ProducerStrategy.class);

    private List<ConsumerCluster> clustersSet = new ArrayList<ConsumerCluster>();

    private List<ConsumerCluster> focusTopicGroup = null;

    public ProducerStrategy() {
    }

    // 对 producer 发送过来的消息进行处理
    public void messageDispatch(RequestMessage request, ResponseMessage response) {
        Message message = (Message) request.getMsgParams();
        // 如果有消费者对发送的消息进行订阅，那么就将消息推送给这些 consumer，主要完成以下 4 件事情:
        // 1.获取消息的主题，并且得到订阅了该主题的消费者集群
        // 2.如果没有消费者集群订阅该消息主题，那么直接创建一个 ProducerAck，保存到 AckTaskQueue，然后返回
        // 3.否则，为每一个订阅了该主题的消费者集群创建一个 MessageDispatchTask（一个消费者集群对应一个 task），然后将这些 task 保存到 MessageTaskQueue.
        // 这个 MessageDispatchTask 会把消息分发给消费者集群中的一个消费者（注意，不是所有的消费者，只是其中一个）
        // 4.如果有消费者集群订阅该消息主题，那么就会根据这个消息的 msgId 创建一个标识，然后将其保存到 AckMessageCache 中，
        // broker 会根据 AckMessageCache 中的标识创建 ProducerAck，并且将其保存到 AckTaskQueue 中，最后并行将其分发到对应的 producer 端
        handleProducerMessage(message, request.getMsgId(), channelHandler.channel());
    }

    private void filterByTopic(final String topic) {
        // 谓词，用来判断某一个消费者集群是否关注了对应的 topic
        Predicate focusAllPredicate = new Predicate() {
            public boolean evaluate(Object object) {
                ConsumerCluster cluster = (ConsumerCluster) object;
                return cluster.findSubscriptionData(topic) != null;
            }
        };

        // AnyPredicate 实现了 Predicate 接口，在调用其 evaluate 方法时，会遍历 iPredicates 数组中的
        // 每一个 iPredicate 类对象，分别调用其 evaluate 方法，只要有一个返回 true，那么最后就会直接返回 true
        AnyPredicate any = new AnyPredicate(new Predicate[]{focusAllPredicate});

        Closure trueClosure = new Closure() {
            public void execute(Object input) {
                if (input instanceof ConsumerCluster) {
                    ConsumerCluster cluster = (ConsumerCluster) input;
                    clustersSet.add(cluster);
                }
            }
        };

        Closure falseClosure = new Closure() {
            public void execute(Object input) {
            }
        };

        // if any.evaluate(): return trueClosure.execute() else: return falseClosure.execute
        Closure ifClosure = ClosureUtils.ifClosure(any, trueClosure, falseClosure);
        // 遍历 focusTopicGroup 中的每一个消费者集群，如果这个消费者集群关注了 topic 主题的话，就将其添加到 clustersSet 中
        CollectionUtils.forAllDo(focusTopicGroup, ifClosure);
    }

    private boolean checkClustersSet(Message msg, String requestId) {
        // 如果 clustersSet 的 size 为 0，那么说明没有消费者集群订阅对应的主题 topic，并且创建一个 ack 对象，
        // 并且将其保存到 AckTaskQueue 中，最后返回 false
        if (clustersSet.size() == 0) {
            logger.info("ZeroMQ does not have matched clusters for topic " + msg.getTopic() );
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

    // 将要派发的消息包装成 MessageDispatchTask，然后保存到 MessageTaskQueue 中，等待派发
    private void dispatchTask(Message msg, String topic) {

        List<MessageDispatchTask> tasks = new ArrayList<>();
        // 遍历 clustersSet 中每一个订阅了主题 topic 的消费者集群，并且创建 MessageDispatchTask，
        // 并且将其保存到 MessageTaskQueue 中
        for (int i = 0; i < clustersSet.size(); i++) {
            MessageDispatchTask task = new MessageDispatchTask();
            task.setClusterId(clustersSet.get(i).getClustersId());
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

    public void handleProducerMessage(Message msg, String msgId, Channel channel) {
        // 将 msgId 和 channel 一一对应保存在 ChannelCache 中，也就是将到 producer 的连接保存起来，
        // 之后再返回 ProducerAck 消息给 producer 时，还会用到
        ChannelCache.pushRequest(msgId, channel);

        String topic = msg.getTopic();
        // 获取到关注这个主题 topic 的消费者集群列表
        focusTopicGroup = ConsumerContext.selectByTopic(topic);
        // 再进行一次过滤，筛选出关注了主题 topic 的消费者集群
        filterByTopic(topic);

        // 检查是否有消费者集群订阅上面的 topic 主题：
        // 1.如果没有的话，就创建 ProducerAckMessage 对象，并且将其保存到 AckTaskQueue 中，然后返回 false，跳过下面的 if 语句
        // 2.如果有的话，就直接返回 true
        if (checkClustersSet(msg, msgId)) {
            // 为 clustersSet 中的每一个消费者集群创建一个 MessageDispatchTask，并且保存到 MessageTaskQueue 中
            dispatchTask(msg, topic);
            // 根据 msg 的 requestId 生成一个 key 保存到 AckMessageCache 中
            taskAck(msg, msgId);
            clustersSet.clear();
        }
    }

    public void setChannelHandler(ChannelHandlerContext channelHandler) {
        this.channelHandler = channelHandler;
    }

}
