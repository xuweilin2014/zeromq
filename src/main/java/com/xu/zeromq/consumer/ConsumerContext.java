package com.xu.zeromq.consumer;

import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.model.SubscriptionData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.xu.zeromq.msg.SubscribeAckMessage;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.iterators.FilterIterator;

public class ConsumerContext {

    // 消费者集群关系的定义，ClustersRelation 可以看成是一个二元组 <cluster_id, ConsumerCluster> 的封装
    private static final CopyOnWriteArrayList<ClustersRelation> relationArray = new CopyOnWriteArrayList<>();
    // 消费者集群状态，ClusterState 可以看成是一个二元组 <cluster_id, cluster_state> 的封装
    private static final CopyOnWriteArrayList<ClustersState> stateArray = new CopyOnWriteArrayList<ClustersState>();

    public static void addClustersStat(String clusters, int stat) {
        stateArray.add(new ClustersState(clusters, stat));
    }

    // 根据消费者集群编码 cluster_id 获取一个消费者集群的状态
    public static int getClustersStat(final String clusters) {

        Predicate predicate = new Predicate() {
            public boolean evaluate(Object object) {
                String clustersId = ((ClustersState) object).getClusters();
                return clustersId.compareTo(clusters) == 0;
            }
        };

        // 获取特定消费者集群的状态
        Iterator iterator = new FilterIterator(stateArray.iterator(), predicate);

        ClustersState state = null;
        if (iterator.hasNext()) {
            state = (ClustersState) iterator.next();
        }

        return (state != null) ? state.getState() : 0;
    }

    // 根据消费者集群编码 cluster_id 查找一个消费者集群
    public static ConsumerCluster selectByClusterId(final String clusterId) {
        Predicate predicate = new Predicate() {
            public boolean evaluate(Object object) {
                String id = ((ClustersRelation) object).getClusterId();
                return id.compareTo(clusterId) == 0;
            }
        };

        // 获取特定消费者集群对象
        Iterator iterator = new FilterIterator(relationArray.iterator(), predicate);

        ClustersRelation relation = null;
        while (iterator.hasNext()) {
            relation = (ClustersRelation) iterator.next();
            break;
        }

        return (relation != null) ? relation.getCluster() : null;
    }

    // 查找一下关注这个主题的消费者集群集合
    public static List<ConsumerCluster> selectByTopic(String topic) {

        List<ConsumerCluster> clusters = new ArrayList<>();

        // 遍历所有的消费者集群所订阅的主题信息
        for (int i = 0; i < relationArray.size(); i++) {
            // subscriptionTable 保存了某一个消费者集群所关注的所有主题
            ConcurrentHashMap<String, SubscriptionData> subscriptionTable = relationArray.get(i).getCluster().getSubMap();
            // 如果当前这个消费者集群关注了这个主题，那么就将其加入到 clusters 数组中返回
            if (subscriptionTable.containsKey(topic)) {
                clusters.add(relationArray.get(i).getCluster());
            }
        }

        return clusters;
    }

    // 添加消费者集群
    public static SubscribeAckMessage addClusters(String clusterId, RemoteChannelData channelinfo, String msgId) {

        SubscribeAckMessage ack = new SubscribeAckMessage();
        ack.setMsgId(msgId);

        // clusterId，也就是消费者集群的标识
        ConsumerCluster cluster = selectByClusterId(clusterId);
        // 如果没有 clusterId 对应的消费者集群
        if (cluster == null) {
            ConsumerCluster newCluster = new ConsumerCluster(clusterId);
            newCluster.attachConsumer(channelinfo.getConsumerId(), channelinfo);
            relationArray.add(new ClustersRelation(clusterId, newCluster));
        // 如果在消费者集群中找到了 consumerId 对应的消费者
        } else if (cluster.findConsumer(channelinfo.getConsumerId()) != null) {
            // 删除旧连接，添加新连接
            cluster.detachConsumer(channelinfo.getConsumerId());
            cluster.attachConsumer(channelinfo.getConsumerId(), channelinfo);
        // 如果在消费者集群中没有找到 consumerId 对应的消费者
        } else {
            String topic = channelinfo.getSubscript().getTopic();
            // 判断当前消费者集群中是否含有当前这个消费者订阅主题 topic
            boolean contained = cluster.getSubMap().containsKey(topic);
            if (contained) {
                cluster.attachConsumer(channelinfo.getConsumerId(), channelinfo);
            } else {
                // 一个消费者集群中所有消费者订阅主题应该是一致的，所以当不一致时，应该抛出异常
                ack.setAck("consumer cluster " + clusterId + " does not contain the topic" + channelinfo.getSubscript().getTopic());
                ack.setStatus(SubscribeAckMessage.FAIL);
                return ack;
            }
        }

        ack.setStatus(SubscribeAckMessage.SUCCESS);
        ack.setAck("consumer " + channelinfo.getConsumerId() + " subscribe topic " + channelinfo.getSubscript().getTopic() + " successfully!");
        return ack;
    }

    // 从一个消费者集群中删除一个消费者
    public static void unLoad(String clientId, String clusterId) {
        if (clusterId == null || clusterId.length() == 0)
            return;

        // 遍历 relationArray 中的每一个 ConsumerClusters 对象
        for (int i = 0; i < relationArray.size(); i++) {
            String id = relationArray.get(i).getClusterId();

            if (!clusterId.equals(id))
                continue;

            ConsumerCluster cluster = relationArray.get(i).getCluster();

            // 查看 ConsumerClusters 是否包含 client_id 这个消费者，如果包含，则移除
            if (cluster.findConsumer(clientId) != null) {
                cluster.detachConsumer(clientId);
            }

            if (cluster.getChannelMap().size() == 0) {
                relationArray.remove(id);
            }
        }
    }
}
