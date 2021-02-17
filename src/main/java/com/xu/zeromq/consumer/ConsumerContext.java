package com.xu.zeromq.consumer;

import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.model.SubscriptionData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    public static ConsumerClusters selectByClusters(final String clusters) {
        Predicate predicate = new Predicate() {
            public boolean evaluate(Object object) {
                String id = ((ClustersRelation) object).getId();
                return id.compareTo(clusters) == 0;
            }
        };

        // 获取特定消费者集群对象
        Iterator iterator = new FilterIterator(relationArray.iterator(), predicate);

        ClustersRelation relation = null;
        while (iterator.hasNext()) {
            relation = (ClustersRelation) iterator.next();
            break;
        }

        return (relation != null) ? relation.getClusters() : null;
    }

    // 查找一下关注这个主题的消费者集群集合
    public static List<ConsumerClusters> selectByTopic(String topic) {

        List<ConsumerClusters> clusters = new ArrayList<>();

        for (int i = 0; i < relationArray.size(); i++) {
            // subscriptionTable 保存了某一个消费者集群所关注的所有主题
            ConcurrentHashMap<String, SubscriptionData> subscriptionTable = relationArray.get(i).getClusters().getSubMap();
            // 如果当前这个消费者集群关注了这个主题，那么就将其加入到 clusters 数组中返回
            if (subscriptionTable.containsKey(topic)) {
                clusters.add(relationArray.get(i).getClusters());
            }
        }

        return clusters;
    }

    // 添加消费者集群
    public static void addClusters(String clusters, RemoteChannelData channelinfo) {
        // 这里的 clusters 可以看成是 cluster_id，也就是消费者集群的标识
        ConsumerClusters manage = selectByClusters(clusters);
        // 如果没有 cluster_id 对应的消费者集群
        if (manage == null) {
            ConsumerClusters newClusters = new ConsumerClusters(clusters);
            newClusters.attachRemoteChannelData(channelinfo.getClientId(), channelinfo);
            relationArray.add(new ClustersRelation(clusters, newClusters));
        // 如果在消费者集群中找到了 clientId 对应的消费者
        } else if (manage.findRemoteChannelData(channelinfo.getClientId()) != null) {
            manage.detachRemoteChannelData(channelinfo.getClientId());
            manage.attachRemoteChannelData(channelinfo.getClientId(), channelinfo);
        // 如果在消费者集群中没有找到 clientId 对应的消费者
        } else {
            String topic = channelinfo.getSubscript().getTopic();
            boolean touchChannel = manage.getSubMap().containsKey(topic);
            if (touchChannel) {
                manage.attachRemoteChannelData(channelinfo.getClientId(), channelinfo);
            } else {
                manage.getSubMap().clear();
                manage.getChannelMap().clear();
                manage.attachRemoteChannelData(channelinfo.getClientId(), channelinfo);
            }
        }
    }

    // 从一个消费者集群中删除一个消费者
    public static void unLoad(String clientId) {
        // 遍历 relationArray 中的每一个 ConsumerClusters 对象
        for (int i = 0; i < relationArray.size(); i++) {
            String id = relationArray.get(i).getId();
            ConsumerClusters manage = relationArray.get(i).getClusters();

            // 查看 ConsumerClusters 是否包含 client_id 这个消费者，如果包含，则移除
            if (manage.findRemoteChannelData(clientId) != null) {
                manage.detachRemoteChannelData(clientId);
            }

            if (manage.getChannelMap().size() == 0) {
                ClustersRelation relation = new ClustersRelation();
                relation.setId(id);
                relationArray.remove(id);
            }
        }
    }
}
