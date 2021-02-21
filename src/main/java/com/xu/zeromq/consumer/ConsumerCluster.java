package com.xu.zeromq.consumer;

import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.model.SubscriptionData;
import com.xu.zeromq.netty.NettyUtil;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;

public class ConsumerCluster {

    // 轮询调度（Round-Robin Scheduling）位置标记
    private int next = 0;

    // 当前消费者集群的标识
    private final String clustersId;

    // SubscriptionData 只有一个属性 topic，表明消息的主题，记录当前消费者集群订阅的全部主题
    // 生产者消息主题 -> 消息对应的 topic 信息数据结构
    private final ConcurrentHashMap<String, SubscriptionData> subMap = new ConcurrentHashMap<>();

    // 消费者标识编码 clientId -> 对应的消费者的 netty 网络通信管道信息
    // 记录了当前消费者集群所有的消费者的连接信息
    private final ConcurrentHashMap<String, RemoteChannelData> channelMap = new ConcurrentHashMap<String, RemoteChannelData>();

    private final List<RemoteChannelData> channelList = Collections.synchronizedList(new ArrayList<RemoteChannelData>());

    public ConsumerCluster(String clustersId) {
        this.clustersId = clustersId;
    }

    public String getClustersId() {
        return clustersId;
    }

    public ConcurrentHashMap<String, SubscriptionData> getSubMap() {
        return subMap;
    }

    public ConcurrentHashMap<String, RemoteChannelData> getChannelMap() {
        return channelMap;
    }

    // 添加一个消费者到消费者集群中
    public void attachRemoteChannelData(String consumerId, RemoteChannelData channelinfo) {
        // 判断在 channelMap 中是否存在此 consumerId 对应的连接信息
        if (findRemoteChannelData(channelinfo.getConsumerId()) == null) {
            // 将 consumerId -> channel 连接信息保存到 channelMap 中
            channelMap.put(consumerId, channelinfo);
            // 将 topic 信息 -> subscription 保存到 subMap 中
            subMap.put(channelinfo.getSubscript().getTopic(), channelinfo.getSubscript());
            channelList.add(channelinfo);
        } else {
            System.out.println("consumer clusters exists! it's clientId:" + consumerId);
        }
    }

    // 从消费者集群中删除一个消费者
    public void detachRemoteChannelData(final String consumerId) {
        channelMap.remove(consumerId);

        Predicate predicate = new Predicate() {
            public boolean evaluate(Object object) {
                String id = ((RemoteChannelData) object).getConsumerId();
                return id.compareTo(consumerId) == 0;
            }
        };

        // 从 channelList 中找到 clientId 等于传入参数 clientId 的 RemoteChannelData 对象，并且从 channelList 中删除掉
        RemoteChannelData data = (RemoteChannelData) CollectionUtils.find(channelList, predicate);
        if (data != null) {
            channelList.remove(data);
        }
    }

    // 根据消费者标识编码，在消费者集群中查找定位一个消费者，如果不存在返回 null
    public RemoteChannelData findRemoteChannelData(String clientId) {
        return (RemoteChannelData) MapUtils.getObject(channelMap, clientId);
    }

    // 负载均衡，根据连接到 broker 的顺序，依次投递消息给消费者。这里的均衡算法直接采用
    // 轮询调度（Round-Robin Scheduling）
    public RemoteChannelData nextRemoteChannelData() {
        Predicate predicate = new Predicate() {
            public boolean evaluate(Object object) {
                RemoteChannelData data = (RemoteChannelData) object;
                Channel channel = data.getChannel();
                return NettyUtil.validateChannel(channel);
            }
        };

        // 过滤掉 channelList 中没有开启，不处于 active 状态，以及不可写入的 channel
        CollectionUtils.filter(channelList, predicate);
        // 通过轮询算法进行负载均衡，选择 channel
        return channelList.get(next++ % channelList.size());
    }

    public SubscriptionData findSubscriptionData(String topic) {
        return this.subMap.get(topic);
    }
}
