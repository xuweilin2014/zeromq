package com.xu.zeromq.core;

import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.broker.SendMessageLauncher;
import com.xu.zeromq.consumer.ClustersState;
import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.model.MessageDispatchTask;
import com.xu.zeromq.netty.NettyUtil;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;

// 一个消费集群如果订阅了某个主题的消息，那么一个相关主题的消息只会被发送到这个集群中的某一个 consumer 中
public class SendMessageTask implements Callable<Void> {

    private MessageDispatchTask[] tasks;
    private Phaser phaser;
    private SendMessageLauncher launcher = SendMessageLauncher.getInstance();

    public SendMessageTask(Phaser phaser, MessageDispatchTask[] tasks) {
        this.phaser = phaser;
        // tasks 为当前线程所负责派发的消息数组
        this.tasks = tasks;
    }

    public Void call() {
        // 遍历 MessageDispatchTask
        // MessageDispatchTask 可以看成是 <消费者集群 cluster_id, topic, message> 三个属性的封装
        for (MessageDispatchTask task : tasks) {
            Message msg = task.getMessage();

            if (ConsumerContext.selectByClusters(task.getClusters()) != null) {
                // 根据 cluster_id 获取到对应的消费者集群，并且根据负载均衡策略，选择一个集群中的一个消费者
                // 也就是一个消费集群如果订阅了某个主题的消息，那么一个相关主题的消息只会被发送到这个集群中的某一个 consumer 中
                RemoteChannelData channel = ConsumerContext.selectByClusters(task.getClusters()).nextRemoteChannelData();

                ResponseMessage response = new ResponseMessage();
                response.setMsgType(MessageType.Message);
                response.setMsgParams(msg);
                response.setMsgId(new MessageIdGenerator().generate());

                try {
                    // 确认 channel 是否是 active，是否开启 open，以及是否可以写入
                    // 如果没有满足上述条件，则说明该消费者集群的网络条件出现故障
                    if (!NettyUtil.validateChannel(channel.getChannel())) {
                        ConsumerContext.addClustersStat(task.getClusters(), ClustersState.NETWORKERR);
                        continue;
                    }

                    // 向 consumer 发送订阅的消息，并且阻塞等待消费者端返回 ConsumerAckMessage 确认消息
                    RequestMessage request = (RequestMessage) launcher.launcher(channel.getChannel(), response);
                    ConsumerAckMessage result = (ConsumerAckMessage) request.getMsgParams();

                    // 如果消费成功，会将消费者集群的状态设置为 success
                    if (result.getStatus() == ConsumerAckMessage.SUCCESS) {
                        ConsumerContext.addClustersStat(task.getClusters(), ClustersState.SUCCESS);
                    }
                } catch (Exception e) {
                    ConsumerContext.addClustersStat(task.getClusters(), ClustersState.ERROR);
                }
            }
        }

        phaser.arriveAndAwaitAdvance();
        return null;
    }
}
