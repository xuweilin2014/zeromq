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
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.model.MessageDispatchTask;
import com.xu.zeromq.netty.NettyUtil;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;

public class SendMessageTask implements Callable<Void> {

    private MessageDispatchTask[] tasks;
    private Phaser phaser;
    private SendMessageLauncher launcher = SendMessageLauncher.getInstance();

    public SendMessageTask(Phaser phaser, MessageDispatchTask[] tasks) {
        this.phaser = phaser;
        this.tasks = tasks;
    }

    public Void call() {
        // 遍历 MessageDispatchTask
        // MessageDispatchTask 可以看成是 <消费者集群 cluster_id, topic, message> 三个属性的封装
        for (MessageDispatchTask task : tasks) {
            Message msg = task.getMessage();

            if (ConsumerContext.selectByClusters(task.getClusters()) != null) {
                // 根据 cluster_id 获取到对应的消费者集群，并且根据负载均衡策略，选择一个集群中的一个消费者
                RemoteChannelData channel = ConsumerContext.selectByClusters(task.getClusters()).nextRemoteChannelData();

                ResponseMessage response = new ResponseMessage();
                response.setMsgSource(MessageSource.AvatarMQBroker);
                response.setMsgType(MessageType.AvatarMQMessage);
                response.setMsgParams(msg);
                response.setMsgId(new MessageIdGenerator().generate());

                try {
                    if (!NettyUtil.validateChannel(channel.getChannel())) {
                        ConsumerContext.addClustersStat(task.getClusters(), ClustersState.NETWORKERR);
                        continue;
                    }

                    RequestMessage request = (RequestMessage) launcher.launcher(channel.getChannel(), response);
                    ConsumerAckMessage result = (ConsumerAckMessage) request.getMsgParams();

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
