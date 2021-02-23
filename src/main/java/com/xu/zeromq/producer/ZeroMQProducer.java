package com.xu.zeromq.producer;

import com.xu.zeromq.core.MessageIdGenerator;
import com.xu.zeromq.core.ZeroMQAction;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.netty.MessageProcessor;
import java.util.concurrent.atomic.AtomicLong;

public class ZeroMQProducer extends MessageProcessor implements ZeroMQAction {

    private volatile boolean running = false;
    // Broker 服务器的地址
    private String brokerServerAddress;
    // 要发送的主题名称
    private String topic;
    // producer 集群默认的名称
    private String defaultClusterId = "AvatarMQProducerClusters";
    // producer 集群的名称
    private String clusterId = "";

    private AtomicLong msgId = new AtomicLong(0L);

    public ZeroMQProducer(String brokerServerAddress, String topic) {
        super(brokerServerAddress);
        this.brokerServerAddress = brokerServerAddress;
        this.topic = topic;
    }

    // 连接到 broker 端
    public void start() {
        // 设置 ProducerHandler，也就是 producer -> Broker 的连接 pipeline 中可以有这个 handler 来进行处理
        super.getConnection().setMessageHandler(new ProducerHandler(this));

        // producer 连接到 broker 端
        super.getConnection().connect();
        // 设置 producer 已经处于运行状态
        running = true;

        logger.info("producer [clusterId=" + clusterId + ", topic=" + topic + "] starts successfully!");
    }

    public ProducerAckMessage deliver(Message message) {
        // 如果 producer 不处于连接状态，那么就直接返回 ProducerAckMessage
        if (!getConnection().isConnected() && getConnection().isClosed()){
            ProducerAckMessage ack = new ProducerAckMessage();
            ack.setStatus(ProducerAckMessage.FAIL);
            return ack;
        }

        // 如果不处于 running 状态，则说明 producer 没有启动，因此直接返回 null
        if (!running){
            return null;
        }

        // Message 对象的 msgId 和 MessageRequest 的 msgId 设置保持一致
        // 设置消息的主题，主题 topic 是给每个 producer 设置
        message.setTopic(topic);
        // 设置消息发送的时间戳
        message.setTimeStamp(System.currentTimeMillis());
        // 在 zeromq 中，所有的消息都是包装在 RequestMessage 和 ResponseMessage 中
        RequestMessage request = new RequestMessage();
        // 发送的消息 id
        request.setMsgId(new MessageIdGenerator().generate());
        // 发送的消息具体内容
        request.setMsgParams(message);
        // 发送的消息的类型
        request.setMsgType(MessageType.Message);
        // 设置消息的 id
        message.setMsgId(request.getMsgId());

        // 发送消息，并且阻塞等待 broker 返回确认消息 ack
        ProducerAckMessage ack = (ProducerAckMessage) sendSyncMessage(request);
        // 如果没有返回 response，那么直接返回 ack 错误消息，这里应该是等待超时，还没有返回
        if (ack == null || ack.getStatus() == ProducerAckMessage.FAIL) {
            logger.warn("producer failed to send the message [messageId=" + message.getMsgId() + ", topic=" + topic + "]" +
                   ", caused by " + ((ack == null) ? "" : "caused by " + ack.getException().getMessage()));
        }

        return ack;
    }

    public void shutdown() {
        if (running) {
            running = false;
            // 将此连接返回给连接池
            returnConnection();

            logger.info("producer [clusterId=" + clusterId + ", topic=" + topic + "] is shutdown.");
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
