package com.xu.zeromq.producer;

import com.xu.zeromq.core.ZeroMQAction;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.netty.MessageProcessor;
import java.util.concurrent.atomic.AtomicLong;

public class ZeroMQProducer extends MessageProcessor implements ZeroMQAction {

    private boolean brokerConnect = false;

    private boolean running = false;
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
        // producer 连接到 broker 端
        super.getConnection().connect();
        // 设置 producer 已经成功连接到 broker，并且处于运行状态
        brokerConnect = true;
        running = true;
    }

    // 设置 netty 参数，为连接到 broker 端做好准备
    public void init() {
        ProducerHookMessageEvent hook = new ProducerHookMessageEvent();
        hook.setBrokerConnect(brokerConnect);
        hook.setRunning(running);
        // 设置 ProducerHandler，也就是 Producer -> Broker 的连接 pipeline 中可以有这个 handler 来进行处理
        super.getConnection().setMessageHandler(new ProducerHandler(this, hook));
    }

    public ProducerAckMessage deliver(Message message) {
        // 如果 producer 不处于连接状态，那么就直接返回 ProducerAckMessage
        if (!brokerConnect){
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
        request.setMsgId(String.valueOf(msgId.incrementAndGet()));
        // 发送的消息具体内容
        request.setMsgParams(message);
        // 发送的消息的类型
        request.setMsgType(MessageType.Message);
        // 设置消息的 id
        message.setMsgId(request.getMsgId());

        // 发送消息，并且阻塞等待 broker 返回确认消息 ack
        ResponseMessage response = (ResponseMessage) sendSyncMessage(request);
        // 如果没有返回 response，那么直接返回 ack 错误消息，这里应该是等待超时，还没有返回
        if (response == null) {
            ProducerAckMessage ack = new ProducerAckMessage();
            ack.setStatus(ProducerAckMessage.FAIL);
            return ack;
        }

        return (ProducerAckMessage) response.getMsgParams();
    }

    public void shutdown() {
        if (running) {
            running = false;
            // 将此
            returnConnection();
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
