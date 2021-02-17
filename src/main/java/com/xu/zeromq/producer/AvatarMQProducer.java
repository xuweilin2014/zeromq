package com.xu.zeromq.producer;

import com.xu.zeromq.core.AvatarMQAction;
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.netty.MessageProcessor;
import java.util.concurrent.atomic.AtomicLong;

public class AvatarMQProducer extends MessageProcessor implements AvatarMQAction {

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

    public AvatarMQProducer(String brokerServerAddress, String topic) {
        super(brokerServerAddress);
        this.brokerServerAddress = brokerServerAddress;
        this.topic = topic;
    }

    private ProducerAckMessage checkMode() {
        // 如果 producer 和 broker 不处于连接状态，那么直接返回失败响应 ProducerAckMessage
        if (!brokerConnect) {
            ProducerAckMessage ack = new ProducerAckMessage();
            ack.setStatus(ProducerAckMessage.FAIL);
            return ack;
        }

        return null;
    }

    public void start() {
        // producer 连接到 broker 端
        super.getMessageConnectFactory().connect();
        // 设置 producer 已经成功连接到 broker，并且处于运行状态
        brokerConnect = true;
        running = true;
    }

    public void init() {
        ProducerHookMessageEvent hook = new ProducerHookMessageEvent();
        hook.setBrokerConnect(brokerConnect);
        hook.setRunning(running);
        // 设置 MessageProducerHandler，也就是 Producer -> Broker 的连接 pipeline 中可以有这个 handler 来进行处理
        super.getMessageConnectFactory().setMessageHandler(new MessageProducerHandler(this, hook));
    }

    public ProducerAckMessage deliver(Message message) {
        // 如果 producer 不处于运行状态，或者不处于连接状态，那么就直接返回 ProducerAckMessage
        if (!running || !brokerConnect) {
            return checkMode();
        }

        // 设置消息的主题，主题 topic 是给每个 producer 设置
        message.setTopic(topic);
        // 设置消息发送的时间戳
        message.setTimeStamp(System.currentTimeMillis());

        RequestMessage request = new RequestMessage();
        // 发送的消息 id
        request.setMsgId(String.valueOf(msgId.incrementAndGet()));
        // 发送的消息具体内容
        request.setMsgParams(message);
        // 发送的消息的类型
        request.setMsgType(MessageType.AvatarMQMessage);
        // 发送这个消息的来源是 zeromq producer
        request.setMsgSource(MessageSource.AvatarMQProducer);
        // 设置消息的 id
        message.setMsgId(request.getMsgId());

        ResponseMessage response = (ResponseMessage) sendAsyncMessage(request);
        // 如果没有返回 response，那么直接返回 ack 错误消息
        if (response == null) {
            ProducerAckMessage ack = new ProducerAckMessage();
            ack.setStatus(ProducerAckMessage.FAIL);
            return ack;
        }

        ProducerAckMessage result = (ProducerAckMessage) response.getMsgParams();
        return result;
    }

    public void shutdown() {
        if (running) {
            running = false;
            super.getMessageConnectFactory().close();
            super.closeMessageConnectFactory();
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
