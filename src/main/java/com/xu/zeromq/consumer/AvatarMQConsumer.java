package com.xu.zeromq.consumer;

import com.google.common.base.Joiner;
import com.xu.zeromq.core.AvatarMQAction;
import com.xu.zeromq.core.MessageIdGenerator;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.msg.SubscribeMessage;
import com.xu.zeromq.msg.UnSubscribeMessage;
import com.xu.zeromq.netty.MessageProcessor;

public class AvatarMQConsumer extends MessageProcessor implements AvatarMQAction {

    // 有 consumer 自己定义的消息消费方法
    private ProducerMessageHook hook;

    private String brokerServerAddress;

    private String topic;

    private boolean subscribeMessage = false;

    private boolean running = false;

    // 默认消费者集群 id
    private String defaultClusterId = "AvatarMQConsumerClusters";

    private String clusterId = "";

    private String consumerId = "";

    public AvatarMQConsumer(String brokerServerAddress, String topic, ProducerMessageHook hook) {
        super(brokerServerAddress);
        this.hook = hook;
        this.brokerServerAddress = brokerServerAddress;
        // 当前消费者要订阅的消息主题
        this.topic = topic;
    }

    private void unRegister() {
        RequestMessage request = new RequestMessage();
        request.setMsgType(MessageType.AvatarMQUnsubscribe);
        request.setMsgId(new MessageIdGenerator().generate());
        request.setMsgParams(new UnSubscribeMessage(consumerId));
        sendSyncMessage(request);
        super.getMessageConnectFactory().close();
        super.closeMessageConnectFactory();
        running = false;
    }

    // 发送消息到 broker 端，表明此 consumer 订阅的主题以及 consumerId 和 clusterId
    private void register() {
        // 发送或者接收到的 broker 消息分为两种：RequestMessage 以及 ResponseMessage
        RequestMessage request = new RequestMessage();
        // 发送的消息类型为订阅消息
        request.setMsgType(MessageType.AvatarMQSubscribe);
        request.setMsgId(new MessageIdGenerator().generate());

        SubscribeMessage subscript = new SubscribeMessage();
        subscript.setClusterId((clusterId.equals("") ? defaultClusterId : clusterId));
        subscript.setTopic(topic);
        subscript.setConsumerId(consumerId);

        request.setMsgParams(subscript);

        sendAsyncMessage(request);
    }

    public void init() {
        // ConsumerHookMessage 用来调用用户自己定义的 hook 对象对消息进行处理，然后返回 ConsumerAckMessage
        super.getMessageConnectFactory().setMessageHandler(new MessageConsumerHandler(this, new ConsumerHookMessageEvent(hook)));
        Joiner joiner = Joiner.on(MessageSystemConfig.MessageDelimiter).skipNulls();
        // 消费者集群 id（clusterId） + @ + topic + @ + msgId
        consumerId = joiner.join((clusterId.equals("") ? defaultClusterId : clusterId), topic, new MessageIdGenerator().generate());
    }

    public void start() {
        // 判断 subscribeMessage 是否为 true
        if (isSubscribeMessage()) {
            // 尝试连接到 broker 服务器
            super.getMessageConnectFactory().connect();
            register();
            running = true;
        }
    }

    public void receiveMode() {
        setSubscribeMessage(true);
    }

    public void shutdown() {
        if (running) {
            unRegister();
        }
    }

    public String getBrokerServerAddress() {
        return brokerServerAddress;
    }

    public void setBrokerServerAddress(String brokerServerAddress) {
        this.brokerServerAddress = brokerServerAddress;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isSubscribeMessage() {
        return subscribeMessage;
    }

    public void setSubscribeMessage(boolean subscribeMessage) {
        this.subscribeMessage = subscribeMessage;
    }

    public String getDefaultClusterId() {
        return defaultClusterId;
    }

    public void setDefaultClusterId(String defaultClusterId) {
        this.defaultClusterId = defaultClusterId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
