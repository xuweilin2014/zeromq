package com.xu.zeromq.consumer;

import com.google.common.base.Joiner;
import com.xu.zeromq.core.ZeroMQAction;
import com.xu.zeromq.core.MessageIdGenerator;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.msg.SubscribeAckMessage;
import com.xu.zeromq.msg.SubscribeMessage;
import com.xu.zeromq.msg.UnSubscribeMessage;
import com.xu.zeromq.netty.MessageProcessor;

public class ZeroMQConsumer extends MessageProcessor implements ZeroMQAction {

    // 由 consumer 自己定义的消息消费方法
    private ProducerMessageHook hook;

    private String brokerServerAddress;

    private String topic;

    private volatile boolean subscribeMessage = false;

    private volatile boolean running = false;

    // 默认消费者集群 id
    private String defaultClusterId = "AvatarMQConsumerClusters";

    private String clusterId = "";

    private String consumerId = "";

    public ZeroMQConsumer(String brokerServerAddress, String topic, ProducerMessageHook hook) {
        // 在父类的 MessageProcessor 中，会获取到一个 Connection 对象，用于和 broker 建立连接
        // 每个 consumer 都有一个 Connection 对象作为属性。在 Connection 中，
        // 还有一个 futureMap，用来保存已经发送的 subscribe 请求（还没有收到响应）
        super(brokerServerAddress);
        this.hook = hook;
        this.brokerServerAddress = brokerServerAddress;
        // 当前消费者要订阅的消息主题
        this.topic = topic;
    }

    // 向 broker 发送取消订阅消息，会把此 consumer 从对应的
    private void unsubscribe() {
        // 如果订阅了主题，才会取消订阅，否则，直接抛出异常
        if (isSubscribeMessage()){
            RequestMessage request = new RequestMessage();
            request.setMsgType(MessageType.Unsubscribe);
            request.setMsgId(new MessageIdGenerator().generate());

            UnSubscribeMessage msg = new UnSubscribeMessage(consumerId, clusterId);

            request.setMsgParams(msg);
            sendSyncMessage(request);
        }

        throw new RuntimeException("consumer " + consumerId + " have not subscribed any topic yet.");
    }

    // 发送消息到 broker 端，表明此 consumer 订阅的主题以及 consumerId 和 clusterId
    private void subscribe() {
        // 发送或者接收到的 broker 消息分为两种：RequestMessage 以及 ResponseMessage
        // RequestMessage 中只有 msgId
        RequestMessage request = new RequestMessage();
        // 发送的消息类型为订阅消息
        request.setMsgType(MessageType.Subscribe);
        request.setMsgId(new MessageIdGenerator().generate());

        // SubscriptMessage 中则有 clusterId 以及 consumerId
        SubscribeMessage subscript = new SubscribeMessage();
        subscript.setClusterId((clusterId.equals("") ? defaultClusterId : clusterId));
        subscript.setTopic(topic);
        subscript.setConsumerId(consumerId);

        request.setMsgParams(subscript);

        // 异步发送 subscribe 请求给 broker，然后阻塞等待 broker 端的响应
        // 在作者的实现中，似乎发送了 Subscribe 请求之后，broker 返回的 SubscribeAck 响应被无视了，直接等待阻塞超时
        SubscribeAckMessage ack = (SubscribeAckMessage) sendSyncMessage(request);
        if (ack == null || ack.getStatus() == SubscribeAckMessage.FAIL){
            logger.warn("consumer [consumerId=" + consumerId + ", topic=" + topic + "] subscribe topic "
                    + topic + " fail, " + ((ack == null) ? "" : "caused by " + ack.getException().getMessage()));
        }else {
            logger.info("consumer [consumerId=" + consumerId + ", topic=" + topic + "] subscribe topic "
                    + topic + " successfully!");
        }
    }

    public void start() {
        // ConsumerHookMessage 用来调用用户自己定义的 hook 对象对消息进行处理，然后返回 ConsumerAckMessage
        super.getConnection().setMessageHandler(new ConsumerHandler(this, new ConsumerHookMessageEvent(hook)));
        Joiner joiner = Joiner.on(MessageSystemConfig.MessageDelimiter).skipNulls();
        // 消费者集群 id（clusterId） + @ + topic + @ + msgId
        consumerId = joiner.join((clusterId.equals("") ? defaultClusterId : clusterId), topic, new MessageIdGenerator().generate());

        // 尝试连接到 broker 服务器
        super.getConnection().connect();
        subscribe();
        running = true;
        subscribeMessage = true;

        logger.info("consumer [consumerId=" + consumerId + ", clusterId=" + clusterId + ", topic=" + topic + "] starts successfully! ");
    }

    public void shutdown() {
        if (running) {
            // 取消订阅主题
            unsubscribe();
            // 关闭 consumer 并不真正关闭掉连接，而是将其归还到连接池中
            super.returnConnection();
            running = false;

            logger.info("consumer [" + consumerId + "] is shutdown");
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
