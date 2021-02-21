package com.xu.zeromq.model;

public enum MessageType {

    /**
     * consumer 发送给 broker 的订阅消息
     */
    Subscribe(1),

    /**
     * broker 返回给 consumer 的订阅响应
     */
    SubscribeAck(2),

    /**
     * consumer 发送给 broker 的取消订阅消息
     */
    Unsubscribe(3),

    UnsubscribeAck(4),

    /**
     * producer 发送给 broker 的消息
     */
    Message(5),

    /**
     * broker 接收到 producer 发送的消息之后，返回的响应
     */
    ProducerAck(6),

    /**
     * broker 将消息发送给 consumer 之后，consumer 消费消息之后返回的响应
     */
    ConsumerAck(7);

    private int messageType;

    private MessageType(int messageType) {
        this.messageType = messageType;
    }

    int getMessageType() {
        return messageType;
    }

}
