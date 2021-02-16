package com.xu.zeromq.model;

public enum MessageType {

    AvatarMQSubscribe(1),
    AvatarMQUnsubscribe(2),
    AvatarMQMessage(3),
    AvatarMQProducerAck(4),
    AvatarMQConsumerAck(5);

    private int messageType;

    private MessageType(int messageType) {
        this.messageType = messageType;
    }

    int getMessageType() {
        return messageType;
    }
}
