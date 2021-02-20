package com.xu.zeromq.model;

public enum MessageType {

    Subscribe(1),

    SubscribeAck(2),

    Unsubscribe(3),

    UnsubscribeAck(4),

    Message(5),

    ProducerAck(6),

    ConsumerAck(7);

    private int messageType;

    private MessageType(int messageType) {
        this.messageType = messageType;
    }

    int getMessageType() {
        return messageType;
    }

}
