package com.xu.zeromq.model;

public enum MessageSource {

    AvatarMQConsumer(1),
    AvatarMQBroker(2),
    AvatarMQProducer(3);

    private int source;

    private MessageSource(int source) {
        this.source = source;
    }

    public int getSource() {
        return source;
    }
}
