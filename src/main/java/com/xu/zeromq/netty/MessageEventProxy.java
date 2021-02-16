package com.xu.zeromq.netty;


public interface MessageEventProxy {

    void beforeMessage(Object msg);

    void afterMessage(Object msg);
}
