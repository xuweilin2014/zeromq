package com.xu.zeromq.spring;

public interface Container {

    void start();

    void stop();

    Context<?> getContext();
}
