package com.xu.zeromq.spring;

import org.springframework.context.support.AbstractApplicationContext;

public final class ZeroMQContext implements Context<AbstractApplicationContext> {

    private final AbstractApplicationContext applicationContext;

    public ZeroMQContext(final AbstractApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public AbstractApplicationContext get() {
        return applicationContext;
    }
}
