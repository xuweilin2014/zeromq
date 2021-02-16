package com.xu.zeromq.spring;

import org.springframework.context.support.AbstractApplicationContext;

public final class AvatarMQContext implements Context<AbstractApplicationContext> {

    private final AbstractApplicationContext applicationContext;

    public AvatarMQContext(final AbstractApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public AbstractApplicationContext get() {
        return applicationContext;
    }
}
