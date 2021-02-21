package com.xu.zeromq.spring;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ZeroMQContainer implements Container {

    public static final String AvatarMQConfigFilePath = "classpath:zeromq-broker.xml";

    private ZeroMQContext springContext;

    public void start() {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext(AvatarMQConfigFilePath);
        springContext = new ZeroMQContext(context);
        context.start();
    }

    public void stop() {
        if (null != springContext && null != springContext.get()) {
            springContext.get().close();
            springContext = null;
        }
    }

    public ZeroMQContext getContext() {
        return springContext;
    }
}
