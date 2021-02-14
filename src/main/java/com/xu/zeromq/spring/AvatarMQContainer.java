package com.xu.zeromq.spring;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class AvatarMQContainer implements Container {

    public static final String AvatarMQConfigFilePath = "classpath:zeromq-broker.xml";

    private AvatarMQContext springContext;

    public void start() {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext(AvatarMQConfigFilePath);
        springContext = new AvatarMQContext(context);
        context.start();
    }

    public void stop() {
        if (null != springContext && null != springContext.get()) {
            springContext.get().close();
            springContext = null;
        }
    }

    public AvatarMQContext getContext() {
        return springContext;
    }
}
