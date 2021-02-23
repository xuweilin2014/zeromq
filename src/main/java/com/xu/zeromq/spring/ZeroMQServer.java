package com.xu.zeromq.spring;

import com.xu.zeromq.broker.server.ZeroMQBrokerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ZeroMQServer extends ZeroMQBrokerServer implements ApplicationContextAware, InitializingBean {

    public static final Logger logger = LoggerFactory.getLogger(ZeroMQServer.class);

    private String serverAddress;

    public ZeroMQServer(String serverAddress) {
        super(serverAddress);
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        logger.info("ZeroMQ Server Starts Successfully!");
    }

    public void afterPropertiesSet() throws Exception {
        init();
        start();
    }
}
