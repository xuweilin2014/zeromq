package com.xu.zeromq.netty;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.pool.impl.GenericObjectPool;

public class MessageConnectPool extends GenericObjectPool<MessageConnectFactory> {

    private static MessageConnectPool pool = null;
    private static Properties messageConnectConfigProperties = null;
    private static String configPropertiesString = "zeromq.messageconnect.properties";
    private static String serverAddress = "";

    public static MessageConnectPool getMessageConnectPoolInstance() {
        if (pool == null) {
            synchronized (MessageConnectPool.class) {
                if (pool == null) {
                    pool = new MessageConnectPool();
                }
            }
        }
        return pool;
    }

    private MessageConnectPool() {
        try {
            messageConnectConfigProperties = new Properties();

            InputStream inputStream = MessageConnectPool.class.getClassLoader().getResourceAsStream(configPropertiesString);

            messageConnectConfigProperties.load(inputStream);
            inputStream.close();

            this.serverAddress = serverAddress;
        } catch (IOException e) {
            e.printStackTrace();
        }

        int maxActive = Integer.parseInt(messageConnectConfigProperties.getProperty("maxActive"));
        int minIdle = Integer.parseInt(messageConnectConfigProperties.getProperty("minIdle"));
        int maxIdle = Integer.parseInt(messageConnectConfigProperties.getProperty("maxIdle"));
        int maxWait = Integer.parseInt(messageConnectConfigProperties.getProperty("maxWait"));
        int sessionTimeOut = Integer.parseInt(messageConnectConfigProperties.getProperty("sessionTimeOut"));

        System.out.printf("MessageConnectPool[maxActive=%d,minIdle=%d,maxIdle=%d,maxWait=%d,sessionTimeOut=%d]\n", maxActive, minIdle, maxIdle, maxWait, sessionTimeOut);

        this.setMaxActive(maxActive);
        this.setMaxIdle(maxIdle);
        this.setMinIdle(minIdle);
        this.setMaxWait(maxWait);
        this.setTestOnBorrow(false);
        this.setTestOnReturn(false);
        this.setTimeBetweenEvictionRunsMillis(10 * 1000);
        this.setNumTestsPerEvictionRun(maxActive + maxIdle);
        this.setMinEvictableIdleTimeMillis(30 * 60 * 1000);
        this.setTestWhileIdle(true);

        this.setFactory(new MessageConnectPoolableObjectFactory(serverAddress, sessionTimeOut));
    }

    public MessageConnectFactory borrow() {
        assert pool != null;
        try {
            return (MessageConnectFactory) pool.borrowObject();
        } catch (Exception e) {
            System.out.printf("get message connection throw the error from message connection pool, error message is %s\n",
                    e.getMessage());
        }
        return null;
    }

    public void restore() {
        assert pool != null;
        try {
            pool.close();
        } catch (Exception e) {
            System.out.printf("throw the error from close message connection pool, error message is %s\n",
                    e.getMessage());
        }
    }

    public static String getServerAddress() {
        return serverAddress;
    }

    public static void setServerAddress(String ipAddress) {
        serverAddress = ipAddress;
    }
}
