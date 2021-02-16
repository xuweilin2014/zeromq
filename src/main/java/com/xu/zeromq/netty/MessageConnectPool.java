package com.xu.zeromq.netty;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConnectPool extends GenericObjectPool<MessageConnectFactory> {

    public static final Logger logger = LoggerFactory.getLogger(MessageConnectPool.class);

    private volatile static MessageConnectPool pool = null;
    private static Properties messageConnectConfigProperties = null;
    // zeromq 网络连接参数
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
        // 读取 zeromq.messageconnect.properties 文件中的参数到 properties 对象中
        try (InputStream inputStream = MessageConnectPool.class.getClassLoader().getResourceAsStream(configPropertiesString)){
            messageConnectConfigProperties = new Properties();
            messageConnectConfigProperties.load(inputStream);
        } catch (IOException e) {
            logger.warn(e.getMessage());
        }

        int maxActive = Integer.parseInt(messageConnectConfigProperties.getProperty("maxActive"));
        int minIdle = Integer.parseInt(messageConnectConfigProperties.getProperty("minIdle"));
        int maxIdle = Integer.parseInt(messageConnectConfigProperties.getProperty("maxIdle"));
        int maxWait = Integer.parseInt(messageConnectConfigProperties.getProperty("maxWait"));
        int sessionTimeOut = Integer.parseInt(messageConnectConfigProperties.getProperty("sessionTimeOut"));

        logger.info("MessageConnectPool[maxActive=" + maxActive + ",minIdle=" + minIdle + ",maxIdle=" + maxIdle +
                                ",maxWait=" + maxWait + ",sessionTimeOut=" + sessionTimeOut + "]");

        /*
         * 我们也可以设计一个对象个数动态变化的池子：池子有一个最大值 maxActive 和最小值 minIdle，最大值是对象个数的上限，
         * 当池子一段时间没有使用后，就去回收超过最小值个数的对象，这样在系统繁忙时，就可以充分复用对象，在系统空闲时，又可以释放不必要的对象
         */

        // 池子可以最多容纳多少个对象
        this.setMaxActive(maxActive);
        this.setMaxIdle(maxIdle);
        // 最小的空闲对象个数，无论对象如何被释放，保证池子里面最少的对象个数
        this.setMinIdle(minIdle);
        // 在 borrow 对象时，阻塞等待的最长时间
        this.setMaxWait(maxWait);
        // testOnBorrow 表示在从池子借取对象时，是否进行校验
        this.setTestOnBorrow(false);
        // testOnReturn 表示在向池子归还对象时，是否进行校验
        this.setTestOnReturn(false);
        // 驱赶线程扫描池子空闲对象的时间间隔
        this.setTimeBetweenEvictionRunsMillis(10 * 1000);
        // 驱赶线程每一次最多扫描多少个空闲对象
        this.setNumTestsPerEvictionRun(maxActive + maxIdle);
        // 池中的对象在被清理之前可以维持空闲的最小时间
        this.setMinEvictableIdleTimeMillis(30 * 60 * 1000);
        this.setTestWhileIdle(true);

        this.setFactory(new MessageConnectPoolableObjectFactory(serverAddress, sessionTimeOut));
    }

    public MessageConnectFactory borrow() {
        assert pool != null;
        try {
            // 从对象池中获取 MessageConnectFactory 对象
            return pool.borrowObject();
        } catch (Exception e) {
            logger.warn("error occurs when trying to borrow connection, error is " + e.getMessage());
        }
        return null;
    }

    public void restore() {
        assert pool != null;
        try {
            pool.close();
        } catch (Exception e) {
            logger.warn("error occurs when trying to close connection pool, error is " + e.getMessage());
        }
    }

    public static String getServerAddress() {
        return serverAddress;
    }

    public static void setServerAddress(String ipAddress) {
        serverAddress = ipAddress;
    }
}
