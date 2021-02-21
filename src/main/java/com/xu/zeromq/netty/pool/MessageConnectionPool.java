package com.xu.zeromq.netty.pool;

import com.xu.zeromq.netty.Connection;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageConnectionPool implements Closeable {

    private static GenericKeyedObjectPool<String, Connection> pool;

    private static Map<String, PoolConfig> propertiesMap = new ConcurrentHashMap<>();

    public static final String poolPropertiesPath = "pool.properties";

    static {
        ClassLoader cls = MessageConnectionPool.class.getClassLoader();
        InputStream inputStream = cls.getResourceAsStream(poolPropertiesPath);
        Properties poolProperties = new Properties();
        try {
            poolProperties.load(inputStream);
            Set<String> addresses = poolProperties.stringPropertyNames();
            for (String address : addresses) {
                String path = (String) poolProperties.get(address);
                InputStream is = cls.getResourceAsStream(path);
                Properties properties = new Properties();
                properties.load(is);

                PoolConfig poolConfig = new PoolConfig(properties);

                propertiesMap.put(address, poolConfig);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized Connection borrowConnection(String address){
        Connection connection = null;
        try{
            if (pool == null){
                MessageConnectionFactory factory = new MessageConnectionFactory();
                // 获取用户配置的到特定地址的连接池参数
                PoolConfig poolConfig = propertiesMap.get(address);

                // 如果用户没有配置 address 对应的连接池参数，那么就使用默认的参数
                if (poolConfig == null){
                    poolConfig = new PoolConfig();
                }

                pool = new GenericKeyedObjectPool<>(factory, poolConfig);

                // 初始化最大数量的连接加入连接池中
                for (int i = 0; i < poolConfig.getMaxTotalPerKey(); i++) {
                    pool.addObject(address);
                }
            }

            connection = pool.borrowObject(address);
        }catch (Throwable t){
            throw new RuntimeException("Could not get connection, caused by " + t.getMessage());
        }

        return connection;
    }

    public static synchronized void returnConnection(String address, Connection connection){
        if (connection != null){
            pool.returnObject(address, connection);
        }
    }

    @Override
    public void close() {
        try {
            pool.close();
        } catch (Exception e) {
            throw new RuntimeException("error occurs when closing the pool, caused by " + e.getMessage());
        }
    }
}
