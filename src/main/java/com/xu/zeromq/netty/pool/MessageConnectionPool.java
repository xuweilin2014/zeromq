package com.xu.zeromq.netty.pool;

import com.xu.zeromq.netty.Connection;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MessageConnectionPool implements Closeable {

    private volatile static GenericKeyedObjectPool<String, Connection> pool;

    private static PoolConfig config;

    private static List<String> addressList = new CopyOnWriteArrayList<>();

    public static final String poolPropertiesPath = "zeromq.pool.properties";

    // 从连接池中获取到对应地址的连接，如果没有的话，就先创建一些连接（默认为 8 个）放入到连接池中
    public static Connection borrowConnection(String address){
        Connection connection;
        try{
            // pool 是单例，只能创建一个
            if (pool == null){
                synchronized (MessageConnectionPool.class){
                    if (pool == null){
                        // factory 是工厂，用来创建 connection
                        MessageConnectionFactory factory = new MessageConnectionFactory();
                        ClassLoader cls = MessageConnectionPool.class.getClassLoader();
                        // zeromq.pool.properties 为连接池参数配置文件，可以在这个文件中配置 pool 的各个参数
                        InputStream inputStream = cls.getResourceAsStream(poolPropertiesPath);

                        Properties poolProperties = new Properties();
                        poolProperties.load(inputStream);

                        // 创建连接池参数配置对象 PoolConfig
                        config = new PoolConfig(poolProperties);

                        pool = new GenericKeyedObjectPool<>(factory, config);
                    }
                }
            }

            // 如果 addressList 中没有 address 对应的地址，就说明连接池中没有对应的地址，
            // 因此会事先创建一部分连接保存到 pool 中
            if (!addressList.contains(address) && config != null){
                // 初始化最大数量的连接加入连接池中，默认为 8
                for (int i = 0; i < config.getMaxTotalPerKey(); i++) {
                    pool.addObject(address);
                }
                addressList.add(address);
            }

            connection = pool.borrowObject(address);
        }catch (Throwable t){
            throw new RuntimeException("Could not get connection, caused by " + t.getMessage());
        }

        return connection;
    }

    public static synchronized void returnConnection(String address, Connection connection){
        if (connection != null){
            // 返回 address 地址的连接 connection 到连接池中
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
