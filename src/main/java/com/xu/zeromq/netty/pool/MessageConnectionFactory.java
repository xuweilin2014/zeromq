package com.xu.zeromq.netty.pool;

import com.xu.zeromq.netty.Connection;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// PooledObjectFactory 抽象了对象在池子生命周期中每个节点的方法
public class MessageConnectionFactory implements KeyedPooledObjectFactory<String, Connection> {

    public static final Logger logger = LoggerFactory.getLogger(MessageConnectionFactory.class);

    @Override
    public PooledObject<Connection> makeObject(String address) {
        return new DefaultPooledObject<>(new Connection(address));
    }

    @Override
    public void destroyObject(String key, PooledObject<Connection> p) {
        Connection conn = p.getObject();
        try{
            if (!conn.isClosed()){
                conn.close();
            }
        } catch (Throwable t){
            logger.warn(t.getMessage());
        }
    }

    @Override
    public boolean validateObject(String key, PooledObject<Connection> p) {
        Connection connection = p.getObject();
        if (connection != null){
            if (!connection.isClosed() && connection.isConnected()){
                return true;
            }
        }
        return false;
    }

    @Override
    public void activateObject(String key, PooledObject<Connection> p) {
    }

    @Override
    public void passivateObject(String key, PooledObject<Connection> p) {
    }

}
