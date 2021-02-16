package com.xu.zeromq.netty;

import org.apache.commons.pool.PoolableObjectFactory;

// PooledObjectFactory 抽象了对象在池子生命周期中每个节点的方法
public class MessageConnectPoolableObjectFactory implements PoolableObjectFactory<MessageConnectFactory> {

    private String serverAddress;
    private int sessionTimeOut = 3 * 1000;

    public MessageConnectPoolableObjectFactory(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public MessageConnectPoolableObjectFactory(String serverAddress, int sessionTimeOut) {
        this.serverAddress = serverAddress;
        this.sessionTimeOut = sessionTimeOut;
    }

    // makeObject 定义了如何生成对象
    public MessageConnectFactory makeObject() throws Exception {
        return new MessageConnectFactory(serverAddress);
    }

    // destroyObject 定义了如何摧毁对象，比如释放资源
    public void destroyObject(MessageConnectFactory obj) throws Exception {
        if (obj != null) {
            obj.close();
        }
    }

    // validateObject 定义了如何校验对象
    public boolean validateObject(MessageConnectFactory obj) {
        return true;
    }

    // activateObject 定义了如何初始化对象
    public void activateObject(MessageConnectFactory obj) throws Exception {
    }

    // passivateObject 定义了如何重置对象
    public void passivateObject(MessageConnectFactory obj) throws Exception {
        MessageConnectFactory factory = (MessageConnectFactory) obj;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public void setSessionTimeOut(int sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
    }
}
