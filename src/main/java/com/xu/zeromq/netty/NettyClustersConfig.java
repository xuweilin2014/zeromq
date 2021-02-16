package com.xu.zeromq.netty;

import com.xu.zeromq.core.MessageSystemConfig;

public class NettyClustersConfig {

    private int clientSocketSndBufSize = MessageSystemConfig.SocketSndbufSize;

    private int clientSocketRcvBufSize = MessageSystemConfig.SocketRcvbufSize;

    private static int workerThreads = Runtime.getRuntime().availableProcessors() * 2;

    public static int getWorkerThreads() {
        return workerThreads;
    }

    public static void setWorkerThreads(int workers) {
        workerThreads = workers;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }
}
