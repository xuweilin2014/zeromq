package com.xu.zeromq.broker;

import com.xu.zeromq.core.AckMessageCache;
import com.xu.zeromq.core.MessageSystemConfig;
import java.util.concurrent.Callable;

public class TransferAckController implements Callable<Void> {

    private volatile boolean stopped = false;

    public Void call() {
        AckMessageCache ref = AckMessageCache.getAckMessageCache();
        int timeout = MessageSystemConfig.AckMessageControllerTimeOutValue;
        while (!stopped) {
            if (ref.hold(timeout)) {
                ref.commit();
            }
        }
        return null;
    }

    public void stop() {
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }
}
