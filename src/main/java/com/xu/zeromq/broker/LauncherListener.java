package com.xu.zeromq.broker;

import com.xu.zeromq.core.CallBackFuture;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class LauncherListener implements ChannelFutureListener {

    private CallBackFuture<Object> invoker = null;

    public LauncherListener(CallBackFuture<Object> invoker) {
        this.invoker = invoker;
    }

    public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
            invoker.setReason(future.cause());
        }
    }
}
