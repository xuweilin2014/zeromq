package com.xu.zeromq.netty;

import io.netty.channel.ChannelHandlerContext;

public interface MessageEventHandler {

    void handleMessage(ChannelHandlerContext ctx, Object msg);

}
