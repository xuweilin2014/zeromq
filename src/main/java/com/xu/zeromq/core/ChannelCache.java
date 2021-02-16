package com.xu.zeromq.core;

import io.netty.channel.Channel;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelCache {

    private static ConcurrentHashMap<String, Channel> producerMap = new ConcurrentHashMap<String, Channel>();

    public static void pushRequest(String requestId, Channel channel) {
        producerMap.put(requestId, channel);
    }

    public static Channel findChannel(String requestId) {
        Channel channel = producerMap.remove(requestId);
        return channel;
    }
}
