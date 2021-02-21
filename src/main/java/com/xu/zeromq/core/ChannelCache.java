package com.xu.zeromq.core;

import io.netty.channel.Channel;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelCache {

    private static ConcurrentHashMap<String, Channel> producerMap = new ConcurrentHashMap<String, Channel>();

    public static void pushRequest(String msgId, Channel channel) {
        producerMap.put(msgId, channel);
    }

    public static Channel findChannel(String msgId) {
        return producerMap.remove(msgId);
    }
}
