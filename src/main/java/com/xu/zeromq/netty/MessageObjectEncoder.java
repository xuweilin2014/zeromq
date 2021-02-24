package com.xu.zeromq.netty;

import com.xu.zeromq.serialize.MessageCodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageObjectEncoder extends MessageToByteEncoder<Object> {

    private MessageCodecUtil util;

    public MessageObjectEncoder(final MessageCodecUtil util) {
        this.util = util;
    }

    protected void encode(final ChannelHandlerContext ctx, final Object msg, final ByteBuf out) throws Exception {
        util.encode(out, msg);
    }

}
