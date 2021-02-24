package com.xu.zeromq.serialize;

import io.netty.buffer.ByteBuf;
import java.io.IOException;

public interface MessageCodecUtil {

    final public static int MESSAGE_LENGTH = 4;

    public void encode(final ByteBuf out, final Object message) throws IOException;

    public Object decode(byte[] body) throws IOException;

}
