package com.xu.zeromq.netty;

import com.xu.zeromq.serialize.MessageCodecUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageObjectDecoder extends ByteToMessageDecoder {

    final public static int MESSAGE_LENGTH = MessageCodecUtil.MESSAGE_LENGTH;

    private MessageCodecUtil util;

    public MessageObjectDecoder(final MessageCodecUtil util) {
        this.util = util;
    }

    /*
     * 对消息进行解码，zeromq 中消息的编解码分成两部分：消息长度、消息体
     * 其中消息长度为 4 个字节，是一个 int 数据类型，表示消息体中的字节数目
     */
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 如果读取的字节数还没有达到 4 个字节
        if (in.readableBytes() < MessageObjectDecoder.MESSAGE_LENGTH) {
            return;
        }

        in.markReaderIndex();
        // 读取消息体的长度
        int messageLength = in.readInt();

        if (messageLength < 0) {
            ctx.close();
        }

        // 如果获取到的消息字节数还达不到消息体的全部长度
        if (in.readableBytes() < messageLength) {
            in.resetReaderIndex();
        } else {
            byte[] messageBody = new byte[messageLength];
            in.readBytes(messageBody);

            try {
                // 使用 kryo 进行反序列化，生成 Object 对象，并且将解码得到的对象保存到 out 列表中
                Object obj = util.decode(messageBody);
                out.add(obj);
            } catch (IOException ex) {
                Logger.getLogger(MessageObjectDecoder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
