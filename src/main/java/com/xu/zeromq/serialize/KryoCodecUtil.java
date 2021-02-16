package com.xu.zeromq.serialize;

import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.buffer.ByteBuf;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KryoCodecUtil implements MessageCodecUtil {

    private KryoPool pool;

    public KryoCodecUtil(KryoPool pool) {
        this.pool = pool;
    }

    public void encode(final ByteBuf out, final Object message) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            KryoSerialize kryoSerialization = new KryoSerialize(pool);
            kryoSerialization.serialize(byteArrayOutputStream, message);
            byte[] body = byteArrayOutputStream.toByteArray();
            int dataLength = body.length;
            out.writeInt(dataLength);
            out.writeBytes(body);
        } finally {
            byteArrayOutputStream.close();
        }
    }

    public Object decode(byte[] body) throws IOException {
        ByteArrayInputStream byteArrayInputStream = null;
        try {
            byteArrayInputStream = new ByteArrayInputStream(body);
            KryoSerialize kryoSerialization = new KryoSerialize(pool);
            Object obj = kryoSerialization.deserialize(byteArrayInputStream);
            return obj;
        } finally {
            byteArrayInputStream.close();
        }
    }
}
