
package com.xu.zeromq.netty;

import com.xu.zeromq.core.HookMessageEvent;
import io.netty.channel.ChannelHandler;

@ChannelHandler.Sharable
public class ShareMessageEventWrapper<T> extends MessageEventWrapper<T> {

    public ShareMessageEventWrapper() {
        super.setWrapper(this);
    }

    public ShareMessageEventWrapper(MessageProcessor processor) {
        super(processor, null);
        super.setWrapper(this);
    }

    public ShareMessageEventWrapper(MessageProcessor processor, HookMessageEvent<T> hook) {
        super(processor, hook);
        super.setWrapper(this);
    }
}
