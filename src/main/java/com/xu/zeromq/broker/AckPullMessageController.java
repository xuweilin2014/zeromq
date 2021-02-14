/**
 * Copyright (C) 2016 Newland Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xu.zeromq.broker;

import com.xu.zeromq.msg.ProducerAckMessage;
import com.xu.zeromq.core.AckTaskQueue;
import com.xu.zeromq.core.ChannelCache;
import com.xu.zeromq.core.MessageSystemConfig;
import com.xu.zeromq.core.SemaphoreCache;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.netty.NettyUtil;
import io.netty.channel.Channel;
import java.util.concurrent.Callable;

/**
 * @filename:AckPullMessageController.java
 * @description:AckPullMessageController功能模块
 * @author tangjie<https://github.com/tang-jie>
 * @blog http://www.cnblogs.com/jietang/
 * @since 2016-8-11
 */
public class AckPullMessageController implements Callable<Void> {

    private volatile boolean stoped = false;

    public void stop() {
        stoped = true;
    }

    public boolean isStoped() {
        return stoped;
    }

    public Void call() {
        while (!stoped) {
            SemaphoreCache.acquire(MessageSystemConfig.AckTaskSemaphoreValue);
            ProducerAckMessage ack = AckTaskQueue.getAck();
            String requestId = ack.getAck();
            ack.setAck("");

            Channel channel = ChannelCache.findChannel(requestId);
            if (NettyUtil.validateChannel(channel)) {
                ResponseMessage response = new ResponseMessage();
                response.setMsgId(requestId);
                response.setMsgSource(MessageSource.AvatarMQBroker);
                response.setMsgType(MessageType.AvatarMQProducerAck);
                response.setMsgParams(ack);

                channel.writeAndFlush(response);
            }
        }
        return null;
    }
}
