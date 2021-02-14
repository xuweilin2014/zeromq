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
package com.xu.zeromq.core;

import com.xu.zeromq.msg.ConsumerAckMessage;
import com.xu.zeromq.msg.Message;
import com.xu.zeromq.broker.SendMessageLauncher;
import com.xu.zeromq.consumer.ClustersState;
import com.xu.zeromq.consumer.ConsumerContext;
import com.xu.zeromq.model.MessageType;
import com.xu.zeromq.model.RequestMessage;
import com.xu.zeromq.model.ResponseMessage;
import com.xu.zeromq.model.RemoteChannelData;
import com.xu.zeromq.model.MessageSource;
import com.xu.zeromq.model.MessageDispatchTask;
import com.xu.zeromq.netty.NettyUtil;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;

/**
 * @filename:SendMessageTask.java
 * @description:SendMessageTask功能模块
 * @author tangjie<https://github.com/tang-jie>
 * @blog http://www.cnblogs.com/jietang/
 * @since 2016-8-11
 */
public class SendMessageTask implements Callable<Void> {

    private MessageDispatchTask[] tasks;
    private Phaser phaser = null;
    private SendMessageLauncher launcher = SendMessageLauncher.getInstance();

    public SendMessageTask(Phaser phaser, MessageDispatchTask[] tasks) {
        this.phaser = phaser;
        this.tasks = tasks;
    }

    public Void call() throws Exception {
        for (MessageDispatchTask task : tasks) {
            Message msg = task.getMessage();

            if (ConsumerContext.selectByClusters(task.getClusters()) != null) {
                RemoteChannelData channel = ConsumerContext.selectByClusters(task.getClusters()).nextRemoteChannelData();

                ResponseMessage response = new ResponseMessage();
                response.setMsgSource(MessageSource.AvatarMQBroker);
                response.setMsgType(MessageType.AvatarMQMessage);
                response.setMsgParams(msg);
                response.setMsgId(new MessageIdGenerator().generate());

                try {
                    if (!NettyUtil.validateChannel(channel.getChannel())) {
                        ConsumerContext.setClustersStat(task.getClusters(), ClustersState.NETWORKERR);
                        continue;
                    }

                    RequestMessage request = (RequestMessage) launcher.launcher(channel.getChannel(), response);

                    ConsumerAckMessage result = (ConsumerAckMessage) request.getMsgParams();

                    if (result.getStatus() == ConsumerAckMessage.SUCCESS) {
                        ConsumerContext.setClustersStat(task.getClusters(), ClustersState.SUCCESS);
                    }
                } catch (Exception e) {
                    ConsumerContext.setClustersStat(task.getClusters(), ClustersState.ERROR);
                }
            }
        }
        phaser.arriveAndAwaitAdvance();
        return null;
    }
}
