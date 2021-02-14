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

import com.xu.zeromq.model.MessageDispatchTask;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @filename:MessageTaskQueue.java
 * @description:MessageTaskQueue功能模块
 * @author tangjie<https://github.com/tang-jie>
 * @blog http://www.cnblogs.com/jietang/
 * @since 2016-8-11
 */
public class MessageTaskQueue {

    private static AtomicBoolean isInit = new AtomicBoolean(false);
    private static ConcurrentLinkedQueue<MessageDispatchTask> taskQueue = null;

    private volatile static MessageTaskQueue task = null;

    private MessageTaskQueue() {
    }

    public static MessageTaskQueue getInstance() {
        if (isInit.compareAndSet(false, true)) {
            taskQueue = new ConcurrentLinkedQueue<MessageDispatchTask>();
            task = new MessageTaskQueue();
        }
        return task;
    }

    public boolean pushTask(MessageDispatchTask task) {
        return taskQueue.offer(task);
    }

    public boolean pushTask(List<MessageDispatchTask> tasks) {
        return taskQueue.addAll(tasks);
    }

    public MessageDispatchTask getTask() {
        return taskQueue.poll();
    }
}
