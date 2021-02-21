package com.xu.zeromq.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CallBackFuture<T> {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private T messageResult;

    private List<CallBackListener<T>> listeners = Collections.synchronizedList(new ArrayList<CallBackListener<T>>());

    private String requestId;

    private Throwable reason;

    public void setReason(Throwable reason) {
        this.reason = reason;
        publish();
        countDownLatch.countDown();
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public CallBackFuture() {
    }

    public void setMessageResult(T messageResult) {
        this.messageResult = messageResult;
        publish();
        countDownLatch.countDown();
    }

    public Object getMessageResult(long timeout, TimeUnit unit) {
        try {
            countDownLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }
        if (reason != null) {
            return null;
        }
        return messageResult;
    }

    public void addListener(CallBackListener<T> listener) {
        this.listeners.add(listener);
    }

    private void publish() {
        for (CallBackListener<T> listener : listeners) {
            listener.onCallBack(messageResult, reason);
        }
    }
}
