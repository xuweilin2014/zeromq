package com.xu.zeromq.core;

public interface CallBackListener<T> {

    void onCallBack(T t, Throwable reason);

}
