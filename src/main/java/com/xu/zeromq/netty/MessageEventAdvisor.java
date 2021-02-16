package com.xu.zeromq.netty;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class MessageEventAdvisor implements MethodInterceptor {

    private MessageEventProxy proxy;
    private Object msg;

    public MessageEventAdvisor(MessageEventProxy proxy, Object msg) {
        this.proxy = proxy;
        this.msg = msg;
    }

    public Object invoke(MethodInvocation invocation) throws Throwable {
        proxy.beforeMessage(msg);
        Object obj = invocation.proceed();
        proxy.afterMessage(msg);
        return obj;
    }
}
