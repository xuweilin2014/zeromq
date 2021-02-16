package com.xu.zeromq.netty;

import com.xu.zeromq.core.HookMessageEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;

public class MessageEventWrapper<T> extends ChannelInboundHandlerAdapter implements MessageEventHandler, MessageEventProxy {

    final public static String proxyMappedName = "handleMessage";
    protected MessageProcessor processor;
    protected Throwable cause;
    protected HookMessageEvent<T> hook;
    protected MessageConnectFactory factory;
    private MessageEventWrapper<T> wrapper;

    public MessageEventWrapper() {
    }

    public MessageEventWrapper(MessageProcessor processor) {
        this(processor, null);
    }

    public MessageEventWrapper(MessageProcessor processor, HookMessageEvent<T> hook) {
        this.processor = processor;
        this.hook = hook;
        this.factory = processor.getMessageConnectFactory();
    }

    public void handleMessage(ChannelHandlerContext ctx, Object msg) {
    }

    public void beforeMessage(Object msg) {
    }

    public void afterMessage(Object msg) {
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    /**
     * MessageEventWrapper 是 MessageBrokerHandler、MessageProducerHandler、MessageConsumerHandler
     * 这三个类的父类，这三个类分别实现了 MessageEventWrapper 的 beforeMessage、handleMessage 以及
     * afterMessage 方法，并且都复用了这里的 channelRead 方法。当有消息发送过来时，就会调用 channelRead
     * 方法，也就是依次调用上面三个 handler 中的 beforeMessage、handleMessage、afterMessage 方法
     */
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);

        // 这里的 wrapper 对象其实就是 MessageBrokerHandler、MessageProducerHandler、MessageConsumerHandler
        // 这三个对象。wrapper 作为目标对象，也就是在调用其特定方法时（这里是 handleMessage 方法），会被拦截，进行
        // 增强。其实具体会调用 MessageEventAdvisor 中的 invoke 方法，来在 handleMessage 真正被调用前后进行处理
        // 1.创建代理工厂 ProxyFactory，并且在构造函数中指明 wrapper，会同时设置好目标对象以及设置代理实现的接口
        ProxyFactory weaver = new ProxyFactory(wrapper);
        NameMatchMethodPointcutAdvisor advisor = new NameMatchMethodPointcutAdvisor();
        // 2.指定要拦截的具体方法名称
        advisor.setMappedName(MessageEventWrapper.proxyMappedName);
        // 3.前置增强和后置增强可以通过下面的环绕增强统一进行处理
        advisor.setAdvice(new MessageEventAdvisor(wrapper, msg));
        weaver.addAdvisor(advisor);
        // 4.获取到代理对象
        MessageEventHandler proxyObject = (MessageEventHandler) weaver.getProxy();
        proxyObject.handleMessage(ctx, msg);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        this.cause = cause;
        cause.printStackTrace();
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    public Throwable getCause() {
        return cause;
    }

    public void setWrapper(MessageEventWrapper<T> wrapper) {
        this.wrapper = wrapper;
    }
}
