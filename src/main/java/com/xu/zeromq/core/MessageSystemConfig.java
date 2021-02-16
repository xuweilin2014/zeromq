package com.xu.zeromq.core;

public class MessageSystemConfig {

    public static final String SystemPropertySocketSndbufSize = "com.xu.zeromq.netty.socket.sndbuf.size";

    public static int SocketSndbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "65535"));

    public static final String SystemPropertySocketRcvbufSize = "com.xu.zeromq.netty.socket.rcvbuf.size";

    public static int SocketRcvbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "65535"));

    public static final String SystemPropertyAckTaskSemaphoreValue = "com.xu.zeromq.semaphore.ackTaskSemaphoreValue";

    public static String AckTaskSemaphoreValue = System.getProperty(SystemPropertyAckTaskSemaphoreValue, "Ack");

    public static final String SystemPropertyNotifyTaskSemaphoreValue = "com.xu.zeromq.semaphore.NotifyTaskSemaphoreValue";

    public static String NotifyTaskSemaphoreValue = System.getProperty(SystemPropertyNotifyTaskSemaphoreValue, "Notify");

    public static final String SystemPropertySemaphoreCacheHookTimeValue = "com.xu.zeromq.semaphore.hooktime";

    public static int SemaphoreCacheHookTimeValue = Integer.parseInt(System.getProperty(SystemPropertySemaphoreCacheHookTimeValue, "5"));

    public static final String SystemPropertyMessageTimeOutValue = "com.xu.zeromq.system.normal.timeout";

    public static int MessageTimeOutValue = Integer.parseInt(System.getProperty(SystemPropertyMessageTimeOutValue, "3000"));

    public static final String SystemPropertyAckMessageControllerTimeOutValue = "com.xu.zeromq.system.ack.timeout";

    public static int AckMessageControllerTimeOutValue = Integer.parseInt(System.getProperty(SystemPropertyAckMessageControllerTimeOutValue, "1000"));

    public static final String SystemPropertySendMessageControllerPeriodTimeValue = "com.xu.zeromq.system.send.period";

    public static int SendMessageControllerPeriodTimeValue = Integer.parseInt(System.getProperty(SystemPropertySendMessageControllerPeriodTimeValue, "3000"));

    public static final String SystemPropertySendMessageControllerTaskCommitValue = "com.xu.zeromq.system.send.taskcommit";

    public static int SendMessageControllerTaskCommitValue = Integer.parseInt(System.getProperty(SystemPropertySendMessageControllerTaskCommitValue, "1"));

    public static final String SystemPropertySendMessageControllerTaskSleepTimeValue = "com.xu.zeromq.system.send.sleeptime";

    public static int SendMessageControllerTaskSleepTimeValue = Integer.parseInt(System.getProperty(SystemPropertySendMessageControllerTaskSleepTimeValue, "5000"));

    public final static String MessageDelimiter = "@";

    public final static String IpV4AddressDelimiter = ":";
}
