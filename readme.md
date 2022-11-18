# ZeroMQ

ZeroMQ 项目是一个轻量级的消息队列系统，使用 Netty 进行网络通讯，项目的具体功能如下：

+ 支持多个消费者和多个生产者之间的消息，网络通信依赖 Netty
+ 生产者和消费者的关系可以是一对多、多对一、多对多的关系
+ 多个消费者可以组成一个消费者集群，从而生产者可以向这个消费者集群投递消息
+ 目前支持的路由策略是关键字的精确匹配，支持简单的消费者负载均衡策略
+ Broker 的消息派发、负载均衡、应答处理基于异步多线程模型进行开发设计。
+ Broker 消息的投递，目前仅仅支持严格的顺序消息。其中 Broker 还支持消息的缓冲派发，Broker 会缓存一定数量的消息
  之后，再批量分配给对此消息感兴趣的消费者
  
本项目是在 AvatarMQ 的基础上进行了一些改进，并且添加了大量注释，供自己和他人学习使用，下面是 tangjie 的 github 与博客地址：

+ 项目地址：https://github.com/tang-jie/AvatarMQ
+ 博客地址：https://www.cnblogs.com/jietang/p/5808735.html
_________________

ZeroMQ 项目更加详细的分析可以查看 [ZeroMQ 结构分析](https://github.com/xuweilin2014/zeromq/issues/1)