package com.xu.zeromq.netty.pool;


import com.xu.zeromq.netty.Connection;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PoolConfig extends GenericKeyedObjectPoolConfig<Connection> {

    public static final Logger logger = LoggerFactory.getLogger(PoolConfig.class);

    public PoolConfig(Properties properties){
        int maxTotalPerKey = Integer.parseInt(properties.getProperty("maxTotalPerKey"));
        int maxTotal = Integer.parseInt(properties.getProperty("maxTotal"));
        int minIdlePerKey = Integer.parseInt(properties.getProperty("minIdlePerKey"));
        int maxIdlePerKey = Integer.parseInt(properties.getProperty("maxIdlePerKey"));

        logger.info("MessageConnectPool[maxTotalPerKey=" + maxTotalPerKey + ",maxTotal=" + minIdlePerKey + ",minIdlePerKey=" + maxIdlePerKey +
                    ",maxIdlePerKey=" + maxTotal + "]");

        /*
         * 我们也可以设计一个对象个数动态变化的池子：池子有一个最大值 maxActive 和最小值 minIdle，最大值是对象个数的上限，
         * 当池子一段时间没有使用后，就去回收超过最小值个数的对象，这样在系统繁忙时，就可以充分复用对象，在系统空闲时，又可以释放不必要的对象
         */

        // 池子可以最多容纳多少个对象
        this.setMaxTotal(maxTotal);
        this.setMaxIdlePerKey(maxIdlePerKey);
        // 最小的空闲对象个数，无论对象如何被释放，保证池子里面最少的对象个数
        this.setMinIdlePerKey(minIdlePerKey);
        // 在 borrow 对象时，阻塞等待的最长时间
        this.setMaxIdlePerKey(maxTotalPerKey);
        // testOnBorrow 表示在从池子借取对象时，是否进行校验
        this.setTestOnBorrow(false);
        // testOnReturn 表示在向池子归还对象时，是否进行校验
        this.setTestOnReturn(false);
        // 驱赶线程扫描池子空闲对象的时间间隔
        this.setTimeBetweenEvictionRunsMillis(10 * 1000);
        // 驱赶线程每一次最多扫描多少个空闲对象
        this.setNumTestsPerEvictionRun(maxTotalPerKey + maxIdlePerKey);
        // 池中的对象在被清理之前可以维持空闲的最小时间
        this.setMinEvictableIdleTimeMillis(30 * 60 * 1000);
        this.setTestWhileIdle(true);
    }

    public PoolConfig(){

    }

}
