package com.mall.RocketMQDemo.jms;

/**
 * description
 *
 * @author husheng 2019/12/19 9:32 PM
 */
public class JmsConfig {

    //双主双从、同步双写、异步刷盘，一共四个虚拟机，开启两个nameserver和四个broker
    public static final String NAME_SERVER="172.16.74.128:9876;172.16.74.129:9876";

    public static final String TOPIC="rocketmq_pay_test_topic";

    public static final String ORDERLY_TOPIC="rocketmq_pay_test_topic_orderly";

}
