package com.mall.RocketMQDemo.mq;

import com.mall.RocketMQDemo.jms.JmsConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

/**
 * description
 *
 * @author evan 2019/12/19 2:48 PM
 */
@Component
public class PayProducer {

    private DefaultMQProducer producer;

    public PayProducer(){
        String producerGroup = "pay_producer_group";
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        start();
    }

    public DefaultMQProducer getProducer(){
        return this.producer;
    }

    /**
     *
     * 对象在使用之前必须要调用一次，只能初始化一次
     *
     * @author husheng 2019-12-19 8:33 PM
\     */
    private void start(){
        try {
            this.producer.start();
        }catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * 一般在应用上下文，使用上下文监听器，进行关闭
     *
     * @author husheng 2019-12-19 8:33 PM
     */
    public void shutdown(){
        this.producer.shutdown();
    }

}
