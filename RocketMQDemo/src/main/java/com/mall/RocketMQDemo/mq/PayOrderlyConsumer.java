package com.mall.RocketMQDemo.mq;

import com.mall.RocketMQDemo.jms.JmsConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

/**
 * description
 *
 * @author husheng 2020/01/14 4:10 PM
 */
@Component
public class PayOrderlyConsumer {

    public PayOrderlyConsumer() throws MQClientException {
        String consumerGroup = "pay_orderly_consumer_group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //默认是集群方式，可以更改为广播模式，但是广播模式不支持重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe(JmsConfig.ORDERLY_TOPIC,"*");
        /*//用tag过滤
        consumer.subscribe(JmsConfig.ORDERLY_TOPIC,"order_create || order_pay || order_finish");
        //用Sql过滤
        consumer.subscribe(JmsConfig.ORDERLY_TOPIC, MessageSelector.bySql("amount > 5"));*/
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) ->{
            MessageExt msg = msgs.get(0);
            try{
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }catch (Exception e) {
                e.printStackTrace();
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });

        /*consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                try{
                    Message msg = msgs.get(0);
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                    String topic =msg.getTopic();
                    String body = new String(msg.getBody(),"utf-8");
                    String tags = msg.getTags();
                    String keys =msg.getKeys();
                    System.out.println("topic="+ topic +", tags=" +tags+ ", keys=" +keys +", msg="+ body);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                }catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });*/

        consumer.start();
        System.out.println("consumer start ...");
    }
}
