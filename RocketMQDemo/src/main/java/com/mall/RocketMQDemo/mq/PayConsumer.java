package com.mall.RocketMQDemo.mq;

import java.nio.charset.StandardCharsets;

import com.mall.RocketMQDemo.jms.JmsConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

/**
 * description
 *
 * @author husheng 2019/12/19 9:31 PM
 */
@Component
public class PayConsumer {

    public PayConsumer() throws MQClientException {
        String consumerGroup = "pay_consumer_group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //默认是集群方式，可以更改为广播模式，但是广播模式不支持重试
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe(JmsConfig.TOPIC,"*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) ->{
            MessageExt msg = msgs.get(0);
            int times =  msg.getReconsumeTimes();
            System.out.println("重复次数:"+times);
            try{
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                String topic =msg.getTopic();
                String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                String tags = msg.getTags();
                String keys =msg.getKeys();
                System.out.println("topic="+ topic +", tags=" +tags+ ", keys=" +keys +", msg="+ body);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }catch (Exception e) {
                System.out.println("消费异常");
                if (times >= 2){
                    System.out.println("重试两次不成功，发送短信或告知对应人员");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
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
