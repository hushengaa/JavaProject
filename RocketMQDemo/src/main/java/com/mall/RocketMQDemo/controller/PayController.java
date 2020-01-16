package com.mall.RocketMQDemo.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mall.RocketMQDemo.domain.ProductOrder;
import com.mall.RocketMQDemo.jms.JmsConfig;
import com.mall.RocketMQDemo.mq.PayProducer;
import com.mall.RocketMQDemo.mq.TransactionProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * description
 *
 * @author husheng 2019/12/19 8:34 PM
 */
@RestController
public class PayController {

    private final PayProducer payProducer;

    private final TransactionProducer transactionProducer;

    @Autowired
    public PayController(PayProducer payProducer, TransactionProducer transactionProducer) {
        this.payProducer = payProducer;
        this.transactionProducer = transactionProducer;
    }

    @RequestMapping("api/v1/pay_cb")
    public Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message message = new Message(JmsConfig.TOPIC,"taga", "6688", ("Hello rocketmq"+text).getBytes());
      /*  //同步发送（速度快，有发送结果反馈，不丢失）。主要是重要邮件和短信通知
        //设置队列为1
        SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                int queueNum = Integer.parseInt(arg.toString());
                return mqs.get(queueNum);
            }
        }, 1);
        System.out.printf("发送结果=%s,msg=%s",sendResult.getSendStatus(),sendResult.toString());*/

        /*//单向发（速度最快，没有发送结果反馈，可能丢失）。主要是日志收集-LogServer
        payProducer.getProducer().sendOneway(message);*/

        //异步发送（速度快，有发送结果反馈，不丢失）。支持更高并发，回调成功触发对应业务，如注册成功后通知积分系统发放优惠券
        /*payProducer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("发送结果=%s,msg=%s",sendResult.getSendStatus(),sendResult.toString());
            }
            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                //TODO 补偿机制，根据业务判断是否需要使用重试
            }
        });*/
        //异步发送至指定queue
        //设置消息延迟发送10秒，2为第二个等级。
        message.setDelayTimeLevel(2);
        //取模(取余数)7除10,余为7,结果为7
        int orderId = 7 % 10;
        payProducer.getProducer().send(message, (mqs, msg, arg) ->{
            int id = Integer.parseInt(arg.toString());
            //取模(取余数)7除4,余为3,结果为3,队列为3
            int index = id % mqs.size();
            return mqs.get(index);
        }, orderId, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("发送结果=%s,msg=%s",sendResult.getSendStatus(),sendResult.toString());
            }
            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                //TODO 补偿机制，根据业务判断是否需要使用重试
            }
        });
        return new HashMap<>();
    }

    /**
     * 消息按照顺序队列投递
     * @return Object
     * @throws Exception 异常
     */
    @RequestMapping("api/v2/pay_cb")
    public Object callBack(String tags, String amount) throws Exception{
        List<ProductOrder> list = ProductOrder.getOrderList();
        for (ProductOrder order : list) {
            Message message = new Message(JmsConfig.ORDERLY_TOPIC, tags, order.getOrderId() + "", order.toString().getBytes());
            //MessageSelector.bySql过滤
            message.putUserProperty("amount",amount);
            SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, order.getOrderId());
            System.out.printf("发送结果=%s, sendResult=%s, orderId=%s, type=%s\n", sendResult.getSendStatus(), sendResult.toString(), order.getOrderId(), order.getType());
        }
        return new HashMap<>();
    }

    /**
     * 用事务投递消息
     * @return Object
     * @throws Exception 异常
     */
    @RequestMapping("api/v3/pay_cb")
    public Object sendTransaction(String tags, String otherParam) throws Exception{
        List<ProductOrder> list = ProductOrder.getOrderList();
        for (ProductOrder order : list) {
            Message message = new Message(JmsConfig.ORDERLY_TOPIC, tags, order.getOrderId() + "", order.toString().getBytes());
            SendResult sendResult = transactionProducer.getProducer()
                    .sendMessageInTransaction(message, otherParam);
            System.out.printf("发送结果=%s, sendResult=%s, orderId=%s, type=%s\n", sendResult.getSendStatus(), sendResult.toString(), order.getOrderId(), order.getType());
        }
        return new HashMap<>();
    }
}
