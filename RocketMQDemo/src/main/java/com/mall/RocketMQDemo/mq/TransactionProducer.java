package com.mall.RocketMQDemo.mq;

import java.util.concurrent.*;

import com.mall.RocketMQDemo.jms.JmsConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

/**
 * description
 *
 * @author evan 2019/12/19 2:48 PM
 */
@Component
public class TransactionProducer {

    private TransactionMQProducer producer;

    public TransactionProducer(){
        String producerGroup = "trac_producer_group";
        producer = new TransactionMQProducer(producerGroup);
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        //事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        //一般定线程池的时候，需要给线程加个名称
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        producer.setExecutorService(executorService);
        start();
    }

    public TransactionMQProducer getProducer(){
        return this.producer;
    }

    /**
     *
     * 对象在使用之前必须要调用一次，只能初始化一次
     *
     * @author husheng 2019-12-19 8:33 PM
     */
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

class TransactionListenerImpl implements TransactionListener{

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("========executeLocalTransaction========");
        String body = new String(msg.getBody());
        String key = msg.getKeys();
        String transactionId = msg.getTransactionId();
        System.out.println("TransactionId="+transactionId+", key="+key+", body="+body);
        //TODO 执行本地事务开始

        //TODO 执行本地事务结束

        int status = Integer.parseInt(arg.toString());
        //二次确认消息，然后消费者可以消费
        if (status == 1){
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        //回滚消息，Broker端会删除半消息
        if (status == 2){
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        //Broker端会进行回查消息。或者什么都不发送也会回查消息
        if (status == 3){
            return LocalTransactionState.UNKNOW;
        }
        return null;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("========checkLocalTransaction========");
        String body = new String(msg.getBody());
        String key = msg.getKeys();
        String transactionId = msg.getTransactionId();
        System.out.println("TransactionId="+transactionId+", key="+key+", body="+body);
        //要么commit，要么rollback
        //可以根据key去检查本地事务消息是否完成，完成则Commit
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}