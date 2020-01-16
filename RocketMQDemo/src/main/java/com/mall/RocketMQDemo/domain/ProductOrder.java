package com.mall.RocketMQDemo.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * description
 *
 * @author husheng 2020/01/14 3:38 PM
 */
@EqualsAndHashCode
@Data
@ToString
public class ProductOrder implements Serializable {

    //订单ID
    private Long orderId;

    //操作类型
    private String type;

    public ProductOrder(){}

    private ProductOrder(Long orderId, String type){
        this.orderId = orderId;
        this.type = type;
    }

    public static List<ProductOrder> getOrderList(){
        List<ProductOrder> list = new ArrayList<>(16);
        list.add(new ProductOrder(111L,"创建订单"));
        list.add(new ProductOrder(222L,"创建订单"));
        list.add(new ProductOrder(111L,"支付订单"));
        list.add(new ProductOrder(222L,"支付订单"));
        list.add(new ProductOrder(111L,"完成订单"));
        list.add(new ProductOrder(333L,"创建订单"));
        list.add(new ProductOrder(222L,"完成订单"));
        list.add(new ProductOrder(333L,"支付订单"));
        list.add(new ProductOrder(333L,"完成订单"));
        return list;
    }

}
