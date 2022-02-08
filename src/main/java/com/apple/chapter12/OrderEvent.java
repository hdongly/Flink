package com.apple.chapter12;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Created by wushengran on 2021/8/28  15:29
 */
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long ts;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long ts) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + ts +
                '}';
    }
}
