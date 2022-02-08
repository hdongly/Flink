package com.apple.datas;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderInfo {
    Integer id;
    Long user_id;
    Double total_amount;
    Long create_time;

//    public OrderInfo() {
//
//    }
//
//    public  OrderInfo(Integer id, Long user_id, Double total_amount, Long create_time) {
//        this.id = id;
//        this.user_id = user_id;
//        this.total_amount = total_amount;
//        this.create_time = create_time;
//    }
//
//    @Override
//    public String toString() {
//        return "OrderInfo{" +
//                "id=" + id +
//                ", user_id=" + user_id +
//                ", total_amount=" + total_amount +
//                ", create_time=" + create_time +
//                '}';
//    }
//
//    public Integer getId() {
//        return id;
//    }
//
//    public void setId(Integer id) {
//        this.id = id;
//    }
//
//    public Long getUser_id() {
//        return user_id;
//    }
//
//    public void setUser_id(Long user_id) {
//        this.user_id = user_id;
//    }
//
//    public Double getTotal_amount() {
//        return total_amount;
//    }
//
//    public void setTotal_amount(Double total_amount) {
//        this.total_amount = total_amount;
//    }
//
//    public Long getCreate_time() {
//        return create_time;
//    }
//
//    public void setCreate_time(Long create_time) {
//        this.create_time = create_time;
//    }
}
