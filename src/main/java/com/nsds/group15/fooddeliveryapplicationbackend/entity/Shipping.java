package com.nsds.group15.fooddeliveryapplicationbackend.entity;

import java.util.StringTokenizer;

public class Shipping {
    private int orderCode;
    private String customerEmail;
    private String address;
    private Boolean delivered=false;

    public Shipping(String record){
        StringTokenizer stringTokenizer = new StringTokenizer(record, "#");
        this.orderCode=Integer.parseInt(stringTokenizer.nextToken());
        this.customerEmail=stringTokenizer.nextToken();
    }
    public int getOrderCode() {
        return orderCode;
    }
    public void setOrderCode(int orderCode) {
        this.orderCode = orderCode;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Boolean getDelivered() {
        return delivered;
    }

    public void setDelivered(Boolean delivered) {
        this.delivered = delivered;
    }

    public String getCustomerEmail() {
        return customerEmail;
    }
    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

}
