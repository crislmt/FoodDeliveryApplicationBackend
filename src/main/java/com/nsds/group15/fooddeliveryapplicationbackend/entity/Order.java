package com.nsds.group15.fooddeliveryapplicationbackend.entity;

import java.util.StringTokenizer;

public class Order {

    private int code;
    private String customerEmail;
    private String productName;
    private int quantity;

    public Order(String customerEmail, String productName, int quantity){
        this.customerEmail=customerEmail;
        this.productName=productName;
        this.quantity=quantity;
    }

    public Order(String record){
        StringTokenizer stk = new StringTokenizer(record, "#");
        this.customerEmail = stk.nextToken();
        this.productName= stk.nextToken();
        this.quantity = Integer.parseInt(stk.nextToken());
    }

    @Override
    public String toString(){
        return "Order n. "+code+" by "+customerEmail+" for "+productName+" with quantity "+quantity;
    }


    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }


}
