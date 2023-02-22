package com.nsds.group15.fooddeliveryapplicationbackend.entity;

public class Order {

    private int code;

    private String customerEmail;
    private String productName;
    private int quantity;

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
