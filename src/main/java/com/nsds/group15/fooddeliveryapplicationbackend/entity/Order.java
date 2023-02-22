package com.nsds.group15.fooddeliveryapplicationbackend.entity;

public class Order {
    private String productName;
    private int quantity;
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



}
