package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.exception.ProductAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.ProductDoNotExistsException;

import java.util.HashMap;
import java.util.Map;

public class OrderService {

    private Map<String, Integer> productQuantity;

    public OrderService(){
        productQuantity=new HashMap<>();
    }


    //Method to change the quantity of a product in the local state, can be persisted
    public void updateQuantity(String productName, int quantity) throws ProductDoNotExistsException {
        if(productQuantity.containsKey(productName)){
            throw new ProductDoNotExistsException();
        }
        else{
            int newQuantity=productQuantity.get(productName);
            newQuantity=newQuantity+quantity;
            productQuantity.put(productName,newQuantity);
        }

    }



    public void addProduct(String productName, int quantity) throws ProductAlreadyExistsException {
        if(productQuantity.containsKey(productName)){
            throw new ProductAlreadyExistsException();
        }
        else{
            productQuantity.put(productName,quantity);
        }
    }
}
