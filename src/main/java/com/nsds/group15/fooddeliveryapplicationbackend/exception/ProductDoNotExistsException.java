package com.nsds.group15.fooddeliveryapplicationbackend.exception;

public class ProductDoNotExistsException extends Exception{
    public String toString(){
        return "The product you're looking for do not exists";
    }
}
