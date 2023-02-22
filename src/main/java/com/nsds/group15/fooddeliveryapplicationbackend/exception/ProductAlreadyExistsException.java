package com.nsds.group15.fooddeliveryapplicationbackend.exception;

public class ProductAlreadyExistsException extends Exception{
    public String toString(){
        return "Product with the same name already exists";
    }
}
