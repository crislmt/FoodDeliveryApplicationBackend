package com.nsds.group15.fooddeliveryapplicationbackend.exception;

public class QuantityNotAvailableException extends Exception{
    public String toString(){
        return "The requested amount is not available";
    }
}
