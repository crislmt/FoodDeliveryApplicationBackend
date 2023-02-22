package com.nsds.group15.fooddeliveryapplicationbackend.exception;

public class NegativeQuantityException extends Exception {
    public String toString(){
        return "Products cannot have negative quantity";
    }
}
