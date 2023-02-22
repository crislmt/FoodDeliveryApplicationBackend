package com.nsds.group15.fooddeliveryapplicationbackend.exception;

public class CustomerAlreadyExistsException extends Exception {
    public String toString(){
        return "Customer with the same email already exists";
    }

}
