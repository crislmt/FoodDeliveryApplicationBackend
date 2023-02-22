package com.nsds.group15.fooddeliveryapplicationbackend.services;


import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import org.apache.catalina.User;

import java.util.List;
import java.util.Map;

public class ShippingService {
    private Map<Integer, Boolean> shippings;
    private List<Customer> customers;
    private String insertOrderTopic="InsertOrderTopic";
    private String registrationTopic="RegistrationTopic";
    private static final String serverAddr = "localhost:9092";
    private static final String offsetResetStrategy = "latest";
    private static final boolean readUncommitted = false;




}
