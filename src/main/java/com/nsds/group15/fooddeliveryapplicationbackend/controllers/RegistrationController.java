package com.nsds.group15.fooddeliveryapplicationbackend.controllers;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.CustomerAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.CustomerAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.services.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class RegistrationController {
    @Autowired
    private CustomerService customerService;

    @PostMapping("/register")
    public ResponseEntity registerUser(@RequestParam String email, @RequestParam String name, @RequestParam String surname,@RequestParam String address){
        try{
            Customer c = new Customer(email,name,surname,address);
            customerService.registration(c);
            return new ResponseEntity<>("Ok", HttpStatus.OK);
        }
        catch(CustomerAlreadyExistsException caee){
            return new ResponseEntity<>("User already exists", HttpStatus.BAD_REQUEST);
        }
    }
}