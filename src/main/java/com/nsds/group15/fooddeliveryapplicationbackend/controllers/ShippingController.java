package com.nsds.group15.fooddeliveryapplicationbackend.controllers;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Shipping;
import com.nsds.group15.fooddeliveryapplicationbackend.services.ShippingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/shippings")
public class ShippingController {
    @Autowired
    ShippingService shippingService;

    @GetMapping("/getShippingsByEmail")
    public ResponseEntity getShippingsByEmail(@RequestParam String email){
        try{
            List<Shipping> shippingList = shippingService.getShippingsByEmail(email);
            return new ResponseEntity(shippingList, HttpStatus.OK);
        }
        catch (Exception e){
            return new ResponseEntity<>("Not ok", HttpStatus.BAD_REQUEST);
        }
    }

}
