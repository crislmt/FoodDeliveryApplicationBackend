package com.nsds.group15.fooddeliveryapplicationbackend.controllers;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Shipping;
import com.nsds.group15.fooddeliveryapplicationbackend.services.ShippingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping("/getShippings")
    public ResponseEntity getShippings(){
        try{
            return new ResponseEntity(shippingService.getShippings(), HttpStatus.OK);
        }
        catch(Exception e){
            return new ResponseEntity("Not ok", HttpStatus.BAD_REQUEST);
        }
    }

    @PutMapping("/deliveryShipping")
    public ResponseEntity<String> deliveryShipping(@RequestParam int code){
        try{
            shippingService.deliveryShipping(code);
            return new ResponseEntity<>("The order "+code+" has been shipped",HttpStatus.OK);
        }
        catch(Exception e){
            return new ResponseEntity<>("Not ok", HttpStatus.BAD_REQUEST);
        }
    }

}
