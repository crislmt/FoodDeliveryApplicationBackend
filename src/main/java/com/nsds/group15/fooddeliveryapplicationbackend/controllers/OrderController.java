package com.nsds.group15.fooddeliveryapplicationbackend.controllers;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.NegativeQuantityException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.ProductAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.ProductDoNotExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.QuantityNotAvailableException;
import com.nsds.group15.fooddeliveryapplicationbackend.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/order")
public class OrderController {
    @Autowired
    OrderService orderService;

    @PutMapping("/addProduct")
    public ResponseEntity addProduct(@RequestParam String productName, @RequestParam int quantity){
        try{
            orderService.addProduct(productName,quantity);
            return new ResponseEntity<>("Ok", HttpStatus.OK);
        } catch (ProductAlreadyExistsException paee) {
            return new ResponseEntity<>(paee.toString(),HttpStatus.BAD_REQUEST);
        } catch(NegativeQuantityException nqe){
            return new ResponseEntity<>(nqe.toString(), HttpStatus.BAD_REQUEST);
        }
    };

    @PostMapping("/updateProduct")
    public ResponseEntity updateProduct(@RequestParam String productName, @RequestParam int quantity){
        try{
            orderService.updateQuantity(productName, quantity);
            return new ResponseEntity<>("Ok", HttpStatus.OK);
        } catch (ProductDoNotExistsException paee) {
            return new ResponseEntity<>(paee.toString(),HttpStatus.BAD_REQUEST);
        } catch(NegativeQuantityException nqe){
            return new ResponseEntity<>(nqe.toString(), HttpStatus.BAD_REQUEST);
        }
    };

    @PostMapping("/insertOrder")
    public ResponseEntity insertOrder(@RequestBody Order order){
        try{
            orderService.insertOrder(order);
            return new ResponseEntity<>("Ok", HttpStatus.OK);
        } catch (QuantityNotAvailableException paee) {
            return new ResponseEntity<>(paee.toString(),HttpStatus.BAD_REQUEST);
        } catch(NegativeQuantityException nqe){
            return new ResponseEntity<>(nqe.toString(), HttpStatus.BAD_REQUEST);
        }
    };


}
