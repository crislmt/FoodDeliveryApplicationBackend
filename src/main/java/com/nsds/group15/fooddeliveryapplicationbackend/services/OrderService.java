package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.*;
import com.nsds.group15.fooddeliveryapplicationbackend.utils.ProducerConsumerFactory;
import org.apache.catalina.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class OrderService {

    private static int id=0;

    private List<Order> orders;
    private Map<String, Integer> productQuantity;
    private List<User> users;

    /****** SERVER AND TOPICS ******/
    private String insertOrderTopic="InsertOrderTopic";
    private  String serverAddr = "localhost:9092";


    /****** PRODUCER FOR ORDERS ******/
    private KafkaProducer<String,String> orderProducer;
    private static final String producerTransactionalId = "OrderServiceTransactionalId";
    private KafkaConsumer<String,String> orderConsumer;


    /****** CONSUMER FOR USERS ******/ //TODO Add messages comspution of user registrations
    private KafkaConsumer<String,String> registrationConsumer;


    /****** FAULT TOLLERANCE ******/
    private KafkaConsumer<String,String> recoverConsumer;
    private static final String offsetResetStrategy = "earliest";
    private static final String isolationLevelStrategy="read_committed";
    private static final String ordersGroup = "ordersGroup";



    public OrderService(){
        productQuantity=new HashMap<>();
        orders=new ArrayList<>();
        orderProducer = ProducerConsumerFactory.initializeTransactionalProducer(serverAddr, producerTransactionalId);
        registrationConsumer= ProducerConsumerFactory.initializeConsumer(serverAddr, ordersGroup, isolationLevelStrategy);
    }


    //Method to change the quantity of a product in the local state, can be persisted
    public void updateQuantity(String productName, int quantity) throws ProductDoNotExistsException, NegativeQuantityException {
        if(productQuantity.containsKey(productName)){
            throw new ProductDoNotExistsException();
        }
        else if(quantity<0) throw new NegativeQuantityException();
        else{
            int newQuantity=productQuantity.get(productName);
            newQuantity=newQuantity+quantity;
            productQuantity.put(productName,newQuantity);
        }

    }



    public void addProduct(String productName, int quantity) throws ProductAlreadyExistsException, NegativeQuantityException {
        if(productQuantity.containsKey(productName)){
            throw new ProductAlreadyExistsException();
        }
        else if(quantity<0) throw new NegativeQuantityException();
        else{
            productQuantity.put(productName,quantity);
        }
    }

    //TODO need to add check on user email
    public void insertOrder(Order o) throws QuantityNotAvailableException, NegativeQuantityException {

        orderProducer.beginTransaction();
        int quantity=productQuantity.get(o.getProductName());
        int newQuantity=quantity-o.getQuantity();
        if(o.getQuantity()<0) throw new NegativeQuantityException();
        if(newQuantity<0) throw new QuantityNotAvailableException();
        synchronized (this){
            o.setCode(id);
            id++;
        }
        String orderMessage=o.getCode()+"#"+o.getCustomerEmail();
        String key="Key1"; //TODO for now we use a single key for all message and one single partition
        ProducerRecord<String, String> record = new ProducerRecord<>(insertOrderTopic, key, orderMessage);
        final Future<RecordMetadata> future = orderProducer.send(record);
        try {
            RecordMetadata ack = future.get();
            productQuantity.put(o.getProductName(),newQuantity);
            orders.add(o);
            System.out.println("Success!");
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
        }
        orderProducer.commitTransaction();
    }

    public List<Order> getOrderByEmail(String email){
        List<Order> result=new ArrayList<>();
        for(Order o : orders){
            if(o.getCustomerEmail().equals(email)){
                result.add(o);
            }
        }
        return result;
    }

    private void recoverOrders(){
        recoverConsumer= ProducerConsumerFactory.initializeConsumer(serverAddr, ordersGroup, isolationLevelStrategy);

        recoverConsumer.subscribe(Collections.singletonList(insertOrderTopic));
        int counter=0;
        if(orders.isEmpty()){
            ConsumerRecords<String,String> records= recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for(ConsumerRecord<String,String> record : records){
                orders.add(new Order(record.value()));
                counter++;
            }
            System.out.println(counter+ " Orders succesfully retrieved");
            orders.forEach((value) -> System.out.println("Order with code "+ value.getCode()));

        }
        recoverConsumer.unsubscribe();
    }


}
