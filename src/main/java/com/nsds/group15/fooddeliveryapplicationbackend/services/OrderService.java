package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class OrderService {

    private static int id=0;
    private Map<String, Integer> productQuantity;
    private String insertOrderTopic="InsertOrderTopic";
    private KafkaProducer<String,String> producer;
    private  String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "OrderServiceTransactionalId";



    public OrderService(){
        productQuantity=new HashMap<>();
        initialize();
    }
    private void initialize(){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        producer = new KafkaProducer<>(props);
    }


    //Method to change the quantity of a product in the local state, can be persisted
    public void updateQuantity(String productName, int quantity) throws ProductDoNotExistsException {
        if(productQuantity.containsKey(productName)){
            throw new ProductDoNotExistsException();
        }
        else{
            int newQuantity=productQuantity.get(productName);
            newQuantity=newQuantity+quantity;
            productQuantity.put(productName,newQuantity);
        }

    }



    public void addProduct(String productName, int quantity) throws ProductAlreadyExistsException {
        if(productQuantity.containsKey(productName)){
            throw new ProductAlreadyExistsException();
        }
        else{
            productQuantity.put(productName,quantity);
        }
    }

    public void insertOrder(Order o) throws QuantityNotAvailableException, NegativeQuantityException {
        producer.initTransactions();
        producer.beginTransaction();
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
        final Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata ack = future.get();
            productQuantity.put(o.getProductName(),newQuantity);
            System.out.println("Success!");
        } catch (InterruptedException | ExecutionException e1) {
            e1.printStackTrace();
        }
        producer.commitTransaction();
    }



}
