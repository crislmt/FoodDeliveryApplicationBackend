package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.*;
import com.nsds.group15.fooddeliveryapplicationbackend.utils.MessagesUtilities;
import com.nsds.group15.fooddeliveryapplicationbackend.utils.ProducerConsumerFactory;
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

    /***** DATA STRUCTURES *******/
    private Map<Integer, Order> orders;
    private Map<String, Integer> productQuantity;
    private Map<String,Customer> customers;

    /****** SERVER AND TOPICS ******/
    private String insertOrderTopic="InsertOrderTopic";
    private  String serverAddr = "localhost:9092";

    /****** PRODUCER FOR ORDERS ******/
    private KafkaProducer<String,String> orderProducer;
    private static final String producerTransactionalId = "OrderServiceTransactionalId";
    private KafkaConsumer<String,String> orderConsumer;

    /****** CONSUMER FOR USERS *****/
    private KafkaConsumer<String,String> registrationConsumer;

    /****** PRODUCER FOR ITEMS ******/
    private KafkaProducer<String,String> productsProducer; //TODO Send messages in all product related methods

    /****** FAULT TOLERANCE FOR USERS, ITEMS AND ORDERS ******/
    private KafkaConsumer<String,String> recoverCustomersConsumer;
    private KafkaConsumer<String,String> recoverProductsConsumer;
    private KafkaConsumer<String,String> recoverOrdersConsumer;
    private static final String offsetResetStrategy = "earliest";
    private static final String isolationLevelStrategy="read_committed";
    private static final String ordersGroup = "ordersGroup";
    private static final String customersGroup = "customersGroup";
    private static final String productsGroup = "productsGroup";
    private static final String customersTopic = "customersTopic";
    private static final String productsTopic = "productsTopic";

    public OrderService(){
        productQuantity=new HashMap<>();
        orders=new HashMap<>();
        customers= new HashMap<>();
        orderProducer = ProducerConsumerFactory.initializeTransactionalProducer(serverAddr, producerTransactionalId);
        productsProducer= ProducerConsumerFactory.initializeProducer(serverAddr);
        registrationConsumer= ProducerConsumerFactory.initializeConsumer(serverAddr, ordersGroup, isolationLevelStrategy);
        registrationConsumer.subscribe(Collections.singletonList(customersTopic));
        recoverCustomers();
        recoverProducts();
        recoverOrders();
    }

    //TODO Has to be transactional?
    public void addProduct(String productName, int quantity) throws ProductAlreadyExistsException, NegativeQuantityException {
        if(productQuantity.containsKey(productName)){
            throw new ProductAlreadyExistsException();
        }
        else if(quantity<0) throw new NegativeQuantityException();
        else{
            productQuantity.put(productName,quantity);
            String product=productName+"#"+quantity;
            System.out.println(product);
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(productsTopic,"Key1", product);
            productsProducer.send(record);
        }
    }

    //TODO Has to be transactional?
    public void updateQuantity(String productName, int quantity) throws ProductDoNotExistsException, NegativeQuantityException {
        if(!productQuantity.containsKey(productName)){
            throw new ProductDoNotExistsException();
        }
        else if(quantity<0) throw new NegativeQuantityException();
        else{
            int newQuantity=productQuantity.get(productName);
            newQuantity=newQuantity+quantity;
            productQuantity.put(productName,newQuantity);
            String product=productName+"#"+quantity;
            System.out.println(product);
            ProducerRecord<String,String> record = new ProducerRecord<>(productsTopic, "Key1", product);
            productsProducer.send(record);
        }
    }

    /*
    public void insertOrder(Order o) throws QuantityNotAvailableException, NegativeQuantityException, NoSuchUserException{
        updateListOfCustomers();
        orderProducer.beginTransaction();
        try{
            if(!customers.containsKey(o.getCustomerEmail())){
                orderProducer.abortTransaction();
                throw new NoSuchUserException();
            }
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
            ProducerRecord<String, String> recordOrder = new ProducerRecord<>(insertOrderTopic, key, orderMessage);
            final Future<RecordMetadata> futureOrder = orderProducer.send(recordOrder);
            RecordMetadata ackProduct = futureOrder.get();

            ProducerRecord<String,String> recordProduct = new ProducerRecord<>(productsTopic, key, productQuantity.get(o.getProductName())+"#"+newQuantity);
            final Future<RecordMetadata> futureProduct= productsProducer.send(recordProduct);
            RecordMetadata ackOrder = futureProduct.get();

            productQuantity.put(o.getProductName(),newQuantity);
            orders.put(o.getCode(), o);
            System.out.println("Success!");
            orderProducer.commitTransaction();
        }
        catch (InterruptedException | ExecutionException e1 ){
            e1.printStackTrace();
        }
        catch(QuantityNotAvailableException quantityNotAvailableException) { throw new QuantityNotAvailableException(); }
        catch(NoSuchUserException noSuchUserException){ throw new NoSuchUserException();}
        catch(NegativeQuantityException negativeQuantityException){throw new NegativeQuantityException();}
        finally{
            orderProducer.abortTransaction();
        }
    }
    */

    public void insertOrder(Order o) throws QuantityNotAvailableException, NegativeQuantityException, NoSuchUserException {
        updateListOfCustomers();
        orderProducer.beginTransaction();
        if(!customers.containsKey(o.getCustomerEmail())) { orderProducer.abortTransaction(); throw new NoSuchUserException();}
        int quantity=productQuantity.get(o.getProductName());
        int newQuantity=quantity-o.getQuantity();
        if(o.getQuantity()<0) { orderProducer.abortTransaction(); throw new NegativeQuantityException();}
        if(newQuantity<0) {orderProducer.abortTransaction(); throw new QuantityNotAvailableException();}
        synchronized (this){
            o.setCode(id);
            id++;
        }
        String orderMessage=o.getCode()+"#"+o.getCustomerEmail();
        String key="Key1"; //TODO for now we use a single key for all message and one single partition
        ProducerRecord<String, String> record = new ProducerRecord<>(insertOrderTopic, key, orderMessage);
        ProducerRecord<String,String> recordProduct = new ProducerRecord<>(productsTopic, key, o.getProductName()+"#"+newQuantity);
        final Future<RecordMetadata> futureProduct= productsProducer.send(recordProduct);
        final Future<RecordMetadata> future = orderProducer.send(record);
        try {
            RecordMetadata ack = future.get();
            RecordMetadata ackOrder = futureProduct.get();
            productQuantity.put(o.getProductName(),newQuantity);
            orders.put(o.getCode(), o);
            System.out.println("Success!");
        } catch (InterruptedException | ExecutionException e1) {
            orderProducer.abortTransaction();
            e1.printStackTrace();
        }
        orderProducer.commitTransaction();
    }

    public List<Order> getOrderByEmail(String email){
        updateListOfCustomers();
        List<Order> result=new ArrayList<>();
        for(int orderCode : orders.keySet()){
            if(orders.get(orderCode).getCustomerEmail().equals(email)){
                result.add(orders.get(orderCode));
            }
        }
        return result;
    }

    //Used to retrieve registration messages
    private void updateListOfCustomers(){
        final ConsumerRecords<String, String> records = registrationConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        for (final ConsumerRecord<String, String> record : records) {
            Customer customer = new Customer(record.value());
            customers.put(customer.getEmail(), customer);
            MessagesUtilities.printRecord(record, "OrderService");
        }
    }

    private void recoverCustomers(){
        KafkaConsumer recoverConsumer = ProducerConsumerFactory.initializeConsumer(serverAddr,customersGroup,isolationLevelStrategy);
        recoverConsumer.subscribe(Collections.singletonList(customersTopic));
        int counter=0;
        if(customers.isEmpty()) {
            ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for (ConsumerRecord<String, String> record : records) {
                Customer c = new Customer(record.value());
                customers.put(c.getEmail(), c);
                counter++;
            }
            System.out.println(counter + "  messages for topic " + customersTopic + " succesfully retrieved");
            customers.keySet().forEach((value) -> System.out.print(customers.get(value)));
            System.out.println(" retrieved");
        }
        recoverConsumer.unsubscribe();
    }

    private void recoverProducts(){
        KafkaConsumer recoverConsumer = ProducerConsumerFactory.initializeConsumer(serverAddr,productsGroup,isolationLevelStrategy);
        recoverConsumer.subscribe(Collections.singletonList(productsTopic));
        int counter=0;
        if(productQuantity.isEmpty()) {
            ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for (ConsumerRecord<String, String> record : records) {
                String[] keyValue=record.value().split("#");
                productQuantity.put(keyValue[0], Integer.parseInt(keyValue[1]));
                counter++;
            }
            System.out.println(counter + " messages for topic " + productsTopic + " succesfully retrieved");
            System.out.println(productQuantity.keySet());
            productQuantity.keySet().forEach((value) -> System.out.println(value+" with quantity "+productQuantity.get(value)));
        }
        recoverConsumer.unsubscribe();
    }
    private void recoverOrders(){
        KafkaConsumer recoverConsumer = ProducerConsumerFactory.initializeConsumer(serverAddr,ordersGroup,isolationLevelStrategy);
        recoverConsumer.subscribe(Collections.singletonList(insertOrderTopic));
        int counter=0;
        if(orders.isEmpty()) {
            ConsumerRecords<String, String> records = recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for (ConsumerRecord<String, String> record : records) {
                Order o = new Order(record.value());
                orders.put(o.getCode(), o);
                counter++;
            }
            System.out.println(counter + "  messages for topic " + insertOrderTopic + " succesfully retrieved");
            orders.keySet().forEach((value) -> System.out.print(orders.get(value)));
            System.out.println("retrieved");
        }
        recoverConsumer.unsubscribe();
    }

    public static void main(String[] args) throws NegativeQuantityException, NoSuchUserException, QuantityNotAvailableException, ProductAlreadyExistsException {
    }
}


