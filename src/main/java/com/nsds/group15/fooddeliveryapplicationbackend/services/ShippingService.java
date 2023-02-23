package com.nsds.group15.fooddeliveryapplicationbackend.services;



import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.entity.Shipping;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ShippingService {
    private List<Shipping> shippings;
    private Map<String, Customer> customers;
    private List<Order> orders;
    private String insertOrderTopic="InsertOrderTopic";
    private String registrationTopic="RegistrationTopic";
    private static final String groupId="shippingGroup";
    KafkaConsumer<String, String> registrationConsumer;
    KafkaConsumer<String, String> orderConsumer;
    private static final String serverAddr = "localhost:9092";
    private static final String offsetResetStrategy = "latest";
    private static final boolean readUncommitted = false;


    public ShippingService(){
        initialize();
        this.shippings=new ArrayList<>();
        this.orders=new ArrayList<>();
        this.customers=new ArrayList<>();
    }


    private void initialize(){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty("log4j.logger.kafka", "WARN");
        registrationConsumer = new KafkaConsumer<>(props);
        orderConsumer=new KafkaConsumer(props);
        registrationConsumer.subscribe(Collections.singleton(registrationTopic));
        orderConsumer.subscribe(Collections.singleton(insertOrderTopic));
    }


    private void updateListOfCustomers(){
        final ConsumerRecords<String, String> records = registrationConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        for (final ConsumerRecord<String, String> record : records) {
            StringTokenizer stringTokenizer = new StringTokenizer(record.value(), "#");
            Customer customer = new Customer("", "", "", "");
            customer.setEmail(stringTokenizer.nextToken());
            customer.setName(stringTokenizer.nextToken());
            customer.setSurname(stringTokenizer.nextToken());
            customer.setAddress(stringTokenizer.nextToken());
            customers.put(customer.getEmail(), customer);
            System.out.println("Partition: " + record.partition() +
                    "\tOffset: " + record.offset() +
                    "\tKey: " + record.key() +
                    "\tValue: " + record.value()
            );

        }
    }

    private void updateListOfShippings(){
        final ConsumerRecords<String, String> records = orderConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
        for (final ConsumerRecord<String, String> record : records) {
            Shipping shipping= new Shipping(record.value());
            Customer customer=customers.get(shipping.getCustomerEmail());
            shipping.setAddress(customer.getAddress());
            System.out.println("Partition: " + record.partition() +
                    "\tOffset: " + record.offset() +
                    "\tKey: " + record.key() +
                    "\tValue: " + record.value()
            );
        }
    }


    public static void main(String[] args){

        ShippingService ss=new ShippingService();

        while(true){
            ss.updateListOfCustomers();
        }
    }





}
