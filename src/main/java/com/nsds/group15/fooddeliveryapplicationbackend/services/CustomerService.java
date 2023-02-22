package com.nsds.group15.fooddeliveryapplicationbackend.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class Customer{
    private String email,name,surname,address;

    public Customer(String email, String name, String surname, String address) {
        this.email = email;
        this.name = name;
        this.surname = surname;
        this.address = address;
    }

    public boolean equals(Object obj){
        if (obj == null || !(obj instanceof Customer)) {
            return false;
        }
        Customer other = (Customer) obj;
        return this.email.equals(other.email);
    }

    public String getSsn() {
        return email;
    }

    public void setSsn(String ssn) {
        this.email = ssn;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}


public class CustomerService {

    private List<Customer> customers;
    private String registrationTopic="RegistrationTopic";
    private KafkaProducer<String,String> producer;
    private  String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "custmerServiceTransactionalId";


    public CustomerService(){
        initialize();
    }
    public CustomerService(String serverAddr){
        initialize();
        this.serverAddr=serverAddr;
    }



    public void Registration(Customer c) throws CustomerAlreadyExistsException{
        //all this has to be in a transaction, because if one thing fail the registration must fail
        producer.initTransactions();
        producer.beginTransaction();//not totally sure if the beginTransaction has to be before the if statement
        //The list of the customer must become persistent in someway, fix this later
        if(!customers.contains(c)){
            customers.add(c);
            String value=c.getSsn()+"#"+c.getName()+"#"+c.getSurname()+"#"+c.getAddress();
            String key="Key1"; //to be changed, for now we use a single key for all message and one single partition
            ProducerRecord<String, String> record = new ProducerRecord<>(registrationTopic, key, value);
            final Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata ack = future.get();
                System.out.println("Success!");
            } catch (InterruptedException | ExecutionException e1) {
                e1.printStackTrace();
            }
        }
        else {
            throw new CustomerAlreadyExistsException();
        }
        producer.commitTransaction();
    }

    public void initialize(){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        producer = new KafkaProducer<>(props);
    }


}
