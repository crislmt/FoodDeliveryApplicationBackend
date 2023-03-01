package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.CustomerAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.FailInRegistrationExceptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
@Service
public class CustomerService {

    private List<Customer> customers;
    private String registrationTopic="RegistrationTopic";
    private KafkaProducer<String,String> producer;
    private  String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "custmerServiceTransactionalId";



    public CustomerService(){
        initialize();
        this.customers=new ArrayList<>();
    }



    public void registration(Customer c) throws CustomerAlreadyExistsException, FailInRegistrationExceptions {

        //all this has to be in a transaction, because if one thing fail the registration must fail

       //not totally sure if the beginTransaction has to be before the if statement
        //TODO The list of the customer must become persistent in someway, fix this later
        if(!customers.contains(c)){
            producer.beginTransaction();
            String value=c.getEmail()+"#"+c.getName()+"#"+c.getSurname()+"#"+c.getAddress();
            String key="Key1"; //TODO for now we use a single key for all message and one single partition
            ProducerRecord<String, String> record = new ProducerRecord<>(registrationTopic, key, value);
            final Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata ack = future.get();
                System.out.println("Success!");
                customers.add(c);
                for(Customer c1:customers){
                    System.out.println(c1.getEmail());
                }
                producer.commitTransaction();
            } catch (InterruptedException | ExecutionException e1) {
                producer.abortTransaction();
                throw new FailInRegistrationExceptions();
            }
        }
        else {
            throw new CustomerAlreadyExistsException();
        }

    }


    private void initialize(){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }




}
