package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.CustomerAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.FailInRegistrationExceptions;
import com.nsds.group15.fooddeliveryapplicationbackend.utils.ProducerConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
@Service
public class CustomerService {

    /****** DATA STRUCTURES ******/
    private List<Customer> customers=new ArrayList<>();

    /****** BROKER SERVER ADDRESS ******/
    private  String serverAddr = "localhost:9092";

    /****** CUSTOMER PRODUCER ******/
    private static final String producerTransactionalId = "customerServiceTransactionalId";
    private String customersTopic ="customersTopic";
    private KafkaProducer<String,String> producer;

    /****** FAULT TOLLERANCE ******/
    private KafkaConsumer<String,String> recoverConsumer;
    private static final String offsetResetStrategy = "earliest";
    private static final String isolationLevelStrategy="read_committed";
    private static final String customerGroup = "customerGroup";


    public CustomerService(){
        producer=ProducerConsumerFactory.initializeTransactionalProducer(serverAddr,customersTopic);
        recoverCustomers();
    }

    public void registration(Customer c) throws CustomerAlreadyExistsException, FailInRegistrationExceptions {
         if(!customers.contains(c)){
            producer.beginTransaction();
            String value=c.getEmail()+"#"+c.getName()+"#"+c.getSurname()+"#"+c.getAddress();
            String key=c.getEmail();
            ProducerRecord<String, String> record = new ProducerRecord<>(customersTopic, key, value);
            final Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata ack = future.get();
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

    private void recoverCustomers(){
        recoverConsumer= ProducerConsumerFactory.initializeConsumer(serverAddr, customerGroup, isolationLevelStrategy);
        recoverConsumer.subscribe(Collections.singletonList(customersTopic));
        int counter=0;
        if(customers.isEmpty()){
            ConsumerRecords<String,String> records= recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for(ConsumerRecord<String,String> record : records){
                customers.add(new Customer(record.value()));
                counter++;
            }
            System.out.println(counter+ " customers succesfully retrieved");
            customers.forEach((value) -> System.out.println(value.getEmail()));
        }
        recoverConsumer.unsubscribe();
    }


    public static void main(String[] args) {
        CustomerService cs=new CustomerService();

        for(int i=9;i<11;i++) {
            String email = "e" + i;
            String nome = "n" + i;
            String surname = "s" + i;
            String address = "a" + i;
            Customer c = new Customer(email, nome, surname, address);
            try {
                cs.registration(c);
            } catch (CustomerAlreadyExistsException | FailInRegistrationExceptions e) {
                e.printStackTrace();
            }
        }

    }


}
