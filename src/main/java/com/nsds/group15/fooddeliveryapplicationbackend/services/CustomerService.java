package com.nsds.group15.fooddeliveryapplicationbackend.services;

import com.nsds.group15.fooddeliveryapplicationbackend.entity.Customer;
import com.nsds.group15.fooddeliveryapplicationbackend.entity.Order;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.CustomerAlreadyExistsException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.FailInRegistrationExceptions;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.NegativeQuantityException;
import com.nsds.group15.fooddeliveryapplicationbackend.exception.QuantityNotAvailableException;
import com.nsds.group15.fooddeliveryapplicationbackend.utils.ProducerConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
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

    private List<Customer> customers=new ArrayList<>();
    private String registrationTopic="RegistrationTopic";
    private KafkaProducer<String,String> producer;
    private  String serverAddr = "localhost:9092";
    private static final String producerTransactionalId = "customerServiceTransactionalId";


    /****** FAULT TOLLERANCE ******/
    private KafkaConsumer<String,String> recoverConsumer;
    private static final String offsetResetStrategy = "earliest";
    private static final String isolationLevelStrategy="read_committed";
    private static final String customerGroup = "customerGroup";


    public CustomerService(){
        initialize();
        recoverCustomers();
    }



    public void registration(Customer c) throws CustomerAlreadyExistsException, FailInRegistrationExceptions {

        //all this has to be in a transaction, because if one thing fail the registration must fail

        //not totally sure if the beginTransaction has to be before the if statement
        //TODO The list of the customer must become persistent in someway, fix this later
        if(!customers.contains(c)){
            producer.beginTransaction();
            String value=c.getEmail()+"#"+c.getName()+"#"+c.getSurname()+"#"+c.getAddress();
            String key="Key1"; //TODO for now we use a single key for all messages and one single partition
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

    private void recoverCustomers(){
        recoverConsumer= ProducerConsumerFactory.initializeConsumer(serverAddr, customerGroup, isolationLevelStrategy);

        recoverConsumer.subscribe(Collections.singletonList(registrationTopic));
        int counter=0;
        if(customers.isEmpty()){
            ConsumerRecords<String,String> records= recoverConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            recoverConsumer.seekToBeginning(records.partitions());
            for(ConsumerRecord<String,String> record : records){
                customers.add(new Customer(record.value()));
                counter++;
            }
            System.out.println("<<< Cached Users >>>");
            System.out.println(counter+ " customers succesfully retrieved");
            customers.forEach((value) -> System.out.println(value.getEmail()));
        }
        recoverConsumer.unsubscribe();
    }


    public static void main(String[] args) {
        CustomerService cs=new CustomerService();

        for(int i=9;i<100;i++) {
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
