package com.nsds.group15.fooddeliveryapplicationbackend.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessagesUtilities {
    public static void printRecord(ProducerRecord pr, String serviceName){
        System.out.println("Registration message sent by "+serviceName);
        System.out.println("Partition: " + pr.partition() +
                "\tKey: " + pr.key() +
                "\tValue: " + pr.value()
        );
    }

    public static void printRecord(ConsumerRecord cr, String serviceName){
        System.out.println("Registration message read by "+serviceName);
        System.out.println("Partition: " + cr.partition() +
                "\tOffset: " + cr.offset() +
                "\tKey: " + cr.key() +
                "\tValue: " + cr.value()
        );
    }
}
