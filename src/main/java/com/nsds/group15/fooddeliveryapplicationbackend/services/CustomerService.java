package com.nsds.group15.fooddeliveryapplicationbackend.services;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;

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
    private KafkaProducer<String,String> message;


    public boolean Registration(Customer c) throws CustomerAlreadyExistsException{
        if(!customers.contains(c)){
            customers.add(c);
        }
        throw new CustomerAlreadyExistsException();
    }

    public void initialize(){

    }


}
