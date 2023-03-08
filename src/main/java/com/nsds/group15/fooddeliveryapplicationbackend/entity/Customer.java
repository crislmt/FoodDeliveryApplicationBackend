package com.nsds.group15.fooddeliveryapplicationbackend.entity;

import java.util.StringTokenizer;

public class Customer{

    private String email,name,surname,address;

    public Customer(String email, String name, String surname, String address) {
        this.email = email;
        this.name = name;
        this.surname = surname;
        this.address = address;
    }

    public Customer(String msg){
        StringTokenizer stk = new StringTokenizer(msg,"#");
        this.email=stk.nextToken();
        this.name=stk.nextToken();
        this.surname=stk.nextToken();
        this.address=stk.nextToken();
    }

    public boolean equals(Object obj){
        if (obj == null || !(obj instanceof Customer)) {
            return false;
        }
        Customer other = (Customer) obj;
        return this.email.equals(other.email);
    }

    @Override
    public String toString(){
        return this.email;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
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