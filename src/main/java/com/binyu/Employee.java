package com.binyu;

import lombok.Data;

@Data
public class Employee {
    private int id;
    private String name;
    private String position;
    private double salary;

    // standard constructor, getters, setters
}