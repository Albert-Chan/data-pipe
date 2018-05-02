package com.dataminer.DAG.v1.example.pojo;

import java.io.Serializable;

public class StudentFullProperties implements Serializable {
	
	private static final long serialVersionUID = -6880316066852006432L;
	
	private String name;
	private int age;
	private String country;

	public StudentFullProperties(String name, int age, String country) {
		this.name = name;
		this.age = age;
		this.country = country;
	}

	public String getName() {
		return name;
	}
	
	public int getAge() {
		return age;
	}

	public String getCountry() {
		return country;
	}

	public String toString() {
		return name + "," + age + "," + country;
	}

}