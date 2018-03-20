package com.dataminer.example.pojo;

import java.io.Serializable;

public class StudentCountry implements Serializable {
	
	private static final long serialVersionUID = -6880316066852006432L;
	
	private String name;
	private String country;

	public StudentCountry(String name, String country) {
		this.name = name;
		this.country = country;
	}

	public String getName() {
		return name;
	}

	public String getCountry() {
		return country;
	}

	public String toString() {
		return name + "," + country;
	}

}