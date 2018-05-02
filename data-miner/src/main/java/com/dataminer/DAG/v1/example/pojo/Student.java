package com.dataminer.DAG.v1.example.pojo;

import java.io.Serializable;

public class Student implements Serializable {
	private static final long serialVersionUID = -3167129622840426500L;

	private String name;
	private int age;

	public Student(String name, int age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public String toString() {
		return name + "," + age;
	}

}