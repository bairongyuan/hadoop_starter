package com.kkb.practice01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PersonBean implements WritableComparable<PersonBean> {
    private String name;
    private int age;
    private int salary;

    public PersonBean() {
    }

    public PersonBean(String name, int age, int salary) {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return name + " " + age + " " + salary;
    }

    @Override
    public int compareTo(PersonBean o) {
        int compareSalary = Integer.compare(this.salary, o.salary);
        if (compareSalary == 0) {
            return Integer.compare(this.age, o.age);
        }
        return -compareSalary;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(salary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        age = in.readInt();
        salary = in.readInt();
    }
}
