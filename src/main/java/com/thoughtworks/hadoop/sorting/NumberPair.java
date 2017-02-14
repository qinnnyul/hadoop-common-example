package com.thoughtworks.hadoop.sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberPair implements Writable, Comparable<NumberPair> {
    private IntWritable userID;
    private DoubleWritable followers;

    public NumberPair(IntWritable userID, DoubleWritable followers) {
        this.userID = userID;
        this.followers = followers;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.userID.write(dataOutput);
        this.followers.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userID.readFields(dataInput);
        this.followers.readFields(dataInput);
    }

    @Override
    public int compareTo(NumberPair other) {
        return this.followers.compareTo(other.getFollowers());
    }

    public IntWritable getUserID() {
        return userID;
    }

    public DoubleWritable getFollowers() {
        return followers;
    }
}
