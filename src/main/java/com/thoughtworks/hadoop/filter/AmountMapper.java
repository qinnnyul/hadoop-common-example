package com.thoughtworks.hadoop.filter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AmountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final int USERNAME_POSITION = 1;
    public static final int AMOUNT_POSITION = 3;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        String username = data[USERNAME_POSITION];
        String amount = data[AMOUNT_POSITION];
        context.write(new Text(username), new IntWritable(Integer.valueOf(amount)));
    }
}
