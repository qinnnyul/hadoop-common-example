package com.thoughtworks.hadoop.summaries;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, NumPair> {

    public static final int MARITAL_STATUS_POSITION = 0;
    private static final int WORKING_HOURS_POSITION = 2;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split(",");

        String maritalStatus = data[MARITAL_STATUS_POSITION];
        Double hrs = Double.parseDouble(data[WORKING_HOURS_POSITION]);
        context.write(new Text(maritalStatus), new NumPair(hrs, 1));
    }
}