package com.thoughtworks.hadoop.sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SocialNetworkMapper extends Mapper<LongWritable, Text, Text, NumberPair> {

    public static final int FOLLOWER_POSITION = 1;
    public static final int USER_ID_POSITION = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        int userId = Integer.valueOf(data[USER_ID_POSITION]);
        Double followers = Double.valueOf(data[FOLLOWER_POSITION]);
        context.write(new Text("A"), new NumberPair(new IntWritable(userId),
                new DoubleWritable(followers)));
    }
}
