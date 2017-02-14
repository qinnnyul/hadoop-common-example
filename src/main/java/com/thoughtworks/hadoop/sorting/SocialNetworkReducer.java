package com.thoughtworks.hadoop.sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SocialNetworkReducer extends Reducer<Text, NumberPair, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<NumberPair> values, Context context) throws IOException, InterruptedException {
        for (NumberPair numberPair : values) {
            context.write(numberPair.getUserID(), numberPair.getFollowers());
        }

    }
}
