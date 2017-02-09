package com.thoughtworks.hadoop.filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BookMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static final int PRODUCT_TYPE = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        String productType = data[PRODUCT_TYPE];
        if ("Books".equalsIgnoreCase(productType)) {
            context.write(NullWritable.get(), value);
        }
    }
}
