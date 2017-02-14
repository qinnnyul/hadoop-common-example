package com.thoughtworks.hadoop.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class TopnMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private PriorityQueue<User> followersProprityQueue = new PriorityQueue<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        Integer followers = Integer.parseInt(data[1]);

        User user = followersProprityQueue.peek();

        if (followersProprityQueue.size() <= 3 || followers > user.getFollowers()) {
            followersProprityQueue.add(new User(followers, value));

            if (followersProprityQueue.size() > 3) {
                followersProprityQueue.poll();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!followersProprityQueue.isEmpty()) {
            context.write(NullWritable.get(), followersProprityQueue.poll().getRecord());
        }
    }
}
