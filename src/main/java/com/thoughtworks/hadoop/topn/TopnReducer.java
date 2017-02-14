package com.thoughtworks.hadoop.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class TopnReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private PriorityQueue<User> priorityQueue = new PriorityQueue<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] data = value.toString().split("\t");
            int followers = Integer.parseInt(data[1]);

            User user = priorityQueue.peek();
            if (priorityQueue.size() <= 3 || followers > user.getFollowers()) {
                priorityQueue.add(new User(followers, value));
                if (priorityQueue.size() > 3){
                    priorityQueue.remove(user);
                }
            }
        }
        while(!priorityQueue.isEmpty()){
            context.write(NullWritable.get(), priorityQueue.poll().getRecord());
        }
    }
}
