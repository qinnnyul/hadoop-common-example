package com.thoughtworks.hadoop.topn;

import org.apache.hadoop.io.Text;

public class User implements Comparable<User> {

    private int followers;
    private Text record;

    public User(int followers, Text record) {
        this.followers = followers;
        this.record = record;
    }

    @Override
    public int compareTo(User other) {
        return this.followers - other.followers;
    }

    public int getFollowers() {
        return followers;
    }

    public Text getRecord() {
        return record;
    }
}
