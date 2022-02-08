package com.apple.bean;

import java.sql.Timestamp;

/**
 * @author atguigu-mqx
 */

public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public static Event of(String user, String url, Long timestamp) {
        return new Event(user, url, timestamp);
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
