package com.atguigu.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {
    private List<Event> HeaderEvents;
    @Override
    public void initialize() {
        HeaderEvents = new ArrayList<>();
    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();
//        if (body.contains("hello")) headers.put("type","atguigu");
//        headers.put("")
        if (body.contains("hello")) {
            headers.put("type", "atguigu");
        } else {
            headers.put("type", "bigdata");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        HeaderEvents.clear();

        for (Event event : list) {
            intercept(event);
        }

        return HeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
