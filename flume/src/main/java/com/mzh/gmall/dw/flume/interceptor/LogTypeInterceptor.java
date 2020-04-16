package com.mzh.gmall.dw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();

        String log = new String(body, Charset.forName("UTF-8"));

        Map<String, String> headers = event.getHeaders();

        if (log.contains("start")) {
            headers.put("topic", "topic_start");
        } else {
            headers.put("topic", "topic_event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


    public static void main(String[] args) {
        String log = "1586760247731|{\"cm\":{\"ln\":\"-66.8\",\"sv\":\"V2.7.9\",\"os\":\"8.0.7\",\"g\":\"64908Z4U@gmail.com\",\"mid\":\"999\",\"nw\":\"4G\",\"l\":\"en\",\"vc\":\"2\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"999\",\"t\":\"1586702571923\",\"la\":\"-3.8\",\"md\":\"HTC-12\",\"vn\":\"1.0.8\",\"ba\":\"HTC\",\"sr\":\"K\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1586749119067\",\"en\":\"display\",\"kv\":{\"goodsid\":\"260\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"4\",\"category\":\"83\"}},{\"ett\":\"1586750400309\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1586663907054\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1586723571086\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1586698161388\",\"en\":\"favorites\",\"kv\":{\"course_id\":2,\"id\":0,\"add_time\":\"1586722082970\",\"userid\":5}}]}";
        System.out.println(LogUtils.validateEvent(log));
    }
}
