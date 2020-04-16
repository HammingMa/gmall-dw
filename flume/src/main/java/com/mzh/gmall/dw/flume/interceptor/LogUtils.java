package com.mzh.gmall.dw.flume.interceptor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    public static boolean validateStart(String startLog) {

        String log = startLog.trim();
        if (StringUtils.isBlank(log)) {
            return false;
        }

        if (!log.startsWith("{") || !log.endsWith("}")) {
            return false;
        }

        return true;
    }

    public static boolean validateEvent(String eventLog) {
        String log = eventLog.trim();
        if (StringUtils.isBlank(log)) {
            return false;
        }

        String[] split = log.split("\\|");

        String timestamp = split[0].trim();

        if (!NumberUtils.isDigits(timestamp) || timestamp.length() != 13) {
            return false;
        }

        String events = split[1].trim();


        if (!events.startsWith("{") || !events.endsWith("}")) {
            return false;
        }

        return true;
    }
}
