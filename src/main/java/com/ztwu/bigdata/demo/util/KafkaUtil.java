package com.ztwu.bigdata.demo.util;

import java.util.Properties;

public class KafkaUtil {
    public static String KAFKA_TOPIC = "****";

    public static Properties makeKafkaProperties(boolean isOnline) {
        // 设置kafka相关逻辑
        Properties properties = new Properties();
        properties.setProperty("****.****", "****:9092");
        properties.setProperty("group.id", "****-****-****");
        properties.setProperty("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "true");
        if (isOnline) {
            properties.put("zookeeper.servers",
                    "****:2181,****:2181,****:2181");
            properties.put("bootstrap.servers", "****:9092");
        }
        return properties;
    }
}
