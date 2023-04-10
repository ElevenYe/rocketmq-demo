package org.rocketmq.example.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

    public String getClusterIps() {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("rocketmq-config.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
//          properties.list(System.out);
            return properties.getProperty("NAMESRV_ADDRS");
        } catch (IOException e) {
            throw new RuntimeException("rocketmq-config 配置文件的 NAMESRV_ADDRS 配置错误，请检查相关配置！");
        }
    }
}
