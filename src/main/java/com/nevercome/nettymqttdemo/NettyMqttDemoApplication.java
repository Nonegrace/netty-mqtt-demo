package com.nevercome.nettymqttdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NettyMqttDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(NettyMqttDemoApplication.class, args);
        new BootNettyServer().startUp();
    }

}
