package com.aograph.characteristics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"com.aograph"})
public class CharacteristicsApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(CharacteristicsApplication.class, args);
    }

}
