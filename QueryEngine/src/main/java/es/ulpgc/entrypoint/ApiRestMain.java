package es.ulpgc.entrypoint;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "es.ulpgc")
@EnableScheduling // Enable scheduled tasks
public class ApiRestMain {
    public static void main(String[] args) {
        SpringApplication.run(ApiRestMain.class, args);
    }
}