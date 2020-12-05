package com.hadoop.conf;

import com.hadoop.yarn.WordCountDriver;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import javax.inject.Inject;

@RestController
@ComponentScan("com.hadoop")
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class Application extends WebMvcConfigurationSupport {

    @Inject
    private WordCountDriver wordCountDriver;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
    }

    @RequestMapping("/home")
    String home() {
        return "Hello World!";
    }

    @RequestMapping("/runWordCountJob")
    String runWordCountJob() throws Exception {
        boolean res = wordCountDriver.runWordCountJob();
        return res ? "SUCCESS" : "FAIL";
    }
}