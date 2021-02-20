package org.satel.streamjoin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@ComponentScans({
//        @ComponentScan(basePackageClasses = KafkaStreamConfig.class)
//})
public class StreamJoinApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamJoinApplication.class, args);
    }
}
