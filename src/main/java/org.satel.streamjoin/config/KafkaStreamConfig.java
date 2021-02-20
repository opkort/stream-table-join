package org.satel.streamjoin.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Slf4j
@Configuration
public class KafkaStreamConfig {

    @Value("${service.kafka.inputTopic:in}")
    private String inputTopic;

    @Value("${service.kafka.outputTopic:out}")
    private String outputTopic;

    @Value("${service.kafka.server}")
    private String bootstrapServers;

    @Value("${service.appName}")
    private String appName;


    @Bean
    public KafkaStreams kafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamText = builder.stream("stream-join");
        KTable<String, String> tableText = builder.table("table-join");
        //streamText.peek((k,v) -> log.info("Input message: " + " key: " + k + " value: " + v));
        streamText
                .join(tableText, (lValue, rValue) -> "left" + lValue.toUpperCase() + ", right=" + rValue)
                .peek((k, v) -> log.info(k + " : " + v))
                .to("result");
//        tableText.toStream().peek((k,v) -> log.info("Input message: " + " key: " + k + " value: " + v));

        //KafkaStreams kafkaStreams = stream.kStreams(testApp, inputTopic, outputTopic);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
        return kafkaStreams;
    }
}
