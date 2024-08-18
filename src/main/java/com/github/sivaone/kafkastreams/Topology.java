package com.github.sivaone.kafkastreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Suppressed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Configuration
@Component
@Slf4j
public class Topology {

    private final String inputTopic;
    private final String outputTopic;

    @Autowired
    public Topology(@Value("${app.topic.input}") final String inputTopic,
                    @Value("${app.topic.output}") final String outputTopic
    ) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Autowired
    public void defaultTopology(final StreamsBuilder streamsBuilder) {
        log.info("Topology builder start");
        streamsBuilder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
                .groupByKey()
                .count()
                .suppress(Suppressed.untilTimeLimit(Duration.ofMillis(5), Suppressed.BufferConfig.unbounded()))
                .toStream()
                .to(outputTopic);
    }
}
