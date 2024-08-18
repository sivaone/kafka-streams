package com.github.sivaone.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.time.Duration;
import java.util.List;

@SpringBootTest
public class TopologyTest {

    private final Logger log = LoggerFactory.getLogger(TopologyTest.class);

    private TopologyTestDriver testDriver;

    @Value("${app.topic.input}")
    private String inputTopicName;

    @Value("${app.topic.output}")
    private String outputTopicName;

    private TestInputTopic<Integer, String> testInputTopic;
    private TestOutputTopic<Integer, Long> testOutputTopic;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @BeforeEach
    public void setUp() {
        this.testDriver = new TopologyTestDriver(
                streamsBuilderFactoryBean.getTopology(),
                streamsBuilderFactoryBean.getStreamsConfiguration()
        );

        log.info("Topology description: {}", streamsBuilderFactoryBean.getTopology().describe().toString());
        log.info("Input topic: {}, output topic: {}", inputTopicName, outputTopicName);
        this.testInputTopic = testDriver.createInputTopic(inputTopicName, Serdes.Integer().serializer(), Serdes.String().serializer());
        this.testOutputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.Integer().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    public void tearDown() {
        if(this.testDriver != null) {
            this.testDriver.close();
        }
    }

    @Test
    public void testTopologyLogic() {
        testInputTopic.pipeInput(1, "test", 1L);
        testInputTopic.pipeInput(1, "test", 10L);
        testInputTopic.pipeInput(2, "test", 2L);

        Awaitility.waitAtMost(Duration.ofSeconds(5)).until(() -> testOutputTopic.getQueueSize() == 2L);
        Assertions.assertThat(testOutputTopic.readValuesToList()).isEqualTo(List.of(2L, 1L));
    }
}
