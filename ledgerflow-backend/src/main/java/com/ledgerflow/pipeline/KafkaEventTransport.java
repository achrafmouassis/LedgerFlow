package com.ledgerflow.pipeline;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "ledgerflow.pipeline.enabled", havingValue = "true")
public class KafkaEventTransport implements EventTransport {
    private final KafkaTemplate<String,String> kafka;
    private final ObjectMapper mapper;
    private final String topic;
    public KafkaEventTransport(KafkaTemplate<String,String> kafka, ObjectMapper mapper,
            @Value("${ledgerflow.pipeline.topic}") String topic) {
        this.kafka = kafka; this.mapper = mapper; this.topic = topic;
    }
    @Override public void send(OperationEvent event) throws Exception {
        kafka.send(topic, event.eventId().toString(), mapper.writeValueAsString(event))
            .get(15, TimeUnit.SECONDS);
    }
}
