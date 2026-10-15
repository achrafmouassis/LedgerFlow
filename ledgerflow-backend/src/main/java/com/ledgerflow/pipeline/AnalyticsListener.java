package com.ledgerflow.pipeline;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.core.JacksonException;

@Component
@ConditionalOnProperty(name = "ledgerflow.pipeline.enabled", havingValue = "true")
public class AnalyticsListener {
    private final ObjectMapper mapper;
    private final AnalyticsProjection projection;
    public AnalyticsListener(ObjectMapper mapper, AnalyticsProjection projection) {
        this.mapper = mapper; this.projection = projection;
    }
    @KafkaListener(id = "ledgerflowAnalytics", topics = "${ledgerflow.pipeline.topic}",
        groupId = "${ledgerflow.pipeline.consumer-group}")
    public void receive(String json) {
        OperationEvent event;
        try { event = mapper.readValue(json,OperationEvent.class); }
        catch (JacksonException failure) { throw new InvalidEventException("Malformed operation JSON",failure); }
        if (event == null) throw new InvalidEventException("Missing operation event");
        projection.apply(event);
        // RECORD ack occurs after this returns, hence after the projection DB commit.
    }
}
