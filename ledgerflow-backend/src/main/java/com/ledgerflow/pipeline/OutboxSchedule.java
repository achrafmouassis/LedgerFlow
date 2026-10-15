package com.ledgerflow.pipeline;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "ledgerflow.pipeline", name = {"enabled","publisher-scheduling-enabled"}, havingValue = "true")
public class OutboxSchedule {
    private final OutboxRelay relay;
    private final EventTransport transport;
    public OutboxSchedule(OutboxRelay relay, EventTransport transport) { this.relay = relay; this.transport = transport; }
    @Scheduled(fixedDelayString = "${ledgerflow.pipeline.poll-delay-ms:1000}", initialDelayString = "${ledgerflow.pipeline.poll-delay-ms:1000}")
    public void publish() {
        for (int i=0;i<25;i++) {
            if (relay.publishNext(transport) != OutboxRelay.Result.PUBLISHED) break;
        }
    }
}
