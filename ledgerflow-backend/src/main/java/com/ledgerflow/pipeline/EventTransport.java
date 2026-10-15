package com.ledgerflow.pipeline;

/** Broker boundary; separated so failure/crash windows can be tested without Kafka. */
public interface EventTransport {
    void send(OperationEvent event) throws Exception;
}
