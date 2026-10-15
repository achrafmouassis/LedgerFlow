package com.ledgerflow.pipeline;

import com.ledgerflow.banking.BankingService;
import static com.ledgerflow.banking.BankingApi.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="TEST_KAFKA_BOOTSTRAP",matches=".+")
@ActiveProfiles("test")
@SpringBootTest(properties={
    "ledgerflow.pipeline.enabled=true",
    "ledgerflow.pipeline.publisher-scheduling-enabled=false",
    "ledgerflow.pipeline.topic=ledgerflow.operations.integration.v1",
    "ledgerflow.pipeline.consumer-group=ledgerflow-integration-v1",
    "spring.kafka.bootstrap-servers=${TEST_KAFKA_BOOTSTRAP}"
})
class KafkaPipelineTests {
    @Autowired BankingService banking;
    @Autowired OutboxRelay relay;
    @Autowired EventTransport transport;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;
    static final String TOPIC="ledgerflow.operations.integration.v1";
    void await(BooleanSupplier ready) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        while(System.nanoTime()<deadline) {
            if(ready.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail("Kafka processing did not complete within 30 seconds");
    }
    long facts(UUID id) { return jdbc.queryForObject("SELECT COUNT(*) FROM analytics.transaction_facts WHERE event_id=?",Long.class,id); }
    void awaitCommitted(RecordMetadata metadata) throws Exception {
        try(var admin=AdminClient.create(Map.of("bootstrap.servers",bootstrap))) {
            await(() -> {
                try {
                    var offsets=admin.listConsumerGroupOffsets("ledgerflow-integration-v1").partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);
                    var value=offsets.get(new TopicPartition(metadata.topic(),metadata.partition()));
                    return value!=null && value.offset()>metadata.offset();
                } catch(Exception unavailable) { return false; }
            });
        }
    }
    @Test void realBrokerCarriesCommittedOperationsAndDuplicateDeliveryIsIgnored() throws Exception {
        var a=banking.create(new CreateAccount("Kafka source","MAD",BigDecimal.ZERO));
        var b=banking.create(new CreateAccount("Kafka destination","MAD",BigDecimal.ZERO));
        var deposit=banking.deposit(a.id(),UUID.randomUUID().toString(),new CashRequest(new BigDecimal("50")));
        var withdrawal=banking.withdraw(a.id(),UUID.randomUUID().toString(),new CashRequest(new BigDecimal("20")));
        var transfer=banking.transfer(UUID.randomUUID().toString(),new CreateTransfer(a.id(),b.id(),new BigDecimal("10")));
        for(int i=0;i<1000;i++) {
            OutboxRelay.Result result=relay.publishNext(transport);
            if(result==OutboxRelay.Result.EMPTY) break;
            assertEquals(OutboxRelay.Result.PUBLISHED,result);
        }
        await(() -> facts(deposit.id())==1 && facts(withdrawal.id())==1 && facts(transfer.id())==1);
        var e=new OperationEvent(1,deposit.id(),deposit.id(),"DEPOSIT",null,a.id(),deposit.amount(),deposit.currency(),deposit.createdAt());
        var metadata=kafka.send(TOPIC,e.eventId().toString(),mapper.writeValueAsString(e)).get(15,TimeUnit.SECONDS).getRecordMetadata();
        awaitCommitted(metadata);
        assertEquals(1,facts(deposit.id()));
        assertEquals(new BigDecimal("20.00"),banking.account(a.id()).balance());
        assertEquals(new BigDecimal("10.00"),banking.account(b.id()).balance());
    }
    void assertDeadLetter(String json) throws Exception {
        Map<String,Object> props=new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"dlt-test-"+UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        try(var consumer=new KafkaConsumer<String,String>(props)) {
            consumer.subscribe(List.of(TOPIC+".DLT"));
            var metadata=kafka.send(TOPIC,UUID.randomUUID().toString(),json).get(15,TimeUnit.SECONDS).getRecordMetadata();
            boolean found=false;
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
            while(!found && System.nanoTime()<deadline)
                for(var record:consumer.poll(Duration.ofMillis(250)))
                    if(json.equals(record.value())) found=true;
            assertTrue(found,"Invalid event must be preserved in the dead-letter topic");
            awaitCommitted(metadata);
        }
    }
    @Test void malformedJsonIsPreservedInDeadLetterTopic() throws Exception {
        assertDeadLetter("not-json-"+UUID.randomUUID());
    }
    @Test void unsupportedSchemaIsPreservedWithoutCreatingFact() throws Exception {
        UUID id=UUID.randomUUID();
        var incompatible=new OperationEvent(2,id,id,"DEPOSIT",null,UUID.randomUUID(),BigDecimal.ONE,"MAD",java.time.Instant.now());
        assertDeadLetter(mapper.writeValueAsString(incompatible));
        assertEquals(0,facts(id));
    }
}
