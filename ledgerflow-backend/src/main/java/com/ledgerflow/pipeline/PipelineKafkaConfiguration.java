package com.ledgerflow.pipeline;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.kafka.core.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.*;
import java.util.*;
import java.time.Duration;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "ledgerflow.pipeline.enabled", havingValue = "true")
public class PipelineKafkaConfiguration {
    @Bean public ProducerFactory<String,String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,10000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,5000);
        return new DefaultKafkaProducerFactory<>(props);
    }
    @Bean public KafkaTemplate<String,String> kafkaTemplate(ProducerFactory<String,String> factory) {
        return new KafkaTemplate<>(factory);
    }
    @Bean public ConsumerFactory<String,String> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        return new DefaultKafkaConsumerFactory<>(props);
    }
    @Bean public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory(
            ConsumerFactory<String,String> factory, KafkaTemplate<String,String> kafka) {
        var listener = new ConcurrentKafkaListenerContainerFactory<String,String>();
        listener.setConsumerFactory(factory);
        listener.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        var deadLetter = new DeadLetterPublishingRecoverer(kafka,
            (record,error) -> new org.apache.kafka.common.TopicPartition(record.topic()+".DLT",record.partition()));
        deadLetter.setFailIfSendResultIsError(true);
        deadLetter.setWaitForSendResultTimeout(Duration.ofSeconds(15));
        var errors = new DefaultErrorHandler(deadLetter,new FixedBackOff(1000L,FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.addNotRetryableExceptions(InvalidEventException.class);
        listener.setCommonErrorHandler(errors);
        return listener;
    }
    @Bean public KafkaAdmin.NewTopics pipelineTopics(@Value("${ledgerflow.pipeline.topic}") String topic) {
        return new KafkaAdmin.NewTopics(new NewTopic(topic,3,(short)1),new NewTopic(topic+".DLT",3,(short)1));
    }
}
