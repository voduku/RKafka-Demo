package com.example.rkafka;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  String server;

  @Bean
  public NewTopic topic() {
    return TopicBuilder.name("topic")
        .partitions(1)
        .replicas(1)
        .build();

  }

  @Bean
  public ReactiveKafkaProducerTemplate<Integer, String> reactiveProducer() {
    return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
        ProducerConfig.CLIENT_ID_CONFIG, "producer",
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    )));
  }

  @Bean
  public ReactiveKafkaConsumerTemplate<Integer, String> reactiveConsumer() {
    ReceiverOptions<Integer, String> options = ReceiverOptions.create(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
        ConsumerConfig.GROUP_ID_CONFIG, "consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
    ));
    return new ReactiveKafkaConsumerTemplate<>(options.subscription(List.of("topic")));
  }

  @Bean
  public KafkaTemplate<Integer, String> producer() {
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
        ProducerConfig.CLIENT_ID_CONFIG, "producer",
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    )));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<Integer, String> consumer() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> fac = new ConcurrentKafkaListenerContainerFactory<>();
    fac.setConsumerFactory(new DefaultKafkaConsumerFactory<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
        ConsumerConfig.GROUP_ID_CONFIG, "consumer",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
    )));

    return fac;
  }

}
