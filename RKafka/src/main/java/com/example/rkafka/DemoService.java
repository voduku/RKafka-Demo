package com.example.rkafka;

import java.util.stream.IntStream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.kafka.sender.SenderResult;

@Slf4j
@Value
@Service
public class DemoService {

  ReactiveKafkaProducerTemplate<Integer, String> rProducer;
  ReactiveKafkaConsumerTemplate<Integer, String> rConsumer;
  KafkaTemplate<Integer, String> producer;

//  @EventListener(ApplicationStartedEvent.class)
//  public void produce() {
//    IntStream.range(0, 1000000000)
//        .parallel()
//        .boxed()
//        .map(i -> "message_" + i)
//
//        .peek(msg -> producer.send("topic", msg))
//        .forEach(msg -> log.info("sending {}", msg));
//  }
//
//  @KafkaListener(topics = {"topic"}, groupId = "consumer-1")
//  public void consume(String msg) {
//    log.info("Consumed {}", msg);
//  }

  @EventListener(ApplicationStartedEvent.class)
  public ParallelFlux<SenderResult<?>> produce() {
    return Flux.range(0, 100)
        .parallel()
        .map(i -> "message_" + i)
        .doOnNext(msg -> log.info("sending {}", msg))
        .flatMap(msg -> rProducer.send("topic", msg));
  }

  @EventListener(ApplicationStartedEvent.class)
  public Flux<String> startKafkaConsumer() {
    return rConsumer
        .receiveAutoAck()
        // .delayElements(Duration.ofSeconds(2L)) // BACKPRESSURE
        .map(ConsumerRecord::value)
        .doOnNext(msg -> log.info("Consuming {}", msg))
        .doOnError(throwable -> log.error("something bad happened while consuming : {}",
            throwable.getMessage()));
  }
}
