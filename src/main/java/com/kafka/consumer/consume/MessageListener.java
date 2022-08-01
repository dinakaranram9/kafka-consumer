package com.kafka.consumer.consume;

import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;
@Slf4j
public class MessageListener {
		
	@KafkaListener(topics = "${spring.kafka.consumer.t1.topic}", id = "kafkaecmsconsumer")
	public void consumeMessageFromT1(String message) {
			log.info(message);
			System.out.println("consumed message from T1 : {}"+ message);
		
	}
	
}
