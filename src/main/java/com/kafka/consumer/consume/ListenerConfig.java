package com.kafka.consumer.consume;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;


@Configuration
@EnableKafka
public class ListenerConfig {
	@Value("${spring.kafka.consumer.bootstrap-servers}")
	private String bootstrapServers;
	
	@Value("${spring.kafka.consumer.key-deserializer}")
	private String keyDeserializer;
	
	@Value("${spring.kafka.consumer.value-deserializer}")
	private String valueDeserializer;
	
	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;
	
	@Value("${spring.kafka.consumer.enable-auto-commit}")
	private String enableAutoCommit;
	
	@Value("${consumer.session.timeout.ms}")
	private String sessionTimeoutMSec;
	
	@Value("${spring.kafka.consumer.fetch-min-size}")
	private String fetchMinBytes;
	
	@Value("${consumer.receive.buffer.bytes}")
	private String receiveBufferBytes;
	
	@Value("${consumer.max.partition.fetch.bytes}")
	private String maxPartitionFetchBytes;
	
	@Value("${spring.kafka.consumer.max-poll-records}")
	private String maxPollRecords;
	
	@Value("${spring.kafka.consumer.auto-commit-interval}")
	private String autoCommitInterval;
	
	@Value("${consumer.request.timeout.ms}")
	private String requestTimeout;
	
	@Value("${spring.kafka.consumer.heartbeat-interval}")
	private String heartbeatInterval;
	
	@Value("${spring.kafka.consumer.auto-offset-reset}")
	private String consumerAutoOffset;
	
	@Value("${spring.kafka.consumer.fetch-max-wait}")
	private String consumerFetchWait;
	
	/**
	 * Build kafka consumer properties
	 * 
	 * @return Map<String, Object>
	 */
	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		// list of host:port pairs used for establishing the initial connections
		// to the Kakfa cluster
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		// consumer groups allow a pool of processes to divide the work of
		// consuming and processing records
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMSec);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
		props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerAutoOffset);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, consumerFetchWait);
		props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
		return props;
	}

	/**
	 * Build kafka consumer factory from consumer properties
	 * 
	 * @return ConsumerFactory<Integer, String>
	 */
	@Bean
	public ConsumerFactory<Integer, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	/**
	 * Build concurrent kafka listener
	 * 
	 * @return ConcurrentKafkaListenerContainerFactory<Integer, String>
	 */
	@Bean
	public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setPollTimeout(10000);
		return factory;
	}
	
	@Bean
	public MessageListener receiver() {
		return new MessageListener();
	}
}
