package org.z.entities.converter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
 
import java.io.IOException; 
import java.lang.reflect.Constructor; 
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.z.entities.converter.model.EntityReport;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;

public class ConverterUtils {

	private ActorSystem system;
	private SchemaRegistryClient schemaRegistry;
	private String topic;
	final static public Logger logger = Logger.getLogger(ConverterUtils.class);
	static {
		setDebugLevel(logger);
	}

	public ConverterUtils(ActorSystem system, SchemaRegistryClient schemaRegistry) {
		this.system = system;
		this.schemaRegistry = schemaRegistry; 
		this.topic = System.getenv("INTERFACE_NAME");
	}

	public ConverterUtils() {
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> getSink() {
		ProducerSettings<Object, Object> producerSettings = createProducerSettings(system); 

		return Producer.plainSink(producerSettings);
	}

	public ConsumerSettings<String, String> createConsumerSettings(ActorSystem system) {

		return ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer()) 
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"))
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}


	private ProducerSettings<Object, Object> createProducerSettings(ActorSystem system) {

		KafkaAvroSerializer keySerializer = new KafkaAvroSerializer(schemaRegistry);
		keySerializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
		return ProducerSettings
				.create(system, keySerializer, new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"));
	}

	public <converterClass> AbstractConverter getConverterForInterface(String interfaceName) throws Exception     {

		/*
		 * To create a new converter for interface - 
		 * 1. Create a new class that extends from AbstractConverter
		 * 2. Add the interface name and the full class name to converter_for_interface.properties 
		 * */		

		//String directory =    "src/main/resources";
		URL url = ClassLoader.getSystemResource("converter_for_interface.properties"); 

		Properties prop = new Properties();
		prop.load(url.openStream());  
		String className = prop.getProperty(interfaceName);
		logger.debug("The converter class is "+className);
		if(Strings.isNullOrEmpty(className)) {
			return null;
		}  
		Class<?> cl = Class.forName(className);
		Constructor<?> constructor = cl.getConstructor(String.class,ConcurrentHashMap.class);
		AbstractConverter converter = (AbstractConverter) constructor.newInstance(interfaceName,getPartitionsMap());

		return converter; 

	}

	public Schema getSchema(String name) throws IOException, RestClientException {
		int id = schemaRegistry.getLatestSchemaMetadata(name).getId();
		return schemaRegistry.getByID(id);
	}

	public EntityReport getEntityReportFromJson(String json) throws JsonParseException, JsonMappingException, IOException {

		EntityReport entityReport = new ObjectMapper().readValue(json, EntityReport.class);  

		return entityReport;
	} 

	/**
	 * The option are - 
	 * Trace < Debug < Info < Warn < Error < Fatal. 
	 * Trace is of the lowest priority and Fatal is having highest priority.  
	 * When we define logger level, anything having higher priority logs are also getting printed
	 * 
	 * @param debugLevel
	 */
	public static void setDebugLevel(Logger logger) {

		String debugLevel = System.getenv("DEBUG_LEVEL");		
		if( Strings.isNullOrEmpty(debugLevel)) {
			debugLevel = "ALL";
		} 

		switch (debugLevel) {

		case "ALL":
			logger.setLevel(Level.ALL);
			break;
		case "DEBUG":
			logger.setLevel(Level.DEBUG);
			break;
		case "INFO":
			logger.setLevel(Level.INFO);
			break;
		case "ERROR":
			logger.setLevel(Level.ERROR);
			break;
		case "WARNING":
			logger.setLevel(Level.WARN); 
			break;
		default:
			logger.setLevel(Level.ALL);
		} 
	} 

	public int getPartition (int numPartitions,String key) {  		 
		try(StringSerializer stringSerializer = new StringSerializer()) {
			byte[] keyBytes	 = stringSerializer.serialize(topic, key); 
			return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;					
		}	
	} 

	public ConcurrentHashMap<Integer, AtomicLong> getPartitionsMap() {		
		ConcurrentHashMap<Integer, AtomicLong> map = new ConcurrentHashMap<Integer, AtomicLong>();

		Properties props = getKafkaConsumerProperties(); 
		try(KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props)) {
			int numPartitions = consumer.partitionsFor(topic).size();		
			for(int i = 0; i < numPartitions; i++) {			
				map.put(i, getLastOffestForTopic(consumer,topic, i));			
			}   
		} 
		return map;
	}

	private AtomicLong getLastOffestForTopic(KafkaConsumer<Object, Object> consumer,String topic, int partition) {

		TopicPartition topicPartition = new TopicPartition(topic, partition); 
		long lastOffset; 
		consumer.assign(Arrays.asList(topicPartition));
		consumer.seekToEnd(Arrays.asList(topicPartition));
		lastOffset  = consumer.position(topicPartition);   

		System.out.println("The latest offset is "+lastOffset+ " for "+topicPartition.toString());
		return new AtomicLong(lastOffset);
	}

	private Properties getKafkaConsumerProperties() {

		String kafkaIP = System.getenv("KAFKA_ADDRESS");
		String schemaRegistryIP = System.getenv("SCHEMA_REGISTRY_IP");

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIP);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

		props.put("schema.registry.url", schemaRegistryIP);
		props.put("group.id", "group1");

		return props;
	}

}
