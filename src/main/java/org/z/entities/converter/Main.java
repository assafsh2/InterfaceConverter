package org.z.entities.converter; 

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;  

import java.util.Arrays; 
import java.util.concurrent.CompletionStage; 

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;  
import org.apache.kafka.common.serialization.StringSerializer; 
import org.apache.log4j.Logger;  

import akka.Done;
import akka.actor.ActorSystem; 
import akka.kafka.ProducerSettings;
import akka.kafka.Subscription;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer; 
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer; 
import akka.stream.javadsl.Flow; 
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class Main {

	public static boolean testing = false;
	final static public Logger logger = Logger.getLogger(Main.class);
	static {
		ConverterUtils.setDebugLevel(logger);
	}

	public static void main(String[] args) throws Exception {

		logger.debug("KAFKA_ADDRESS::::::::" + System.getenv("KAFKA_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_ADDRESS::::::::" + System.getenv("SCHEMA_REGISTRY_ADDRESS"));
		logger.debug("SCHEMA_REGISTRY_IDENTITY::::::::" + System.getenv("SCHEMA_REGISTRY_IDENTITY")); 
		logger.debug("INTERFACE_NAME::::::::" + System.getenv("INTERFACE_NAME"));
		
		final ActorSystem system = ActorSystem.create();
		SchemaRegistryClient schemaRegistry;
		String interfaceName;

		if(testing) {
			schemaRegistry = new MockSchemaRegistryClient();
			interfaceName = "source0"; 
		}
		else {
			interfaceName = System.getenv("INTERFACE_NAME");
			schemaRegistry = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_ADDRESS"), Integer.parseInt(System.getenv("SCHEMA_REGISTRY_IDENTITY")));		
		}
		
		ConverterUtils utils = new ConverterUtils(schemaRegistry); 
		AbstractConverter converter = utils.getConverterForInterface(interfaceName);
		if(converter == null) {

			logger.error("Converter doesn't exist for "+interfaceName);
			System.exit(-1);
		}

		final ActorMaterializer materializer = ActorMaterializer.create(system); 	
		Consumer.plainSource(utils.createConsumerSettings(system),
				(Subscription) Subscriptions.assignment(new TopicPartition(interfaceName+"-raw-data", 0)))
				.via(Flow.fromFunction(converter::apply))
				.to(utils.getSink())
				.run(materializer);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				system.terminate();	 
			}
		});
		

		if(testing) { 
		 	writeSomeData(system,materializer);
		}

		logger.debug("Ready");
		while(true) {
			Thread.sleep(3000);
		} 

	} 

	public static void writeSomeData(ActorSystem system, ActorMaterializer materializer ) { 

		/*
		 * 
		 * {"basicAttributes": {"coordinate": {"lat": 4.5, "long": 3.4}, "isNotTracked": false, "entityOffset": 50, "sourceName": "source1"},
		 *  "speed": 4.7, "elevation": 7.8, 
		 * "course": 8.3, "nationality": "USA", "category": "boat", "pictureURL": "huh?", "height": 6.1, 
		 * "nickname": "rerere", "externalSystemID": "id1"}
		 * 
		 */
		ProducerSettings<String, String> producerSettings = ProducerSettings
				.create(system, new StringSerializer(),  new StringSerializer())
				.withBootstrapServers(System.getenv("KAFKA_ADDRESS"));

		Sink<ProducerRecord<String, String>, CompletionStage<Done>> sink = Producer.plainSink(producerSettings);


		String timestamp = Long.toString(System.currentTimeMillis());

		String externalSystemID = "source1_id1";
		String lat = "4.4";
		String xLong = "6.6";
		String sourceName = System.getenv("INTERFACES_NAME");


		String json = "{\"id\":\""+externalSystemID+"\"," 
				+"\"lat\":\""+lat+"\"," 
				+"\"xlong\":\""+xLong+"\"," 
				+"\"source_name\":\""+sourceName+"\"," 
				+"\"category\":\"boat\","
				+"\"speed\":\"444\", "
				+"\"course\":\"5.55\", "
				+"\"elevation\":\"7.8\"," 
				+"\"nationality\":\"USA\"," 
				+"\"picture_url\":\"URL\", "
				+"\"height\":\"44\","
				+"\"nickname\":\"mick\"," 
				+" \"timestamp\":\""+timestamp+"\"  }"; 

		ProducerRecord<String, String> producerRecord  = new ProducerRecord<String, String>("source0-raw-data", json);
		
		
	 

		Source.from(Arrays.asList(producerRecord))
		.to(sink)
		.run(materializer); 
	} 
}

