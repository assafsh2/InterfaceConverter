package org.z.entities.converter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException; 
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import org.apache.avro.Schema; 
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.apache.kafka.common.serialization.StringDeserializer; 
import org.z.entities.converter.implement.Source1Converter;
import org.z.entities.converter.model.EntityReport;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;

public class Utils {

	private ActorSystem system;
	private SchemaRegistryClient schemaRegistry;

	public Utils(ActorSystem system, SchemaRegistryClient schemaRegistry) {

		this.system = system;
		this.schemaRegistry = schemaRegistry; 
	}

	public Utils() {
	}

	public Sink<ProducerRecord<Object, Object>, CompletionStage<Done>> getSink() {
		ProducerSettings<Object, Object> producerSettings = createProducerSettings(system); 
		
		return Producer.plainSink(producerSettings);

	}

	public ConsumerSettings<String, String> createConsumerSettings(ActorSystem system) {


		return ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer()) 
				.withBootstrapServers(getKafkaURL())
				.withGroupId("group1")
				.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}


	private ProducerSettings<Object, Object> createProducerSettings(ActorSystem system) {

		KafkaAvroSerializer keySerializer = new KafkaAvroSerializer(schemaRegistry);
		keySerializer.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), true);
		return ProducerSettings
				.create(system, keySerializer, new KafkaAvroSerializer(schemaRegistry))
				.withBootstrapServers(getKafkaURL());
	}


	public AbstractConverter getConverterForInterface(String interfaceName) {

		AbstractConverter converter = null;

		switch(interfaceName) {

		case "source0":	
		case "source1":	
		case "source2":
		case "source3":
		case "source4":
		case "source5":
		case "source6":
		case "source7":
		case "source8":
		case "source9":
		case "source10":
		case "source11":
		case "source12":

			converter = new Source1Converter(interfaceName);
			break;
		default: 

		}

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

	public String getKafkaURL() {

		String kafkaUrl;
		if(Main.testing) {
			kafkaUrl = "192.168.0.51:9092";
		}
		else {
			kafkaUrl = System.getenv("KAFKA_ADDRESS");
		}
		return kafkaUrl;
	}

}
