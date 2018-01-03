
package org.z.entities.converter.implement;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException; 
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord; 
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.z.entities.converter.AbstractConverter;
import org.z.entities.converter.ConverterUtils;
import org.z.entities.converter.Main;
import org.z.entities.converter.model.EntityReport;
import org.z.entities.schema.BasicEntityAttributes;
import org.z.entities.schema.Category;
import org.z.entities.schema.Coordinate; 
import org.z.entities.schema.DetectionEvent;
import org.z.entities.schema.GeneralEntityAttributes;
import org.z.entities.schema.Nationality;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;

public class Source1Converter extends AbstractConverter {

	private ConcurrentHashSet<String> set;
	private KafkaProducer<Object, Object> producer;
	private Object obj= new Object();
	final static public Logger logger = Logger.getLogger(Source1Converter.class);	
	static {
		ConverterUtils.setDebugLevel(logger); 
	}

	public Source1Converter(String interfaceName, ConcurrentHashMap<Integer, AtomicLong> map) {
		super(interfaceName,map);
		set = new ConcurrentHashSet<String>();
		producer = new KafkaProducer<>(getProperties(true));

	}

	@Override
	public ProducerRecord<Object, Object> apply(ConsumerRecord<String,String> record) {
		logger.debug("Source1Converter "+ interfaceName+" got "+record.toString());
		try {  
			ProducerRecord<Object, Object> convertedData = getGenericRecordFromJson((String)record.value());
			logger.debug("Source1Converter send "+convertedData.toString());

			return convertedData;
		} catch (IOException | RestClientException e) {

			e.printStackTrace();
		}
		return null;
	}

	private ProducerRecord<Object, Object> getGenericRecordFromJson(String data) throws IOException, RestClientException {

		EntityReport entityReport = utils.getEntityReportFromJson(data);
		String metadata = entityReport.getMetadata();
		if(Strings.isNullOrEmpty(metadata)) {
			metadata = (String) GeneralEntityAttributes.SCHEMA$.getField("metadata").defaultVal();
		}

		Coordinate coordinate = Coordinate.newBuilder().setLat(entityReport.getLat())
				.setLong$(entityReport.getXlong())
				.build();

		int partition = utils.getPartitionByKey(interfaceName,entityReport.getId(), map.keySet().size());
		AtomicLong lastOffset = map.get(partition);

		String externalSystemId = entityReport.getId();


		if(!set.contains(externalSystemId)) {	
			System.out.println("New externalSystemId "+externalSystemId);
			publishToCreationTopic(externalSystemId, metadata, lastOffset.get(), partition);
			set.add(externalSystemId);			
		}


		BasicEntityAttributes basicEntity = BasicEntityAttributes.newBuilder().setCoordinate(coordinate) 
				.setIsNotTracked(false)
				.setSourceName(entityReport.getSource_name())
				.build();

		System.out.println("ExternalSystemID "+entityReport.getId()+" partition "+partition+ " offset "+lastOffset); 
		GeneralEntityAttributes entity = GeneralEntityAttributes.newBuilder()
				.setCategory(Category.valueOf(entityReport.getCategory()))
				.setCourse(entityReport.getCourse())
				.setElevation(entityReport.getElevation())
				.setExternalSystemID(entityReport.getId())
				.setHeight(entityReport.getHeight())
				.setNationality(Nationality.valueOf(entityReport.getNationality().toUpperCase()))
				.setNickname(entityReport.getNickname())
				.setPictureURL(entityReport.getPicture_url())
				.setSpeed(entityReport.getSpeed())
				.setBasicAttributes(basicEntity)
				.setMetadata(metadata)
				.setLastStateOffset(lastOffset.get())
				.build();

		lastOffset.incrementAndGet(); 
		return new ProducerRecord<>(interfaceName ,entityReport.getId(), entity);
	}

	private void publishToCreationTopic(String externalSystemId, String metadata, long lastOffset, int partition) {
		try {
			ProducerRecord<Object, Object> record = new ProducerRecord<>("create",externalSystemId,getGenericRecordForCreation(externalSystemId, metadata,lastOffset,partition));
			utils.getKafkaProducer().send(record); 
			System.out.println("Sent to create "+record);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
	}

	protected GenericRecord getGenericRecordForCreation(String externalSystemID, String metadata,long lastOffset,int partition)
			throws IOException, RestClientException { 
		DetectionEvent detectionEvent = DetectionEvent.newBuilder()
				.setSourceName(interfaceName)
				.setExternalSystemID(externalSystemID)
				.setDataOffset(lastOffset)
				.setMetadata(metadata)
				.setPartition(partition)
				.build();

		return detectionEvent;
	} 



	public Properties getProperties(boolean isAvro) {

		String kafkaAddress;
		String schemaRegustryUrl;
		if(Main.testing) {
			kafkaAddress = "192.168.0.50:9092 ";
			schemaRegustryUrl = "http://schema-registry.kafka:8081";
		}
		else {
			kafkaAddress = System.getenv("KAFKA_ADDRESS");
			schemaRegustryUrl = System.getenv("SCHEMA_REGISTRY_ADDRESS"); 		
		}		

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		if(isAvro) {
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroSerializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		}
		else {
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);	
		} 	
		props.put("schema.registry.url", schemaRegustryUrl);
		props.put("group.id", "group1");

		return props;
	}
}