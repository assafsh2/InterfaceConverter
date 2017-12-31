package org.z.entities.converter.implement;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException; 
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;

import org.apache.kafka.clients.consumer.ConsumerRecord; 
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.apache.log4j.Logger;
import org.z.entities.converter.AbstractConverter;
import org.z.entities.converter.ConverterUtils;
import org.z.entities.converter.model.EntityReport;
import org.z.entities.schema.BasicEntityAttributes;
import org.z.entities.schema.Category;
import org.z.entities.schema.Coordinate; 
import org.z.entities.schema.GeneralEntityAttributes;
import org.z.entities.schema.Nationality;
 
public class Source1Converter extends AbstractConverter {
	

	final static public Logger logger = Logger.getLogger(Source1Converter.class);
	static {
		ConverterUtils.setDebugLevel(logger); 
	}

	public Source1Converter(String interfaceName, ConcurrentHashMap<Integer, AtomicLong> map) {
		super(interfaceName,map);
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
}