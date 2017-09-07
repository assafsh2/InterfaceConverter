package org.z.entities.converter.implement;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.z.entities.converter.AbstractConverter;
import org.z.entities.converter.model.EntityReport;
import org.z.entities.schema.basicEntityAttributes;
import org.z.entities.schema.category;
import org.z.entities.schema.coordinate;
import org.z.entities.schema.generalEntityAttributes;
import org.z.entities.schema.nationality;

public class Source1Converter extends AbstractConverter {

	public Source1Converter(String interfaceName) {
		super(interfaceName);

	}

	@Override
	public ProducerRecord<Object, Object> apply(ConsumerRecord<String,String> record) { 

		System.out.println("Source1Converter "+ interfaceName+" got "+record.toString());

		ProducerRecord<Object, Object> convertedData;
		try {  
			convertedData = new ProducerRecord<>(interfaceName ,getGenericRecordFromJson((String)record.value()));
			System.out.println("Source1Converter send "+convertedData.toString());

			return convertedData;
		} catch (IOException | RestClientException e) {

			e.printStackTrace();
		}

		return null;
	}

	private GenericRecord getGenericRecordFromJson(String data) throws IOException, RestClientException {

		EntityReport entityReport = utils.getEntityReportFromJson(data);

		coordinate location = coordinate.newBuilder().setLat(entityReport.getLat())
				.setLong$(entityReport.getXlong())
				.build();

		basicEntityAttributes basicEntity = basicEntityAttributes.newBuilder().setCoordinate(location)
				.setEntityOffset(0)
				.setIsNotTracked(false)
				.setSourceName(entityReport.getSource_name())
				.build();

		generalEntityAttributes entity = generalEntityAttributes.newBuilder()
				.setCategory(category.valueOf(entityReport.getCategory()))
				.setCourse(entityReport.getCourse())
				.setElevation(entityReport.getElevation())
				.setExternalSystemID(entityReport.getId())
				.setHeight(entityReport.getHeight())
				.setNationality(nationality.valueOf(entityReport.getNationality().toUpperCase()))
				.setNickname(entityReport.getNickname())
				.setPictureURL(entityReport.getPicture_url())
				.setSpeed(entityReport.getSpeed())
				.setBasicAttributes(basicEntity)
				.build();

		return entity;
	}
}