package org.z.entities.converter;

import java.util.function.Function; 

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

 
public abstract class  AbstractConverter implements Function<ConsumerRecord<String,String>, ProducerRecord<Object, Object>> {
	
	protected  String interfaceName;
	protected Utils utils;
	
	public AbstractConverter(String interfraceName) {
		this.interfaceName = interfraceName;
		this.utils = new Utils();
	}

	@Override
	abstract public ProducerRecord<Object, Object> apply(ConsumerRecord<String,String> record);
}
