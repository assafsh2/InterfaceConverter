package org.z.entities.converter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function; 

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

 
public abstract class  AbstractConverter implements Function<ConsumerRecord<String,String>, ProducerRecord<Object, Object>> {
	
	protected String interfaceName;
	protected ConverterUtils utils;
	protected ConcurrentHashMap<Integer, AtomicLong> map; 
	
	public AbstractConverter(String interfraceName, ConcurrentHashMap<Integer, AtomicLong> map) {
		this.interfaceName = interfraceName;
		this.map = map;
		this.utils = new ConverterUtils();
	}

	@Override
	abstract public ProducerRecord<Object, Object> apply(ConsumerRecord<String,String> record);
}
