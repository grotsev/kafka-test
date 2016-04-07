package com.test.groups;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {
  
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
    Producer<String, String> producer = new KafkaProducer<>(props);
    long sum = 0;
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10 * 1000 * 1000; i++) {
      producer.send(new ProducerRecord<String, String>("test-topic2", Integer.toString(i), Integer
          .toString(i)));
      sum += i;
    }
    long end = System.currentTimeMillis();
    
    System.out.println("Sum : " + sum);
    System.out.println("Time : " + (end - start));
    
    producer.close();
  }
  
}
