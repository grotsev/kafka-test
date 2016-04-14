package com.test.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NewApi {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test2");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("topic"));
    //final int minBatchSize = 200;
    List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      System.out.println(records.partitions());
      for (ConsumerRecord<String, String> record : records) {
        buffer.add(record);
      }
      //if (buffer.size() >= minBatchSize) {
      insertIntoDb(buffer);
      consumer.commitSync();
      buffer.clear();
      //}
    }
  }
  
  private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {
    System.out.println("Start " + buffer.size());
    int i = 0;
    for (ConsumerRecord<String, String> record : buffer) {
      //      System.out.println(i + " " + record.value());
      i++;
    }
    //    System.out.println("End " + buffer.size());
  }
}
