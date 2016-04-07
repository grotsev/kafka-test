package com.test.groups;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerTest implements Runnable {
  private KafkaStream m_stream;
  private int m_threadNumber;
  
  public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
    m_threadNumber = a_threadNumber;
    m_stream = a_stream;
  }
  
  public void run() {
    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    int i = 0;
    long sum = 0;
    
    long start = System.currentTimeMillis();
    while (it.hasNext()) {
      String msg = new String(it.next().message());
      int num = Integer.parseInt(msg);
      sum += num;
      i++;
    }
    long end = System.currentTimeMillis();
    
    System.out.println("i : " + i);
    System.out.println("Sum : " + sum);
    System.out.println("Time : " + (end - start));
    System.out.println("Shutting down Thread: " + m_threadNumber);
  }
}
