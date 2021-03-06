package com.ztwu.bigdata.demo

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaProducerDemo2 {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "kafka:9092")// 指定响应方式
    //prop.put("acks", "0")
    prop.put("acks", "all")
    // 请求失败重试次数
    //prop.put("retries", "3")
    // 指定key的序列化方式, key是用于存放数据对应的offset
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "60000")
    //prop.put("batch.size", "16384")
    //prop.put("linger.ms", "1")
    //prop.put("buffer.memory", "33554432")

    // 得到生产者的实例
    val producer = new KafkaProducer[String, String](prop)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置时间格式
    // 模拟一些数据并发送给kafka
    for (i <- 1 to 100) {
      val dNow = new Date()
      val time = sdf.format(dNow)
      var msg = ""
      if(i >= 10){
        msg = "{'browse_user': 'user_1', 'browse_time': '2020-12-21 17:30:00'}"
      }else{
        msg =s"{'browse_time': '${time}', 'browse_user': 'user_${i}'}"
      }
      println("send -->" + msg)
      // 得到返回值
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String]("kafkademo1", msg)).get()
      println(rmd.toString)
      Thread.sleep(500)
    }
    producer.close()
  }
}
