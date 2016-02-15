package com.sksamuel.kafka.embedded

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}

class EmbeddedKafkaTest extends WordSpec with Matchers with Eventually with StrictLogging {

  "EmbeddedKafka" should {
    "receive messages" in {

      val topic = "test-topic"

      val config = EmbeddedKafkaConfig()
      val kafka = new EmbeddedKafka(config)
      kafka.start()

      val producerProps = new Properties
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + config.kafkaPort)
      val producer = new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)
      val record = new ProducerRecord[String, String](topic, "hello world")
      producer.send(record).get(1, TimeUnit.MINUTES)
      logger.info("Msg sent")

      producer.close(1, TimeUnit.MINUTES)

      val consumerProps = new Properties
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + config.kafkaPort)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "myconsumer")
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val consumer = new KafkaConsumer[String, String](consumerProps, new StringDeserializer, new StringDeserializer)
      consumer.subscribe(util.Arrays.asList(topic))

      val records = consumer.poll(5000)
      records.iterator.next.value shouldBe "hello world"

      consumer.close()
      kafka.stop()
    }
  }
}
