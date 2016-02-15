package com.sksamuel.kafka.embedded

import com.typesafe.config.{Config, ConfigFactory}

case class EmbeddedKafkaConfig(zookeeperPort: Int = 2400,
                               kafkaPort: Int = 9400,
                               autoCreateTopics: Boolean = true,
                               brokerId: Int = 1,
                               defaultReplicationFactor: Int = 1) {
  def zookeeperBroker = s"localhost:$zookeeperPort"
  def brokerList = s"locahost:$kafkaPort"
}

object EmbeddedKafkaConfig {
  def apply(): EmbeddedKafkaConfig = apply(ConfigFactory.load())
  def apply(config: Config): EmbeddedKafkaConfig = EmbeddedKafkaConfig(
    config.getInt("embedded-kafka.zookeeper.port"),
    config.getInt("embedded-kafka.kafka.port"),
    config.getBoolean("embedded-kafka.kafka.autoCreateTopics"),
    config.getInt("embedded-kafka.kafka.brokerId"),
    config.getInt("embedded-kafka.kafka.defaultReplicationFactor")
  )
}