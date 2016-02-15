package com.sksamuel.kafka.embedded

import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.{TimeUnit, Executors}

import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

import scala.util.Try

class EmbeddedKafka(config: EmbeddedKafkaConfig) extends StrictLogging {

  private val datadir = Files.createTempDirectory("kafka_data_dir")
  private val logDir = Files.createTempDirectory("kafka_log_dir")
  private val logFlushInterval = 1

  private val zookeeperProps = new Properties()
  zookeeperProps.setProperty("clientPort", config.zookeeperPort.toString)
  zookeeperProps.setProperty("dataDir", datadir.toFile.getAbsolutePath)

  private val quorumConfiguration = new QuorumPeerConfig
  quorumConfiguration.parseProperties(zookeeperProps)

  private val zookeeperConfig = new ServerConfig
  zookeeperConfig.readFrom(quorumConfiguration)

  val zookeeperServer = new ZooKeeperServerMain {
    // this is needed to expose the stop method as its protected in the super class
    def stop(): Try[Unit] = Try {
      logger.info(s"Stopping embedded zookeeper [${config.zookeeperBroker}]")
      super.shutdown()
      logger.info(s"Zookeeper stopped")
    }
  }

  val kafkaProps = new Properties
  kafkaProps.setProperty("host.name", "localhost")
  kafkaProps.setProperty("default.replication.factor", config.defaultReplicationFactor.toString)
  kafkaProps.setProperty("port", config.kafkaPort.toString)
  kafkaProps.setProperty("broker.id", config.brokerId.toString)
  kafkaProps.setProperty("zookeeper.connect", config.zookeeperBroker)
  kafkaProps.setProperty("auto.create.topics.enable", config.autoCreateTopics.toString)
  kafkaProps.setProperty("log.dir", logDir.toFile.getAbsolutePath)
  kafkaProps.setProperty("log.flush.interval.messages", logFlushInterval.toString)
  kafkaProps.setProperty("advertised.host.name", "localhost")

  val kafkaServer = new KafkaServer(new KafkaConfig(kafkaProps))

  private val executor = Executors.newFixedThreadPool(1)

  def start(): Unit = {
    import com.sksamuel.scalax.concurrent.ExecutorImplicits._
    executor.submit {
      logger.info(s"Starting embedded zookeeper [${config.zookeeperBroker}]")
      zookeeperServer.runFromConfig(zookeeperConfig)
    }
    // give zookeeper chance to startup
    Thread.sleep(2000)
    logger.info(s"Starting embedded kakfa server [${config.brokerList}]")
    kafkaServer.startup()
  }

  def stop(): Unit = {
    logger.info(s"Stopping embedded kakfa server [${config.brokerList}]")
    kafkaServer.shutdown()
    kafkaServer.awaitShutdown()
    zookeeperServer.stop()
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.MINUTES)
  }
}


