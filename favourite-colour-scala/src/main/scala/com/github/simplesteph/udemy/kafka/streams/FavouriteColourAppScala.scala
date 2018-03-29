package com.github.simplesteph.udemy.kafka.streams

import java.lang
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}

object FavouriteColourAppScala {
  def main(args: Array[String]): Unit = {

    val config: Properties = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-scala")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    val builder: StreamsBuilder = new StreamsBuilder

    // Step 1: We create the topic of users keys to colours
    val textLines: KStream[String, String] = builder.stream[String, String]("favourite-colour-input")

    val usersAndColours: KStream[String, String] = textLines
      // 1 - we ensure that a comma is here as we will split on it
      .filter((key: String, value: String) => value.contains(","))
      // 2 - we select a key that will be the user id (lowercase for safety)
      .selectKey[String]((key: String, value: String) => value.split(",")(0).toLowerCase)
      // 3 - we get the colour from the value (lowercase for safety)
      .mapValues[String](new ValueMapper[String, String] {
        override def apply(value: String): String =  { value.split(",")(1).toLowerCase }
      })
      // 4 - we filter undesired colours (could be a data sanitization step)
      .filter((user: String, colour: String) => List("green", "blue", "red").contains(colour))

    val intermediaryTopic = "user-keys-and-colours-scala"
    usersAndColours.to(intermediaryTopic)

    // step 2 - we read that topic as a KTable so that updates are read correctly
    val usersAndColoursTable: KTable[String, String] = builder.table(intermediaryTopic)

    val stringSerde: Serde[String] = Serdes.String
    val longSerde: Serde[lang.Long] = Serdes.Long

    // step 3 - we count the occurences of colours
    val favouriteColours: KTable[String, lang.Long] = usersAndColoursTable
      // 5 - we group by colour within the KTable
      .groupBy(
        (user: String, colour: String) => new KeyValue[String, String](colour, colour),
        Serialized.`with`(stringSerde, stringSerde)
      )
      .count(Materialized.as[String, lang.Long, KeyValueStore[Bytes, Array[Byte]]]("CountsByColours")
        .withKeySerde(stringSerde)
        .withValueSerde(longSerde))

    // 6 - we output the results to a Kafka Topic - don't forget the serializers
    favouriteColours.toStream.to("favourite-colour-output-scala", Produced.`with`(stringSerde, longSerde))

    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.cleanUp()
    streams.start()

    // print the topology
    streams.localThreadsMetadata().forEach(t => System.out.print(t.toString))

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
  }
}
