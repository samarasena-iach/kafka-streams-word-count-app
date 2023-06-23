package com.samiach.wordcountapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/**
 * Youtube Tutorial
 * Kafka Streams: Zero to Hero - Introduction
 * https://www.youtube.com/watch?v=MR5hllNC9hk
 **/
public class WordCountApp {
    public static void main(String[] args) {
        Properties props = new Properties();

        // Configuration that tells Kafka, the group-id that we'll be using in our application
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");

        // Location of our Kakfa Server
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        // Serdes (SERialize-DESerialize) classes are classes that we use inorder to tell Kafka Streams,
        // how to serialize & deserialize the messages in our Kafka Topics
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // This tells Kafka Streams to always read the earliest message in the queue
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // This tells the Kafka Streams application, how many bytes that we have to use for buffering
        // We're setting it to 0 as this is for testing purposes and that we want to see the immediate outputs
        // But in production, we must NOT set it to 0
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Build our topology
        streamsBuilder.<String, String>stream("sentences")
                .flatMapValues((readOnlyKey, value) -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to("word-count", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
