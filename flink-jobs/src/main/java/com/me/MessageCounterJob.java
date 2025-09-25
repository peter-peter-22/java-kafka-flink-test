package com.me;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class MessageCounterJob {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:29092")
            .setTopics("test-topic")
            .setGroupId("consumer-flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create data stream from Kafka
        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()),
            "Kafka Source"
        );

        // Process the stream: count messages per minute
        stream
            .flatMap(new MessageSplitter())
            .keyBy(value -> value.f0)  // Group by message content
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .sum(1)  // Sum the counts
            .print();  // Print results to stdout

        // Execute the job
        env.execute("Message Counter Job");
    }

    // Helper class to split messages and assign count of 1
    public static class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // For simplicity, we'll use the message content as the key
            // In a real scenario, you might want to extract specific fields
            out.collect(new Tuple2<>(value, 1));
        }
    }
}