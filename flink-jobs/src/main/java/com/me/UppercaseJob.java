package com.me;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UppercaseJob {
        public static void main(String[] args) throws Exception {
                // Set up the execution environment
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // Configure env
                env.setParallelism(32);

                // ENABLE CHECKPOINTING (for persistence)
                env.enableCheckpointing(60_000); // Checkpoint every 10 seconds
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
                env.getCheckpointConfig().setCheckpointTimeout(600_000);
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

                // Configure Kafka source
                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers("kafka:29092")
                                .setTopics("test-topic")
                                .setGroupId("consumer-flink-uppercase")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                // Create data stream from Kafka
                DataStream<String> stream = env.fromSource(
                                source,
                                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner(
                                                                (event, timestamp) -> System.currentTimeMillis()),
                                "Kafka Source");

                stream
                                .map(String::toUpperCase)
                                .print(); // Print results to stdout

                // Execute the job
                env.execute("Uppercase Job");
        }
}
