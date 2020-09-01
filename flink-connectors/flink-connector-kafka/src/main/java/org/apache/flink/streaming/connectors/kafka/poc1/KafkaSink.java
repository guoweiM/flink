package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class KafkaSink<T> implements USink<T, FlinkKafkaProducer.KafkaTransactionState> {

	private final Properties properties;

	private final KafkaSerializationSchema<T> kafkaSerializationSchema;

	private final String name;

	private long checkpointInterval;

	public KafkaSink(Properties properties, KafkaSerializationSchema<T> kafkaSerializationSchema, String name, long checkpointInterval) {
		this.properties = properties;
		this.kafkaSerializationSchema = kafkaSerializationSchema;
		this.name = name;
		this.checkpointInterval = checkpointInterval;
	}

	@Override
	public Writer<T, FlinkKafkaProducer.KafkaTransactionState> createWriter(InitialContext context) throws Exception {
		return new KafkaWriter<>(name, checkpointInterval, context, properties, kafkaSerializationSchema);
	}

	@Override
	public CommitFunction<FlinkKafkaProducer.KafkaTransactionState> createCommitFunction() {
		return new KafkaCommitter(properties);
	}
}
