package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

class KafkaWriter<T> implements Writer<T, FlinkKafkaProducer.KafkaTransactionState> {

	private final Properties producerConfig;

	private final String attemptId;

	private final KafkaSerializationSchema<T> kafkaSchema;

	// ============================== Runtime =====================================

	private FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState;

	private Integer count;

	public KafkaWriter(final USink.InitialContext initialContext, final Properties producerConfig, KafkaSerializationSchema<T> kafkaSchema) {
		this.producerConfig = producerConfig;
		this.kafkaSchema = kafkaSchema;
		this.attemptId = initialContext.getSubtaskIndex() + "-" + initialContext.getSessionId();
		this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		this.count = 0;
		this.kafkaTransactionState = create();
	}

	@Override
	public void write(T t, Context context, Collector<FlinkKafkaProducer.KafkaTransactionState> collector) throws Exception {
		ProducerRecord<byte[], byte[]> record = kafkaSchema.serialize(t, System.currentTimeMillis());
		kafkaTransactionState.getProducer().send(record);
	}

	@Override
	public void persistent(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws Exception {
		kafkaTransactionState.getProducer().flush();
		kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
		output.collect(kafkaTransactionState);

		kafkaTransactionState = create();

	}

	@Override
	public void flush(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws IOException {
		kafkaTransactionState.getProducer().flush();
//		kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
		output.collect(kafkaTransactionState);
	}

	@Override
	public void close() {

	}

	private FlinkKafkaProducer.KafkaTransactionState create() {
		final Properties properties = new Properties();
		properties.putAll(this.producerConfig);
		final String transactionId = attemptId + "-" + count;
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
		final FlinkKafkaInternalProducer kafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
		kafkaInternalProducer.initTransactions();
		kafkaInternalProducer.beginTransaction();
		count++;
		return new FlinkKafkaProducer.KafkaTransactionState(transactionId, kafkaInternalProducer);
	}
}
