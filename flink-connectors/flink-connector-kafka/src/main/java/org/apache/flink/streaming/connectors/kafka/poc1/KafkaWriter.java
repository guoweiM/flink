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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

class KafkaWriter<T> implements Writer<T, FlinkKafkaProducer.KafkaTransactionState> {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

	private final Properties producerConfig;

	private final String attemptId;

	private final KafkaSerializationSchema<T> kafkaSchema;

	// ============================== Runtime =====================================

	private FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState;

	private Integer count;

	private FlinkKafkaInternalProducer producer;

	public KafkaWriter(final USink.InitialContext initialContext, final Properties producerConfig, KafkaSerializationSchema<T> kafkaSchema) {
		this.producerConfig = producerConfig;
		this.kafkaSchema = kafkaSchema;
		this.attemptId = initialContext.getSubtaskIndex() + "-" + initialContext.getSessionId();
		this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put("isolation.level", "read_committed");

		this.count = 0;
		this.kafkaTransactionState = create();

		this.producer = create2();
	}

	@Override
	public void write(T t, Context context, Collector<FlinkKafkaProducer.KafkaTransactionState> collector) throws Exception {
		ProducerRecord<byte[], byte[]> record = kafkaSchema.serialize(t, System.currentTimeMillis());
//		kafkaTransactionState.getProducer().send(record);

		producer.send(record);
	}

	@Override
	public void persistent(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws Exception {

		producer.flush();
		long producerId = producer.getProducerId();
		short epoch = producer.getEpoch();
		producer.close(Duration.ofSeconds(0));


		FlinkKafkaInternalProducer<String, String> resumeProducer = new FlinkKafkaInternalProducer<>(producerConfig);
		LOG.info("-||||||||||||||||||||---------------------------------------");
		try {
			resumeProducer.resumeTransaction(producerId, epoch);
			resumeProducer.commitTransaction();
		} finally {
			resumeProducer.close(Duration.ofSeconds(5));
		}


		producer = create2();


//		kafkaTransactionState.getProducer().flush();
//		kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
//		LOG.info(
//			kafkaTransactionState.getTransactionalId() + ":::" +
//				kafkaTransactionState.getProducer().getProducerId() + ":::" +
//				kafkaTransactionState.getProducer().getEpoch());
//		//output.collect(kafkaTransactionState);
//
//		final Properties properties = new Properties();
//		properties.putAll(this.producerConfig);
//		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, kafkaTransactionState.getTransactionalId());
//		final FlinkKafkaInternalProducer<byte[], byte[]> flinkKafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
//
//		flinkKafkaInternalProducer.resumeTransaction(kafkaTransactionState.getProducerId(), kafkaTransactionState.getEpoch());
//
//		flinkKafkaInternalProducer.commitTransaction();
//
//		flinkKafkaInternalProducer.close(Duration.ofSeconds(5));
//		kafkaTransactionState = create();

	}

	@Override
	public void flush(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws IOException {
//		kafkaTransactionState.getProducer().flush();
//		kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
//		output.collect(kafkaTransactionState);
	}

	@Override
	public void close() {
		//kafkaTransactionState.getProducer().close(Duration.ofSeconds(5));
	}

	private FlinkKafkaProducer.KafkaTransactionState create() {
		final Properties properties = new Properties();
		properties.putAll(this.producerConfig);
//		final String transactionId = attemptId + "-" + count;
		final String transactionId = UUID.randomUUID().toString();
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

		final FlinkKafkaInternalProducer kafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
		kafkaInternalProducer.initTransactions();
		kafkaInternalProducer.beginTransaction();
		count++;
		return new FlinkKafkaProducer.KafkaTransactionState(transactionId, kafkaInternalProducer);
	}

	private FlinkKafkaInternalProducer<byte[], byte[]> create2() {
		final String transactionId = UUID.randomUUID().toString();
		this.producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
		FlinkKafkaInternalProducer<byte[], byte[]> fp = new FlinkKafkaInternalProducer<>(producerConfig);
		fp.initTransactions();
		fp.beginTransaction();
		return fp;
	}
}
