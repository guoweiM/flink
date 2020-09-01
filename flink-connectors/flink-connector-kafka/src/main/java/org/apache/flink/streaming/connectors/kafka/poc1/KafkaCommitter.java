package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

class KafkaCommitter implements CommitFunction<FlinkKafkaProducer.KafkaTransactionState> {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);

	private final Properties properties;

	KafkaCommitter(Properties properties) {
		this.properties = properties;
		this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	}

	@Override
	public void commit(FlinkKafkaProducer.KafkaTransactionState commit) {

		LOG.info("1. ....... ..... ........ commit: " + commit);

		final Properties p = new Properties();
		p.putAll(properties);
		p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, commit.getTransactionalId());

		final FlinkKafkaInternalProducer<byte[], byte[]> flinkKafkaInternalProducer = new FlinkKafkaInternalProducer<>(p);

		flinkKafkaInternalProducer.resumeTransaction(commit.getProducerId(), commit.getEpoch());
		LOG.info("2. ....... ..... ........ commit: " + commit);

		flinkKafkaInternalProducer.commitTransaction();
		LOG.info("2. ....... ..... ........ commit: " + commit);

		flinkKafkaInternalProducer.close(Duration.ofSeconds(5));
	}
}
