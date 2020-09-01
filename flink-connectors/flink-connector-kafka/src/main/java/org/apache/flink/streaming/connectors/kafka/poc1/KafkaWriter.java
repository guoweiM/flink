package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink.CleanUpUnmanagedCommittable;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class KafkaWriter<T> implements Writer<T, FlinkKafkaProducer.KafkaTransactionState>, CleanUpUnmanagedCommittable {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

	private static final ListStateDescriptor<TransactionInfo> TRANSACTION_INFO_STATE_DESC =
		new ListStateDescriptor<>("transaction-info-state", TransactionInfo.class);

	private final Properties producerConfig;

	private final KafkaSerializationSchema<T> kafkaSchema;

	private final ListState<TransactionInfo> transactionInfoState;

	private final List<TransactionInfo> mightNeedToCleanUp;

	// ============================== Runtime =====================================

	private FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState;

	private TransactionInfo transactionInfo;

	public KafkaWriter(
		final String sessionPrefix,
		final Long checkpointInterval,
		final USink.InitialContext initialContext,
		final Properties producerConfig,
		KafkaSerializationSchema<T> kafkaSchema) throws Exception {
		this.producerConfig = producerConfig;
		this.kafkaSchema = kafkaSchema;
		this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		this.transactionInfo = new TransactionInfo(
			sessionPrefix,
			initialContext.getSubtaskIndex(),
			initialContext.getAttemptNum(),
			0,
			checkpointInterval);
		this.kafkaTransactionState = create(transactionInfo);

		this.transactionInfoState = initialContext.getUnionListState(TRANSACTION_INFO_STATE_DESC);

		this.mightNeedToCleanUp = new ArrayList<>();

		for (TransactionInfo transactionInfo : transactionInfoState.get()) {
			mightNeedToCleanUp.add(transactionInfo);
		}
	}

	@Override
	public void write(T t, Context context, Collector<FlinkKafkaProducer.KafkaTransactionState> collector) throws Exception {
		ProducerRecord<byte[], byte[]> record = kafkaSchema.serialize(t, System.currentTimeMillis());
		kafkaTransactionState.getProducer().send(record);

		kafkaTransactionState.setNum(kafkaTransactionState.getNum() + 1);
	}

	@Override
	public void persistent(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws Exception {

		if (kafkaTransactionState.getNum() == 0) {
			kafkaTransactionState.getProducer().abortTransaction();
		} else {
			kafkaTransactionState.getProducer().flush();
			kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
			output.collect(kafkaTransactionState);
		}

		transactionInfoState.clear();
		for (TransactionInfo ti : mightNeedToCleanUp) {
			transactionInfoState.add(ti);
		}
		transactionInfoState.add(transactionInfo);

		transactionInfo = new TransactionInfo(transactionInfo);
		kafkaTransactionState = create(transactionInfo);

	}

	@Override
	public void flush(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws IOException {
		kafkaTransactionState.getProducer().abortTransaction();

//		if (kafkaTransactionState.getNum() == 0) {
//			kafkaTransactionState.getProducer().abortTransaction();
//		} else {
//			kafkaTransactionState.getProducer().flush();
//			kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
//			output.collect(kafkaTransactionState);
//		}
	}

	@Override
	public void close() {
		//kafkaTransactionState.getProducer().close(Duration.ofSeconds(5));
	}

	private FlinkKafkaProducer.KafkaTransactionState create(TransactionInfo transactionInfo) {
		final Properties properties = new Properties();
		properties.putAll(this.producerConfig);
		final String transactionId = transactionInfo.getTransactionId();
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

		final FlinkKafkaInternalProducer kafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
		kafkaInternalProducer.initTransactions();
		kafkaInternalProducer.beginTransaction();
		return new FlinkKafkaProducer.KafkaTransactionState(transactionId, kafkaInternalProducer);
	}

	@Override
	public void cleanUp() {
		for (TransactionInfo  transactionInfo : mightNeedToCleanUp) {
			LOG.info("[USK] Cleanup ................ " + transactionInfo);
			final List<String> ids = transactionInfo.getAllPossibleIdleTransactionIds();
			for (String id : ids) {
				final Properties myConfig = new Properties();
				myConfig.putAll(producerConfig);
				myConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, id);
				final FlinkKafkaInternalProducer cleanup =  new FlinkKafkaInternalProducer<>(myConfig);
				// it suffices to call initTransactions - this will abort any lingering transactions
				cleanup.initTransactions();
				cleanup.close(Duration.ofSeconds(0));
			}
		}
		mightNeedToCleanUp.clear();
	}

	static class TransactionInfo {

		// The transaction name prefix
		private final String sessionPrefix;

		// The transaction belongs to which subtask;
		private final Integer subtaskIndex;

		// The transaction belongs to which attempt;
		private final Integer attemptNum;

		// The last traction begin time
		private final Long transactionBeginTime;

		// The transaction count in this session
		private final int count;

		// Use this to infer the max transaction id; But this is a very hacky way!!!
		private final Long checkpointInterval;

		// Transaction format
		private static final String ID_FORMAT = "%s-%s-%s-%s";

		TransactionInfo(
			String sessionPrefix,
			Integer subtaskIndex,
			Integer attemptNum,
			int count,
			Long checkpointInterval) {

			this.sessionPrefix = sessionPrefix;
			this.subtaskIndex = subtaskIndex;
			this.attemptNum = attemptNum;
			this.transactionBeginTime = System.currentTimeMillis();
			this.count = count;
			this.checkpointInterval = checkpointInterval;
		}

		TransactionInfo(TransactionInfo transactionInfo) {
			this.sessionPrefix = transactionInfo.getSessionPrefix();
			this.subtaskIndex = transactionInfo.getSubtaskIndex();
			this.attemptNum = transactionInfo.getAttemptNum();
			this.transactionBeginTime = System.currentTimeMillis();
			this.count = transactionInfo.getCount() + 1;
			this.checkpointInterval = transactionInfo.getCheckpointInterval();
		}

		public List<String> getAllPossibleIdleTransactionIds() {
			// TODO:: if time is too long so there might no need to cleanup.
			final Long currentTime = System.currentTimeMillis();
			final Long num = (currentTime - transactionBeginTime) / checkpointInterval;
			final List<String> r = new ArrayList<>();
			for (int i = 0; i <= num; i++) {
				r.add(String.format(ID_FORMAT, sessionPrefix, subtaskIndex, attemptNum, count + i));
			}
			return r;
		}

		// The transaction id is {sessionPrefix}-{subIndex}-{attemptNum}-{count};
		public String getTransactionId() {
			return String.format(ID_FORMAT, sessionPrefix, subtaskIndex, attemptNum, count);
		}

		public String getSessionPrefix() {
			return sessionPrefix;
		}

		public Integer getAttemptNum() {
			return attemptNum;
		}

		public Long getTransactionBeginTime() {
			return transactionBeginTime;
		}

		public int getCount() {
			return count;
		}

		public Long getCheckpointInterval() {
			return checkpointInterval;
		}

		public Integer getSubtaskIndex() {
			return subtaskIndex;
		}

		@Override
		public String toString() {
			return "TransactionInfo{" +
				"sessionPrefix='" + sessionPrefix + '\'' +
				", subtaskIndex=" + subtaskIndex +
				", attemptNum=" + attemptNum +
				", transactionBeginTime=" + transactionBeginTime +
				", count=" + count +
				", checkpointInterval=" + checkpointInterval +
				'}';
		}
	}
}
