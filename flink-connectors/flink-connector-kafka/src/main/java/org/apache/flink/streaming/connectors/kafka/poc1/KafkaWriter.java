package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

class KafkaWriter<T> implements Writer<T, FlinkKafkaProducer.KafkaTransactionState> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

	//TODO:: change the name and configure it
	private static final Integer MAX_SET_NUM = 128;

	private static final String NEXT_SET_ID_BEGIN_KEY = "next-set-id-begin";

	private static final String LAST_PARALLELISM_KEY = "last-parallelism";

	private static final ListStateDescriptor<SetStatus> TRANSACTION_IDS_OF_MANAGED_SETS =
		new ListStateDescriptor<>("managed-set-status", SetStatus.class);

	private static final MapStateDescriptor<String, Integer> SET_ID_INFO =
		new MapStateDescriptor<>("set-id-info", String.class, Integer.class);

	private final Properties producerConfig;

	private final KafkaSerializationSchema<T> kafkaSchema;

	private final ListState<SetStatus> managedSetStatus;

	private final List<SetStatus> currentManagedSetStatus;

	private final SetStatus activeSet;

	private final BroadcastState<String, Integer> setIdInfo;

	private final Integer nextSetIdBegin;

	// we need to create this topic
	private final String topicForState;

	private final String consumerGroupId = "TODO_TO_NEED_TO_CHANGE_LATER";

	private final String setNamePrefix = "TODO_TO_CHANGE_TO_OPERATOR_ID";

	// ============================== Runtime =====================================

	private FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState;

	public KafkaWriter(
		final USink.InitialContext initialContext,
		final Properties producerConfig,
		final String topicForState,
		KafkaSerializationSchema<T> kafkaSchema) throws Exception {

		this.producerConfig = producerConfig;
		this.kafkaSchema = kafkaSchema;
		this.topicForState = topicForState;
		this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		this.managedSetStatus = initialContext.getListState(TRANSACTION_IDS_OF_MANAGED_SETS);
		this.setIdInfo = initialContext.getBroadcastState(SET_ID_INFO);

		if (setIdInfo.get(LAST_PARALLELISM_KEY) == null) {
			setIdInfo.put(LAST_PARALLELISM_KEY, initialContext.getParallelism());
			setIdInfo.put(NEXT_SET_ID_BEGIN_KEY, 0);
			nextSetIdBegin = 0;
		} else {
			final Integer lastParallelism = setIdInfo.get(LAST_PARALLELISM_KEY);
			if (initialContext.getParallelism() > lastParallelism) {
				final Integer lastMaxSetId = setIdInfo.get(NEXT_SET_ID_BEGIN_KEY);
				setIdInfo.put(LAST_PARALLELISM_KEY, initialContext.getParallelism());
				setIdInfo.put(NEXT_SET_ID_BEGIN_KEY, lastMaxSetId + initialContext.getParallelism());
			}
			nextSetIdBegin = setIdInfo.get(NEXT_SET_ID_BEGIN_KEY);
		}

		LOG.info("[USK] CleanUp Managed Set ................ ");

		currentManagedSetStatus = cleanUpTheManagedSets(initialContext);
		Collections.sort(currentManagedSetStatus);
		this.activeSet = currentManagedSetStatus.get(0);

		LOG.info("[USK] CleanUp Un-Managed Set ................ ");
//		cleanUpUnmanagedSets(newManagedSetNum, initialContext.getSubtaskIndex());


		this.kafkaTransactionState = create(activeSet);
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
		managedSetStatus.clear();
		for (SetStatus setStatus : currentManagedSetStatus) {
			managedSetStatus.add(setStatus);
		}
		activeSet.addElement();
		recordSetStatus(activeSet);
		kafkaTransactionState = create(activeSet);
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

	private FlinkKafkaProducer.KafkaTransactionState create(SetStatus setStatus) {
		final Properties properties = new Properties();
		properties.putAll(this.producerConfig);
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, setStatus.getTransactionId());

		final FlinkKafkaInternalProducer kafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
		kafkaInternalProducer.initTransactions();
		kafkaInternalProducer.beginTransaction();
		return new FlinkKafkaProducer.KafkaTransactionState(setStatus.getTransactionId(), kafkaInternalProducer);
	}

	static class SetStatus implements Comparable<SetStatus> {

		private final String setNamePrefix;

		private final Integer setIndex;

		private Long nextElementNo;

		public SetStatus(String name, Integer setIndex) {
			this.setNamePrefix = name;
			this.setIndex = setIndex;
			this.nextElementNo = 0L;
		}

		public SetStatus(SetStatus setStatus) {
			this(setStatus.getSetNamePrefix(), setStatus.getSetIndex());
			this.nextElementNo = setStatus.getNextElementNo();
		}

		public SetStatus(String name, Integer setIndex, Long nextElementNo) {
			this(name, setIndex);
			this.nextElementNo = nextElementNo;
		}

		public String getSetNamePrefix() {
			return setNamePrefix;
		}

		public Integer getSetIndex() {
			return setIndex;
		}

		public Long getNextElementNo() {
			return nextElementNo;
		}

		public void addElement() {
			nextElementNo++;
		}

		public String getTransactionId() {
			return String.format("%s-%s-%s", setNamePrefix, setIndex, nextElementNo);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			SetStatus set = (SetStatus) o;
			return setNamePrefix.equals(set.setNamePrefix) &&
				setIndex.equals(set.setIndex) &&
				nextElementNo.equals(set.nextElementNo);
		}

		@Override
		public int hashCode() {
			return Objects.hash(setNamePrefix, setIndex, nextElementNo);
		}

		@Override
		public int compareTo(SetStatus o) {
			if (setNamePrefix.equals(o.getSetNamePrefix())) {
				if (setIndex.equals(o.getSetIndex())) {
					if (nextElementNo.equals(o.getNextElementNo())) {
						return 0;
					} else {
						if (nextElementNo - o.getNextElementNo() > 0) {
							return 1;
						} else {
							return -1;
						}
					}
				} else {
					return setIndex - o.getSetIndex();
				}
			} else {
				return setNamePrefix.compareTo(o.getSetNamePrefix());
			}
		}

		@Override
		public String toString() {
			return "SetStatus{" +
				"setNamePrefix='" + setNamePrefix + '\'' +
				", setIndex=" + setIndex +
				", nextElementNo=" + nextElementNo +
				'}';
		}
	}

	//We assume that this would be success always!
	private void recordSetStatus(final SetStatus setStatus) {

		final Properties properties = new Properties();
		properties.putAll(producerConfig);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

		final Map<TopicPartition, OffsetAndMetadata> metas = new HashMap<>();

		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, setStatus.getSetNamePrefix() + "-" + setStatus.getSetIndex());

		metas.put(
			new TopicPartition(topicForState, setStatus.getSetIndex()),
			new OffsetAndMetadata(setStatus.getNextElementNo(), setStatus.getTransactionId()));

		final KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties);

		producer.initTransactions();
		producer.beginTransaction();

		LOG.info("[USK] Transaction-Id :" + setStatus.getTransactionId());
		producer.sendOffsetsToTransaction(metas, consumerGroupId);

		producer.flush();
		producer.commitTransaction();
		producer.close();
	}

	@Nullable
	private SetStatus querySetStatusFromExternal(final String setNamePrefix, final Integer setIndex) {

		final Properties properties = new Properties();
		properties.putAll(producerConfig);

		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "");

		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.put("group.id", consumerGroupId);

		final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topicForState));
		final TopicPartition topicPartition = new TopicPartition(topicForState, setIndex);
		final OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
		consumer.close(Duration.ofSeconds(1));
		if (offsetAndMetadata != null) {
			return new SetStatus(setNamePrefix, setIndex, offsetAndMetadata.offset());
		} else {
			return null;
		}
	}

	private void deleteElementFromSet(final SetStatus setStatus, Long num) {

		LOG.info("[USK] abort begin ................ :" + setStatus + ":::" + num);
		final SetStatus s = new SetStatus(setStatus);
		int i = 0;
		do {
			final Properties properties = new Properties();
			properties.putAll(producerConfig);
			properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, s.getTransactionId());

			final FlinkKafkaInternalProducer cleanup =  new FlinkKafkaInternalProducer<>(properties);
			// it suffices to call initTransactions - this will abort any lingering transactions
			cleanup.initTransactions();
			cleanup.close(Duration.ofSeconds(0));
			LOG.info("[USK] abort send ................ :" + s.getTransactionId());
			i++;
			s.addElement();
		} while(i < num);

		LOG.info("[USK] abort finish ................ :" + setStatus + ":::" + num);

	}

	private List<SetStatus> cleanUpTheManagedSets(USink.InitialContext initialContext) throws Exception {

		final List<SetStatus> assignedManageSets = new ArrayList<>();

		if (initialContext.isRestored()) { // The some managed sets are already assigned
			// clean up all the assigned managed sets
			for (SetStatus setStatus : managedSetStatus.get()) {
				assignedManageSets.add(setStatus);
				cleanUnmanagedElementInTheSet(setStatus, false);
			}
			if (assignedManageSets.size() == 0) {
				// Assign no set to this sub task for example the parallelism is from 5 to 10.
				// SubTask choose the set itself.
				final SetStatus setStatus = new SetStatus(setNamePrefix, nextSetIdBegin + initialContext.getSubtaskIndex(), 0L);
				cleanUnmanagedElementInTheSet(setStatus, false);
				assignedManageSets.add(setStatus);
			}
		} else {// The managed set are not assigned to any one so we could
			final SetStatus setStatus = new SetStatus(setNamePrefix, nextSetIdBegin + initialContext.getSubtaskIndex(), 0L);
			cleanUnmanagedElementInTheSet(setStatus, false);
			assignedManageSets.add(setStatus);
		}

		return assignedManageSets;
	}

	private void cleanUpUnmanagedSets(int managedSetsNum, int subtaskIndex) {

		// clean up the unmanaged set
		for (int i = 1; i < MAX_SET_NUM / managedSetsNum; i++) {
			final Integer setIndex = i * managedSetsNum + subtaskIndex;
			final SetStatus setStatus = new SetStatus(setNamePrefix, setIndex, 0L);
			LOG.info("[USK] clean up ... " + setStatus);

			cleanUnmanagedElementInTheSet(setStatus, false);
		}
		//TODO:: some left sets
	}

	private void cleanUnmanagedElementInTheSet(SetStatus setStatus, boolean update) {
		// query the how many unmanaged data
		final SetStatus queriedStatus = querySetStatusFromExternal(setStatus.getSetNamePrefix(), setStatus.getSetIndex());
		// how many num
		final Long unmanagedElementNum;
		if (queriedStatus == null) {
			unmanagedElementNum = 0L;
			if (update) {
				recordSetStatus(setStatus);
			}
			return;
		} else {
			unmanagedElementNum = queriedStatus.getNextElementNo() - setStatus.getNextElementNo();
		}
		// delete elements and it's resources
		deleteElementFromSet(setStatus, unmanagedElementNum);
		//TODO:: sometime we do not need update

		recordSetStatus(setStatus);
	}
}
