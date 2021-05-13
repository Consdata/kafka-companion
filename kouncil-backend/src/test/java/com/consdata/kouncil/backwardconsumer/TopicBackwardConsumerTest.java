package com.consdata.kouncil.backwardconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicBackwardConsumerTest {
    private final static String TOPIC = "topic";
    private final static int PARTITION0 = 0;
    private final static int PARTITION1 = 1;
    private final static int PARTITION2 = 2;
    private final static TopicPartition TOPIC_PARTITION0 = new TopicPartition(TOPIC, PARTITION0);
    private final static TopicPartition TOPIC_PARTITION1 = new TopicPartition(TOPIC, PARTITION1);
    private final static TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final TopicBackwardConsumer<String, String> subject = new TopicBackwardConsumer<>(
            consumer,
            Arrays.asList(
                    TOPIC_PARTITION0,
                    TOPIC_PARTITION1,
                    TOPIC_PARTITION2
            ),
            3
    );

    @Before
    public void before() {
        consumer.updatePartitions(TOPIC, Arrays.asList(
                new PartitionInfo(TOPIC, PARTITION0, null, new Node[0], new Node[0]),
                new PartitionInfo(TOPIC, PARTITION1, null, new Node[0], new Node[0]),
                new PartitionInfo(TOPIC, PARTITION2, null, new Node[0], new Node[0])
        ));
    }

    @Test
    public void shouldGetOnlyNewestFromAllPartition() {
        //given
        consumer.updateEndOffsets(Map.of(
                TOPIC_PARTITION0, 3L,
                TOPIC_PARTITION1, 3L,
                TOPIC_PARTITION2, 3L
        ));
        consumer.updateBeginningOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));

        ConsumerRecord<String, String> record_0_2 = createRecord(PARTITION0, 2, 7, "k0", "v0");
        ConsumerRecord<String, String> record_1_2 = createRecord(PARTITION1, 2, 8, "k1", "v1");
        ConsumerRecord<String, String> record_2_2 = createRecord(PARTITION2, 2, 9, "k2", "v2");

        Runnable taskToAddRecordsDependOnAssigned = () -> {
            if (consumer.assignment().contains(TOPIC_PARTITION0)) {
                addRecord(createRecord(PARTITION0, 0, 1, "k0", "v0"));
                addRecord(createRecord(PARTITION0, 1, 2, "k1", "v1"));
                addRecord(record_0_2);
            }
            if (consumer.assignment().contains(TOPIC_PARTITION1)) {
                addRecord(createRecord(PARTITION1, 0, 3, "k0", "v0"));
                addRecord(createRecord(PARTITION1, 1, 4, "k1", "v1"));
                addRecord(record_1_2);
            }
            if (consumer.assignment().contains(TOPIC_PARTITION2)) {
                addRecord(createRecord(PARTITION2, 0, 5, "k0", "v0"));
                addRecord(createRecord(PARTITION2, 1, 6, "k1", "v1"));
                addRecord(record_2_2);
            }
        };

        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll();

        //then
        assertThat(actual).containsExactly(record_2_2, record_1_2, record_0_2);
    }

    @Test
    public void shouldSetStartAndMinOffset() {
        //given
        consumer.updateEndOffsets(Map.of(
                TOPIC_PARTITION0, 3L,
                TOPIC_PARTITION1, 3L,
                TOPIC_PARTITION2, 3L
        ));
        consumer.updateBeginningOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));
        subject.setStartOffset(TOPIC_PARTITION0, 2L);
        subject.setStartOffset(TOPIC_PARTITION1, 2L);
        subject.setStartOffset(TOPIC_PARTITION2, 2L);
        subject.setMinOffset(TOPIC_PARTITION0, 1L);
        subject.setMinOffset(TOPIC_PARTITION1, 1L);
        subject.setMinOffset(TOPIC_PARTITION2, 1L);

        ConsumerRecord<String, String> record_0_1 = createRecord(PARTITION0, 1, 3, "k0", "v0");
        ConsumerRecord<String, String> record_1_1 = createRecord(PARTITION1, 1, 1, "k1", "v1");
        ConsumerRecord<String, String> record_2_1 = createRecord(PARTITION2, 1, 2, "k2", "v2");

        Runnable taskToAddRecordsDependOnAssigned = () -> {
            if (consumer.assignment().contains(TOPIC_PARTITION0)) {
                addRecord(createRecord(PARTITION0, 0, 4, "k0", "v0"));
                addRecord(record_0_1);
                addRecord(createRecord(PARTITION0, 2, 7, "k1", "v1"));
            }
            if (consumer.assignment().contains(TOPIC_PARTITION1)) {
                addRecord(createRecord(PARTITION1, 0, 5, "k0", "v0"));
                addRecord(record_1_1);
                addRecord(createRecord(PARTITION1, 2, 8, "k1", "v1"));
            }
            if (consumer.assignment().contains(TOPIC_PARTITION2)) {
                addRecord(createRecord(PARTITION2, 0, 6, "k0", "v0"));
                addRecord(record_2_1);
                addRecord(createRecord(PARTITION2, 2, 9, "k1", "v1"));
            }
        };

        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll();

        //then
        assertThat(actual).containsExactly(record_0_1, record_2_1, record_1_1);
    }

    @Test
    public void shouldGetOnlyNewestFromOnePartition() {
        //given
        consumer.updateEndOffsets(Map.of(
                TOPIC_PARTITION0, 3L,
                TOPIC_PARTITION1, 3L,
                TOPIC_PARTITION2, 0L
        ));
        consumer.updateBeginningOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));

        ConsumerRecord<String, String> record_0_0 = createRecord(PARTITION0, 0, 7, "k0", "v0");
        ConsumerRecord<String, String> record_0_1 = createRecord(PARTITION0, 1, 8, "k1", "v1");
        ConsumerRecord<String, String> record_0_2 = createRecord(PARTITION0, 2, 9, "k2", "v2");

        Runnable taskToAddRecordsDependOnAssigned = () -> {
            if (consumer.assignment().contains(TOPIC_PARTITION0)) {
                addRecord(record_0_0);
                addRecord(record_0_1);
                addRecord(record_0_2);
            }
            if (consumer.assignment().contains(TOPIC_PARTITION1)) {
                addRecord(createRecord(PARTITION1, 0, 1, "k0", "v0"));
                addRecord(createRecord(PARTITION1, 1, 2, "k1", "v1"));
                addRecord(createRecord(PARTITION1, 2, 3, "k2", "v2"));
            }
        };

        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll();

        //then
        assertThat(actual).containsExactly(record_0_2, record_0_1, record_0_0);
    }

    @Test
    public void shouldReturnEmptyWhenKafkaIsEmpty() {
        //given
        consumer.updateEndOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));
        consumer.updateBeginningOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll();

        //then
        assertThat(actual).isEmpty();
    }

    @Test
    public void shouldDetectPartitions() {
        //given
        TopicBackwardConsumer<String, String> subject = new TopicBackwardConsumer<>(consumer, TOPIC, 3);
        consumer.updateEndOffsets(Map.of(
                TOPIC_PARTITION0, 3L,
                TOPIC_PARTITION1, 3L,
                TOPIC_PARTITION2, 3L
        ));
        consumer.updateBeginningOffsets(Map.of(
                TOPIC_PARTITION0, 0L,
                TOPIC_PARTITION1, 0L,
                TOPIC_PARTITION2, 0L
        ));

        ConsumerRecord<String, String> record_0_2 = createRecord(PARTITION0, 2, 7, "k0", "v0");
        ConsumerRecord<String, String> record_1_2 = createRecord(PARTITION1, 2, 8, "k1", "v1");
        ConsumerRecord<String, String> record_2_2 = createRecord(PARTITION2, 2, 9, "k2", "v2");

        Runnable taskToAddRecordsDependOnAssigned = () -> {
            if (consumer.assignment().contains(TOPIC_PARTITION0)) {
                addRecord(createRecord(PARTITION0, 0, 1, "k0", "v0"));
                addRecord(createRecord(PARTITION0, 1, 2, "k1", "v1"));
                addRecord(record_0_2);
            }
            if (consumer.assignment().contains(TOPIC_PARTITION1)) {
                addRecord(createRecord(PARTITION1, 0, 3, "k0", "v0"));
                addRecord(createRecord(PARTITION1, 1, 4, "k1", "v1"));
                addRecord(record_1_2);
            }
            if (consumer.assignment().contains(TOPIC_PARTITION2)) {
                addRecord(createRecord(PARTITION2, 0, 5, "k0", "v0"));
                addRecord(createRecord(PARTITION2, 1, 6, "k1", "v1"));
                addRecord(record_2_2);
            }
        };

        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);
        consumer.schedulePollTask(taskToAddRecordsDependOnAssigned);

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll();

        //then
        assertThat(actual).containsExactly(record_2_2, record_1_2, record_0_2);
    }

    private ConsumerRecord<String, String> createRecord(int partition, long offset, long timestamp, String key, String value) {
        return new ConsumerRecord<>(TOPIC, partition, offset,
                timestamp, TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
                key, value);
    }

    private void addRecord(ConsumerRecord<String, String> record) {
        consumer.addRecord(record);
    }

}