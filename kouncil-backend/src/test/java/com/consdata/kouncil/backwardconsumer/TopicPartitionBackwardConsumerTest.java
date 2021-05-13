package com.consdata.kouncil.backwardconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicPartitionBackwardConsumerTest {
    private final static String TOPIC = "topic";
    private final static int PARTITION = 5;
    private final static TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    private final TopicPartitionBackwardConsumer<String, String> subject = new TopicPartitionBackwardConsumer<>(
            consumer,
            TOPIC_PARTITION,
            false
    );

    @Test
    public void shouldReturnEmptyListWhenKafkaIsEmpty() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 0L));

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll(3);

        //then
        assertThat(actual).isEmpty();
    }

    @Test
    public void shouldSetMinimumOffsetAndPollToThatOffset() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 3L));
        ConsumerRecord<String, String> expected1 = createRecord(1L, "k01", "v01");
        ConsumerRecord<String, String> expected2 = createRecord(2L, "k02", "v02");

        consumer.schedulePollTask(() -> {
            addRecord(createRecord(0L, "k00", "v00"));
            addRecord(expected1);
            addRecord(expected2);
        });

        //when
        subject.seekStartOffset(10L);
        subject.setMinOffset(1L);
        List<ConsumerRecord<String, String>> actual = subject.poll(3);

        //then
        assertThat(actual).containsExactly(expected2, expected1);
    }

    @Test
    public void shouldSeekStartOffsetAndPoll() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 3L));
        ConsumerRecord<String, String> expected0 = createRecord(0L, "k00", "v00");
        ConsumerRecord<String, String> expected1 = createRecord(1L, "k01", "v01");

        consumer.schedulePollTask(() -> {
            addRecord(expected0);
            addRecord(expected1);
            addRecord(createRecord(2L, "k02", "v02"));
        });

        //when
        subject.seekStartOffset(2L);
        List<ConsumerRecord<String, String>> actual = subject.poll(3);

        //then
        assertThat(actual).containsExactly(expected1, expected0);
    }

    @Test
    public void shouldPollFullListAlsoWhenIsTransactionalOffsetInTheMiddle() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 3L));
        ConsumerRecord<String, String> expected0 = createRecord(0L, "k00", "v00");
        ConsumerRecord<String, String> expected2 = createRecord(2L, "k02", "v02");

        consumer.schedulePollTask(() -> {
            addRecord(expected0);
            addRecord(expected2);
        });

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll(3);

        //then
        assertThat(actual).containsExactly(expected2, expected0);
    }

    @Test
    public void shouldPollFullListInPartialKafkaPolls() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 3L));
        ConsumerRecord<String, String> expected1 = createRecord(1L, "k01", "v01");
        ConsumerRecord<String, String> expected2 = createRecord(2L, "k02", "v02");

        consumer.schedulePollTask(() -> {
            addRecord(createRecord(0L, "k00", "v00"));
            addRecord(expected1);
        });
        consumer.schedulePollTask(() -> addRecord(expected2));

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll(2);

        //then
        assertThat(actual).containsExactly(expected2, expected1);
    }

    @Test
    public void shouldCountByOffset() {
        //given
        consumer.updateBeginningOffsets(Map.of(TOPIC_PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(TOPIC_PARTITION, 251L));
        ConsumerRecord<String, String> expected051 = createRecord(51L, "k051", "v051");
        ConsumerRecord<String, String> expected249 = createRecord(249L, "k249", "v249");
        ConsumerRecord<String, String> expected250 = createRecord(250L, "k250", "v250");

        consumer.schedulePollTask(() -> {
            addRecord(createRecord(0L, "k001", "v001"));
            addRecord(createRecord(1L, "k002", "v002"));
            addRecord(createRecord(50L, "k050", "v050"));
            addRecord(expected051);
            addRecord(expected249);
            addRecord(expected250);
        });


        TopicPartitionBackwardConsumer<String, String> subject = new TopicPartitionBackwardConsumer<>(
                consumer,
                TOPIC_PARTITION,
                true
        );

        //when
        List<ConsumerRecord<String, String>> actual = subject.poll(200);

        //then
        assertThat(actual).containsExactly(expected250, expected249, expected051);
    }

    private ConsumerRecord<String, String> createRecord(long offset, String key, String value) {
        return new ConsumerRecord<>(TOPIC, PARTITION, offset, key, value);
    }

    private void addRecord(ConsumerRecord<String, String> record) {
        consumer.addRecord(record);
    }

}