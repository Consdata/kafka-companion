package com.consdata.kouncil.backwardconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class TopicBackwardConsumer<K, V> {
    private final Map<TopicPartition, TopicPartitionBackwardConsumer<K, V>> consumers;
    private final int limit;

    public TopicBackwardConsumer(Consumer<K, V> consumer, String topic, int limit) {
        this(
                consumer,
                consumer.partitionsFor(topic).stream()
                        .map(v -> new TopicPartition(v.topic(), v.partition()))
                        .collect(Collectors.toSet()),
                limit
        );
    }

    public TopicBackwardConsumer(Consumer<K, V> consumer, Collection<TopicPartition> partitions, int limit) {
        this.consumers = partitions.stream()
                .collect(Collectors.toMap(k -> k, v -> new TopicPartitionBackwardConsumer<>(consumer, v, true)));
        this.limit = limit;
    }

    public List<ConsumerRecord<K, V>> poll() {
        List<ConsumerRecord<K, V>> recordsFromAllPartitions = new LinkedList<>();
        consumers.values().forEach(consumer -> {
            List<ConsumerRecord<K, V>> records = consumer.poll(limit);
            addRecordsSortAndRespectLimitWithSettingStartOffset(recordsFromAllPartitions, records);
        });
        return recordsFromAllPartitions;
    }

    private void addRecordsSortAndRespectLimitWithSettingStartOffset(
            List<ConsumerRecord<K, V>> allRecords,
            List<ConsumerRecord<K, V>> newRecords
    ) {
        allRecords.addAll(newRecords);
        allRecords.sort(Comparator.<ConsumerRecord<K, V>>comparingLong(ConsumerRecord::timestamp).reversed());

        Map<TopicPartition, Long> startOffsetsToReset = new HashMap<>();
        for (int i = allRecords.size() - 1; i >= limit; i--) {
            ConsumerRecord<K, V> record = allRecords.get(i);
            allRecords.remove(i);
            startOffsetsToReset.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        }

        startOffsetsToReset.forEach((topicPartition, startOffset) ->
                consumers.get(topicPartition).seekStartOffset(startOffset));
    }

    public void setStartOffset(TopicPartition topicPartition, Long startOffset) {
        TopicPartitionBackwardConsumer<K, V> consumer = consumers.get(topicPartition);
        if (consumer != null) {
            consumer.seekStartOffset(startOffset);
        }
    }

    public void setMinOffset(TopicPartition topicPartition, Long minOffset) {
        TopicPartitionBackwardConsumer<K, V> consumer = consumers.get(topicPartition);
        if (consumer != null) {
            consumer.setMinOffset(minOffset);
        }
    }
}
