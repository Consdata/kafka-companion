package com.consdata.kouncil.backwardconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class TopicPartitionBackwardConsumer<K, V> {
    private static final int MAX_RECURSIVE_PULL = 10;

    private final Consumer<K, V> consumer;
    private final TopicPartition topicPartition;
    private final boolean countByOffset;

    private Long startOffset;
    private Long minOffset;

    public TopicPartitionBackwardConsumer(Consumer<K, V> consumer, TopicPartition topicPartition, boolean countByOffset) {
        this.consumer = consumer;
        this.topicPartition = topicPartition;
        this.countByOffset = countByOffset;
    }

    public List<ConsumerRecord<K, V>> poll(int limit) {
        return poll(limit, 0);
    }

    private List<ConsumerRecord<K, V>> poll(int limit, int countRecursive) {
        Set<TopicPartition> topicPartitionCollection = Collections.singleton(topicPartition);
        consumer.assign(topicPartitionCollection);

        long endOffset = calculateEndOffsetFromState(topicPartitionCollection);
        minOffset = calculateMinimumOffset(topicPartitionCollection);
        int maxLimit = calculateMaximumLimitFromState(limit, endOffset);
        startOffset = calculateStartOffsetFromState(maxLimit, endOffset);

        if (startOffset < 0) {
            return Collections.emptyList();
        }

        consumer.seek(topicPartition, startOffset);

        List<ConsumerRecord<K, V>> limitedRecords = new ArrayList<>();

        long latestOffset = startOffset;
        long pollCounter = 0;
        int counterByOffset = 0;
        while (limitedRecords.size() < maxLimit && pollCounter < maxLimit && latestOffset < endOffset) {
            pollCounter++;
            for (ConsumerRecord<K, V> record : consumer.poll(Duration.ofMillis(100))) {
                long previousLatestOffset = latestOffset;
                latestOffset = Math.max(latestOffset, record.offset());
                if (latestOffset >= endOffset) {
                    break;
                }
                counterByOffset += limitedRecords.isEmpty() ? 1 : latestOffset - previousLatestOffset;
                limitedRecords.add(record);
            }
        }
        Collections.reverse(limitedRecords);

        int countedRecords = countByOffset
                ? counterByOffset
                : limitedRecords.size();

        if (countedRecords < maxLimit && countRecursive < MAX_RECURSIVE_PULL) {
            limitedRecords.addAll(
                    poll(maxLimit - countedRecords, countRecursive + 1)
            );
        }

        return limitedRecords;
    }

    private long calculateMinimumOffset(Set<TopicPartition> topicPartitionCollection) {
        Long minOffset = consumer.beginningOffsets(topicPartitionCollection).getOrDefault(topicPartition, 0L);
        if (this.minOffset != null && this.minOffset > minOffset) {
            return this.minOffset;
        }
        return minOffset;
    }

    private int calculateMaximumLimitFromState(int limit, long endOffset) {
        return (int) Math.min(limit, endOffset - minOffset);
    }

    private long calculateEndOffsetFromState(Set<TopicPartition> topicPartitionCollection) {
        long endOffsetFromKafka = consumer
                .endOffsets(topicPartitionCollection)
                .getOrDefault(topicPartition, 0L);
        return startOffset == null
                ? endOffsetFromKafka
                : Math.min(startOffset, endOffsetFromKafka);
    }

    private long calculateStartOffsetFromState(int maxLimit, long endOffset) {
        long startOffset = maxLimit == 0 ? -1 : endOffset - maxLimit;
        if (startOffset < minOffset) {
            return endOffset > minOffset ? minOffset : -1L;
        }
        return startOffset;
    }

    public void seekStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }
}
