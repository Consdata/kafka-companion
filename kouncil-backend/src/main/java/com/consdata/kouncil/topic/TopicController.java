package com.consdata.kouncil.topic;

import com.consdata.kouncil.KouncilConfiguration;
import com.consdata.kouncil.backwardconsumer.TopicBackwardConsumer;
import com.consdata.kouncil.logging.EntryExitLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RestController
public class TopicController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KouncilConfiguration kouncilConfiguration;

    public TopicController(KafkaTemplate<String, String> kafkaTemplate,
                           KouncilConfiguration kouncilConfiguration) {
        this.kafkaTemplate = kafkaTemplate;
        this.kouncilConfiguration = kouncilConfiguration;
    }

    @GetMapping("/api/topic/messages/{topicName}/{partition}/{offset}")
    public TopicMessagesDto getTopicMessages(@PathVariable("topicName") String topicName,
                                             @PathVariable("partition") String selectedPartitions,
                                             @PathVariable("offset") String offset,
                                             @RequestParam("offset") String offsetShiftParam,
                                             @RequestParam("limit") String limitParam,
                                             @RequestParam(value = "beginningTimestampMillis", required = false) Long beginningTimestampMillis,
                                             @RequestParam(value = "endTimestampMillis", required = false) Long endTimestampMillis) {
        log.debug("TCM01 topicName={}, partition={}, offset={}, offsetParam={}, limit={}, beginningTimestampMillis={}, endTimestampMillis={}",
                topicName, selectedPartitions, offset, offsetShiftParam, limitParam, beginningTimestampMillis, endTimestampMillis);
        int limit = Integer.parseInt(limitParam);
        long offsetShift = Long.parseLong(offsetShiftParam);
        Properties props = createCommonProperties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Set<TopicPartition> allTopicPartitions = getAllTopicPartitions(topicName, consumer);
            Set<TopicPartition> selectedTopicPartitions = getSelectedTopicParitions(topicName, selectedPartitions, allTopicPartitions);

            final Map<TopicPartition, Long> beginningOffsets = getBeginningOffsets(beginningTimestampMillis, consumer, allTopicPartitions);
            final Map<TopicPartition, Long> endOffsets = getEndOffsets(endTimestampMillis, consumer, allTopicPartitions);

            log.debug("TCM03 beginningOffsets={}", beginningOffsets);
            log.debug("TCM04 endOffsets={}", endOffsets);

            TopicBackwardConsumer<String, String> backwardConsumer = new TopicBackwardConsumer<>(
                    consumer,
                    selectedTopicPartitions.stream()
                            .filter(p -> beginningOffsets.get(p) >= 0)
                            .collect(Collectors.toSet()),
                    limit
            );

            beginningOffsets.forEach(backwardConsumer::setMinOffset);
            endOffsets.forEach((p, o) -> backwardConsumer.setStartOffset(
                    p,
                    o - (selectedTopicPartitions.size() == 1 && selectedTopicPartitions.contains(p) ? offsetShift : 0L)
            ));


            List<TopicMessage> messages = backwardConsumer.poll().stream()
                    .map(record -> TopicMessage
                            .builder()
                            .key(record.key())
                            .value(record.value())
                            .offset(record.offset())
                            .partition(record.partition())
                            .timestamp(record.timestamp())
                            .build())
                    .collect(Collectors.toList());

            log.debug("TCM20 poll completed records.size={}", messages.size());
            TopicMessagesDto topicMessagesDto = TopicMessagesDto.builder()
                    .messages(messages)
                    .partitionOffsets(beginningOffsets.entrySet().stream()
                            .collect(Collectors.toMap(k -> k.getKey().partition(), Map.Entry::getValue)))
                    .partitionEndOffsets(endOffsets.entrySet().stream()
                            .collect(Collectors.toMap(k -> k.getKey().partition(), Map.Entry::getValue)))
                    // pagination works only for single selected partition
                    .totalResults(getTotalResultsForPagination(selectedTopicPartitions, beginningOffsets, endOffsets))
                    .build();
            log.debug("TCM99 topicName={}, partition={}, offset={} topicMessages.size={}", topicName, selectedPartitions, offset, topicMessagesDto.getMessages().size());
            return topicMessagesDto;

        }
    }

    private Long getTotalResultsForPagination(Set<TopicPartition> selectedTopicPartitions, Map<TopicPartition, Long> beginningOffsets, Map<TopicPartition, Long> endOffsets) {
        TopicPartition firstPartition = selectedTopicPartitions.iterator().next();
        return selectedTopicPartitions.size() == 1
                ? endOffsets.get(firstPartition) - beginningOffsets.get(firstPartition)
                : null;
    }

    private Set<TopicPartition> getSelectedTopicParitions(String topicName, String partitions, Set<TopicPartition> allTopicPartitions) {
        if (partitions.equalsIgnoreCase("all")) {
            return allTopicPartitions;
        }

        Set<TopicPartition> selectedPartitionsFromParam = Arrays.stream(partitions.split(","))
                .map(v -> new TopicPartition(topicName, Integer.parseInt(v)))
                .collect(Collectors.toSet());

        return allTopicPartitions.stream()
                .filter(selectedPartitionsFromParam::contains)
                .collect(Collectors.toSet());
    }

    private Set<TopicPartition> getAllTopicPartitions(String topicName, KafkaConsumer<String, String> consumer) {
        Set<TopicPartition> topicPartitions = consumer.partitionsFor(topicName).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toSet());
        log.debug("TCM02 topicPartitions.size={}, topicPartitions={}", topicPartitions.size(), topicPartitions);
        return topicPartitions;
    }

    private Map<TopicPartition, Long> getBeginningOffsets(Long beginningTimestampMillis,
                                                          KafkaConsumer<String, String> consumer,
                                                          Collection<TopicPartition> allTopicPartitions) {
        if (beginningTimestampMillis == null) {
            return consumer
                    .beginningOffsets(allTopicPartitions).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        Map<TopicPartition, Long> beginningTimestamps = allTopicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignore -> beginningTimestampMillis));

        return consumer.offsetsForTimes(beginningTimestamps).entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        v -> v.getValue() == null ? -1 : v.getValue().offset()
                ));
    }

    private Map<TopicPartition, Long> getEndOffsets(Long endTimestampMillis,
                                                    KafkaConsumer<String, String> consumer,
                                                    Collection<TopicPartition> allTopicPartitions) {
        final Map<TopicPartition, Long> globalEndOffsets = consumer.endOffsets(allTopicPartitions).entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (endTimestampMillis == null) {
            return globalEndOffsets;
        }

        Map<TopicPartition, Long> endTimestamps = allTopicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), ignore -> endTimestampMillis + 1));

        return consumer.offsetsForTimes(endTimestamps).entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        v -> v.getValue() == null ? globalEndOffsets.get(v.getKey()) : v.getValue().offset()
                ));
    }

    @PostMapping("/api/topic/send/{topic}/{count}")
    @EntryExitLogger
    public void send(@PathVariable("topic") String topic,
                     @PathVariable("count") int count,
                     @RequestBody TopicMessage message) {
        for (int i = 0; i < count; i++) {
            kafkaTemplate.send(topic, replaceTokens(message.getKey(), i), replaceTokens(message.getValue(), i));
        }
        kafkaTemplate.flush();
    }

    private String replaceTokens(String data, int i) {
        return data
                .replace("{{count}}", String.valueOf(i))
                .replace("{{timestamp}}", String.valueOf(System.currentTimeMillis()))
                .replace("{{uuid}}", UUID.randomUUID().toString());
    }

    private Properties createCommonProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kouncilConfiguration.getBootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
