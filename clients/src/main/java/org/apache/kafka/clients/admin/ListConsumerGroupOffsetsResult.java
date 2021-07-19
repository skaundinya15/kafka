/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * The result of the {@link Admin#listConsumerGroupOffsets(List)} and
 * {@link Admin#listConsumerGroupOffsets(String)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsResult {

    final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future;
    final Map<CoordinatorKey, KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>>> futures;

    ListConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        this.future = future;
        this.futures = null;
    }

    ListConsumerGroupOffsetsResult(final Map<CoordinatorKey, KafkaFutureImpl<Map<TopicPartition,
        OffsetAndMetadata>>> futures) {
        this.futures = futures;
        this.future = null;
    }

    /**
     * Return a future which yields a map of topic partitions to OffsetAndMetadata objects.
     * If the group does not have a committed offset for this partition, the corresponding value in the returned map will be null.
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata() {
        return future;
    }

    /**
     * Return a future which yields a map of group ids to a map of topic partitions to
     * OffsetAndMetadata objects. If the group doesn't have a committed offset for a specific
     * partition, the corresponding value in the returned map for that group id will be null.
     */
    public Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> groupIdsToPartitionsAndOffsetAndMetadata() {
        assert futures != null;
        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> listedConsumerGroupOffsets = new HashMap<>(futures.size());
        futures.forEach((key, future) -> listedConsumerGroupOffsets.put(key.idValue, future));
        return listedConsumerGroupOffsets;
    }

    /**
     * Return a future which yields all Map<String, Map<TopicPartition, OffsetAndMetadata> objects,
     * if all the describes succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata>>> all() {
        assert futures != null;
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                Map<String, Map<TopicPartition, OffsetAndMetadata>> listedConsumerGroupOffsets = new HashMap<>(futures.size());
                futures.forEach((key, future) -> {
                    try {
                        listedConsumerGroupOffsets.put(key.idValue, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, since the KafkaFuture#allOf already ensured
                        // that all of the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                });
                return listedConsumerGroupOffsets;
            });
    }
}
