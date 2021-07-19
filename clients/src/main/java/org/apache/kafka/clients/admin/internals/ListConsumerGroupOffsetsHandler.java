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
package org.apache.kafka.clients.admin.internals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class ListConsumerGroupOffsetsHandler implements AdminApiHandler<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> {

    private final CoordinatorKey groupId;
    private final List<TopicPartition> partitions;
    private final Map<String, List<TopicPartition>> groupIdToTopicPartitions;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public ListConsumerGroupOffsetsHandler(
        String groupId,
        List<TopicPartition> partitions,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.partitions = partitions;
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        groupIdToTopicPartitions = new HashMap<>();
    }

    public ListConsumerGroupOffsetsHandler(
        Map<String, List<TopicPartition>> groupIdToTopicPartitions,
        LogContext logContext
    ) {
        this.log = logContext.logger(ListConsumerGroupOffsetsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.groupIdToTopicPartitions = groupIdToTopicPartitions;
        partitions = new ArrayList<>();
        groupId = null;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition,
        OffsetAndMetadata>> newFuture(List<String> groupIds) {
        return AdminApiFuture.forKeys(groupIds
            .stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet()));
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    @Override
    public String apiName() {
        return "offsetFetch";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    private void validateKeys(Set<CoordinatorKey> groupIds) {
        if (groupIdToTopicPartitions.isEmpty()) {
            if (!groupIds.equals(Collections.singleton(groupId))) {
                throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                    " (expected only " + Collections.singleton(groupId) + ")");
            }
        } else {
            if (!groupIds.equals(groupIdToTopicPartitions.keySet()
                .stream()
                .map(CoordinatorKey::byGroupId)
                .collect(Collectors.toSet()))) {
                throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                    " (expected only " + Collections.singleton(groupId) + ")");
            }
        }
    }

    @Override
    public OffsetFetchRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> groupIds) {
        validateKeys(groupIds);
        // Set the flag to false as for admin client request,
        // we don't need to wait for any pending offset state to clear.
        if (groupIdToTopicPartitions.isEmpty()) {
            // using older version of offsetFetch request if group to topic partition map is empty
            return new OffsetFetchRequest.Builder(groupId.idValue, false, partitions, false);
        }
        return new OffsetFetchRequest.Builder(groupIdToTopicPartitions, false, false);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final OffsetFetchResponse response = (OffsetFetchResponse) abstractResponse;

        if (groupIdToTopicPartitions.isEmpty()) {
            // the groupError will contain the group level error for v0-v8 OffsetFetchResponse
            Errors groupError = response.groupLevelError(groupId.idValue);
            if (groupError != Errors.NONE) {
                final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
                final List<CoordinatorKey> groupsToUnmap = new ArrayList<>();

                handleGroupError(groupId, groupError, failed, groupsToUnmap);

                return new ApiResult<>(Collections.emptyMap(), failed, new ArrayList<>(groupsToUnmap));
            } else {
                final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
                Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionDataMap =
                    response.partitionDataMap(groupId.idValue);
                for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : partitionDataMap.entrySet()) {
                    final TopicPartition topicPartition = entry.getKey();
                    OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                    final Errors error = partitionData.error;

                    if (error == Errors.NONE) {
                        final long offset = partitionData.offset;
                        final String metadata = partitionData.metadata;
                        final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                        // Negative offset indicates that the group has no committed offset for this partition
                        if (offset < 0) {
                            groupOffsetsListing.put(topicPartition, null);
                        } else {
                            groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                        }
                    } else {
                        log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                    }
                }

                return new ApiResult<>(
                    Collections.singletonMap(groupId, groupOffsetsListing),
                    Collections.emptyMap(),
                    Collections.emptyList()
                );
            }
        } else {
            Map<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> completed = new HashMap<>();
            Map<CoordinatorKey, Throwable> failed = new HashMap<>();
            List<CoordinatorKey> unmapped = new ArrayList<>();
            for (Map.Entry<String, List<TopicPartition>> entry : groupIdToTopicPartitions.entrySet()) {
                String group = entry.getKey();
                if (response.groupHasError(group)) {
                    handleGroupError(CoordinatorKey.byGroupId(group), response.groupLevelError(entry.getKey()), failed, unmapped);
                } else {
                    final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = new HashMap<>();
                    Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = response.partitionDataMap(group);
                    for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> partitionEntry : responseData.entrySet()) {
                        final TopicPartition topicPartition = partitionEntry.getKey();
                        OffsetFetchResponse.PartitionData partitionData = partitionEntry.getValue();
                        final Errors error = partitionData.error;

                        if (error == Errors.NONE) {
                            final long offset = partitionData.offset;
                            final String metadata = partitionData.metadata;
                            final Optional<Integer> leaderEpoch = partitionData.leaderEpoch;
                            // Negative offset indicates that the group has no committed offset for this partition
                            if (offset < 0) {
                                groupOffsetsListing.put(topicPartition, null);
                            } else {
                                groupOffsetsListing.put(topicPartition, new OffsetAndMetadata(offset, leaderEpoch, metadata));
                            }
                        } else {
                            log.warn("Skipping return offset for {} due to error {}.", topicPartition, error);
                        }
                    }
                    completed.put(CoordinatorKey.byGroupId(group), groupOffsetsListing);
                }
            }
            return new ApiResult<>(completed, failed, unmapped);
        }
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        List<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`OffsetFetch` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`OffsetFetch` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`OffsetFetch` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`OffsetFetch` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }
}
