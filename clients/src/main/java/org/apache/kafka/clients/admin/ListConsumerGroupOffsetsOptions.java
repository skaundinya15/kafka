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
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * Options for {@link Admin#listConsumerGroupOffsets(List)} and
 * {@link Admin#listConsumerGroupOffsets(String)}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsOptions extends AbstractOptions<ListConsumerGroupOffsetsOptions> {

    private List<TopicPartition> topicPartitions;

    private Map<String, List<TopicPartition>> groupToTopicPartitions = new HashMap<>();

    /**
     * Default constructor for {@code ListConsumerGroupOffsetsOptions}. Sets the topic partitions
     * to fetch for each group id to {@code null}, which indicates to fetch all offsets for all
     * topic partitions for that group.
     * */
    public ListConsumerGroupOffsetsOptions(List<String> groupIds) {
        for (String group : groupIds) {
            groupToTopicPartitions.put(group, null);
        }
    }

    public ListConsumerGroupOffsetsOptions() {
        topicPartitions = null;
    }

    /**
     * Set the topic partitions to list as part of the result.
     * {@code null} includes all topic partitions.
     *
     * @param topicPartitions List of topic partitions to include
     * @return This ListGroupOffsetsOptions
     */
    public ListConsumerGroupOffsetsOptions topicPartitions(List<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    /**
     * Set the topic partitions for each group we want to fetch offsets for as part of the result.
     * {@code null} mapping for a specific group id means to fetch offsets for all topic
     * partitions for that specific group.
     * @param groupToTopicPartitions Map of group id to list of topic partitions to fetch offsets
     *                              for.
     * @return This ListGroupOffsetsOptions
     */
    public ListConsumerGroupOffsetsOptions groupToTopicPartitions(Map<String, List<TopicPartition>> groupToTopicPartitions) {
        this.groupToTopicPartitions = groupToTopicPartitions;
        return this;
    }

    /**
     * Returns a list of topic partitions to add as part of the result.
     */
    public List<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    /**
     * Returns a map of group id to topic partitions to fetch offsets for.
     */
    public Map<String, List<TopicPartition>> groupToTopicPartitions() {
        return groupToTopicPartitions;
    }
}
