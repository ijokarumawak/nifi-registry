/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry.provider.flow.git;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class Bucket {
    private final String bucketId;
    private String bucketName;

    /**
     * Flow ID to Flow.
     */
    private Map<String, Flow> flows = new HashMap<>();

    public Bucket(String bucketId) {
        this.bucketId = bucketId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public Flow getFlowOrCreate(String flowId) {
        return this.flows.computeIfAbsent(flowId, k -> new Flow(flowId));
    }

    public Optional<Flow> getFlow(String flowId) {
        return Optional.ofNullable(flows.get(flowId));
    }

    /**
     * Serialize the latest version of this Bucket meta data.
     * @return serialized bucket
     */
    Map<String, Object> serialize() {
        final Map<String, Object> map = new HashMap<>();

        map.put(GitFlowMetaData.LAYOUT_VERSION, GitFlowMetaData.CURRENT_LAYOUT_VERSION);
        map.put(GitFlowMetaData.BUCKET_ID, bucketId);
        map.put(GitFlowMetaData.FLOWS,
                flows.keySet().stream().collect(Collectors.toMap(k -> k, k -> flows.get(k).serialize())));

        return map;
    }
}
