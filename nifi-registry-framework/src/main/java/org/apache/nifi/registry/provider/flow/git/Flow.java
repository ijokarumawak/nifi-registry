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

public class Flow {
    /**
     * The ID of a Flow. It never changes.
     */
    private final String flowId;

    /**
     * A version to a Flow pointer.
     */
    private final Map<Integer, FlowPointer> versions = new HashMap<>();

    public Flow(String flowId) {
        this.flowId = flowId;
    }

    public boolean hasVersion(int version) {
        return versions.containsKey(version);
    }

    public void putVersion(int version, FlowPointer pointer) {
        versions.put(version, pointer);
    }

    public static class FlowPointer {
        private final String gitRev;
        private final String fileName;

        public FlowPointer(String gitRev, String fileName) {
            this.gitRev = gitRev;
            this.fileName = fileName;
        }

        public String getGitRev() {
            return gitRev;
        }

        public String getFileName() {
            return fileName;
        }
    }

}
