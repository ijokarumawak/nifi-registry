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

import org.apache.nifi.registry.flow.FlowPersistenceException;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;

public class GitFlowPersistenceProvider implements FlowPersistenceProvider {

    @Override
    public void saveFlowContent(FlowSnapshotContext context, byte[] content) throws FlowPersistenceException {
        // TODO: Check if
        // TODO: Capture metadata.
        // TODO: Save the content.
        // TODO: Create a Git commit? Or accumulate the change? Creating a commit would be simpler.
        // TODO: What if user rebased commits? Version number to Commit ID mapping will be broken.
    }

    @Override
    public byte[] getFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {
        // TODO: A mapping from bucketId to Dir name is needed
        // TODO: A mapping from flowId to File name is needed
        // TODO: A mapping from File
        // (bucketId, flowId, version) -> Git commit Id, File path
        // - bucketId
        //   - flowId
        //     - version
        //       - number: Represents Registry flow version
        //       - Git commit id, or a Tag?
        //       - File path: Filesystem path to the file, including bucket name, i.e. bucket-name/flow-name

        // The 2nd idea:
        // BucketId to Dir name mapping
        // - Bucket id to Dir name mapping file
        // - Bucket Dir
        //   - Flow files
        //   - Flow id to File name mapping file, it also contains version
        return new byte[0];
    }

    @Override
    public void deleteAllFlowContent(String bucketId, String flowId) throws FlowPersistenceException {

    }

    @Override
    public void deleteFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {

    }

    @Override
    public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {

    }
}
