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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.flow.FlowPersistenceException;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.FlowSnapshotContext;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;
import org.apache.nifi.registry.util.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public class GitFlowPersistenceProvider implements FlowPersistenceProvider {

    private static final Logger logger = LoggerFactory.getLogger(GitFlowMetaData.class);
    static final String FLOW_STORAGE_DIR_PROP = "Flow Storage Directory";

    private File flowStorageDir;
    private GitFlowMetaData flowMetaData;

    @Override
    public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        flowMetaData = new GitFlowMetaData();

        final Map<String,String> props = configurationContext.getProperties();
        if (!props.containsKey(FLOW_STORAGE_DIR_PROP)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " must be provided");
        }

        final String flowStorageDirValue = props.get(FLOW_STORAGE_DIR_PROP);
        if (StringUtils.isBlank(flowStorageDirValue)) {
            throw new ProviderCreationException("The property " + FLOW_STORAGE_DIR_PROP + " cannot be null or blank");
        }

        try {
            flowStorageDir = new File(flowStorageDirValue);
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(flowStorageDir);
            flowMetaData.loadGitRepository(flowStorageDir);
            logger.info("Configured GitFlowPersistenceProvider with Flow Storage Directory {}",
                    new Object[] {flowStorageDir.getAbsolutePath()});
        } catch (IOException|GitAPIException e) {
            throw new ProviderCreationException(e);
        }
    }

    @Override
    public void saveFlowContent(FlowSnapshotContext context, byte[] content) throws FlowPersistenceException {

        // TODO: Check if working dir is clean, any uncommitted file?

        final String bucketId = context.getBucketId();
        final Bucket bucket = flowMetaData.getBucketOrCreate(bucketId);
        bucket.setBucketName(context.getBucketName());

        final Flow flow = bucket.getFlowOrCreate(context.getFlowId());
        final Flow.FlowPointer pointer = new Flow.FlowPointer(context.getFlowName());
        flow.putVersion(context.getVersion(), pointer);

        final File bucketDir = new File(flowStorageDir, bucket.getBucketName());
        final File flowSnippetFile = new File(bucketDir, context.getFlowName());
        final File bucketFile = new File(bucketDir, GitFlowMetaData.BUCKET_FILENAME);


        final boolean mkdirs = bucketDir.mkdirs();
        logger.debug("Bucket directory creation result={}", mkdirs);

        try {
            // Save the content.
            try (final OutputStream os = new FileOutputStream(flowSnippetFile)) {
                os.write(content);
                os.flush();
            }

            // Write a bucket file.
            flowMetaData.saveBucket(bucket, bucketFile);

            // Create a Git Commit.
            final String commitId = flowMetaData.commit(context.getComments(), bucket.getBucketName());
            pointer.setGitRev(commitId);

        } catch (IOException|GitAPIException e) {
            throw new FlowPersistenceException("Failed to persist flow.", e);
        }

        // TODO: Handle Bucket name change.
        // TODO: Handle Flow name change.
        // TODO: What if user rebased commits? Version number to Commit ID mapping will be broken.
    }

    @Override
    public byte[] getFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {

        final Optional<Bucket> bucketOpt = flowMetaData.getBucket(bucketId);
        if (!bucketOpt.isPresent()) {
            throw new FlowPersistenceException(String.format("Bucket ID %s was not found.", bucketId));
        }

        final Bucket bucket = bucketOpt.get();
        final Optional<Flow> flowOpt = bucket.getFlow(flowId);
        if (!flowOpt.isPresent()) {
            throw new FlowPersistenceException(String.format("Flow ID %s was not found in bucket %s:%s.",
                    flowId, bucket.getBucketName(), bucket.getBucketId()));
        }

        final Flow flow = flowOpt.get();
        if (!flow.hasVersion(version)) {
            throw new FlowPersistenceException(String.format("Flow ID %s version %d was not found in bucket %s:%s.",
                    flowId, version, bucket.getBucketName(), bucket.getBucketId()));
        }

        // TODO: Get ObjectId and Load flow binary.
        final Flow.FlowPointer flowPointer = flow.getFlowVersion(version);



        return new byte[0];
    }

    @Override
    public void deleteAllFlowContent(String bucketId, String flowId) throws FlowPersistenceException {

    }

    @Override
    public void deleteFlowContent(String bucketId, String flowId, int version) throws FlowPersistenceException {

    }

}
