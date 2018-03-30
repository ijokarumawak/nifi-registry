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
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class GitFlowMetaData {

    static final int CURRENT_LAYOUT_VERSION = 1;

    static final String LAYOUT_VERSION = "layoutVer";
    static final String BUCKET_ID = "bucketId";
    static final String FLOWS = "flows";
    static final String VER = "ver";
    static final String FILE = "file";
    static final String BUCKET_FILENAME = "bucket.yml";

    private static final Logger logger = LoggerFactory.getLogger(GitFlowMetaData.class);

    private Repository gitRepo;

    /**
     * Bucket ID to Bucket.
     */
    private Map<String, Bucket> buckets = new HashMap<>();

    private Repository openRepository(final File gitProjectRootDir) throws IOException {
        return new FileRepositoryBuilder()
                .readEnvironment()
                .findGitDir(gitProjectRootDir)
                .build();
    }

    // TODO: More Error handling.
    @SuppressWarnings("unchecked")
    public void loadGitRepository(File gitProjectRootDir) throws IOException, GitAPIException {
        gitRepo = openRepository(gitProjectRootDir);

        try (final Git git = new Git(gitRepo)) {
            for (RevCommit commit : git.log().call()) {
                final String shortCommitId = commit.getId().abbreviate(7).name();
                logger.debug("Processing a commit: {}", shortCommitId);
                final RevTree tree = commit.getTree();

                try (final TreeWalk treeWalk = new TreeWalk(gitRepo)) {
                    treeWalk.addTree(tree);

                    // Path -> ObjectId
                    final Map<String, ObjectId> bucketObjectIds = new HashMap<>();
                    while (treeWalk.next()) {
                        if (treeWalk.isSubtree()) {
                            treeWalk.enterSubtree();
                        } else {
                            final String pathString = treeWalk.getPathString();
                            // TODO: what is this nth?? When does it get grater than 0? Tree count seems to be always 1..
                            if (pathString.endsWith("/" + BUCKET_FILENAME)) {
                                bucketObjectIds.put(pathString, treeWalk.getObjectId(0));
                            }
                        }
                    }

                    if (bucketObjectIds.isEmpty()) {
                        logger.debug("Tree at commit {} does not contain any " + BUCKET_FILENAME + ". Skipping it.", shortCommitId);
                        continue;
                    }

                    loadBuckets(gitRepo, commit, bucketObjectIds);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void loadBuckets(Repository gitRepo, RevCommit commit, Map<String, ObjectId> bucketObjectIds) throws IOException {
        final Yaml yaml = new Yaml();
        for (String path : bucketObjectIds.keySet()) {
            final ObjectId bucketObjectId = bucketObjectIds.get(path);
            final Map<String, Object> bucketMeta;
            try (InputStream bucketIn = gitRepo.newObjectReader().open(bucketObjectId).openStream()) {
                bucketMeta = yaml.load(bucketIn);
            }

            if (!validateRequiredValue(bucketMeta, path, LAYOUT_VERSION, BUCKET_ID, FLOWS)) {
                continue;
            }

            int layoutVersion = (int) bucketMeta.get(LAYOUT_VERSION);
            if (layoutVersion > CURRENT_LAYOUT_VERSION) {
                logger.warn("{} has unsupported {} {}. This Registry can only support {} or lower. Skipping it.",
                        path, LAYOUT_VERSION, layoutVersion, CURRENT_LAYOUT_VERSION);
                continue;
            }

            final String bucketId = (String) bucketMeta.get(BUCKET_ID);
            final Bucket bucket = getBucketOrCreate(bucketId);

            // E.g. DirA/DirB/DirC/bucket.yml -> DirC will be the bucket name.
            final String[] pathNames = path.split("/");
            final String bucketName = pathNames[pathNames.length - 2];

            // Since commits are read in LIFO order, avoid old commits overriding the latest bucket name.
            if (StringUtils.isEmpty(bucket.getBucketName())) {
                bucket.setBucketName(bucketName);
            }

            final Map<String, Object> flows = (Map<String, Object>) bucketMeta.get(FLOWS);
            loadFlows(commit, bucket, path, flows);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadFlows(RevCommit commit, Bucket bucket, String path, Map<String, Object> flows) {
        for (String flowId : flows.keySet()) {
            final Map<String, Object> flowMeta = (Map<String, Object>) flows.get(flowId);

            if (!validateRequiredValue(flowMeta, path + ":" + flowId, VER, FILE)) {
                continue;
            }

            final Flow flow = bucket.getFlowOrCreate(flowId);
            final int version = (int) flowMeta.get(VER);
            final String file = (String) flowMeta.get(FILE);

            // Since commits are read in LIFO order, avoid old commits overriding the latest pointer.
            if (!flow.hasVersion(version)) {
                // TODO: Add bucket name to FlowPointer.fil, or capture flow objectId so that we can handle Bucket name changes
                final Flow.FlowPointer pointer = new Flow.FlowPointer(file);
                pointer.setGitRev(commit.getName());
                flow.putVersion(version, pointer);
            }
        }
    }

    private boolean validateRequiredValue(final Map map, String nameOfMap, Object ... keys) {
        for (Object key : keys) {
            if (!map.containsKey(key)) {
                logger.warn("{} does not have {}. Skipping it.", nameOfMap, key);
                return false;
            }
        }
        return true;
    }

    public Bucket getBucketOrCreate(String bucketId) {
        return buckets.computeIfAbsent(bucketId, k -> new Bucket(bucketId));
    }

    public Optional<Bucket> getBucket(String bucketId) {
        return Optional.ofNullable(buckets.get(bucketId));
    }


    void saveBucket(final Bucket bucket, final File destination) throws IOException {
        final Yaml yaml = new Yaml();
        final Map<String, Object> serializedBucket = bucket.serialize();

        try (final Writer writer = new OutputStreamWriter(
                new FileOutputStream(destination), StandardCharsets.UTF_8)) {
            yaml.dump(serializedBucket, writer);
        }
    }

    // TODO: Write lock
    String commit(String message, String ... files) throws GitAPIException {
        try (final Git git = new Git(gitRepo)) {
            final AddCommand add = git.add();
            for (String file : files) {
                add.addFilepattern(file);
            }
            add.call();

            final RevCommit commit = git.commit()
                    .setMessage(message)
                    .call();
            return commit.getName();
        }
    }


}
