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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class GitFlowMetaData {

    private static final Logger logger = LoggerFactory.getLogger(GitFlowMetaData.class);
    private static final int CURRENT_LAYOUT_VERSION = 1;
    private static final String LAYOUT_VERSION = "layoutVer";
    private static final String BUCKET_ID = "bucketId";
    private static final String FLOWS = "flows";
    public static final String VER = "ver";
    public static final String FILE = "file";

    private Repository gitRepo;

    /**
     * Bucket ID to Bucket.
     */
    private Map<String, Bucket> buckets = new HashMap<>();

//    public GitFlowMetaData(Repository gitRepo) {
//        this.gitRepo = gitRepo;
//
//        // TODO: load buckets.yml.
//        final File directory = gitRepo.getDirectory();
//        System.out.println(directory);
//
//        // TODO: populate objects by looking at commits.
//    }

    @SuppressWarnings("unchecked")
    public void load(String gitProjectRootDir) throws IOException {
        final File rootDir = new File(gitProjectRootDir);

        // load buckets.
        final Yaml yaml = new Yaml();
        final File bucketsFile = new File(rootDir, "buckets.yml");
        try (final InputStream is = new FileInputStream(bucketsFile)) {
            final Map<String, Object> buckets = yaml.load(is);
            Map bucketDirs = (Map) buckets.get("bucketDirs");

            for (Object bucketId : bucketDirs.keySet()) {
                final Bucket bucket = getBucketOrCreate(bucketId.toString());
                final String bucketDir = bucketDirs.get(bucketId).toString();
                bucket.setBucketName(bucketDir);

                // load files.
                final File flowsFile = new File(rootDir, bucketDir + "/flows.yml");
                try (final InputStream fis = new FileInputStream(flowsFile)) {
                    final Map<String, Object> flowsMeta = yaml.load(fis);
                    final Map<String, Object> flows = (Map<String, Object>) flowsMeta.get("flows");

                    for (String flowId : flows.keySet()) {
                        final Map<String, Object> flowMeta = (Map<String, Object>) flows.get(flowId);
                        final Flow flow = bucket.getFlowOrCreate(flowId);
                        final int version = (int) flowMeta.get("ver");
                        final String file = (String) flowMeta.get("file");

                        // TODO: Use git data.
                        flow.putVersion(version, new Flow.FlowPointer("HEAD", file));

                    }
                }

                putBucket(bucket);
            }
        }

        System.out.println(this);
    }

    @SuppressWarnings("unchecked")
    // TODO: Error handling.
    public void loadFromGit(String gitProjectRootDir) throws IOException, GitAPIException {
        final Repository gitRepo = new FileRepositoryBuilder()
                .readEnvironment()
                .findGitDir(new File(gitProjectRootDir))
                .build();

        final Yaml yaml = new Yaml();

        try (final Git git = new Git(gitRepo)) {
            for (RevCommit commit : git.log().call()) {
                final String shortCommitId = commit.getId().abbreviate(7).name();
                logger.debug("Processing a commit: {}", shortCommitId);
                final RevTree tree = commit.getTree();

                try (final TreeWalk treeWalk = new TreeWalk(gitRepo)) {
                    treeWalk.addTree(tree);

                    // Path -> ObjectId
                    Map<String, ObjectId> flowsObjectIds = new HashMap<>();
                    while (treeWalk.next()) {
                        if (treeWalk.isSubtree()) {
                            treeWalk.enterSubtree();
                        } else {
                            final String pathString = treeWalk.getPathString();
                            // TODO: what is this nth?? When does it get grater than 0? Tree count seems to be always 1..
                            if (pathString.endsWith("/flows.yml")) {
                                flowsObjectIds.put(pathString, treeWalk.getObjectId(0));
                            }
                        }
                    }

                    if (flowsObjectIds.isEmpty()) {
                        logger.debug("Tree at commit {} does not contain any flows.yml. Skipping it.", shortCommitId);
                        continue;
                    }

                    // Processing flows.yml
                    for (String path : flowsObjectIds.keySet()) {
                        final ObjectId flowsObjectId = flowsObjectIds.get(path);
                        final Map<String, Object> flowsMeta;
                        try (InputStream flowsIn = gitRepo.newObjectReader().open(flowsObjectId).openStream()) {
                            flowsMeta = yaml.load(flowsIn);
                        }

                        if (!validateRequiredValue(flowsMeta, path, LAYOUT_VERSION, BUCKET_ID, FLOWS)) {
                            continue;
                        }

                        int layoutVersion = (int) flowsMeta.get(LAYOUT_VERSION);
                        if (layoutVersion > CURRENT_LAYOUT_VERSION) {
                            logger.warn("{} has unsupported {} {}. This Registry can only support {} or lower. Skipping it.",
                                    path, LAYOUT_VERSION, layoutVersion, CURRENT_LAYOUT_VERSION);
                            continue;
                        }

                        final String bucketId = (String) flowsMeta.get(BUCKET_ID);
                        final Bucket bucket = getBucketOrCreate(bucketId);

                        // E.g. DirA/DirB/DirC/flows.yml -> DirC will be the bucket name.
                        final String[] pathNames = path.split("/");
                        final String bucketName = pathNames[pathNames.length - 2];

                        // Since commits are read in LIFO order, avoid old commits overriding the latest bucket name.
                        if (StringUtils.isEmpty(bucket.getBucketName())) {
                            bucket.setBucketName(bucketName);
                        }

                        final Map<String, Object> flows = (Map<String, Object>) flowsMeta.get(FLOWS);
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
                                flow.putVersion(version, new Flow.FlowPointer(commit.getName(), file));
                            }
                        }
                    }
                }
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

    public void putBucket(Bucket bucket) {
        buckets.put(bucket.getBucketId(), bucket);
    }
}
