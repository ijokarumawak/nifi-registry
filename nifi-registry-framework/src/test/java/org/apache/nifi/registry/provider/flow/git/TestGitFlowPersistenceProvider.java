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

import org.apache.nifi.registry.provider.flow.git.GitFlowMetaData;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class TestGitFlowPersistenceProvider {

    @Test
    public void findCommits() throws IOException, GitAPIException {
        final Repository gitRepo = new FileRepositoryBuilder()
                .readEnvironment()
                .findGitDir(new File("/Users/koji/dev/nifi-registry-data-test"))
                .build();

        final Map<String, Ref> allRefs = gitRepo.getAllRefs();
        System.out.println(allRefs);

        final Git git = new Git(gitRepo);
        for (RevCommit commit : git.log().call()) {
            System.out.println(new String(new char[80]).replace('\0', '-'));
            System.out.println(commit);
            final RevTree tree = commit.getTree();
            printTree(gitRepo, tree);
        }
    }


    @Test
    public void test() throws IOException {
        final Repository gitRepo = new FileRepositoryBuilder()
                .readEnvironment()
                .findGitDir(new File("/Users/koji/dev/nifi-registry-data-test"))
                .build();

        System.out.printf("gitRepo=%s\n", gitRepo);

        final ObjectId head = gitRepo.resolve("HEAD");
        final ObjectId prev = gitRepo.resolve("HEAD~1");

        System.out.printf("head=%s\n", head);

        // How can I get a specific file?
        final RevWalk walk = new RevWalk(gitRepo);
        final RevTree headTree = walk.parseTree(head);
        printTree(gitRepo, headTree);

        System.out.println(new String(new char[20]).replace('\0', '-'));

        final RevTree prevTree = walk.parseTree(prev);
        printTree(gitRepo, prevTree);

    }

    private void printTree(Repository gitRepo, RevTree tree) throws IOException {
        final TreeWalk treeWalk = new TreeWalk(gitRepo);
        treeWalk.addTree(tree);

        while (treeWalk.next()) {
            if (treeWalk.isSubtree()) {
                System.out.printf("Dir: %s\n", treeWalk.getPathString());
                treeWalk.enterSubtree();;
            } else {
                // It seems using this objectId, the content can be retrieved.
                // TODO: what is this nth?? How about if a tree contains more than 1 files? Tree count is always 1?
                System.out.printf("File: %s, treeCount=%d\n", treeWalk.getPathString(), treeWalk.getTreeCount());

                final ObjectId objectId = treeWalk.getObjectId(0);
                final ObjectLoader reader = gitRepo.newObjectReader().open(objectId);
                final byte[] bytes = reader.getBytes();
                final String content = new String(bytes);
                System.out.printf("Content: %s", content);
            }
        }
    }

    @Test
    public void testLoad() throws IOException {
        final Repository gitRepo = new FileRepositoryBuilder()
                .readEnvironment()
                .findGitDir(new File("/Users/koji/dev/nifi-registry-data-test"))
                .build();

        final GitFlowMetaData metaData = new GitFlowMetaData();
        metaData.load("/Users/koji/dev/nifi-registry-data-test");

    }

    @Test
    public void testLoadFromGit() throws IOException, GitAPIException {
        final GitFlowMetaData metaData = new GitFlowMetaData();
        metaData.loadFromGit("/Users/koji/dev/nifi-registry-data-test");
    }

}
