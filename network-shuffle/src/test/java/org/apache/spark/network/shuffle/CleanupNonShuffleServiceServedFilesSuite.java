/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.spark.network.util.ServiceConf;
import org.apache.spark.network.util.TransportConf;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CleanupNonShuffleServiceServedFilesSuite {

    private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";
    private static Set<String> expectedShuffleFilesToKeep =
        ImmutableSet.of("shuffle_782_450_0.index", "shuffle_782_450_0.data");
    private static Set<String> expectedShuffleAndRddFilesToKeep =
        ImmutableSet.of("shuffle_782_450_0.index", "shuffle_782_450_0.data", "rdd_12_34");
    // Same-thread Executor used to ensure cleanup happens synchronously in test thread.
    private Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    private static void assertStillThere(TestShuffleDataContext dataContext) {
        for (String localDir : dataContext.localDirs) {
            assertTrue(localDir + " was cleaned up prematurely", new File(localDir).exists());
        }
    }

    private static Set<String> collectFilenames(File[] files) throws IOException {
        Set<String> result = new HashSet<>();
        for (File file : files) {
            if (file.exists()) {
                try (Stream<Path> walk = Files.walk(file.toPath())) {
                    result.addAll(walk
                        .filter(Files::isRegularFile)
                        .map(x -> x.toFile().getName())
                        .collect(Collectors.toSet()));
                }
            }
        }
        return result;
    }

    private static void assertContainedFilenames(
        TestShuffleDataContext dataContext,
        Set<String> expectedFilenames) throws IOException {
        Set<String> collectedFilenames = new HashSet<>();
        for (String localDir : dataContext.localDirs) {
            File[] dirs = new File[]{new File(localDir)};
            collectedFilenames.addAll(collectFilenames(dirs));
        }
        assertEquals(expectedFilenames, collectedFilenames);
    }

    private static TestShuffleDataContext initDataContext(boolean withFilesToKeep)
        throws IOException {
        TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);
        dataContext.create();
        if (withFilesToKeep) {
            createFilesToKeep(dataContext);
        } else {
            createRemovableTestFiles(dataContext);
        }
        return dataContext;
    }

    private static void createFilesToKeep(TestShuffleDataContext dataContext) throws IOException {
        Random rand = new Random(123);
        dataContext.insertSortShuffleData(rand.nextInt(1000), rand.nextInt(1000), new byte[][]{
            "ABC".getBytes(StandardCharsets.UTF_8),
            "DEF".getBytes(StandardCharsets.UTF_8)});
        dataContext.insertCachedRddData(12, 34, new byte[]{42});
    }

    private static void createRemovableTestFiles(TestShuffleDataContext dataContext)
        throws IOException {
        dataContext.insertSpillData();
        dataContext.insertBroadcastData();
        dataContext.insertTempShuffleData();
    }

    private TransportConf getConf(boolean isFetchRddEnabled) {
        return new TransportConf(
            "shuffle", ServiceConf.getServiceConf());
    }

    @Test
    public void cleanupOnRemovedExecutorWithFilesToKeepFetchRddEnabled() throws IOException {
        cleanupOnRemovedExecutor(true, getConf(true), expectedShuffleAndRddFilesToKeep);
    }

    @Test
    public void cleanupOnRemovedExecutorWithFilesToKeepFetchRddDisabled() throws IOException {
        cleanupOnRemovedExecutor(true, getConf(false), expectedShuffleFilesToKeep);
    }

    @Test
    public void cleanupOnRemovedExecutorWithoutFilesToKeep() throws IOException {
        cleanupOnRemovedExecutor(false, getConf(true), Collections.emptySet());
    }

    private void cleanupOnRemovedExecutor(
        boolean withFilesToKeep,
        TransportConf conf,
        Set<String> expectedFilesKept) throws IOException {
        TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

        ExternalShuffleBlockResolver resolver =
            new ExternalShuffleBlockResolver(conf, sameThreadExecutor);
        resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
        resolver.executorRemoved("exec0", "app");

        assertContainedFilenames(dataContext, expectedFilesKept);
    }

    @Test
    public void cleanupUsesExecutorWithFilesToKeep() throws IOException {
        cleanupUsesExecutor(true);
    }

    @Test
    public void cleanupUsesExecutorWithoutFilesToKeep() throws IOException {
        cleanupUsesExecutor(false);
    }

    private void cleanupUsesExecutor(boolean withFilesToKeep) throws IOException {
        TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

        AtomicBoolean cleanupCalled = new AtomicBoolean(false);

        // Executor which only captures whether it's being used, without executing anything.
        Executor dummyExecutor = runnable -> cleanupCalled.set(true);

        ExternalShuffleBlockResolver manager =
            new ExternalShuffleBlockResolver(getConf(true), dummyExecutor);

        manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
        manager.executorRemoved("exec0", "app");

        assertTrue(cleanupCalled.get());
        assertStillThere(dataContext);
    }

    @Test
    public void cleanupOnlyRemovedExecutorWithFilesToKeepFetchRddEnabled() throws IOException {
        cleanupOnlyRemovedExecutor(true, getConf(true), expectedShuffleAndRddFilesToKeep);
    }

    @Test
    public void cleanupOnlyRemovedExecutorWithFilesToKeepFetchRddDisabled() throws IOException {
        cleanupOnlyRemovedExecutor(true, getConf(false), expectedShuffleFilesToKeep);
    }

    @Test
    public void cleanupOnlyRemovedExecutorWithoutFilesToKeep() throws IOException {
        cleanupOnlyRemovedExecutor(false, getConf(true), Collections.emptySet());
    }

    private void cleanupOnlyRemovedExecutor(
        boolean withFilesToKeep,
        TransportConf conf,
        Set<String> expectedFilesKept) throws IOException {
        TestShuffleDataContext dataContext0 = initDataContext(withFilesToKeep);
        TestShuffleDataContext dataContext1 = initDataContext(withFilesToKeep);

        ExternalShuffleBlockResolver resolver =
            new ExternalShuffleBlockResolver(conf, sameThreadExecutor);
        resolver.registerExecutor("app", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
        resolver.registerExecutor("app", "exec1", dataContext1.createExecutorInfo(SORT_MANAGER));


        resolver.executorRemoved("exec-nonexistent", "app");
        assertStillThere(dataContext0);
        assertStillThere(dataContext1);

        resolver.executorRemoved("exec0", "app");
        assertContainedFilenames(dataContext0, expectedFilesKept);
        assertStillThere(dataContext1);

        resolver.executorRemoved("exec1", "app");
        assertContainedFilenames(dataContext0, expectedFilesKept);
        assertContainedFilenames(dataContext1, expectedFilesKept);

        // Make sure it's not an error to cleanup multiple times
        resolver.executorRemoved("exec1", "app");
        assertContainedFilenames(dataContext0, expectedFilesKept);
        assertContainedFilenames(dataContext1, expectedFilesKept);
    }

    @Test
    public void cleanupOnlyRegisteredExecutorWithFilesToKeepFetchRddEnabled() throws IOException {
        cleanupOnlyRegisteredExecutor(true, getConf(true), expectedShuffleAndRddFilesToKeep);
    }

    @Test
    public void cleanupOnlyRegisteredExecutorWithFilesToKeepFetchRddDisabled() throws IOException {
        cleanupOnlyRegisteredExecutor(true, getConf(false), expectedShuffleFilesToKeep);
    }

    @Test
    public void cleanupOnlyRegisteredExecutorWithoutFilesToKeep() throws IOException {
        cleanupOnlyRegisteredExecutor(false, getConf(true), Collections.emptySet());
    }

    private void cleanupOnlyRegisteredExecutor(
        boolean withFilesToKeep,
        TransportConf conf,
        Set<String> expectedFilesKept) throws IOException {
        TestShuffleDataContext dataContext = initDataContext(withFilesToKeep);

        ExternalShuffleBlockResolver resolver =
            new ExternalShuffleBlockResolver(conf, sameThreadExecutor);
        resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));

        resolver.executorRemoved("exec1", "app");
        assertStillThere(dataContext);

        resolver.executorRemoved("exec0", "app");
        assertContainedFilenames(dataContext, expectedFilesKept);
    }
}
