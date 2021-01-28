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

package org.apache.spark.network.shuffle.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.shuffle.ShuffleIndexInformation;
import org.apache.spark.network.shuffle.ShuffleIndexRecord;
import org.apache.spark.network.shuffle.cache.metrics.BlockManagerMonitor;
import org.apache.spark.network.shuffle.cache.metrics.GenericBlockManagerMonitor;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LevelDBProvider;
import org.apache.spark.network.util.LevelDBProvider.StoreVersion;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Manages converting shuffle BlockIds into physical segments of local files, from a process outside
 * of Executors. Each Executor must register its own configuration about where it stores its files
 * (local dirs) and how (shuffle manager). The logic for retrieval of individual files is replicated
 * from Spark's IndexShuffleBlockResolver.
 */
public class ExternalShuffleBlockResolverWithCache {
    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShuffleBlockResolverWithCache.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * This a common prefix to the key for each app registration we stick in leveldb, so they
     * are easy to find, since leveldb lets you search based on prefix.
     */
    private static final String APP_KEY_PREFIX = "AppExecShuffleInfo";
    private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

    private static final Pattern MULTIPLE_SEPARATORS = Pattern.compile(File.separator + "{2,}");

    // Map containing all registered executors' metadata.
    @VisibleForTesting
    final ConcurrentMap<AppExecId, ExecutorShuffleInfo> executors;

    /**
     * Caches index file information so that we can avoid open/close the index files
     * for each block fetch.
     */
    private final LoadingCache<File, ShuffleIndexInformation> shuffleIndexCache;

    // Single-threaded Java executor used to perform expensive recursive directory deletion.
    private final Executor directoryCleaner;

    private final TransportConf conf;

    @VisibleForTesting
    final File registeredExecutorFile;
    @VisibleForTesting
    final DB db;

    private final List<String> knownManagers = Arrays.asList(
        "org.apache.spark.shuffle.sort.SortShuffleManager",
        "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager");

    // ------------------------------------------------------------------
    // Shuffle cache related fields and properties
    // ------------------------------------------------------------------
    private final MetricRegistry registry;

    private final long readThroughSize;

    // cache invalidate
    private final ConcurrentMap<AppExecId, Set<File>> shuffleFiles;

    // cache read
    private final ConcurrentMap<File, NavigableMap<Long, ShuffleSegment>> fileToSegments;

    private final boolean cacheQuotaEnabled;

    /**
     * Remembers which index files are currently being processed.
     */
    private final HashSet<File> fetching;

    private final ShuffleSegmentCache shuffleSegmentCache;

    private final BlockManagerMonitor cacheMonitor;

    public ExternalShuffleBlockResolverWithCache(
            TransportConf conf,
            File registeredExecutorFile,
            MetricRegistry registry)
        throws IOException {
        this(
            conf,
            registeredExecutorFile,
            Executors.newSingleThreadExecutor(
                    NettyUtils.createThreadFactory("spark-shuffle-directory-cleaner")),
            registry);
    }

    // Allows tests to have more control over when directories are cleaned up.
    @VisibleForTesting
    ExternalShuffleBlockResolverWithCache(
            TransportConf conf,
            File registeredExecutorFile,
            Executor directoryCleaner,
            MetricRegistry registry) throws IOException {
        this.conf = conf;
        this.registeredExecutorFile = registeredExecutorFile;
        this.registry = registry;

        long metricsHistogramTimeWindowSize = conf.metricsHistogramTimeSec();

        String monitorLevel = conf.metricsMonitorLevel();

        if (monitorLevel.equalsIgnoreCase("app")
                || monitorLevel.equalsIgnoreCase("nm")) {
            this.cacheMonitor =
                new GenericBlockManagerMonitor(registry, metricsHistogramTimeWindowSize);
        } else {
            this.cacheMonitor = BlockManagerMonitor.DUMMY_MONITOR;
        }

        String indexCacheSize = "100m";
        CacheLoader<File, ShuffleIndexInformation> indexCacheLoader =
            new CacheLoader<File, ShuffleIndexInformation>() {
                public ShuffleIndexInformation load(File file) throws IOException {
                    return new ShuffleIndexInformation(file);
                }
            };
        shuffleIndexCache = CacheBuilder.newBuilder()
            .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
            .weigher((Weigher<File, ShuffleIndexInformation>) (file, indexInfo) -> indexInfo.getSize())
            .build(indexCacheLoader);
        db = LevelDBProvider.initLevelDB(this.registeredExecutorFile, CURRENT_VERSION, mapper);
        if (db != null) {
            executors = reloadRegisteredExecutors(db);
        } else {
            executors = Maps.newConcurrentMap();
        }
        this.directoryCleaner = directoryCleaner;

        // fields and properties for shuffle segment cache
        this.readThroughSize = conf.cacheReadThroughSize();
        long cacheCapacity = conf.cacheSize();

        this.shuffleFiles = Maps.newConcurrentMap();
        this.fileToSegments = Maps.newConcurrentMap();
        this.fetching = Sets.newHashSet();

        this.cacheQuotaEnabled = conf.cacheQuotaEnabled();
        String cacheImpl = conf.cacheImpl();

        if (cacheImpl.equalsIgnoreCase("caffeine")) {
            this.shuffleSegmentCache = new SegmentCacheCaffeine(cacheCapacity, registry, cacheMonitor, conf);
        } else {
            this.shuffleSegmentCache = new SegmentCacheGuava(cacheCapacity, registry, cacheMonitor, conf);
        }
    }

    public boolean canReadFromCache(String appId) {
        if (!cacheQuotaEnabled) {
            return true;
        }
        return !shuffleSegmentCache.hasExceededQuota(appId);
    }

    public int getRegisteredExecutorsSize() {
        return executors.size();
    }

    /**
     * Registers a new Executor with all the configuration we need to find its shuffle files.
     */
    public void registerExecutor(
        String appId,
        String execId,
        ExecutorShuffleInfo executorInfo) {
        AppExecId fullId = new AppExecId(appId, execId);
        logger.info("Registered executor {} with {}", fullId, executorInfo);
        if (!knownManagers.contains(executorInfo.shuffleManager)) {
            throw new UnsupportedOperationException(
                "Unsupported shuffle manager of executor: " + executorInfo);
        }
        try {
            if (db != null) {
                byte[] key = dbAppExecKey(fullId);
                byte[] value = mapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8);
                db.put(key, value);
            }
        } catch (Exception e) {
            logger.error("Error saving registered executors", e);
        }
        executors.put(fullId, executorInfo);
    }

    /**
     * Obtains a FileSegmentManagedBuffer from (shuffleId, mapId, reduceId, numBlocks). We make assumptions
     * about how the hash and sort based shuffles store their data.
     */
    public ManagedBuffer getBlockData(
            String appId,
            String execId,
            int shuffleId,
            int mapId,
            int reduceId,
            int numBlocks) {
        AppExecId appExecId = new AppExecId(appId, execId);
        ExecutorShuffleInfo executor = executors.get(appExecId);
        checkArgument(numBlocks > 0, "numBlocks should be greater than 0");
        Range<Integer> partitionIds = Range.closed(reduceId, reduceId + numBlocks - 1);

        if (executor == null) {
            throw new RuntimeException(
                String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
        }

        File indexFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.index");
        File dataFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.data");

        shuffleFiles.putIfAbsent(appExecId, Sets.newHashSet());
        Set<File> indexFileSet = shuffleFiles.get(appExecId);
        synchronized (indexFileSet) {
            indexFileSet.add(indexFile);
        }

        try {
            ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFile);
            ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(reduceId, numBlocks);
            splitMapOutputIntoSegmentsIfNeeded(appId, indexFile, dataFile, dataFile.length());
            long startOffsetInFile = shuffleIndexRecord.getOffset();
            long endOffsetInFile = startOffsetInFile + shuffleIndexRecord.getLength();

            if (shuffleIndexRecord.getLength() >= readThroughSize || !canReadFromCache(appId)) {
                cacheMonitor.readThroughRequest(appId, shuffleIndexRecord.getLength());
                logger.debug("Block read_through app {} executor {} shuffle {} map {} reduce {} numBlock {}",
                        appId, execId, shuffleId, mapId, reduceId, numBlocks);
                return new FileSegmentManagedBuffer(
                        conf,
                        dataFile,
                        shuffleIndexRecord.getOffset(),
                        shuffleIndexRecord.getLength());

            } else if (numBlocks == 1) {
                NavigableMap<Long, ShuffleSegment> allSegments = fileToSegments.get(indexFile);
                checkArgument(allSegments != null);
                long floorKey = allSegments.floorKey(startOffsetInFile);
                ShuffleSegment current = allSegments.get(floorKey);
                ManagedBuffer result = getManagedBuffer(
                        appId, dataFile, startOffsetInFile, endOffsetInFile,
                        partitionIds, current);
                if (result.size() != shuffleIndexRecord.getLength()) {
                    throw new IOException(String.format(
                        "Expected read length: %d, actual read length: %d : " +
                            "app %s executor %s shuffle %d map %d reduce %d numBlock %d",
                        shuffleIndexRecord.getLength(), result.size(),
                        appId, execId, shuffleId, mapId, reduceId, numBlocks));
                }
                return result;
            } else {
                NavigableMap<Long, ShuffleSegment> allSegments = fileToSegments.get(indexFile);
                checkArgument(allSegments != null);

                long floorKey = allSegments.floorKey(startOffsetInFile);
                long ceilingKey = allSegments.ceilingKey(endOffsetInFile);

                ShuffleSegment[] targetSegments = allSegments
                    .subMap(floorKey, true, ceilingKey, false)
                    .values()
                    .toArray(new ShuffleSegment[0]);

                int totalNum = targetSegments.length;

                ByteBuf[] results = new ByteBuf[totalNum];

                long length = 0L;
                for (int idx = 0; idx < totalNum; idx ++) {
                    ShuffleSegment current = targetSegments[idx];
                    ManagedBuffer tmp = getManagedBuffer(
                            appId, dataFile, startOffsetInFile, endOffsetInFile,
                            partitionIds, current);
                    length += tmp.size();
                    if (tmp instanceof NettyManagedBuffer) {
                        results[idx] = ((NettyManagedBuffer) tmp).getBuf();
                    } else {
                        results[idx] = Unpooled.wrappedBuffer(tmp.nioByteBuffer());
                    }
                }
                if (length != shuffleIndexRecord.getLength()) {
                    throw new IOException(String.format(
                        "Expected read length: %d, actual read length: %d : " +
                            "app %s executor %s shuffle %d map %d reduce %d numBlock %d",
                        shuffleIndexRecord.getLength(), length,
                        appId, execId, shuffleId, mapId, reduceId, numBlocks));
                }
                return new NettyManagedBuffer(Unpooled.wrappedBuffer(results));
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to open file: " + indexFile, e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data file: " + dataFile, e);
        }
    }

    private ManagedBuffer getManagedBuffer(
            String appId,
            File dataFile,
            long startOffsetInFile,
            long endOffsetInFile,
            Range<Integer> partitionIds,
            ShuffleSegment current) {

        long startOffInCurrentSeg;
        long endOffInCurrentSeg;
        long effectiveLength;

        startOffInCurrentSeg = Math.max(current.getOffset(), startOffsetInFile);
        endOffInCurrentSeg = Math.min(current.getOffset() + current.getLength(), endOffsetInFile);
        effectiveLength = endOffInCurrentSeg - startOffInCurrentSeg;

        if (current.getCacheState() == ShuffleSegment.CacheState.READ_THROUGH) {
            cacheMonitor.readThroughRequest(appId, effectiveLength);
            return new FileSegmentManagedBuffer(
                    conf,
                    dataFile,
                    startOffInCurrentSeg,
                    effectiveLength);
        } else if (current.getCacheState() == ShuffleSegment.CacheState.EVICTED) {
            cacheMonitor.readThroughFromSegment(appId, effectiveLength);
            return new FileSegmentManagedBuffer(
                conf,
                dataFile,
                startOffInCurrentSeg,
                effectiveLength);
        } else {
            long stamp = current.getLock().tryOptimisticRead();
            ByteBuf readCacheResult = null;
            try {
                ByteBuf cached = shuffleSegmentCache.get(current);
                long offsetInBuffer = startOffInCurrentSeg - current.getOffset();
                readCacheResult = cached.retainedSlice((int) offsetInBuffer, (int) effectiveLength);
            } catch (Exception e) {
                // we failed to get buf from cache either because simultaneous eviction
                // or segment load failure, we just swallow the exception and fall back
                // to the un-cached routine.
            }

            if (current.getLock().validate(stamp) && readCacheResult != null) {
                cacheMonitor.readCachedFromSegment(appId, effectiveLength);
                current.setTouched(partitionIds);
                if (current.isFullyRead()) {
                    shuffleSegmentCache.invalidate(current);
                }
                return new NettyManagedBuffer(readCacheResult);
            } else {
                shuffleSegmentCache.checkOODMErrorAndReset();
                cacheMonitor.readThroughFromSegment(appId, effectiveLength);
                return new FileSegmentManagedBuffer(
                        conf,
                        dataFile,
                        startOffInCurrentSeg,
                        effectiveLength);
            }
        }
    }

    private void splitMapOutputIntoSegmentsIfNeeded(String appId, File indexFile, File dataFile, long totalSize) {
        NavigableMap<Long, ShuffleSegment> splitResults;
        long startTime = System.currentTimeMillis();
        synchronized (fetching) {
            // Someone else is splitting it, wait for it to be done
            while (fetching.contains(indexFile)) {
                try {
                    fetching.wait();
                } catch (InterruptedException ie) {
                    // do nothing
                }
            }

            // Either while we waited the split happened successfully, or
            // someone split it in between the get and the fetching.synchronized.
            splitResults = fileToSegments.get(indexFile);
            if (splitResults == null) {
                // We have to do the fetch, get others to wait for us.
                fetching.add(indexFile);
            }
        }

        if (splitResults == null) {
            // We won the race to split the partitions; do so
            // This try-finally prevents hangs due to timeouts:
            try {
                ShuffleIndexInformation indexInfo = shuffleIndexCache.get(indexFile);
                Splitter splitter = new Splitter(indexInfo.getOffsetsArray(), totalSize, readThroughSize);
                splitter.splitBlocksIntoSegments();
                splitResults = splitter.getResults(appId, indexFile, dataFile);
                fileToSegments.put(indexFile, splitResults);
                logger.info("Split map output file {} successfully.", indexFile);
                logger.info("Split Results: {}", splitResults.entrySet().stream()
                        .map(entry -> String.format("Start offset: %d, %s", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(",")));
                logger.info("The split procedure took {} ms", System.currentTimeMillis() - startTime);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to open file: " + indexFile, e);
            } finally {
                synchronized (fetching) {
                    fetching.remove(indexFile);
                    fetching.notifyAll();
                }
            }
        }

        if (splitResults == null) {
            throw new RuntimeException("Failed to split index file" + indexFile);
        }
    }

    /**
     * Removes our metadata of all executors registered for the given application, and optionally
     * also deletes the local directories associated with the executors of that application in a
     * separate thread.
     * <p>
     * It is not valid to call registerExecutor() for an executor with this appId after invoking
     * this method.
     */
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
        logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
        Iterator<Map.Entry<AppExecId, ExecutorShuffleInfo>> it = executors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AppExecId, ExecutorShuffleInfo> entry = it.next();
            AppExecId fullId = entry.getKey();
            final ExecutorShuffleInfo executor = entry.getValue();

            // Only touch executors associated with the appId that was removed.
            if (appId.equals(fullId.appId)) {
                String execId = fullId.execId;

                it.remove();
                if (db != null) {
                    try {
                        db.delete(dbAppExecKey(fullId));
                    } catch (IOException e) {
                        logger.error("Error deleting {} from executor state db", appId, e);
                    }
                }

                if (cleanupLocalDirs) {
                    logger.info("Cleaning up executor {}'s {} local dirs", fullId, executor.localDirs.length);

                    // Execute the actual deletion in a different thread, as it may take some time.
                    directoryCleaner.execute(() -> deleteExecutorDirs(executor.localDirs));
                }
            }
        }

        cacheMonitor.applicationRemoved(appId);
        shuffleSegmentCache.applicationRemoved(appId);

        // collect cached segments and invalidate them all from segment cache
        Iterator<Map.Entry<AppExecId, Set<File>>> fileIt = shuffleFiles.entrySet().iterator();
        while (fileIt.hasNext()) {
            Map.Entry<AppExecId, Set<File>> entry = fileIt.next();
            AppExecId fullId = entry.getKey();
            final Set<File> indexFiles = entry.getValue();

            // Only touch matched app id
            if (appId.equals(fullId.appId)) {
                synchronized (indexFiles) {
                    List<ShuffleSegment> segments =
                        indexFiles.stream()
                            .filter(f -> fileToSegments.containsKey(f))
                            .flatMap(f -> fileToSegments.get(f).values().stream())
                            .filter(s -> s.getLength() > 0)
                            .collect(Collectors.toList());
                    segments.forEach(s -> s.setCacheState(ShuffleSegment.CacheState.EVICTED));
                    shuffleSegmentCache.invalidateAll(segments);
                    indexFiles.stream().forEach(f -> fileToSegments.remove(f));
                    indexFiles.stream().forEach(f -> shuffleIndexCache.invalidate(f));
                }
                shuffleFiles.remove(fullId);
                executors.remove(fullId);
            }
        }
    }

    /**
     * Removes all the non-shuffle files in any local directories associated with the finished
     * executor.
     */
    public void executorRemoved(String executorId, String appId) {
        logger.info("Clean up non-shuffle files associated with the finished executor {}", executorId);
        AppExecId fullId = new AppExecId(appId, executorId);
        final ExecutorShuffleInfo executor = executors.get(fullId);
        if (executor == null) {
            // Executor not registered, skip clean up of the local directories.
            logger.info("Executor is not registered (appId={}, execId={})", appId, executorId);
        } else {
            logger.info("Cleaning up non-shuffle files in executor {}'s {} local dirs", fullId,
                executor.localDirs.length);

            // Execute the actual deletion in a different thread, as it may take some time.
            directoryCleaner.execute(() -> deleteNonShuffleFiles(executor.localDirs));
        }
    }

    /**
     * Synchronously deletes each directory one at a time.
     * Should be executed in its own thread, as this may take a long time.
     */
    private void deleteExecutorDirs(String[] dirs) {
        for (String localDir : dirs) {
            try {
                JavaUtils.deleteRecursively(new File(localDir));
                logger.debug("Successfully cleaned up directory: {}", localDir);
            } catch (Exception e) {
                logger.error("Failed to delete directory: " + localDir, e);
            }
        }
    }

    /**
     * Synchronously deletes non-shuffle files in each directory recursively.
     * Should be executed in its own thread, as this may take a long time.
     */
    private void deleteNonShuffleFiles(String[] dirs) {
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                // Don't delete shuffle data or shuffle index files.
                return !name.endsWith(".index") && !name.endsWith(".data");
            }
        };

        for (String localDir : dirs) {
            try {
                JavaUtils.deleteRecursively(new File(localDir), filter);
                logger.debug("Successfully cleaned up non-shuffle files in directory: {}", localDir);
            } catch (Exception e) {
                logger.error("Failed to delete non-shuffle files in directory: " + localDir, e);
            }
        }
    }

    /**
     * Hashes a filename into the corresponding local directory, in a manner consistent with
     * Spark's DiskBlockManager.getFile().
     */
    @VisibleForTesting
    static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
        int hash = JavaUtils.nonNegativeHash(filename);
        String localDir = localDirs[hash % localDirs.length];
        int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
        return new File(createNormalizedInternedPathname(
            localDir, String.format("%02x", subDirId), filename));
    }

    void close() {
        if (db != null) {
            try {
                db.close();
            } catch (IOException e) {
                logger.error("Exception closing leveldb with registered executors", e);
            }
        }
    }

    /**
     * This method is needed to avoid the situation when multiple File instances for the
     * same pathname "foo/bar" are created, each with a separate copy of the "foo/bar" String.
     * According to measurements, in some scenarios such duplicate strings may waste a lot
     * of memory (~ 10% of the heap). To avoid that, we intern the pathname, and before that
     * we make sure that it's in a normalized form (contains no "//", "///" etc.) Otherwise,
     * the internal code in java.io.File would normalize it later, creating a new "foo/bar"
     * String copy. Unfortunately, we cannot just reuse the normalization code that java.io.File
     * uses, since it is in the package-private class java.io.FileSystem.
     */
    @VisibleForTesting
    static String createNormalizedInternedPathname(String dir1, String dir2, String fname) {
        String pathname = dir1 + File.separator + dir2 + File.separator + fname;
        Matcher m = MULTIPLE_SEPARATORS.matcher(pathname);
        pathname = m.replaceAll("/");
        // A single trailing slash needs to be taken care of separately
        if (pathname.length() > 1 && pathname.endsWith("/")) {
            pathname = pathname.substring(0, pathname.length() - 1);
        }
        return pathname.intern();
    }

    /**
     * Simply encodes an executor's full ID, which is appId + execId.
     */
    public static class AppExecId {
        public final String appId;
        public final String execId;

        @JsonCreator
        public AppExecId(@JsonProperty("appId") String appId, @JsonProperty("execId") String execId) {
            this.appId = appId;
            this.execId = execId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AppExecId appExecId = (AppExecId) o;
            return Objects.equal(appId, appExecId.appId) && Objects.equal(execId, appExecId.execId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(appId, execId);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .toString();
        }
    }

    private static byte[] dbAppExecKey(AppExecId appExecId) throws IOException {
        // we stick a common prefix on all the keys so we can find them in the DB
        String appExecJson = mapper.writeValueAsString(appExecId);
        String key = (APP_KEY_PREFIX + ";" + appExecJson);
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private static AppExecId parseDbAppExecKey(String s) throws IOException {
        if (!s.startsWith(APP_KEY_PREFIX)) {
            throw new IllegalArgumentException("expected a string starting with " + APP_KEY_PREFIX);
        }
        String json = s.substring(APP_KEY_PREFIX.length() + 1);
        AppExecId parsed = mapper.readValue(json, AppExecId.class);
        return parsed;
    }

    @VisibleForTesting
    static ConcurrentMap<AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(DB db)
        throws IOException {
        ConcurrentMap<AppExecId, ExecutorShuffleInfo> registeredExecutors = Maps.newConcurrentMap();
        if (db != null) {
            DBIterator itr = db.iterator();
            itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
            while (itr.hasNext()) {
                Map.Entry<byte[], byte[]> e = itr.next();
                String key = new String(e.getKey(), StandardCharsets.UTF_8);
                if (!key.startsWith(APP_KEY_PREFIX)) {
                    break;
                }
                AppExecId id = parseDbAppExecKey(key);
                logger.info("Reloading registered executors: " + id.toString());
                ExecutorShuffleInfo shuffleInfo = mapper.readValue(e.getValue(), ExecutorShuffleInfo.class);
                registeredExecutors.put(id, shuffleInfo);
            }
        }
        return registeredExecutors;
    }
}
