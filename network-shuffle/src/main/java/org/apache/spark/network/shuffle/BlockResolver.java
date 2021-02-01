package org.apache.spark.network.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BlockResolver {
    private static final Logger logger = LoggerFactory.getLogger(BlockResolver.class);

    private static final Pattern MULTIPLE_SEPARATORS = Pattern.compile(File.separator + "{2,}");

    // Map containing all registered executors' metadata.
    @VisibleForTesting
    protected final ConcurrentMap<AppExecId, ExecutorShuffleInfo> executors;

    protected final ConcurrentMap<String, Byte> knownApps;

    /**
     * Caches index file information so that we can avoid open/close the index files
     * for each block fetch.
     */
    protected final LoadingCache<File, ShuffleIndexInformation> shuffleIndexCache;

    // Single-threaded Java executor used to perform expensive recursive directory deletion.
    protected final Executor directoryCleaner;

    protected final TransportConf conf;

    @VisibleForTesting
    protected final DBProxy db;

    protected final ZKClient zkc;

    final boolean rddFetchEnabled = false;

    private final List<String> knownManagers = Arrays.asList(
        "org.apache.spark.shuffle.sort.SortShuffleManager",
        "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager");

    public BlockResolver(TransportConf conf, Executor directoryCleaner) {
        this.conf = conf;
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
        db = new DBProxy(conf.getServiceConf());
        executors = db.reloadAllExecutorsFromRecoveryFiles();
        knownApps = Maps.newConcurrentMap();
        this.directoryCleaner = directoryCleaner;
        zkc = new ZKClient(conf.getServiceConf().getZookeeper().getHostPort(), this);
    }

    /**
     * Hashes a filename into the corresponding local directory, in a manner consistent with
     * Spark's DiskBlockManager.getFile().
     */
    @VisibleForTesting
    public static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
        int hash = JavaUtils.nonNegativeHash(filename);
        String localDir = localDirs[hash % localDirs.length];
        int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
        return new File(createNormalizedInternedPathname(
            localDir, String.format("%02x", subDirId), filename));
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
    public static String createNormalizedInternedPathname(String dir1, String dir2, String fname) {
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
     * Removes our metadata of all executors registered for the given application, and optionally
     * also deletes the local directories associated with the executors of that application in a
     * separate thread.
     * <p>
     * It is not valid to call registerExecutor() for an executor with this appId after invoking
     * this method.
     */
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
        logger.info("Application {} removed, cleanupLocalDirs = {}", appId, cleanupLocalDirs);
        knownApps.remove(appId);
        Iterator<Map.Entry<AppExecId, ExecutorShuffleInfo>> it = executors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AppExecId, ExecutorShuffleInfo> entry = it.next();
            AppExecId fullId = entry.getKey();
            final ExecutorShuffleInfo executor = entry.getValue();

            // Only touch executors associated with the appId that was removed.
            if (appId.equals(fullId.appId)) {
                it.remove();
                db.removeExecutorInDBs(fullId);
                if (cleanupLocalDirs) {
                    logger.info("Cleaning up executor {}'s {} local dirs", fullId, executor.localDirs.length);

                    // Execute the actual deletion in a different thread, as it may take some time.
                    directoryCleaner.execute(() -> deleteExecutorDirs(executor.localDirs));
                }
            }
        }
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
        db.registerExecutorInDBs(fullId, executorInfo);
        executors.put(fullId, executorInfo);
    }

    public void seenApp(String appId) {
        knownApps.computeIfAbsent(appId, k -> {
            zkc.watchAppDelete(appId);
            return (byte) 0;
        });
    }

    /**
     * Removes all the files which cannot be served by the external shuffle service (non-shuffle and
     * non-RDD files) in any local directories associated with the finished executor.
     */
    public void executorRemoved(String executorId, String appId) {
        logger.info("Clean up non-shuffle and non-RDD files associated with the finished executor {}",
            executorId);
        AppExecId fullId = new AppExecId(appId, executorId);
        final ExecutorShuffleInfo executor = executors.get(fullId);
        if (executor == null) {
            // Executor not registered, skip clean up of the local directories.
            logger.info("Executor is not registered (appId={}, execId={})", appId, executorId);
        } else {
            logger.info("Cleaning up non-shuffle and non-RDD files in executor {}'s {} local dirs",
                fullId, executor.localDirs.length);

            // Execute the actual deletion in a different thread, as it may take some time.
            directoryCleaner.execute(() -> deleteNonShuffleServiceServedFiles(executor.localDirs));
        }
    }

    /**
     * Synchronously deletes each directory one at a time.
     * Should be executed in its own thread, as this may take a long time.
     */
    protected void deleteExecutorDirs(String[] dirs) {
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
     * Synchronously deletes files not served by shuffle service in each directory recursively.
     * Should be executed in its own thread, as this may take a long time.
     */
    protected void deleteNonShuffleServiceServedFiles(String[] dirs) {
        FilenameFilter filter = (dir, name) -> {
            // Don't delete shuffle data, shuffle index files or cached RDD files.
            return !name.endsWith(".index") && !name.endsWith(".data")
                && (!rddFetchEnabled || !name.startsWith("rdd_"));
        };

        for (String localDir : dirs) {
            try {
                JavaUtils.deleteRecursively(new File(localDir), filter);
                logger.debug("Successfully cleaned up files not served by shuffle service in directory: {}",
                    localDir);
            } catch (Exception e) {
                logger.error("Failed to delete files not served by shuffle service in directory: "
                    + localDir, e);
            }
        }
    }

    /**
     * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
     * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
     * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
     */
    protected ManagedBuffer getSortBasedShuffleBlockData(
        ExecutorShuffleInfo executor, int shuffleId, int mapId, int reduceId, int numBlocks) {
        File indexFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "shuffle_" + shuffleId + "_" + mapId + "_0.index");

        try {
            ShuffleIndexInformation shuffleIndexInformation = shuffleIndexCache.get(indexFile);
            ShuffleIndexRecord shuffleIndexRecord = shuffleIndexInformation.getIndex(reduceId, numBlocks);
            return new FileSegmentManagedBuffer(
                conf,
                getFile(executor.localDirs, executor.subDirsPerLocalDir,
                    "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
                shuffleIndexRecord.getOffset(),
                shuffleIndexRecord.getLength());
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to open file: " + indexFile, e);
        }
    }

    public ManagedBuffer getDiskPersistedRddBlockData(
        ExecutorShuffleInfo executor, int rddId, int splitIndex) {
        File file = getFile(executor.localDirs, executor.subDirsPerLocalDir,
            "rdd_" + rddId + "_" + splitIndex);
        long fileLength = file.length();
        ManagedBuffer res = null;
        if (file.exists()) {
            res = new FileSegmentManagedBuffer(conf, file, 0, fileLength);
        }
        return res;
    }

    public void close() {
        db.close();
    }

    public int removeBlocks(String appId, String execId, String[] blockIds) {
        ExecutorShuffleInfo executor = executors.get(new AppExecId(appId, execId));
        if (executor == null) {
            throw new RuntimeException(
                String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
        }
        int numRemovedBlocks = 0;
        for (String blockId : blockIds) {
            File file = getFile(executor.localDirs, executor.subDirsPerLocalDir, blockId);
            if (file.delete()) {
                numRemovedBlocks++;
            } else {
                logger.warn("Failed to delete block: " + file.getAbsolutePath());
            }
        }
        return numRemovedBlocks;
    }
}
