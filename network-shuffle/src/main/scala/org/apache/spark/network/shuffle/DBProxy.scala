package org.apache.spark.network.shuffle

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Maps
import org.apache.hadoop.fs.Path
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.{LevelDBProvider, ServiceConf}
import org.iq80.leveldb.DB

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentMap

case class DBProxy(conf: ServiceConf) extends Logging {
  val allPaths = new Path(conf.getSparkaeExecutorPath) ::
    new Path(conf.getSparkaeExecutorPath) :: Nil
  val dbs: Seq[DB] = allPaths
    .flatMap(initRecoveryDb(_, RECOVERY_FILE_NAME))
    .map(LevelDBProvider.initLevelDB(_, CURRENT_VERSION, mapper))
  private val RECOVERY_FILE_NAME = "registeredExecutors.ldb"
  private val mapper = new ObjectMapper()
  /**
   * This a common prefix to the key for each app registration we stick in leveldb, so they
   * are easy to find, since leveldb lets you search based on prefix.
   */
  private val APP_KEY_PREFIX = "AppExecShuffleInfo"
  private val CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0)

  def reloadAllExecutorsFromRecoveryFiles() = {
    val registeredExecutors = Maps.newConcurrentMap[AppExecId, ExecutorShuffleInfo]()
    dbs.map(reloadRegisteredExecutors(registeredExecutors, _))
    registeredExecutors
  }

  @throws[IOException]
  private def reloadRegisteredExecutors(
    registeredExecutors: ConcurrentMap[AppExecId, ExecutorShuffleInfo], db: DB): Unit = {
    if (db != null) {
      val itr = db.iterator()
      itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8))
      while (itr.hasNext) {
        val e = itr.next
        val key = new String(e.getKey, StandardCharsets.UTF_8)
        if (key.startsWith(APP_KEY_PREFIX)) {
          val id = parseDbAppExecKey(key)
          logInfo("Reloading registered executors: " + id.toString)
          val shuffleInfo = mapper.readValue(e.getValue, classOf[ExecutorShuffleInfo])
          registeredExecutors.put(id, shuffleInfo)
        }
      }
    }
  }

  @throws[IOException]
  private def parseDbAppExecKey(s: String) = {
    if (!s.startsWith(APP_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_KEY_PREFIX)
    }
    val json = s.substring(APP_KEY_PREFIX.length + 1)
    val parsed = mapper.readValue(json, classOf[AppExecId])
    parsed
  }

  def registerExecutorInDBs(fullId: AppExecId, executorInfo: ExecutorShuffleInfo): Unit = {
    dbs.map { db =>
      try {
        val key = dbAppExecKey(fullId)
        val value = mapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8)
        db.put(key, value)
      } catch {
        case e: Exception =>
          logError("Error saving registered executors", e)
      }
    }
  }

  def removeExecutorInDBs(fullId: AppExecId): Unit = {
    dbs.map { db =>
      try {
        db.delete(dbAppExecKey(fullId))
      } catch {
        case e: IOException =>
          logError(s"Error deleting $fullId from executor state db", e)
      }
    }
  }

  @throws[IOException]
  private def dbAppExecKey(appExecId: AppExecId) = {
    // we stick a common prefix on all the keys so we can find them in the DB
    val appExecJson = mapper.writeValueAsString(appExecId)
    val key = APP_KEY_PREFIX + ";" + appExecJson
    key.getBytes(StandardCharsets.UTF_8)
  }

  def close(): Unit = {
    dbs.foreach { db =>
      try {
        db.close()
      } catch {
        case e: IOException =>
          logError("Exception closing leveldb with registered executors", e)
      }
    }
  }

  /**
   * Figure out the recovery path and handle moving the DB if YARN NM recovery gets enabled
   * and DB exists in the local dir of NM by old version of shuffle service.
   */
  protected def initRecoveryDb(path: Path, dbName: String): Option[File] = {
    val recoveryFile = new File(path.toUri.getPath, dbName)
    if (recoveryFile.exists) {
      Some(recoveryFile)
    } else {
      logWarning(s"Executor store file $path/$dbName doesn't exists")
      None
    }
  }
}

