package com.kwai

import org.apache.spark.network.shuffle.{Logging, ShutdownHookManager, Utils}
import org.apache.spark.network.util.ServiceConf

import java.util.concurrent.CountDownLatch

object SparkShuffleService extends Logging {

  val CORE_CONF_NAME = "core.yml"
  private val barrier = new CountDownLatch(1)
  @volatile
  private var server: ShuffleServer = _

  /** A helper main method that allows the caller to call this with a custom shuffle service. */
  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log)

    val is = Utils.getSparkClassLoader.getResourceAsStream(CORE_CONF_NAME)
    val serviceConf = ServiceConf.parseConfFile(is)

    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    server = new ShuffleServer()

    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown()
    }

    server.start(serviceConf)

    // keep running until the process is terminated
    barrier.await()
  }

}
