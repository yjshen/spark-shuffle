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

package org.apache.spark.network.shuffle

import java.util.PriorityQueue
import scala.util.Try

/**
 * Various utility methods used by Spark.
 */
object ShutdownHookManager extends Logging {
  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * Adds a shutdown hook with default priority.
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * Adds a shutdown hook with the given priority. Hooks with higher priority values run
   * first.
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * Remove a previously installed shutdown hook.
   *
   * @param ref A handle returned by `addShutdownHook`.
   * @return Whether the hook was removed.
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

private[spark] class SparkShutdownHookManager {

  private val hooks = new PriorityQueue[SparkShutdownHook]()
  @volatile private var shuttingDown = false

  /**
   * Install a hook to run at shutdown and run all registered hooks in order.
   */
  def install(): Unit = {
    val hookTask = new Thread() {
      override def run(): Unit = runAll()
    }
    Runtime.getRuntime.addShutdownHook(hookTask)
  }

  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ( {
      nextHook = hooks.synchronized {
        hooks.poll()
      }; nextHook != null
    }) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  def add(priority: Int, hook: () => Unit): AnyRef = {
    hooks.synchronized {
      if (shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority, hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized {
      hooks.remove(ref)
    }
  }

}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  override def compareTo(other: SparkShutdownHook): Int = {
    other.priority - priority
  }

  def run(): Unit = hook()

}
