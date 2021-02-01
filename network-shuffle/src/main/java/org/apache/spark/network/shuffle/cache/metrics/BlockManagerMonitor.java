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

package org.apache.spark.network.shuffle.cache.metrics;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.network.shuffle.cache.ShuffleSegment;

public interface BlockManagerMonitor {
    BlockManagerMonitor DUMMY_MONITOR = new BlockManagerMonitor() {
        @Override
        public MetricRegistry getRegistry() {
            return null;
        }

        @Override
        public void applicationRemoved(String appId) {
        }

        @Override
        public void readRequest(String appId, long size) {
        }

        @Override
        public void readThroughRequest(String appId, long size) {
        }

        @Override
        public void readThroughFromSegment(String appId, long size) {
        }

        @Override
        public void readCachedFromSegment(String appId, long size) {
        }

        @Override
        public void segmentLoaded(ShuffleSegment segment) {
        }

        @Override
        public void segmentEvicted(ShuffleSegment segment) {
        }
    };

    MetricRegistry getRegistry();

    void applicationRemoved(String appId);

    void readRequest(String appId, long size);

    void readThroughRequest(String appId, long size);

    void readThroughFromSegment(String appId, long size);

    void readCachedFromSegment(String appId, long size);

    void segmentLoaded(ShuffleSegment segment);

    void segmentEvicted(ShuffleSegment segment);
}
