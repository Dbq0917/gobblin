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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;


/**
 * Unit tests for {@link TrackerBasedWatermarkManager}.
 */
public class TrackerBasedWatermarkManagerTest {

  @Test
  public void testCommitterKeepsRunningAfterUnexpectedError() throws IOException {
    FineGrainedWatermarkTracker tracker = new FineGrainedWatermarkTracker(ConfigFactory.empty());
    tracker.setAutoStart(false);
    Map<String, CheckpointableWatermark> committed = new ConcurrentHashMap<>();
    AtomicInteger remainingErrors = new AtomicInteger(1);
    WatermarkStorage storage = new WatermarkStorage() {
      @Override
      public void commitWatermarks(Iterable<CheckpointableWatermark> watermarks) throws IOException {
        if (remainingErrors.getAndDecrement() > 0) {
          throw new AssertionError("simulated unexpected storage error");
        }
        for (CheckpointableWatermark watermark : watermarks) {
          committed.put(watermark.getSource(), watermark);
        }
      }

      @Override
      public Map<String, CheckpointableWatermark> getCommittedWatermarks(
          Class<? extends CheckpointableWatermark> watermarkClass, Iterable<String> sourcePartitions) {
        return committed;
      }
    };

    TrackerBasedWatermarkManager manager =
        new TrackerBasedWatermarkManager(storage, tracker, 1000, Optional.absent());
    try {
      AcknowledgableWatermark ackable =
          new AcknowledgableWatermark(new DefaultCheckpointableWatermark("default", new LongWatermark(1)));
      tracker.track(ackable);
      ackable.ack();

      manager._watermarkCommitter.run();
      Assert.assertTrue(committed.isEmpty());

      manager._watermarkCommitter.run();
      Assert.assertEquals(committed.size(), 1);
      Assert.assertEquals((long) ((LongWatermark) committed.get("default").getWatermark()).getValue(), 1L);
    } finally {
      manager.close();
      tracker.close();
    }
  }
}
