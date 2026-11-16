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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.ComparableWatermark;
import org.apache.gobblin.source.extractor.Watermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;


/**
 * Unit tests for {@link StateStoreBasedWatermarkStorage}.
 */
public class StateStoreBasedWatermarkStorageTest {

  private static final String JOB_NAME = "testWatermarkJob";
  private static final String SOURCE = "testSource";
  private static final String STORE_ROOT_DIR = "watermark-storage-test";

  private StateStoreBasedWatermarkStorage storage;

  @BeforeClass
  public void setUp() throws IOException {
    FileSystem.getLocal(new Configuration(false)).delete(new Path(STORE_ROOT_DIR), true);

    State state = new State();
    state.setProp(ConfigurationKeys.JOB_NAME_KEY, JOB_NAME);
    state.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "fs");
    state.setProp(
        StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        STORE_ROOT_DIR);
    this.storage = new StateStoreBasedWatermarkStorage(state);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem.getLocal(new Configuration(false)).delete(new Path(STORE_ROOT_DIR), true);
  }

  @Test
  public void testRepeatedCommits() throws IOException {
    this.storage.commitWatermarks(Lists.newArrayList(new TestCheckpointableWatermark(SOURCE, 5)));
    Assert.assertEquals(getCommittedOffset(), 5);

    this.storage.commitWatermarks(Lists.newArrayList(new TestCheckpointableWatermark(SOURCE, 10)));
    Assert.assertEquals(getCommittedOffset(), 10);
  }

  @Test(dependsOnMethods = {"testRepeatedCommits"})
  public void testCommitIsIdempotent() throws IOException {
    this.storage.commitWatermarks(Lists.newArrayList(new TestCheckpointableWatermark(SOURCE, 10)));
    Assert.assertEquals(getCommittedOffset(), 10);
  }

  @Test(dependsOnMethods = {"testCommitIsIdempotent"})
  public void testCommitNeverRegresses() throws IOException {
    this.storage.commitWatermarks(Lists.newArrayList(new TestCheckpointableWatermark(SOURCE, 3)));
    Assert.assertEquals(getCommittedOffset(), 10);

    this.storage.commitWatermarks(Lists.newArrayList(new TestCheckpointableWatermark(SOURCE, 15)));
    Assert.assertEquals(getCommittedOffset(), 15);
  }

  private long getCommittedOffset() throws IOException {
    Map<String, CheckpointableWatermark> committed =
        this.storage.getCommittedWatermarks(TestCheckpointableWatermark.class, Lists.newArrayList(SOURCE));
    Assert.assertTrue(committed.containsKey(SOURCE));
    return ((TestCheckpointableWatermark) committed.get(SOURCE)).offset;
  }

  private static class TestCheckpointableWatermark implements CheckpointableWatermark {
    private String source;
    private long offset;

    TestCheckpointableWatermark(String source, long offset) {
      this.source = source;
      this.offset = offset;
    }

    @Override
    public String getSource() {
      return this.source;
    }

    @Override
    public ComparableWatermark getWatermark() {
      return new LongWatermark(this.offset);
    }

    @Override
    public int compareTo(CheckpointableWatermark other) {
      return Long.compare(this.offset, ((TestCheckpointableWatermark) other).offset);
    }

    @Override
    public JsonElement toJson() {
      return getWatermark().toJson();
    }

    @Override
    public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
      return 0;
    }
  }
}
