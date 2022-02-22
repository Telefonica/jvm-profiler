/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.profiling.reporters;

import com.uber.profiling.profilers.CpuAndMemoryProfiler;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSReporterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test() throws IOException {

    HDFSOutputReporter reporter = new HDFSOutputReporter();

    Map<String, List<String>> arguments = new HashMap<>();
    arguments.put("output", Arrays.asList(String.format("file:///%s/reporter", folder.getRoot())));
    arguments.put("mode", Arrays.asList("overwrite"));

    reporter.updateArguments(arguments);

    Map<String, Object> map = new HashMap<>();

    map.put("epochMillis", 1603833802000L);
    map.put("name", "process1");
    map.put("host", "host1");
    map.put("processUuid", "uuid1");
    map.put("appId", "app1");
    map.put("tag", null);
    map.put("role", "role1");
    map.put("processCpuLoad", 0.2);
    map.put("systemCpuLoad", 0.3);
    map.put("processCpuTime", 1001L);
    map.put("heapMemoryTotalUsed", 2002L);
    map.put("heapMemoryCommitted", 3003L);
    map.put("heapMemoryMax", 4004L);
    map.put("nonHeapMemoryCommitted", 5005L);
    map.put("nonHeapMemoryTotalUsed", 6006L);
    map.put("nonHeapMemoryMax", 7007L);
    map.put("vmRSS", 8001L);
    map.put("vmHWM", 8002L);
    map.put("vmSize", 8003L);
    map.put("vmPeak", 8004L);


    for (int i = 0; i < 100; i++) {
      reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);
      reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);
      reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);
      map.put("epochMillis", 1603833802001L);
      reporter.report(CpuAndMemoryProfiler.PROFILER_NAME, map);
    }

    reporter.close();

    Assert.assertEquals( folder.getRoot().listFiles().length, 1);
  }

}
