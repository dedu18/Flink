/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sensors;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * WordCount Demo
 */
public class WatermarkStreamingJob {

  // 定义任务启动名称，并行度
  private static final String jobName = "Flink Word Count Demo";

  private static final int watermarkInterval = 1000;

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 指定流计算的时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // 定义自动周期间隔插入 Watermark
    env.getConfig().setAutoWatermarkInterval(WatermarkStreamingJob.watermarkInterval);

    DataStream dataStream = env.fromElements(new ArrayList<String>());

    // 也可以在算子上指定周期 AssignerWithPeriodicWatermarks 或非周期 AssignerWithPunctuatedWatermarks 插入 Watermark 。
    dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks() {
      @Nullable
      @Override
      public Watermark getCurrentWatermark() {
        return null;
      }

      @Override
      public long extractTimestamp(Object element, long previousElementTimestamp) {
        return 0;
      }
    });


    env.execute(jobName);
  }

}