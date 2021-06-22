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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * WordCount Demo
 */
public class TableStreamingJob {

  // 定义任务启动名称，并行度
  private static final String jobName = "Flink Word Count Demo";

  private static final int watermarkInterval = 1000;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<List<String>> dataStream = env.fromElements(new ArrayList<>());

    // FLINK STREAMING QUERY
    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

    Table table = fsTableEnv.fromDataStream(dataStream);


    // FLINK BATCH QUERY
    ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
    BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

  }

}