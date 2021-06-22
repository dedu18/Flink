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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.ArrayList;
import java.util.List;

/**
 * WordCount Demo
 */
public class SourceStreamingJob {

  // 定义任务启动名称，并行度
  private static final String jobName = "Flink Word Count Demo";

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    List<String> data = new ArrayList<>();
    data.add("test1");
    data.add("test2");
    data.add("test3");
    DataStream<List<String>> dataStream = env.fromElements(data);
    dataStream.print();
    env.execute(jobName);



    env.addSource(new SourceFunction<Object>() {
      @Override
      public void run(SourceContext<Object> sourceContext) throws Exception {

      }

      @Override
      public void cancel() {

      }
    });

    env.addSource(new RichParallelSourceFunction<Object>() {
      @Override
      public void run(SourceContext<Object> ctx) throws Exception {

      }

      @Override
      public void cancel() {

      }
    });

    env.addSource(new ParallelSourceFunction<Object>() {
      @Override
      public void run(SourceContext<Object> ctx) throws Exception {

      }

      @Override
      public void cancel() {

      }
    });

    env.fromElements(new ArrayList<String>());
  }

}