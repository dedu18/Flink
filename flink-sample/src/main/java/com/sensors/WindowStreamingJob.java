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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * WordCount Demo
 */
public class WindowStreamingJob {

  // 定义任务启动名称，并行度
  private static final String jobName = "Flink Word Count Demo";

  public static void main(String[] args) throws Exception {
    final String ip = "127.0.0.1";
    final int port = 8888;
    final String dataStreamDilimiter = "\n";
    // 读取Socket并获取DataStream
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, port, dataStreamDilimiter);
    // 进行窗口操作
    DataStream<Tuple2<String, Integer>> result = dataStreamSource
            .map(new RichMapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
              }
            })
            .keyBy(0)
            .sum(1);

    // 设置并行度并打印结果
    result.print();
    env.execute();
  }

}