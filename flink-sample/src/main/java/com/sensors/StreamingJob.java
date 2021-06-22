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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * WordCount Demo
 */
public class StreamingJob {

  // 定义任务启动名称，并行度
  private static final String jobName = "Flink Word Count Demo";
  private static final int jobParallelism = 1;
  // 定义ip、端口、数据分隔符
  private static final String ip = "localhost";
  private static final int port = 9090;
  private static final String dataStreamDilimiter = "\n";
  // 定义时间窗口(秒)
  private static final int windowsSize = 3;

  /**
   * 数据流运行入口，输入数据使用nc -l port
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // 获取当前环境上下文
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> dataStreamSource = env.socketTextStream(ip, port, dataStreamDilimiter);

    // 进行 Transformation 算子转换操作
    DataStream<WordModel> dataStream = dataStreamSource
            .map(new WordCountFlatMapFunction())
            .keyBy("word")
            .timeWindow(Time.seconds(windowsSize))
            .sum("count");

    // 进行 Sink 输出操作，设置并行度并打印结果
    dataStream.print().setParallelism(jobParallelism);

    // 执行任务
    env.execute(jobName);
  }


  /**
   * WordCount计算算子
   */
  static class WordCountFlatMapFunction implements MapFunction<String, WordModel> {

    @Override
    public WordModel map(String word) throws Exception {
      return WordModel.of(word, 1);
    }
  }

  /**
   * WordVo
   */
  public static class WordModel {
    // 关键字
    public String word;
    // 出现次数
    public int count;

    public WordModel() {
    }

    public WordModel(String word, int count) {
      this.word = word;
      this.count = count;
    }

    @Override
    public String toString() {
      return "WordWithCount{" +
              "word='" + word + '\'' +
              ", count=" + count +
              '}';
    }

    public static WordModel of(String word, int count) {
      return new WordModel(word, count);
    }
  }

}