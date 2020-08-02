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

package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class SocketJob {

	public static void main(String[] args) throws Exception {

		//1.获取流执行环境
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		//2.监听socket端口 nc -l 9001
		DataStreamSource<String> data = environment.socketTextStream("localhost", 9001);

		//3.数据转化：map/flatMap/filter/reduce
		data.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
				String[] words = s.split("\\W+");
				for(String w:words){
					if(w.length() > 0){
						collector.collect(new Tuple2<>(w,1));
					}
				}
			}
		}).keyBy(0).sum(1).print().setParallelism(1);

		//4.执行
		environment.execute("socket test");

		/*
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

	}
}
