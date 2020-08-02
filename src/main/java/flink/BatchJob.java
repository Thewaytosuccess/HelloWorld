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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		//1.获取执行环境和表环境
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);

		//2.加载数据集
		DataSet<PlayerScore> source = environment
				.readTextFile("/Users/xhzy/test/flink-demo/src/main/resources/score.csv")
				.map(new MapFunction<String, PlayerScore>() {
					@Override
					public PlayerScore map(String s) {
						String[] split = s.split(",");
						return new PlayerScore(split[0],split[1],Float.valueOf(split[2]));
					}
				});

		//3.注册内存表
		tableEnvironment.registerDataSet("test",source,"season,name,score");
		//tableEnvironment.registerTable("test",tableEnvironment.fromDataSet(source));

		//4.创建并执行sql
		String sql = "select name,count(season) as num from test group by name order by num desc ";
		tableEnvironment.toDataSet(tableEnvironment.sqlQuery(sql),RankResult.class).print();

		/*
		 * https://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */
	}

	public static class RankResult {

		private String name;

		private long num;

		public String getName() {
			return name;
		}

		public long getNum() {
			return num;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setNum(long num) {
			this.num = num;
		}

		@Override
		public String toString() {
			return "RankResult{" +
					"name='" + name + '\'' +
					", num=" + num +
					'}';
		}
	}

	public static class PlayerScore {

		private String season;

		private String name;

		private float score;

		public PlayerScore(){}

		public PlayerScore(String season, String name, Float score) {
			this.season = season;
			this.score = score;
			this.name = name;
		}

		public float getScore() {
			return score;
		}

		public String getName() {
			return name;
		}

		public String getSeason() {
			return season;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setScore(float score) {
			this.score = score;
		}

		public void setSeason(String season) {
			this.season = season;
		}

	}
}
