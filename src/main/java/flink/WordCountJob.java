package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author xhzy
 */
public class WordCountJob {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //2.加载数据流
        DataSet<String> data = environment.fromElements("hello world hello flink");

        //3.map/reduce/filter/flatMap操作
        DataSet<Tuple2<String, Integer>> result = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                String[] words = s.toLowerCase().split("\\W+");
                for (String w : words) {
                    if (w.length() > 0) {
                        collector.collect(new Tuple2<>(w, 1));
                    }
                }
            }
        })
        .groupBy(0)
        .sum(1);
        //4.print/export to kafka/redis/db
        result.print();
    }
}
