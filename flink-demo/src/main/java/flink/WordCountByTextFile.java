package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author xhzy
 */
public class WordCountByTextFile {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //2.加载数据源--从文本获取
        DataSource<String> dataSource = environment.readTextFile(
                "/Users/xhzy/test/flink-demo/src/main/resources/text.txt", "utf-8");

        //3.数据分组
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                String[] words = s.split(" ");
                for(String word: words){
                    if(word.length() > 0){
                        collector.collect(new Tuple2<>(word,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
