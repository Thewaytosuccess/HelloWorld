package flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @author xhzy
 */
public class WikipediaAnalysisJob {

    public static void main(String[] args) throws Exception {
         //1.获取流执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.加载数据源
        DataStreamSource<WikipediaEditEvent> data = environment.addSource(new WikipediaEditsSource());

        //3.根据key分组,制定聚合方式
        data.keyBy(e -> e.getUser())
        .timeWindow(Time.seconds(5L))
        .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> t, WikipediaEditEvent e) {
                t.f0 = e.getUser();
                t.f1 += e.getByteDiff();
                return t;
            }
        }).print();

        environment.execute();
    }
}
