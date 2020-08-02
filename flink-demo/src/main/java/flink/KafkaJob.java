package flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author xhzy
 */
public class KafkaJob {

    public static void main(String[] args) throws Exception {
        //1.创建流执行环境和表环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //2.加载数据源
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test",
                new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        SingleOutputStreamOperator<String> source = environment.addSource(consumer)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) {
                        return s;
                    }
        });

        //3.注册内存表
        tableEnvironment.registerDataStream("test",source,"name");

        //4.执行sql
        String sql = "select name,count(1) from test group by name";
        Table table = tableEnvironment.sqlQuery(sql);

        //回退更新
        tableEnvironment.toRetractStream(table, Row.class).print();

        //5.提交执行
        environment.execute();

    }


}
