package flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xhzy
 */
public class WordCountBySQL {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境和表环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);

        //2.获取数据源
        String data = "hello world hello flink";
        List<Info> list = new ArrayList<>();
        String[] strs = data.split(" ");
        for(String s : strs){
            list.add(new Info(s,1L));
        }

        DataSource<Info> dataSource = env.fromCollection(list);

        //3.将数据源注册成内存表
        tableEnvironment.registerDataSet("test",dataSource,"word,frequency");

        //4.执行sql并重新转化为dataset打印
        //String sql = "select word,sum(frequency) as frequency from test group by word";
        //String sql = "select upper(word) as word,sum(frequency) as frequency from test group by word";
        String sql = "select word,sum(frequency) as frequency from test where word = 'hello' group by word";
        tableEnvironment.toDataSet(tableEnvironment.sqlQuery(sql), Info.class).print();

    }

    public static class Info{

        private String word;

        private long frequency;

        public Info(){

        }

        public Info(String word,long frequency){
            this.word = word;
            this.frequency = frequency;
        }

        public void setFrequency(long frequency) {
            this.frequency = frequency;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getFrequency() {
            return frequency;
        }

        public String getWord() {
            return word;
        }

        @Override
        public String toString() {
            return "Info{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
