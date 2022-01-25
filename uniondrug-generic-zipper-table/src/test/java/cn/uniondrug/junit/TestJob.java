package cn.uniondrug.junit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author RWang
 * @Date 2022/1/24
 */

public class TestJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        Row row = new Row(2);
        row.setField(0,11);
        row.setField(1,22);
        DataStream<Row> rowDataStreamSource = env.fromElements(row);
        ste.fromDataStream(rowDataStreamSource).printSchema();
//        ste.createTemporaryView("dda",rowDataStreamSource);
//        ste.sqlQuery("select * from dda").execute().print();
    }
}
