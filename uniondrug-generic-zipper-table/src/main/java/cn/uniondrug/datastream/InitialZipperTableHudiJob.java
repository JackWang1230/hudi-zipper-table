package cn.uniondrug.datastream;

import cn.uniondrug.flatmap.InitialDataIndexFlatMapFunction;
import cn.uniondrug.model.PageStartEndOffset;
import cn.uniondrug.source.MysqlIndexSource;
import cn.uniondrug.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author RWang
 * @Date 2022/1/20
 */

public class InitialZipperTableHudiJob {

    private static final Logger logger = LoggerFactory.getLogger(InitialZipperTableHudiJob.class);

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "uniondrug");
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
//        System.out.println("11");
        // basic flink env
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        DataStreamSource<PageStartEndOffset> mysqlIndexData = env.addSource(new MysqlIndexSource());

        TypeInformation[] types = new TypeInformation[9];
        for (int i = 0; i < 9; i++) {
            types[i]= Types.STRING;
        }
        String[] inputField={"a","b","c","d","e","f","g","h","i"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types);
        DataStream<Row> initialData = mysqlIndexData.flatMap(new InitialDataIndexFlatMapFunction()).returns(rowTypeInfo);
        ste.fromDataStream(initialData).printSchema();
        ste.createTemporaryView("dd",initialData);

        ste.executeSql("select * from dd").print();
        // Table table1 = ste.sqlQuery("select * from " + table);
//        table1.execute().print();
        //DataStream<Row> rowDataStream = ste.toAppendStream(table1, Row.class);
//
//        Expression[] eps = new Expression[inputField.length];
//
//        for (int i = 0; i < inputField.length; i++) {
//            eps[i] = $(inputField[i]);
//        }
//        ste.fromDataStream(initialData, eps).printSchema();
        //ste.sqlQuery("select * from " + table2).execute().print();
        //ste.createTemporaryView("goods_sku_table",table);
//        ste.createTemporaryView("goods_sku_table",initialData,"s1,s2,s3,s4,s5,s6,s7,s8,s9");
//        ste.executeSql("select * from "+ table).print();
//        ste.executeSql(parameterTool.get(BULK_INSERT_TABLE));
//        ste.executeSql(parameterTool.get(SOURCE_DATA_2_HUDI));
        env.execute("dddd");
//        Table result = ste.sqlQuery("select * from dd");
//        result.execute().print();
    }
}
