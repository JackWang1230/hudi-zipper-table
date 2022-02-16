package cn.wr.datastream;

import cn.wr.enums.SqlTypeEnum;
import cn.wr.flatmap.InitialDataIndexFlatMapFunction;
import cn.wr.model.PageStartEndOffset;
import cn.wr.source.MysqlIndexSource;
import cn.wr.utils.ExecutionEnvUtil;
import cn.wr.utils.ParseDdlUtil;
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
        // basic flink env
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        DataStreamSource<PageStartEndOffset> mysqlIndexData = env.addSource(new MysqlIndexSource());

        RowTypeInfo rowTypeInfo = ParseDdlUtil.getRowTypeInfo(parameterTool);
        String tableName = ParseDdlUtil.getTableName(parameterTool,0);
        DataStream<Row> initialData = mysqlIndexData.flatMap(new InitialDataIndexFlatMapFunction()).returns(rowTypeInfo);
        ste.fromDataStream(initialData).printSchema();
        ste.createTemporaryView(tableName,initialData);
        // ste.executeSql("select * from "+ tableName).print();
//        ste.executeSql(parameterTool.get(SqlTypeEnum.getRealValue(0)));
        ste.executeSql(parameterTool.get(SqlTypeEnum.getRealValue(4)));
        env.execute("dddd");
    }
}
