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
        // basic flink env
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        DataStreamSource<PageStartEndOffset> mysqlIndexData = env.addSource(new MysqlIndexSource());

        TypeInformation[] types = new TypeInformation[9];
        types[0] =Types.STRING;
        types[1] =Types.STRING;
        types[2] =Types.STRING;
        types[3] =Types.STRING;
        types[4] =Types.INT;
        types[5] =Types.BIG_DEC;
        types[6] =Types.STRING;
        types[7] =Types.STRING;
        types[8] =Types.STRING;
//        for (int i = 0; i < 9; i++) {
//            types[i]= Types.STRING;
//        }
        String[] inputField={"sku_no","common_name","approval_number","internal_id","merchant_id","price","trade_code","startTime","endTime"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types,inputField);
        DataStream<Row> initialData = mysqlIndexData.flatMap(new InitialDataIndexFlatMapFunction()).returns(rowTypeInfo);
        ste.fromDataStream(initialData).printSchema();
        ste.createTemporaryView("dd",initialData);

        ste.executeSql("select * from dd").print();
//        ste.executeSql(parameterTool.get(BULK_INSERT_TABLE));
//        ste.executeSql(parameterTool.get(SOURCE_DATA_2_HUDI));
        env.execute("dddd");
//        Table result = ste.sqlQuery("select * from dd");
//        result.execute().print();
    }
}
