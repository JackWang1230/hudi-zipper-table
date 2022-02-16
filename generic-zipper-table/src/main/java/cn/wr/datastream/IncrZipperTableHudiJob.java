package cn.wr.datastream;

import cn.wr.constants.PropertiesConstants;
import cn.wr.flatmap.CanalTransModelFlatMap;
import cn.wr.flatmap.UpdateOrInsertDataFlatMap;
import cn.wr.model.CanalTransDataModel;
import cn.wr.utils.ExecutionEnvUtil;
import cn.wr.utils.KafkaConfigUtil;
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

public class IncrZipperTableHudiJob {

    private static final Logger logger = LoggerFactory.getLogger(IncrZipperTableHudiJob.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        // basic flink env
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SOURCE_PARALLELISM,1));
        DataStreamSource<String> data = KafkaConfigUtil.createZipperTableSource(env)
                .setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SOURCE_PARALLELISM, 1));
        DataStream<CanalTransDataModel> canalMode = data.flatMap(new CanalTransModelFlatMap());
        RowTypeInfo rowTypeInfo = ParseDdlUtil.getRowTypeInfo(parameterTool);
        String tableName = ParseDdlUtil.getTableName(parameterTool,0);
        DataStream<Row> goodsSku = canalMode.flatMap(new UpdateOrInsertDataFlatMap()).returns(rowTypeInfo);
        ste.createTemporaryView(tableName, goodsSku);
//        ste.executeSql(parameterTool.get(SqlTypeEnum.getRealValue(3)));
//        ste.executeSql(parameterTool.get(SqlTypeEnum.getRealValue(4)));
        env.execute("increment_upsert_job");
    }
}
