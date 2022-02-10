package cn.wr.datastream;

import cn.wr.constants.PropertiesConstants;
import cn.wr.flatmap.CanalTransModelFlatMap;
import cn.wr.model.CanalTransDataModel;
import cn.wr.utils.ExecutionEnvUtil;
import cn.wr.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
    }
}
