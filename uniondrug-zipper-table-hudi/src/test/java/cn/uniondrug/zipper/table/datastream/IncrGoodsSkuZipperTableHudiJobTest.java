package cn.uniondrug.zipper.table.datastream;

import cn.uniodnrug.zipper.table.constants.PropertiesConstants;
import cn.uniodnrug.zipper.table.constants.SQLConstants;
import cn.uniodnrug.zipper.table.flatmap.CanalTransModelFlatMap;
import cn.uniodnrug.zipper.table.flatmap.UpdateOrInsertFlatMap;
import cn.uniodnrug.zipper.table.model.CanalTransDataModel;
import cn.uniodnrug.zipper.table.model.GoodsSkuInfo;
import cn.uniodnrug.zipper.table.utils.ExecutionEnvUtil;
import cn.uniodnrug.zipper.table.utils.KafkaConfigUtil;
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
 * @Date 2021/12/23
 */

public class IncrGoodsSkuZipperTableHudiJobTest {

    private static final Logger logger= LoggerFactory.getLogger(IncrGoodsSkuZipperTableHudiJobTest.class);

    public static void main(String[] args) throws Exception {

        String proFilePath = ParameterTool.fromArgs(args).get("conf");
        final  ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(proFilePath);
        if (null == parameterTool){
            logger.error("parameterTool is null");
            return;
        }
        // 配置flink 执行环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        env.setParallelism(2);
        logger.info("loading the flink env finished");
        // kafka 接收数据
        DataStreamSource<String> data = KafkaConfigUtil.createZipperTableSource(env)
                .setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SOURCE_PARALLELISM, 1));
        DataStream<CanalTransDataModel> canalMode = data.flatMap(new CanalTransModelFlatMap());
        DataStream<GoodsSkuInfo> goodsSku = canalMode.flatMap(new UpdateOrInsertFlatMap());
        ste.createTemporaryView("goods_sku_table", goodsSku);
        ste.executeSql(SQLConstants.INCR_INSERT_GOODS_SKU_TABLE);
        // 写入hudi
        ste.executeSql(SQLConstants.INSERT_DATA_2_GOODS_SKU_HUDI);
        env.execute("increment_upsert_job");
    }
}
