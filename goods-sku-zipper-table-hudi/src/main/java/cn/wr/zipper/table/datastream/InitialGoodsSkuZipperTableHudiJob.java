package cn.wr.zipper.table.datastream;

import cn.wr.zipper.table.constants.PropertiesConstants;
import cn.wr.zipper.table.constants.SQLConstants;
import cn.wr.zipper.table.flatmap.InitialGoodsSkuIndexFlatMapFunction;
import cn.wr.zipper.table.model.GoodsSkuInfo;
import cn.wr.zipper.table.model.PageStartEndOffset;
import cn.wr.zipper.table.source.MysqlIndexSource;
import cn.wr.zipper.table.utils.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * initial sync mysql snapshot data
 * @author RWang
 * @Date 2021/12/23
 */

public class InitialGoodsSkuZipperTableHudiJob {

    private static final Logger logger = LoggerFactory.getLogger(InitialGoodsSkuZipperTableHudiJob.class);

    public static void main(String[] args) throws Exception {

        String proFilePath = ParameterTool.fromArgs(args).get("conf");
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(proFilePath);
        if (null == parameterTool) {
            logger.error("parameterTool is null");
            return;
        }
        // basic flink env
        StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
        // source
        DataStreamSource<PageStartEndOffset> lines = env.addSource(new MysqlIndexSource());
        // business logic
        DataStream<GoodsSkuInfo> goodsSku = lines.flatMap(new InitialGoodsSkuIndexFlatMapFunction());
        // data into hudi
        ste.createTemporaryView("goods_sku_table", goodsSku);
        ste.executeSql(String.format(SQLConstants.BULK_INSERT_GOODS_SKU_TABLE,
                parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_PATH),
                parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_URIS)));
        ste.executeSql(SQLConstants.INSERT_DATA_2_GOODS_SKU_HUDI);

        env.execute("initial_bulk_insert_job");
    }
}
