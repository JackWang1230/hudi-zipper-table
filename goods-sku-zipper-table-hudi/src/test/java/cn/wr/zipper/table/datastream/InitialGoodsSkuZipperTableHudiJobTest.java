package cn.wr.zipper.table.datastream;

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
 * 初始化同步mysql库快照数据
 * @author RWang
 * @Date 2021/12/23
 */

public class InitialGoodsSkuZipperTableHudiJobTest {

    private static final Logger logger= LoggerFactory.getLogger(InitialGoodsSkuZipperTableHudiJobTest.class);

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "uniondrug");
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
        // mysql 数据库源
        DataStreamSource<PageStartEndOffset> lines = env.addSource(new MysqlIndexSource());
        DataStream<GoodsSkuInfo> goodsSku = lines.flatMap(new InitialGoodsSkuIndexFlatMapFunction());
        ste.createTemporaryView("goods_sku_table", goodsSku);
        ste.executeSql(SQLConstants.BULK_INSERT_GOODS_SKU_TABLE);
        ste.executeSql(SQLConstants.INSERT_DATA_2_GOODS_SKU_HUDI);
        env.execute("initial_bulk_insert_job");
    }
}
