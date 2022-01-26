package cn.wr.zipper.table.datastream;

import cn.wr.zipper.table.constants.PropertiesConstants;
import cn.wr.zipper.table.constants.SQLConstants;
import cn.wr.zipper.table.flatmap.CanalTransModelFlatMap;
import cn.wr.zipper.table.flatmap.UpdateOrInsertFlatMap;
import cn.uniodnrug.zipper.table.model.*;
import cn.wr.zipper.table.utils.ExecutionEnvUtil;
import cn.wr.zipper.table.utils.KafkaConfigUtil;
import cn.wr.zipper.table.model.CanalTransDataModel;
import cn.wr.zipper.table.model.DingAlarmModel;
import cn.wr.zipper.table.model.ErrorMsg;
import cn.wr.zipper.table.model.GoodsSkuInfo;
import cn.wr.zipper.table.utils.DingTalkAlarmsUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;

/**
 * @author RWang
 * @Date 2021/12/23
 */

public class IncrGoodsSkuZipperTableHudiJob {

    private static final Logger logger= LoggerFactory.getLogger(IncrGoodsSkuZipperTableHudiJob.class);

    public static void main(String[] args) throws Exception {

        String proFilePath = ParameterTool.fromArgs(args).get("conf");
        final  ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(proFilePath);
        if (null == parameterTool){
            logger.error("parameterTool is null");
            return;
        }
        try {
            // basic flink env
            StreamExecutionEnvironment env = ExecutionEnvUtil.getEnv(parameterTool);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
            StreamTableEnvironment ste = StreamTableEnvironment.create(env, settings);
            env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SOURCE_PARALLELISM,1));
            logger.info("loading the flink env finished");
            // kafka source
            DataStreamSource<String> data = KafkaConfigUtil.createZipperTableSource(env)
                    .setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SOURCE_PARALLELISM, 1));
            // business logic
            DataStream<CanalTransDataModel> canalMode = data.flatMap(new CanalTransModelFlatMap());
            DataStream<GoodsSkuInfo> goodsSku = canalMode.flatMap(new UpdateOrInsertFlatMap());
            // data into hudi
            ste.createTemporaryView("goods_sku_table", goodsSku);
            ste.executeSql(String.format(SQLConstants.INCR_INSERT_GOODS_SKU_TABLE,
                    parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_PATH),
                    parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_URIS)));
            ste.executeSql(SQLConstants.INSERT_DATA_2_GOODS_SKU_HUDI);

            env.execute("increment_upsert_job");
        } catch (Exception e){
            // dingTalk alarms
            DingAlarmModel dingAlarmModel = new DingAlarmModel();
            ErrorMsg errorMsg = new ErrorMsg();
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            errorMsg.setContent("flink zipper table hudi alarms:\n"+stringWriter.toString()+"\n"
                    +new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
            dingAlarmModel.setText(errorMsg);
            DingTalkAlarmsUtil.sendMsg(DingTalkAlarmsUtil.getHttpPost(parameterTool, dingAlarmModel));

        }
    }
}
