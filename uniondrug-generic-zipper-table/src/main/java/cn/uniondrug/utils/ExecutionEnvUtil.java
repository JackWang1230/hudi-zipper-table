package cn.uniondrug.utils;

import cn.uniondrug.constants.PropertiesConstants;
import cn.uniondrug.enums.SqlType;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.*;
import java.util.Map;
import java.util.Properties;

/**
 * Reference from blog：http://www.54tianzhisheng.cn/
 * @author RWang
 * @Date 2021/11/22
 */

public class ExecutionEnvUtil {

    /**
     * @param args args from external config file
     * @return ParameterTool
     * @throws Exception Exception
     */
    public static ParameterTool createParameterTool(final String[] args) throws Exception{

        Map<String, String> map = ParameterTool.fromArgs(args).toMap();
        Properties props = new Properties();
        for (Map.Entry<String, String> stringStringEntry : map.entrySet()) {
            String key = stringStringEntry.getKey();
            if (key.equals("file") | key.equals("f") | key.equals("sqlFile")){
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(map.get(key))
                        , "utf-8"));
                StringBuilder stringBuilder = new StringBuilder();
                String line="";
                while ((line=bufferedReader.readLine() )!= null){
                    stringBuilder.append(line);
                }
                String s2 = stringBuilder.toString();
                String[] sqlList = s2.split(";");
                for (int i = 0; i < sqlList.length; i++) {
                    props.put(SqlType.getValue1(i),sqlList[i]);
                }
            }else {
                BufferedReader buff = new BufferedReader(new InputStreamReader(new FileInputStream(map.get(key))
                        , "utf-8"));
                props.load(buff);
            }
        }
       return ParameterTool.fromMap((Map) props);
    }

    /**
     *
     * @param proFilePath specific config path
     * @return ParameterTool
     * @throws Exception Exception
     */
    public static ParameterTool createParameterTool(final String proFilePath) throws Exception{
        if (StringUtils.isBlank(proFilePath)) {
            return createParameterTool();
        }
        // Prevent parsing Chinese garbled characters
        Properties props = new Properties();
        InputStream inputStream = new FileInputStream(proFilePath);
        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
        props.load(bf);
        return ParameterTool.fromMap((Map) props);
//        ParameterTool parameterTool = ParameterTool.fromMap((Map) props);
//        return ParameterTool
//                .fromPropertiesFile(proFilePath)
//                .mergeWith(ParameterTool.fromSystemProperties());
    }



    /**
     * no args
     * @return ParameterTool
     */
    public static ParameterTool createParameterTool(){
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        }catch (IOException e){
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    /**
     *
     * @param parameterTool ParameterTool
     * @return StreamExecutionEnvironment
     * @throws Exception Exception
     */
    public static StreamExecutionEnvironment getEnv(ParameterTool parameterTool) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // config restart strategy
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,60000));
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE,true)){
             env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,6000)
                     , CheckpointingMode.EXACTLY_ONCE);
        }
//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(PropertiesConstants.STREAM_CHECKPOINT_PATH));
//        rocksDBStateBackend.isIncrementalCheckpointsEnabled();
//        env.setStateBackend(rocksDBStateBackend);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        embeddedRocksDBStateBackend.isIncrementalCheckpointsEnabled();
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage(parameterTool.get(PropertiesConstants.STREAM_CHECKPOINT_PATH));
        env.getConfig().setGlobalJobParameters(parameterTool);
        return env;
    }
}
