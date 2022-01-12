package cn.uniodnrug.zipper.table.utils;

import cn.uniodnrug.zipper.table.model.CanalTransDataModel;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import cn.uniodnrug.zipper.table.constants.PropertiesConstants;
import org.apache.kafka.common.serialization.StringDeserializer;

import static cn.uniodnrug.zipper.table.constants.PropertiesConstants.KAFKA_ZIPPER_TABLE_OFFSET;

/**
 * @author RWang
 * @Date 2021/11/22
 */

public class KafkaConfigUtil {

    /**
     * get kafka basic config infos
     *
     * @param params config params
     * @return props
     */
    private static Properties createKafkaProps(ParameterTool params) {

        Properties props = params.getProperties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.get(PropertiesConstants.KAFKA_ZIPPER_TABLE_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, params.get(PropertiesConstants.KAFKA_ZIPPER_TABLE_GROUP));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    /**
     *  add kafka consumer data into flink source
     * @param env flink environment
     * @return DataStream
     */
    public static DataStreamSource<String> createZipperTableSource(StreamExecutionEnvironment env) {

        ParameterTool parameters = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameters.getRequired(PropertiesConstants.KAFKA_ZIPPER_TABLE_TOPIC);
        Properties props = createKafkaProps(parameters);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        if (parameters.get(KAFKA_ZIPPER_TABLE_OFFSET).equals("earliest")){
            consumer.setStartFromEarliest();
        }
        return env.addSource(consumer);
    }

    @Deprecated
    public static DataStreamSource<String> createZipperTableSourceTest(StreamExecutionEnvironment env,ParameterTool parameters) {
        String topic = parameters.getRequired(PropertiesConstants.KAFKA_ZIPPER_TABLE_TOPIC);
        Properties props = createKafkaProps(parameters);
        ObjectMapper objectMapper = new ObjectMapper();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            private static final long serialVersionUID = -8685857564669917671L;

            @SneakyThrows
            @Override
            public long extractAscendingTimestamp(String s) {
                CanalTransDataModel canalTransDataModel = objectMapper.readValue(s, CanalTransDataModel.class);
                return canalTransDataModel.getTs();
            }
        });
        consumer.setStartFromEarliest();
        return env.addSource(consumer);
    }
}
