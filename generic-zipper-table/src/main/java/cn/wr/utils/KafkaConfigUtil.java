package cn.wr.utils;

import cn.wr.constants.PropertiesConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static cn.wr.constants.PropertiesConstants.KAFKA_ZIPPER_TABLE_OFFSET;

/**
 * @author RWang
 * @Date 2022/2/10
 */

public class KafkaConfigUtil {

    private static final String G1="_g1";

    /**
     * get kafka basic config infos
     *
     * @param params config params
     * @return props
     */
    private static Properties createKafkaProps(ParameterTool params,String groupId) {

        Properties props = params.getProperties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.get(PropertiesConstants.KAFKA_ZIPPER_TABLE_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     *  add kafka consumer data into flink source
     * @param env flink environment
     * @return DataStream
     */
    public static DataStreamSource<String> createZipperTableSource(StreamExecutionEnvironment env) {

        ParameterTool parameters = (ParameterTool) env.getConfig().getGlobalJobParameters();
        // String topic = parameters.getRequired(PropertiesConstants.KAFKA_ZIPPER_TABLE_TOPIC);
        String topic = ParseDdlUtil.getTableName(parameters, 1);
        String groupId = topic+G1;
        Properties props = createKafkaProps(parameters,groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        if (parameters.get(KAFKA_ZIPPER_TABLE_OFFSET).equals("earliest")){
            consumer.setStartFromEarliest();
        }
        return env.addSource(consumer);
    }
}
