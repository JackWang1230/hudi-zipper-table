import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * @author RWang
 * @Date 2021/11/30
 */

public class Data2Kafka {

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", "127.0.0.1:9092");
        //消息确认机制
        props.put("acks", "all");
        //重试机制
        props.put("retries", 0);
        //批量发送的大小
        props.put("batch.size", 16384);
        //消息延迟
        props.put("linger.ms", 1);
        //批量的缓冲区大小
        props.put("buffer.memory", 33554432);
        //kafka数据中key  value的序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // props.put("partitioner.class", "Job.MyPartition");

        //2 写入到kafka中    将"aa"  写入到kafka
        KafkaProducer kafkaProducer = new KafkaProducer(props);
        File file = new File("/Users/wangrui/Documents/uniondrug-zipper-table-hudi-flink/uniondrug-zipper-table-hudi/src/main/resources/data.txt");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String Line=null;
        while((Line=bufferedReader.readLine())!=""){
            if(!Line.equals("\t\t\t\t\t\t\t\t\t\t")){
                kafkaProducer.send(new ProducerRecord("canal_test11", Line));
                System.out.println(Line);
                Thread.sleep(10000);
            }
        }
        kafkaProducer.close();

    }
}
