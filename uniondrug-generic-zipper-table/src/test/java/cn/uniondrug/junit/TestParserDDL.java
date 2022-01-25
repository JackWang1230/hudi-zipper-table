package cn.uniondrug.junit;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author RWang
 * @Date 2022/1/25
 */

public class TestParserDDL {



    @Test
    public void parserDDl(){
        String ddl = "CREATE TABLE if not exists goods_zipper_hudi\n" +
                "( sku_no STRING,\n" +
                "  common_name STRING,\n" +
                "  approval_number STRING,\n" +
                "  internal_id STRING,\n" +
                "  merchant_id int,\n" +
                "  price decimal(10,2),\n" +
                "  trade_code STRING,\n" +
                "  startTime STRING,\n" +
                "  endTime STRING\n" +
                ")\n" +
                "with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://192.168.3.111:8020/hudi/flink/goods_zipper_hudi',\n" +
                "  'write.precombine.field' = 'sku_no',\n" +
                "  'write.operation' = 'bulk_insert',\n" +
                "  'hoodie.datasource.write.recordkey.field' = 'sku_no,startTime',\n" +
                "  'write.bucket_assign.tasks' = '4',\n" +
                "  'write.tasks' = '4',\n" +
                "  'read.streaming.enabled' = 'true',\n" +
                "  'read.streaming.start-commit' = '20220105000000',\n" +
                "  'table.type' = 'COPY_ON_WRITE',\n" +
                "  'hive_sync.enable' = 'true',\n" +
                "  'hive_sync.mode' = 'hms',\n" +
                "  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',\n" +
                "  'hive_sync.db'='uniondrug_ods',\n" +
                "  'hive_sync.metastore.uris' = 'thrift://192.168.3.116:9083'\n" +
                ")";

        String regex=".*?(?=\\()";


        String ddd = ddl.replaceAll("\n"," ");
        String text = "北京市 ( 海淀区" +
                ")( 朝阳区)(西城区)";
        Pattern pattern = Pattern.compile("(?<=\\().*?(?=(WITH|with))" );
        Matcher matcher = pattern.matcher(ddd);
        String group=null;
        if (matcher.find()) {
            group = matcher.group();
        }
        // 获取到了ddl 的字段名称 和 字段类型
        String substring = group.trim().substring(0, group.trim().length() - 1);
        System.out.println(substring);

        // 对字段进行封装 存储 转换
        //todo 需要指定为 逗号+空格 形式
        String[] split = substring.split(", ");
        HashMap<String,String> hashMap = new HashMap();
        for (String s : split) {
            //System.out.println(s);
            String[] s1 = s.trim().split(" ");
            hashMap.put(s1[0],s1[1]);
        }
        System.out.println("ss");
    }


    }



