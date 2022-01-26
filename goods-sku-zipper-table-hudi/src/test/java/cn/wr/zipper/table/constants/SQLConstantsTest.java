package cn.wr.zipper.table.constants;

/**
 * @author RWang
 * @Date 2022/1/5
 */

public class SQLConstantsTest {


    /** bulk insert initial gc_source_sku snapshot data to hudi in COW */
    public static final String BULK_INSERT_GOODS_SKU_TABLE = "CREATE TABLE if not exists goods_zipper_hudi \n" +
            "(\n" +
            "    skuNo STRING\n" +
            "    ,commonName STRING\n" +
            "    ,approvalNumber STRING\n" +
            "    ,internalId STRING\n" +
            "    ,merchantId int\n" +
            "    ,price decimal(10,2)\n" +
            "    ,tradeCode STRING\n" +
            "    ,startTime STRING\n" +
            "    ,endTime STRING\n" +
            ") " +
            "WITH (\n" +
            "  'connector' = 'hudi',\n" +
//            "  'path' = 'hdfs://192.168.3.111:8020/hudi/flink/goods_zipper_hudi',\n" +
            " %s,\n" +
            "  'write.precombine.field' = 'skuNo',\n" +
            "  'write.operation' = 'bulk_insert',\n" +
            "  'hoodie.datasource.write.recordkey.field' = 'skuNo,startTime',\n" +
            "  'write.bucket_assign.tasks' = '14', "+
            "  'write.tasks' = '14',\n" +
            "  'read.streaming.enabled' = 'true', \n" +
            "  'read.streaming.start-commit' = '20211225000000', \n" +
            "  'table.type' = 'COPY_ON_WRITE',\n" +
            "  'hive_sync.enable' = 'true', \n" +
            "  'hive_sync.mode' = 'hms', \n" +
//            "  'hive_sync.table'='ods_goods_zipper_hudi',\n" +
            "  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',\n" +
            "  'hive_sync.db'='uniondrug_ods',\n" +
//            "  'hive_sync.metastore.uris' = 'thrift://192.168.3.116:9083' " +
            "  'hive_sync.metastore.uris' = 'thrift://172.16.152.240:9083' " +
            ")";
}
