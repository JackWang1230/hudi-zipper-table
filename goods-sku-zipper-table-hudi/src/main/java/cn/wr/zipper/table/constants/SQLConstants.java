package cn.wr.zipper.table.constants;

/**
 * @author RWang
 * @Date 2021/12/7
 */

public class SQLConstants {

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
            "  %s,\n" +
            "  'write.precombine.field' = 'skuNo',\n" +
            "  'write.operation' = 'bulk_insert',\n" +
            "  'hoodie.datasource.write.recordkey.field' = 'skuNo,startTime',\n" +
            "  'write.bucket_assign.tasks' = '4', "+
            "  'write.tasks' = '4',\n" +
            "  'read.streaming.enabled' = 'true', \n" +
            "  'read.streaming.start-commit' = '20220110000000', \n" +
            "  'table.type' = 'COPY_ON_WRITE',\n" +
            "  'hive_sync.enable' = 'true', \n" +
            "  'hive_sync.mode' = 'hms', \n" +
            "  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',\n" +
            "  'hive_sync.db'='uniondrug_ods',\n" +
            "  %s " +
            ")";

    /** incremental update gc_source_sku data used by flink-sql into apache hudi */
    public static final String  INCR_INSERT_GOODS_SKU_TABLE = "CREATE TABLE goods_zipper_hudi \n" +
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
            "  %s,\n" +
            "  'write.precombine.field' = 'skuNo',\n" +
            "  'write.operation' = 'upsert',\n" +
            "  'hoodie.datasource.write.recordkey.field' = 'skuNo,startTime',\n" +
            "  'hoodie.cleaner.commits.retained'='2',\n" +
            "  'write.insert.cluster' = 'true',\n"+
            "  'index.bootstrap.enabled' = 'true', \n" +
            "  'read.streaming.enabled' = 'true', \n" +
            "  'read.streaming.start-commit' = '20220110000000', \n" +
            "  'table.type' = 'COPY_ON_WRITE',\n" +
            "  'hive_sync.enable' = 'true', \n" +
            "  'hive_sync.mode' = 'hms', \n" +
            "  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',\n" +
            "  'hive_sync.db'='uniondrug_ods',\n" +
            "  %s " +
            ")";


    /** channel from flink temp table to hudi table by using insert */
    public static final String  INSERT_DATA_2_GOODS_SKU_HUDI="insert into goods_zipper_hudi " +
            "select skuNo,commonName,approvalNumber,internalId,merchantId," +
            "price,tradeCode,startTime,endTime from goods_sku_table";

    /**   get the max,min index id from mysql table gc_source_sku */
    public static final String GOODS_MAX_MIN_VALUE = "SELECT MAX(id) as max_id,MIN(id) as min_id from gc_source_sku ";

    /**   get the details data from gc_source_sku table through index id */
    public static final String GOODS_DETAIL_INDEX_SQL =" select sku_no,common_name,approval_number," +
            " internal_id,merchant_id,price,trade_code,cast(gmtUpdated as char) as gmtCreated,'9999-12-31' as gmtUpdated " +
            "from gc_source_sku where id between ? and ? ";



    /**    unused ,just for test
     * ahcieve the cnt about gc_source_sku */
    public static final String  GOODS_CNT_SQL =" select count(1) as cnt from gc_source_sku ";

    /**   unused ,just foe test
     * achieve detail info about gc_source_sku */
    public static final String  GOODS_DETAIL_SQL =" select sku_no,common_name,approval_number," +
            " internal_id,merchant_id,price,trade_code,cast(gmtUpdated as char) as start_time,'9999-12-31' as end_time " +
            "from gc_source_sku order by id limit ?,? ";



}
