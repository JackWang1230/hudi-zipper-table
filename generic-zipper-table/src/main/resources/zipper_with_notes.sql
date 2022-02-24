/** goods_zipper_hudi 是hudi的表名称 具体名称需基于hudi命名规范创建，此处建表语句是用于初始化 */
CREATE TABLE if not exists goods_zipper_hudi
( sku_no STRING,
  common_name STRING,
  approval_number STRING,
  internal_id STRING,
  merchant_id int,
  price decimal(10,2),
  trade_code STRING,
  start_time STRING,
  end_time STRING
)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://192.168.3.111:8020/hudi/flink/goods_zipper_hudi',
  'write.precombine.field' = 'sku_no',
  'write.operation' = 'bulk_insert',
  'hoodie.datasource.write.recordkey.field' = 'sku_no,start_time',
  'write.bucket_assign.tasks' = '4',
  'write.tasks' = '4',
  'read.streaming.enabled' = 'true',
  'read.streaming.start-commit' = '20220105000000',
  'table.type' = 'COPY_ON_WRITE',
  'hive_sync.enable' = 'true',
  'hive_sync.mode' = 'hms',
  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',
  'hive_sync.db'='uniondrug_ods',
  'hive_sync.metastore.uris' = 'thrift://192.168.3.116:9083'
);


/** 此处为固定模版，更换时替换gc_source_sku_big_data 为数据库原表即可 */
SELECT MAX(id) as max_id,MIN(id) as min_id from gc_source_sku_big_data;


/** 1. 此处为固定模版，只需要替换gc_source_sku_big_data 为数据库原表 及 将剩余字段名称 替换成 源表中 需要被识别出的字段
 ** 2. cast(gmtUpdated as char) as start_time,'9999-12-31' as end_time 为固定语句，不可替换 */
SELECT sku_no,common_name,approval_number, internal_id,merchant_id,price,trade_code,cast(gmtUpdated as char) as start_time,'9999-12-31' as end_time  FROM gc_source_sku_big_data WHERE id BETWEEN ? AND ?;


/** 此处建表语句用于增量写入部分*/
CREATE TABLE if not exists goods_zipper_hudi
(
  sku_no STRING,
  common_name STRING,
  approval_number STRING,
  internal_id STRING,
  merchant_id int,
  price decimal(10,2),
  trade_code STRING,
  start_time STRING,
  end_time STRING
)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://192.168.3.111:8020/hudi/flink/goods_zipper_hudi',
  'write.precombine.field' = 'sku_no',
  'write.operation' = 'upsert',
  'hoodie.datasource.write.recordkey.field' = 'sku_no,start_time',
  'write.insert.cluster' = 'true',
  'hoodie.cleaner.commits.retained'='2',
  'index.bootstrap.enabled' = 'true',
  'read.streaming.enabled' = 'true',
  'read.streaming.start-commit' = '202201050000000',
  'table.type' = 'COPY_ON_WRITE',
  'hive_sync.enable' = 'true',
  'hive_sync.mode' = 'hms',
  'hive_sync.table'='ods_dc_gc_source_sku_zipper_cow',
  'hive_sync.db'='uniondrug_ods',
  'hive_sync.metastore.uris' = 'thrift://192.168.3.116:9083'
);



/** goods_sku_table为虚拟表，自己指定即可，goods_zipper_hudi为hudi表，具体基于hudi命名规范创建 */
INSERT INTO goods_zipper_hudi SELECT * FROM goods_sku_table