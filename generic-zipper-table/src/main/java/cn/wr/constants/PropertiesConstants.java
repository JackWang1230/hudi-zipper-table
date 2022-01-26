package cn.wr.constants;

/**
 * @author RWang
 * @Date 2021/11/22
 */

public class PropertiesConstants {

    public static final String PROPERTIES_FILE_NAME = "/application.properties";

    // kafka basic config infos
    public static final String KAFKA_ZIPPER_TABLE_SERVERS="kafka.zipper.table.servers";
    public static final String KAFKA_ZIPPER_TABLE_TOPIC="kafka.zipper.table.topic";
    public static final String KAFKA_ZIPPER_TABLE_GROUP="kafka.zipper.table.group";
    public static final String KAFKA_ZIPPER_TABLE_OFFSET="kafka.zipper.table.offset";

    // checkpoint
    public static final String STREAM_CHECKPOINT_ENABLE="stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL="stream.checkpoint.interval";
    public static final String STREAM_CHECKPOINT_PATH="stream.checkpoint.path";

    // parallelism
    public static final String STREAM_SOURCE_PARALLELISM="stream.source.parallelism";

    // the columns identified whether changed
    public static final String DATA_RECORD_KEY_COLUMN="data.record.key.column";

    // loading the data size at once
    public static final String DATA_PAGE_SIZE = "data.page.size";

    // mysql connection basic config infos for druid pools
    public static final String MYSQL_DATABASE_URL = "mysql.database.url";
    public static final String MYSQL_DATABASE_USER = "mysql.database.user";
    public static final String MYSQL_DATABASE_PASSWORD = "mysql.database.password";
    public static final String MYSQL_DATABASE_INITIAL_SIZE = "mysql.database.initialSize";
    public static final String MYSQL_DATABASE_MIN_IDLE= "mysql.database.minIdle";
    public static final String MYSQL_DATABASE_MAX_ACTIVE = "mysql.database.maxActive";
    public static final String MYSQL_DATABASE_MAX_WAIT = "mysql.database.maxWait";
    public static final String MYSQL_DATABASE_TIME_BETWEEN_EVICTABLE_IDLE = "mysql.database.timeBetweenEvictableIdle";
    public static final String MYSQL_DATABASE_MAX_EVICTABLE_IDLE = "mysql.database.maxEvictableIdle";
    public static final String MYSQL_DATABASE_MIN_EVICTABLE_IDLE = "mysql.database.minEvictableIdle";
    public static final String MYSQL_DATABASE_TEST_WHILE_IDLE = "mysql.database.testWhileIdle";
    public static final String MYSQL_DATABASE_TEST_ON_BORROW = "mysql.database.testOnBorrow";
    public static final String MYSQL_DATABASE_TEST_ON_RETURN = "mysql.database.testOnReturn";
    public static final String MYSQL_DATABASE_VALIDATION_QUERY = "mysql.database.validationQuery";

    // hudi basic config
    public static final String HUDI_BASIC_CONFIG_PATH="hudi.basic.config.path";
    public static final String HUDI_BASIC_CONFIG_URIS="hudi.basic.config.uris";

    //dingTalk basic config
    public static final String DINGTALK_WEBHOOK_URL="dingtalk.webhook.url";
    public static final String DINGTALK_USER_PHONE="dingtalk.user.phone";
    public static final String DINGTALK_USER_ID="dingtalk.user.id";

    //sql statement generic config
    public static final String BULK_INSERT_TABLE="bulk.insert.table";
    public static final String ACHIEVE_MAX_MIN_ID="achieve.max.min.id";
    public static final String DATA_DETAIL_BASED_ID="data.detail.based.id";
    public static final String INCR_UPSERT_TABLE="incr.upsert.tablee";
    public static final String SOURCE_DATA_2_HUDI="source.data.2.hudie";


}
