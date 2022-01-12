package cn.uniodnrug.zipper.table.utils;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static cn.uniodnrug.zipper.table.constants.PropertiesConstants.*;

/**
 * this druid tools just used for test by now
 * @author RWang
 * @Date 2021/12/27
 */

@Deprecated
public class JDBCDruidUtils {

    private static final Logger logger = LoggerFactory.getLogger(JDBCDruidUtils.class);

    public static DruidDataSource druidDataSource = new DruidDataSource();
    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static ParameterTool parameterTool;
    private static Connection connection = null;

    public static void initConn(String url, String user, String passWord,int initialSize,int minIdle,int maxActive,
                                int maxWait,long timeBetweenEvictableIdle,long maxEvictableIdle,long minEvictableIdle,
                                boolean testWhileIdle,boolean testOnBorrow,boolean testOnReturn,String validationQuery)
    {
            // DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(MYSQL_DRIVER);
            druidDataSource.setUrl(url);
            druidDataSource.setUsername(user);
            druidDataSource.setPassword(passWord);
            druidDataSource.setInitialSize(initialSize);
            druidDataSource.setMinIdle(minIdle);
            druidDataSource.setMaxActive(maxActive);
            druidDataSource.setMaxWait(maxWait);
            druidDataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictableIdle);
            druidDataSource.setMaxEvictableIdleTimeMillis(maxEvictableIdle);
            druidDataSource.setMinEvictableIdleTimeMillis(minEvictableIdle);
            druidDataSource.setTestWhileIdle(testWhileIdle);
            druidDataSource.setTestOnBorrow(testOnBorrow);
            druidDataSource.setTestOnReturn(testOnReturn);
            druidDataSource.setValidationQuery(validationQuery);
//            druidDataSource.setUseUnfairLock(true);
    }

    public static Connection getConnection(String url, String user, String passWord,int initialSize,int minIdle,
                                           int maxActive, int maxWait,long timeBetweenEvictableIdle,long maxEvictableIdle,
                                           long minEvictableIdle, boolean testWhileIdle,boolean testOnBorrow,
                                           boolean testOnReturn,String validationQuery) throws SQLException {
        if (null != JDBCDruidUtils.connection) {
            try {
                connection.close();
            } catch (Exception e) {
                logger.error("Mysqlutil get connection error:{}", e);
            }
        }
        initConn(url,user,passWord,initialSize,minIdle,maxActive,maxWait,timeBetweenEvictableIdle,maxEvictableIdle,
                minEvictableIdle,testWhileIdle,testOnBorrow,testOnReturn,validationQuery);
        return druidDataSource.getConnection();
    }

    public static Connection getConnection(ParameterTool tool) throws SQLException {
        parameterTool =  tool;
        connection =
         JDBCDruidUtils.getConnection(parameterTool.get(MYSQL_DATABASE_URL),parameterTool.get(MYSQL_DATABASE_USER),
                parameterTool.get(MYSQL_DATABASE_PASSWORD),parameterTool.getInt(MYSQL_DATABASE_INITIAL_SIZE),
                parameterTool.getInt(MYSQL_DATABASE_MIN_IDLE),parameterTool.getInt(MYSQL_DATABASE_MAX_ACTIVE),
                parameterTool.getInt(MYSQL_DATABASE_MAX_WAIT),parameterTool.getLong(MYSQL_DATABASE_TIME_BETWEEN_EVICTABLE_IDLE),
                parameterTool.getLong(MYSQL_DATABASE_MAX_EVICTABLE_IDLE),parameterTool.getLong(MYSQL_DATABASE_MIN_EVICTABLE_IDLE),
                parameterTool.getBoolean(MYSQL_DATABASE_TEST_WHILE_IDLE),parameterTool.getBoolean(MYSQL_DATABASE_TEST_ON_BORROW),
                parameterTool.getBoolean(MYSQL_DATABASE_TEST_ON_RETURN),parameterTool.get(MYSQL_DATABASE_VALIDATION_QUERY));
        return connection;
    }


    /**
     * colse the connection
     * @param connection connection session
     * @param ps PreparedStatement
     */
    public static void close(Connection connection, PreparedStatement ps){

        if (null != connection){
            try {
                connection.close();
            } catch (SQLException e){
                logger.error("MysqlUtil close connection error:{}",e);
            }
        }
        if (null != ps){
            try {
                ps.close();
            } catch (SQLException e){
                logger.error("MysqlUtil close ps error:{}",e);
            }
        }
    }
}
