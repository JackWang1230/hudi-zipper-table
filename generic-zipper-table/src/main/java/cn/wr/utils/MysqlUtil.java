package cn.wr.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static cn.wr.constants.PropertiesConstants.*;


/**
 * @author RWang
 * @Date 2021/12/17
 */

public class MysqlUtil {

    private static final Logger logger = LoggerFactory.getLogger(MysqlUtil.class);

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";


    /**
     *  get mysql database connection
     * @param tool default config
     * @return Connection
     */
    public static Connection getConnection(ParameterTool tool){
        return MysqlUtil.getConnection(tool.get(MYSQL_DATABASE_URL),
                tool.get(MYSQL_DATABASE_USER),
                tool.get(MYSQL_DATABASE_PASSWORD));
    }


    /**
     * get connection based on  basic args
     * @param url url
     * @param user user
     * @param passWord passwd
     * @return Connection
     */
    public static Connection getConnection (String url, String user, String passWord){
        try {
            Class.forName(MYSQL_DRIVER);
            Connection connection = DriverManager.getConnection(url,user,passWord);
            return connection;
        } catch (Exception e){
            logger.error("Mysqlutil get connection error:{}",e);
        }
        return null;
    }

    /**
     * close connection
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
