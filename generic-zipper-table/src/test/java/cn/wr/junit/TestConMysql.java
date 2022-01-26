package cn.wr.junit;

import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author RWang
 * @Date 2022/1/25
 */

public class TestConMysql {


    @Test
    public void connectMysql() throws SQLException {

        Connection connection = null;
        PreparedStatement ps = null;
        try {

            String url="jdbc:mysql://udtest.uniondrug.com:6033/cn_uniondrug_middleend_goodscenter?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2b8";
            String user="develop";
            String password="develop123";
            // Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
            ps = connection.prepareStatement("select * from gc_source_sku_big_data limit 10");
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            HashMap<Object, Object> map = new HashMap<>();
            ArrayList<Object> objects = new ArrayList<>();
            while (rs.next()){
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    map.put(metaData.getColumnName(i),rs.getObject(i));
                    String columnTypeName = metaData.getColumnTypeName(i);
                    System.out.println(columnTypeName);
                }
                objects.add(map);
            }
        } catch ( SQLException e){

            e.printStackTrace();
        }
        finally {
            ps.close();
            connection.close();
        }

    }
}
