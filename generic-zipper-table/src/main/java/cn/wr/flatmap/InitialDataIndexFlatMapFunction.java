package cn.wr.flatmap;

import cn.wr.enums.SqlTypeEnum;
import cn.wr.model.PageStartEndOffset;
import cn.wr.utils.MysqlUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.sql.*;

/**
 * @author RWang
 * @Date 2022/1/25
 */

public class InitialDataIndexFlatMapFunction extends RichFlatMapFunction<PageStartEndOffset, Row> {


    private static final long serialVersionUID = 8195552156939171805L;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void flatMap(PageStartEndOffset value, Collector<Row> collector)  {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Connection connection = MysqlUtil.getConnection(parameterTool);
        String dataDetailBasedIdSql = parameterTool.get(SqlTypeEnum.getRealValue(2));
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(dataDetailBasedIdSql);
            ps.setInt(1, value.getStartOffset());
            ps.setInt(2, value.getEndOffset());
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            Row rowData = new Row(columnCount);
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    // todo 基于数据字段类型返回对应的java数据类型
                    String columnTypeName = metaData.getColumnTypeName(i).toLowerCase();
                    switch (columnTypeName){
                        case "varchar":
                            rowData.setField(i-1, rs.getString(i));
                            break;
                        case "int":
                            rowData.setField(i-1, rs.getInt(i));
                            break;
                        case "bigint":
                            rowData.setField(i-1,new BigInteger(rs.getString(i)));
                            break;
                        case "double":
                            rowData.setField(i-1,rs.getDouble(i));
                            break;
                        case "decimal":
                            rowData.setField(i-1, rs.getBigDecimal(i));
                            break;
                        case "timestamp":
                            rowData.setField(i-1, rs.getTimestamp(i));
                        default:
                            rowData.setField(i-1,rs.getObject(i).toString());
                            break;
                    }
                }
                collector.collect(rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            MysqlUtil.close(connection, ps);
        }
    }
}
