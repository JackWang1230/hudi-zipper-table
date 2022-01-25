package cn.uniondrug.flatmap;

import cn.uniondrug.model.PageStartEndOffset;
import cn.uniondrug.utils.MysqlUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.ArrayList;

import static cn.uniondrug.constants.PropertiesConstants.DATA_DETAIL_BASED_ID;

/**
 * @author RWang
 * @Date 2022/1/25
 */

public class InitialDataIndexFlatMapFunction extends RichFlatMapFunction<PageStartEndOffset, Row> {


    private static final long serialVersionUID = 8195552156939171805L;
    private int columnCount;

    private String[] inputField;


//
//    TypeInformation[] types = new TypeInformation[columnCount];
//        for (int i = 0; i < 9; i++) {
//        types[i]= Types.STRING;
//    }
//    String[] inputField={"a","b","c","d","e","f","g","h","i"};
//    RowTypeInfo rowTypeInfo = new RowTypeInfo(types,inputField);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void flatMap(PageStartEndOffset value, Collector<Row> collector) throws Exception {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Connection connection = MysqlUtil.getConnection(parameterTool);
        String dataDetailBasedIdSql = parameterTool.get(DATA_DETAIL_BASED_ID);
        PreparedStatement ps = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ps = connection.prepareStatement(dataDetailBasedIdSql);
            ps.setInt(1, value.getStartOffset());
            ps.setInt(2, value.getEndOffset());
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            columnCount = metaData.getColumnCount();
//            GenericRowData rowData = new GenericRowData(columnCount);
//            HashMap<String, Object> rowData = Maps.newHashMap();
//            Object[] resultObj = new Object[columnCount];

//            rowData.setField(0,"ss");
//            rowData.setField(1,11);
//            rowData.setField(2,22);
//            rowData.setField(3,33);
//            collector.collect(rowData);
            org.apache.flink.types.Row rowData = org.apache.flink.types.Row.withNames();
            ArrayList<Object> objects = new ArrayList<>();
            while (rs.next()) {
//                Row rowData = new Row(columnCount);

                for (int i = 1; i <= columnCount; i++) {
//                    resultObj[i-1]=rs.getObject(i);
//                     rowData.put(metaData.getColumnLabel(i),rs.getObject(i));
//                    rowData.setField(i-1, rs.getObject(i).toString());
//                    String columnName = metaData.getColumnName(i);
//                    System.out.println(columnName);
                    rowData.setField(metaData.getColumnName(i),rs.getObject(i).toString());
                }
                objects.add(rowData);
//                String s = rowData.toString();
//                System.out.println(s);
//                collector.collect(Row.of(resultObj));
                collector.collect(rowData);
//                collector.collect(objectMapper.convertValue(rowData,GoodsSkuInfo.class));
            }
//            TypeInformation<GenericRowData> typeInfo;
//            typeInfo = TypeExtractor.getForObject(rowData);
//            DataStreamSource.fromCollection(Arrays.asList(data), typeInfo)
//
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            MysqlUtil.close(connection, ps);
        }
    }
}
