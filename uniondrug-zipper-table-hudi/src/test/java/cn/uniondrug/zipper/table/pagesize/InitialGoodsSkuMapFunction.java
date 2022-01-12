package cn.uniondrug.zipper.table.pagesize;

import cn.uniondrug.zipper.table.flatmap.InitialGoodsSkuFlatMapFunction;
import cn.uniodnrug.zipper.table.model.GoodsSkuInfo;
import cn.uniodnrug.zipper.table.utils.MysqlUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import static cn.uniodnrug.zipper.table.constants.SQLConstants.GOODS_DETAIL_SQL;

/**
 * @author RWang
 * @Date 2021/12/20
 */

public class InitialGoodsSkuMapFunction extends RichMapFunction<PageSize, GoodsSkuInfo> {

    private static final long serialVersionUID = 4083669569629651541L;

    private static final Logger logger = LoggerFactory.getLogger(InitialGoodsSkuFlatMapFunction.class);
    private Connection connection=null;
    private PreparedStatement ps = null;
    private ObjectMapper objectMapper = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = MysqlUtil.getConnection(parameterTool);
        ps = connection.prepareStatement(GOODS_DETAIL_SQL);
    }

    @Override
    public GoodsSkuInfo map(PageSize value) {

        try {
            // 具体业务逻辑调整
            ps.setInt(1,value.getStartOffset());
            ps.setInt(2,value.getPageLength());
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 将ResultSet对象的列名和值存到map中，再将map转换为json字符串，最后将json字符串转换为实体类对象
            Map<String, Object> rowData = new HashMap<>();
            while (rs.next()){
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                return objectMapper.convertValue(rowData, GoodsSkuInfo.class);
            }
        } catch (Exception e){
            logger.error("goods sku attr field is error:{}",e);
        }
        return null;

    }

    @Override
    public void close() throws Exception {
        super.close();
        MysqlUtil.close(connection,ps);
    }
}
