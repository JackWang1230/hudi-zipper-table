package cn.uniondrug.zipper.table.flatmap;

import cn.uniodnrug.zipper.table.model.GoodsSkuInfo;
import cn.uniodnrug.zipper.table.utils.MysqlUtil;
import cn.uniondrug.zipper.table.pagesize.PageSize;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import static cn.uniodnrug.zipper.table.constants.SQLConstants.GOODS_DETAIL_INDEX_SQL;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class InitialGoodsSkuFlatMapFunction extends RichFlatMapFunction<PageSize, GoodsSkuInfo> {
    private static final long serialVersionUID = 2853839611658900152L;
    private static final Logger logger = LoggerFactory.getLogger(InitialGoodsSkuFlatMapFunction.class);
    private Connection connection=null;
    private PreparedStatement ps = null;
    private ObjectMapper objectMapper = null;


    /**
     * initial args and mysql session
     * @param parameters default
     * @throws Exception e
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = MysqlUtil.getConnection(parameterTool);
        ps = connection.prepareStatement(GOODS_DETAIL_INDEX_SQL);
    }


    /**
     * the full info about get goodsSkuInfo from mysql based on paging query
     * @param value each page start offset and step size default 10000
     * @param collector container used on collect goodsSkuInfo
     */
    @Override
    public void flatMap(PageSize value, Collector<GoodsSkuInfo> collector) {

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
                collector.collect(objectMapper.convertValue(rowData, GoodsSkuInfo.class));
            }
        } catch (Exception e){
            e.printStackTrace();
            logger.error("goods sku attr field is error:{}",e);
        }

    }

    /**
     * close the mysql session in the end
     * @throws Exception e
     */
    @Override
    public void close() throws Exception {
        super.close();
        MysqlUtil.close(connection,ps);
    }
}
