package cn.wr.zipper.table.flatmap;

import cn.wr.zipper.table.model.GoodsSkuInfo;
import cn.wr.zipper.table.model.PageStartEndOffset;
import cn.wr.zipper.table.utils.MysqlUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
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

import static cn.wr.zipper.table.constants.SQLConstants.GOODS_DETAIL_INDEX_SQL;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class InitialGoodsSkuIndexFlatMapFunction extends RichFlatMapFunction<PageStartEndOffset, GoodsSkuInfo> {
    private static final long serialVersionUID = 2853839611658900152L;
    private static final Logger logger = LoggerFactory.getLogger(InitialGoodsSkuIndexFlatMapFunction.class);

    /**
     * the full info about get goodsSkuInfo from mysql based on paging query
     *
     * @param value     each page start offset and step size default 10000
     * @param collector container used on collect goodsSkuInfo
     */
    @Override
    public void flatMap(PageStartEndOffset value, Collector<GoodsSkuInfo> collector) {


        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Connection connection = MysqlUtil.getConnection(parameterTool);
        PreparedStatement ps = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ps = connection.prepareStatement(GOODS_DETAIL_INDEX_SQL);
            ps.setInt(1, value.getStartOffset());
            ps.setInt(2, value.getEndOffset());
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // stored this resultSet object into map ,then convert jsonString into object from this map
            Map<String, Object> rowData = new HashMap<>();
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                collector.collect(objectMapper.convertValue(rowData, GoodsSkuInfo.class));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("goods sku attr field is error:{}", e);
        } finally {
            MysqlUtil.close(connection, ps);
        }

    }

}
