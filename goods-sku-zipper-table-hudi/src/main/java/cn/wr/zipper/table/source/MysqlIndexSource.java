package cn.wr.zipper.table.source;

import cn.wr.zipper.table.model.PageStartEndOffset;
import cn.wr.zipper.table.utils.MysqlUtil;
import cn.wr.zipper.table.constants.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.wr.zipper.table.constants.SQLConstants.GOODS_MAX_MIN_VALUE;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class MysqlIndexSource extends RichSourceFunction<PageStartEndOffset> {

    private static final long serialVersionUID = 7827616842656567225L;
    private static final Logger logger = LoggerFactory.getLogger(MysqlIndexSource.class);

    /**
     *
     * @param parameters default
     * @throws Exception e
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }


    /**
     * based on connect mysql achieve the data cnt in mysql
     *
     * @param out default
     * @throws Exception e
     */
    @Override
    public void run(SourceContext<PageStartEndOffset> out) {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Connection connection = MysqlUtil.getConnection(parameterTool);
        PreparedStatement ps = null;
        try {
             ps = connection.prepareStatement(GOODS_MAX_MIN_VALUE);
            int pageLength = parameterTool.getInt(PropertiesConstants.DATA_PAGE_SIZE);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int maxId = resultSet.getInt("max_id");
                int minId = resultSet.getInt("min_id");
                for (int i = minId; i < maxId; i+=pageLength) {
                    PageStartEndOffset pageStartEndOffset = new PageStartEndOffset();
                    pageStartEndOffset.setStartOffset(i);
                    pageStartEndOffset.setEndOffset(i+pageLength-1);
                    out.collect(pageStartEndOffset);
                }
            }

        } catch (Exception e) {
            logger.error("page size get error:{}", e);
        }finally {
            MysqlUtil.close(connection,ps);
        }
    }

    /**
     *
     */
    @Override
    public void cancel() {

    }
}
