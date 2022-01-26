package cn.wr.zipper.table.source;

import cn.wr.zipper.table.model.PageStartEndOffset;
import cn.wr.zipper.table.utils.JDBCDruidUtils;
import cn.wr.zipper.table.utils.MysqlUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.wr.zipper.table.constants.PropertiesConstants.DATA_PAGE_SIZE;
import static cn.wr.zipper.table.constants.SQLConstants.GOODS_MAX_MIN_VALUE;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class MysqlIndexSourceTest extends RichSourceFunction<PageStartEndOffset> {

    private static final long serialVersionUID = 7827616842656567225L;

    private static final Logger logger = LoggerFactory.getLogger(MysqlIndexSourceTest.class);
    private Connection connection = null;
    private PreparedStatement ps = null;

    /**
     * initial config mysql connect and get args from config
     *
     * @param parameters default
     * @throws Exception e
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = JDBCDruidUtils.getConnection(parameterTool);
        ps = connection.prepareStatement(GOODS_MAX_MIN_VALUE);
    }


    /**
     * based on connect mysql achieve the data cnt in mysql
     *
     * @param out default
     * @throws Exception e
     */
    @Override
    public void run(SourceContext<PageStartEndOffset> out) {

        try {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            int pageLength = parameterTool.getInt(DATA_PAGE_SIZE);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int maxId = resultSet.getInt("max_id");
//                int maxId = 9000000;
                int minId = resultSet.getInt("min_id");
                PageStartEndOffset pageStartEndOffset = new PageStartEndOffset();
                for (int i = minId; i < maxId; i+=pageLength) {
                    pageStartEndOffset.setStartOffset(i);
                    pageStartEndOffset.setEndOffset(i+pageLength-1);
                    out.collect(pageStartEndOffset);
                }
            }

        } catch (Exception e) {
            logger.error("page size get error:{}", e);
        }
    }

    /**
     * close mysql connect session
     */
    @Override
    public void cancel() {
        MysqlUtil.close(connection, ps);
    }
}
