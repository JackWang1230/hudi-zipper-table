package cn.uniondrug.zipper.table.source;


import cn.uniodnrug.zipper.table.utils.MysqlUtil;
import cn.uniondrug.zipper.table.pagesize.PageSize;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static cn.uniodnrug.zipper.table.constants.PropertiesConstants.DATA_PAGE_SIZE;
import static cn.uniodnrug.zipper.table.constants.SQLConstants.GOODS_CNT_SQL;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class MysqlPageSizeSource extends RichSourceFunction<PageSize> {

    private static final long serialVersionUID = 7827616842656567225L;

    private static final Logger logger = LoggerFactory.getLogger(MysqlPageSizeSource.class);
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
        connection = MysqlUtil.getConnection(parameterTool);
        ps = connection.prepareStatement(GOODS_CNT_SQL);
    }


    /**
     * based on connect mysql achieve the data cnt in mysql
     *
     * @param out default
     * @throws Exception e
     */
    @Override
    public void run(SourceContext<PageSize> out) {

        try {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            int pageLength = parameterTool.getInt(DATA_PAGE_SIZE);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
//                int value = resultSet.getInt("cnt");
                PageSize pageSize = new PageSize();
                pageSize.setPageLength(pageLength);
//                int pageLength=100;
                int value = 5000000;
//                 分页逻辑处理
                int size = value / pageLength;
                int reminder = value % pageLength;
                // size>0 需要分页
                if (size > 0) {
                    // reminder>0 说明有余数
                    if (reminder > 0) {
                        size = size + 1;
                    }
                    for (int i = 0; i < size; i++) {
                        pageSize.setStartOffset(i * pageSize.getPageLength() + 1);
                        out.collect(pageSize);
                    }
                } else {
                    // size<0,不需要分页
                    pageSize.setStartOffset(1);
                    out.collect(pageSize);
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
