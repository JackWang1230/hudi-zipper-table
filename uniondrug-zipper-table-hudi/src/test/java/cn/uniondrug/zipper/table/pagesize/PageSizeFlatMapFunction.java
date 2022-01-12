package cn.uniondrug.zipper.table.pagesize;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.uniodnrug.zipper.table.constants.PropertiesConstants.DATA_PAGE_SIZE;

/**
 * @author RWang
 * @Date 2021/12/17
 */

public class PageSizeFlatMapFunction extends RichFlatMapFunction<Integer, PageSize> {

    private static final long serialVersionUID = -7099386860675295302L;
    private static final Logger  logger = LoggerFactory.getLogger(PageSizeFlatMapFunction.class);

    /**
     * the details logic about paging query
     * @param value the data in mysql cnt
     * @param out container contains each pageSize
     */
    @Override
    public void flatMap(Integer value, Collector<PageSize> out) {

        try {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            int pageLength = parameterTool.getInt(DATA_PAGE_SIZE);
//        int pageLength=100;
//        int value1 = 100;
            // 分页逻辑处理
            PageSize pageSize = new PageSize();
            pageSize.setPageLength(pageLength);
            int size = value/pageLength;
            int reminder = value%pageLength;
            // size>0 需要分页
            if (size>0){
                // reminder>0 说明有余数
                if(reminder>0){
                    size= size+1;
                }
                for (int i = 0; i < size; i++) {
                    pageSize.setStartOffset(i*pageSize.getPageLength()+1);
                    out.collect(pageSize);
                }
            }else {
                // size<0,不需要分页
                pageSize.setStartOffset(1);
                out.collect(pageSize);
            }
        } catch (Exception e){
            logger.error("page size get error:{}",e);
        }
    }
}
