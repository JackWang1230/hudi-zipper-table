package cn.wr.zipper.table.flatmap;

import cn.wr.zipper.table.model.CanalTransDataModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/**
 * json string into canalDataModel object
 * @author RWang
 * @Date 2021/12/23
 */

public class CanalTransModelFlatMap extends RichFlatMapFunction<String, CanalTransDataModel> {

    private static final long serialVersionUID = 7625884571335860993L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<CanalTransDataModel> collector) throws Exception {
        if (StringUtils.isBlank(value)) return;
        CanalTransDataModel canalDataModel = objectMapper.readValue(value, CanalTransDataModel.class);
        collector.collect(canalDataModel);
    }
}
