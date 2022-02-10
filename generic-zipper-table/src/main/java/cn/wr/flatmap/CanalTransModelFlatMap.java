package cn.wr.flatmap;

import cn.wr.model.CanalTransDataModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/**
 * @author RWang
 * @Date 2022/2/10
 */

public class CanalTransModelFlatMap extends RichFlatMapFunction<String, CanalTransDataModel> {
    private static final long serialVersionUID = 7672134673502735148L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<CanalTransDataModel> collector) throws Exception {
        if (StringUtils.isBlank(value)) return;
        CanalTransDataModel canalDataModel = objectMapper.readValue(value, CanalTransDataModel.class);
        collector.collect(canalDataModel);
    }
}
