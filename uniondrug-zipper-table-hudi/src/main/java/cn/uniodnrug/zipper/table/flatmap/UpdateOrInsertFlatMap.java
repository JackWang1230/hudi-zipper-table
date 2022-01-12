package cn.uniodnrug.zipper.table.flatmap;

import cn.uniodnrug.zipper.table.constants.PropertiesConstants;
import cn.uniodnrug.zipper.table.model.CanalTransDataModel;
import cn.uniodnrug.zipper.table.model.GoodsSkuInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * identify whether the data is updated or newly added
 * @author RWang
 * @Date 2021/12/23
 */

public class UpdateOrInsertFlatMap extends RichFlatMapFunction<CanalTransDataModel, GoodsSkuInfo> {
    private static final long serialVersionUID = 957284683679128053L;
    private static final Logger logger = LoggerFactory.getLogger(UpdateOrInsertFlatMap.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void flatMap(CanalTransDataModel canalTransDataModel, Collector<GoodsSkuInfo> collector) throws Exception {


        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String rkList = parameterTool.getRequired(PropertiesConstants.DATA_RECORD_KEY_COLUMN);
        List<String> cols = Arrays.asList(rkList.split(","));
        if (canalTransDataModel.getType().equals("UPDATE")) {

            Object newData = canalTransDataModel.getData();
            Object oldData = canalTransDataModel.getOld();

            JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(oldData));
            GoodsSkuInfo oldGoodsSkuInfo = objectMapper.convertValue(oldData, GoodsSkuInfo.class);
            Iterator keys = jsonObject.keys();
            int index = 0;
            while (keys.hasNext()) {
                // identify whether the column is recognized
                if (cols.contains(keys.next())) {
                    index += 1;
                }
            }
            if (index > 1) { //  each update,the column gmtUpdated always changed ，thus,index must >1
                GoodsSkuInfo newGoodsSkuInfo = objectMapper.convertValue(newData, GoodsSkuInfo.class);
                newGoodsSkuInfo.setStartTime(newGoodsSkuInfo.getEndTime());
                newGoodsSkuInfo.setEndTime("9999-12-31");
                GoodsSkuInfo lastGoodsSkuInfo = new GoodsSkuInfo();
                lastGoodsSkuInfo.setSkuNo(StringUtils.isBlank(oldGoodsSkuInfo.getSkuNo()) ?
                        newGoodsSkuInfo.getSkuNo() : oldGoodsSkuInfo.getSkuNo());
                lastGoodsSkuInfo.setCommonName(StringUtils.isBlank(oldGoodsSkuInfo.getCommonName()) ?
                        newGoodsSkuInfo.getCommonName() : oldGoodsSkuInfo.getCommonName());
                lastGoodsSkuInfo.setApprovalNumber(StringUtils.isBlank(oldGoodsSkuInfo.getApprovalNumber()) ?
                        newGoodsSkuInfo.getApprovalNumber() : oldGoodsSkuInfo.getApprovalNumber());
                lastGoodsSkuInfo.setInternalId(StringUtils.isBlank(oldGoodsSkuInfo.getInternalId()) ?
                        newGoodsSkuInfo.getInternalId() : oldGoodsSkuInfo.getInternalId());
                lastGoodsSkuInfo.setMerchantId(0 == oldGoodsSkuInfo.getMerchantId() ?
                        newGoodsSkuInfo.getMerchantId() : oldGoodsSkuInfo.getMerchantId());
                lastGoodsSkuInfo.setPrice(null == oldGoodsSkuInfo.getPrice() ?
                        newGoodsSkuInfo.getPrice() : oldGoodsSkuInfo.getPrice());
                lastGoodsSkuInfo.setTradeCode(StringUtils.isBlank(oldGoodsSkuInfo.getTradeCode()) ?
                        newGoodsSkuInfo.getTradeCode() : oldGoodsSkuInfo.getTradeCode());
                // Change time of the previous record
                lastGoodsSkuInfo.setStartTime(oldGoodsSkuInfo.getEndTime());
                lastGoodsSkuInfo.setEndTime(newGoodsSkuInfo.getStartTime());
                collector.collect(newGoodsSkuInfo);
                collector.collect(lastGoodsSkuInfo);
            }
        }
        if (canalTransDataModel.getType().equals("INSERT")) {
            Object newData = canalTransDataModel.getData();
            GoodsSkuInfo newGoodsSkuInfo = objectMapper.convertValue(newData, GoodsSkuInfo.class);
            newGoodsSkuInfo.setStartTime(newGoodsSkuInfo.getEndTime());
            newGoodsSkuInfo.setEndTime("9999-12-31");
            collector.collect(newGoodsSkuInfo);

        }
    }
}
