package cn.wr.flatmap;

import cn.wr.model.CanalTransDataModel;
import cn.wr.utils.ParseDdlUtil;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author RWang
 * @Date 2022/2/16
 */

public class UpdateOrInsertDataFlatMap extends RichFlatMapFunction<CanalTransDataModel, Row> {
    private static final long serialVersionUID = -665328047168038404L;
    private static final Logger logger = LoggerFactory.getLogger(UpdateOrInsertDataFlatMap.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String END_TIME_VALUE = "9999-12-31";
    private static final String END_TIME_VALUE1 = "-1";
    private static final String GMT_UPDATED_V1 = "gmtUpdated";
    private static final String GMT_UPDATED_V2 = "gmt_updated";
    private static final String UPDATE = "UPDATE";
    private static final String INSERT = "INSERT";
    private static final String START_TIME = "start_time";
    private static final String END_TIME = "end_time";

    @Override
    public void flatMap(CanalTransDataModel canalTransDataModel, Collector<Row> collector) throws Exception {

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        RowTypeInfo rowTypeInfo = ParseDdlUtil.getRowTypeInfo(parameterTool);
        assert rowTypeInfo != null;
        // 提取需要的字段名称
        String[] fieldNames = rowTypeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        // 如果字段发生变化，但不在需要感知的字段中,会出现一种情况 即 感知字段没有发生变化 但是 更新时间变了
        // 因此数仓侧可能会存在感知字段值 重复，但start_time不同的问题，所以在做处理时
        // 数仓侧 需要先 基于 感知字段 进行group by ,并对start_time和 end_time 取最大值即可
        if (canalTransDataModel.getType().toUpperCase().equals(UPDATE)) {
            Object newData = canalTransDataModel.getData();
            Object oldData = canalTransDataModel.getOld();
            JSONObject jsonNewObject = JSONObject.parseObject(objectMapper.writeValueAsString(newData));
            JSONObject jsonOldObject = JSONObject.parseObject(objectMapper.writeValueAsString(oldData));
            Iterator keys = jsonOldObject.keySet().iterator();
            List<String> colsName = Arrays.asList(fieldNames);
            int index = 0;
            while (keys.hasNext()) {
                // identify whether the column is recognized
                String key = keys.next().toString();
                if (colsName.contains(key) | GMT_UPDATED_V1.equals(key) | GMT_UPDATED_V2.equals(key)) {
                    index += 1;
                }
            }
            Row rowNewData = getRow(fieldNames, fieldTypes, jsonNewObject);
            Row rowOldData;
            if(index > 1) {
                rowOldData = getRow(fieldNames, fieldTypes, jsonNewObject, jsonOldObject, 1);
            }else {
                // todo 如果发生变更的数据不在 需要感知的字段中，则改整条记录的结束时间为-1
                rowOldData = getRow(fieldNames, fieldTypes, jsonNewObject, jsonOldObject, 0);
            }
            collector.collect(rowNewData);
            collector.collect(rowOldData);
        }
        if (canalTransDataModel.getType().toUpperCase().equals(INSERT)) {
            Object newData = canalTransDataModel.getData();
            //  1 .需要先将数据转换成json
            JSONObject jsonObject = JSONObject.parseObject(objectMapper.writeValueAsString(newData));
            Row rowData = getRow(fieldNames, fieldTypes, jsonObject);
            collector.collect(rowData);
        }
    }

    /**
     * 生成 新数据的方法
     * @param fieldNames
     * @param fieldTypes
     * @param jsonObject
     * @return
     * @throws JSONException
     */
    private Row getRow(String[] fieldNames, TypeInformation<?>[] fieldTypes, JSONObject jsonObject) throws JSONException {
        Row rowData = new Row(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            // Object o = jsonObject.get(fieldNames[i]);
            TypeInformation<?> fieldType = fieldTypes[i];
            String colsType = fieldType.toString();
            //Double
            //BigInteger
            //BigDecimal
            switch (colsType) {
                case "String":
                    // startTime 和 endTime 需要映射转换一下
                    if (fieldNames[i].equals(START_TIME)) {
                        rowData.setField(i, StringUtils.isBlank(jsonObject.getString(GMT_UPDATED_V1)) ?
                                jsonObject.getString(GMT_UPDATED_V2) : jsonObject.getString(GMT_UPDATED_V1));
                    } else if (fieldNames[i].equals(END_TIME)) {
                        rowData.setField(i, END_TIME_VALUE);
                    } else {
                        rowData.setField(i, jsonObject.getString(fieldNames[i]));
                    }
                    break;
                case "Integer":
//                    rowData.setField(i, jsonObject.getInt(fieldNames[i]));
                    rowData.setField(i, jsonObject.getIntValue(fieldNames[i]));
                    break;
                case "Double":
                    rowData.setField(i,jsonObject.getDouble(fieldNames[i]));
                case "BigDecimal":
                    rowData.setField(i, new BigDecimal(jsonObject.getString(fieldNames[i])));
                    break;
                case "BigInteger":
                    rowData.setField(i,new BigInteger(jsonObject.getString(fieldNames[i])));
                default:
                    rowData.setField(i, jsonObject.getString(fieldNames[i]));
                    break;
            }
        }
        return rowData;
    }

    /**
     * 基于新数据 生成其上一条数据
     * @param fieldNames 需要被识别处发生变更的字段
     * @param fieldTypes 每个字段所对应的类型
     * @param jsonObject 新的json数据
     * @param jsonOldObject 发生变更的json数据
     * @param bool bool值包含0和1，其中 0 代表发生的字段不在fieldNames中，1代表 发生变更的字段 是属于fieldNames中
     * @return Row
     * @throws JSONException
     */
    private Row getRow(String[] fieldNames, TypeInformation<?>[] fieldTypes, JSONObject jsonObject,
                       JSONObject jsonOldObject,int bool) throws JSONException {
        Row rowData = new Row(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {

            TypeInformation<?> fieldType = fieldTypes[i];
            String colsType = fieldType.toString();
            // todo 次数if else if 语句过于冗余，待优化
            switch (colsType) {
                case "String":
                    // startTime 和 endTime 需要映射转换一下
                    if (fieldNames[i].equals(START_TIME)) {
                        rowData.setField(i, StringUtils.isBlank(jsonOldObject.getString(GMT_UPDATED_V1)) ?
                                jsonOldObject.getString(GMT_UPDATED_V2) : jsonOldObject.getString(GMT_UPDATED_V1));
                    } else if ((fieldNames[i].equals(END_TIME) & bool==1)) {
                        rowData.setField(i, StringUtils.isBlank(jsonObject.getString(GMT_UPDATED_V1)) ?
                                jsonObject.getString(GMT_UPDATED_V2) : jsonObject.getString(GMT_UPDATED_V1));
                    } else if ((fieldNames[i].equals(END_TIME) & bool==0)){
                        rowData.setField(i,END_TIME_VALUE1);
                    }else {
                        rowData.setField(i, StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                                jsonObject.getString(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
                    }
                    break;
                case "Integer":
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            jsonObject.getIntValue(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
                    break;
                case "Double":
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            jsonObject.getDouble(fieldNames[i]):jsonOldObject.getDouble(fieldNames[i]));
                    break;
                case "BigDecimal":
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            new BigDecimal(jsonObject.getString(fieldNames[i])):new BigDecimal(jsonOldObject.getString(fieldNames[i])));
                    break;
                case "BigInteger":
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            new BigInteger(jsonObject.getString(fieldNames[i])):new BigInteger(jsonOldObject.getString(fieldNames[i])));
                    break;
                default:
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            jsonObject.getString(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
                    break;
            }
        }
        return rowData;
    }


}
