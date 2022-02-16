package cn.wr.flatmap;

import cn.wr.model.CanalTransDataModel;
import cn.wr.utils.ParseDdlUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.json.JSONException;
import org.json.JSONObject;
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
        if (canalTransDataModel.getType().toUpperCase().equals(UPDATE)) {
            Object newData = canalTransDataModel.getData();
            Object oldData = canalTransDataModel.getOld();
            // String newJsonStringData = objectMapper.writeValueAsString(newData);
            JSONObject jsonNewObject = new JSONObject(objectMapper.writeValueAsString(newData));
            JSONObject jsonOldObject = new JSONObject(objectMapper.writeValueAsString(oldData));
            Iterator keys = jsonOldObject.keys();
            List<String> colsName = Arrays.asList(fieldNames);
            int index = 0;
            while (keys.hasNext()) {
                // identify whether the column is recognized
                if (colsName.contains(keys.next())) {
                    index += 1;
                }
            }
            if (index > 1) {
                Row rowNewData = getRow(fieldNames, fieldTypes, jsonNewObject);
                Row rowOldData = getRow(fieldNames, fieldTypes, jsonNewObject, jsonOldObject);
                collector.collect(rowNewData);
                collector.collect(rowOldData);
            }
        }
        if (canalTransDataModel.getType().toUpperCase().equals(INSERT)) {
            Object newData = canalTransDataModel.getData();
            //  1 .需要先将数据转换成json
            String newJsonStringData = objectMapper.writeValueAsString(newData);
            JSONObject jsonObject = new JSONObject(newJsonStringData);
            Row rowData = getRow(fieldNames, fieldTypes, jsonObject);
            collector.collect(rowData);
        }
    }

    private Row getRow(String[] fieldNames, TypeInformation<?>[] fieldTypes, JSONObject jsonObject) throws JSONException {
        Row rowData = new Row(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            // Object o = jsonObject.get(fieldNames[i]);
            TypeInformation<?> fieldType = fieldTypes[i];
            String colsType = fieldType.toString();
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
                    rowData.setField(i, jsonObject.getInt(fieldNames[i]));
                    break;
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

    private Row getRow(String[] fieldNames, TypeInformation<?>[] fieldTypes, JSONObject jsonObject,
                       JSONObject jsonOldObject) throws JSONException {
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
                    } else if (fieldNames[i].equals(END_TIME)) {
                        rowData.setField(i, StringUtils.isBlank(jsonObject.getString(GMT_UPDATED_V1)) ?
                                jsonObject.getString(GMT_UPDATED_V2) : jsonObject.getString(GMT_UPDATED_V1));
                    } else {
                        rowData.setField(i, StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                                jsonObject.getString(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
                    }
                    break;
                case "Integer":
                    // rowData.setField(i, jsonObject.getIntValue(fieldNames[i]));
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            jsonObject.getInt(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
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
                    // rowData.setField(i, jsonObject.getString(fieldNames[i]));
                    rowData.setField(i,StringUtils.isBlank(jsonOldObject.getString(fieldNames[i]))?
                            jsonObject.getString(fieldNames[i]):jsonOldObject.getString(fieldNames[i]));
                    break;
            }
        }
        return rowData;
    }

}
