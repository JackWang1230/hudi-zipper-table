package cn.wr.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.wr.constants.PropertiesConstants.BULK_INSERT_TABLE;

/**
 * @author RWang
 * @Date 2022/2/10
 */

public class ParseDdlUtil {

    private static final Logger logger = LoggerFactory.getLogger(ParseDdlUtil.class);

    private static final String REGULAR_EXPRESSION ="(?<=\\().*?(?=(WITH|with))";


    /**
     * 解析ddl
     * @param tool 配置
     * @return 解析后结构
     */
    public static HashMap<String,String> parseDdl(ParameterTool tool){

        HashMap<String,String> hashMap = new LinkedHashMap<>();
        String sqlDdl = tool.get(BULK_INSERT_TABLE).replaceAll("\n"," ");
        // 正则规则
        Matcher matcher = Pattern.compile(REGULAR_EXPRESSION).matcher(sqlDdl);
        try {
            String colsAndTypes=null;
            if (matcher.find()) {
                String initValues = matcher.group().trim();
                colsAndTypes = initValues.substring(0, initValues.length() - 1);
            }
            if (StringUtils.isBlank(colsAndTypes)) {
                return hashMap;
            }
            // 为了防止单逗号冲突 需要指定为 逗号+空格
            String[] colAndTypeList = colsAndTypes.split(", ");
            for (String colAndTypes : colAndTypeList) {
                String[] colAndType = colAndTypes.trim().replaceAll(" +", " ").split(" ");
                hashMap.put(colAndType[0],colAndType[1]);
            }
            return hashMap;
        } catch (Exception e){
            e.printStackTrace();
        }
        return hashMap;
    }

    /**
     * 获取row需要的类型
     * @param tool
     * @return
     */
    public static RowTypeInfo getRowTypeInfo(ParameterTool tool){

        try {
            HashMap<String, String> map = parseDdl(tool);
            int size = map.size();
            if (size>0){
                ArrayList<String> cols = new ArrayList<>();
                ArrayList<String> colsType = new ArrayList<>();
                TypeInformation<?>[] types = new TypeInformation[size];
                for (Map.Entry<String, String> colsTypesEntry : map.entrySet()) {
                    cols.add(colsTypesEntry.getKey());
                    colsType.add(colsTypesEntry.getValue());
                }
                TypeInformation<?>[] typeInfos = getTypeInformation(colsType, types);
                String[] inputFields = cols.toArray(new String[size]);
                return new RowTypeInfo(typeInfos,inputFields);
            }
        } catch ( Exception e){
            logger.error("can not get RowTypeInfo entry");
        }
        return null;
    }


    /**
     * 获取typeInformation
     * @param type
     * @param types
     * @return
     */
    public static  TypeInformation<?>[] getTypeInformation(List<String> type, TypeInformation<?>[] types){

        for (int i = 0; i < type.size(); i++) {

            String fieldType = type.get(i).toLowerCase();
            if (fieldType.contains("decimal")){
                fieldType = "decimal";
            }
            types[i] = convertTypes(fieldType);
        }
        return types;
    }

    /**
     * 转换数据类型为row 需要的
     * @param type
     * @return
     */
    public static TypeInformation<?> convertTypes(String type){

        TypeInformation<?> types;
        switch (type){
            case "string":
                types = Types.STRING;
                break;
            case "int":
                types=Types.INT;
                break;
            case "decimal":
                types=Types.BIG_DEC;
                break;
            default:
                types=Types.STRING;
                break;
        }
        return types;
    }
}
