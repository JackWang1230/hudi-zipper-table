package cn.wr.utils;

import cn.wr.enums.SqlTypeEnum;
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

/**
 * @author RWang
 * @Date 2022/2/10
 */

public class ParseDdlUtil {

    private static final Logger logger = LoggerFactory.getLogger(ParseDdlUtil.class);


    private static final String REGULAR_EXPRESSION = "(?<=\\().*?(?=(WITH|with))";

    // 逗号前面没有一个或多个非逗号字符紧跟一个左括号 (，即逗号不在括号内。逗号后面不是一个数字，即逗号后面没有空格。
    private static final String REGULAR_EXP2 = "(?<!\\([^,]\\+),(?!\\d)";
    private static final String PARTITIONED_BY = "partitioned by";

    /**
     *  将配置文件中的ddl 语句解析成需要的字段名称及字段类型
     * @param tool 配置文件
     * @return HashMap
     */
    public static HashMap<String, String> parseDdl(ParameterTool tool) {

        HashMap<String, String> hashMap = new LinkedHashMap<>();
        String sqlDdl = tool.get(SqlTypeEnum.getRealValue(0)).replaceAll("\n", " ");
        // 正则规则
        Matcher matcher = Pattern.compile(REGULAR_EXPRESSION).matcher(sqlDdl);
        try {
            String colsAndTypes = null;
            if (matcher.find()) {
                String initValues = matcher.group().trim().toLowerCase();
                // 解决场景如下 id int,price decimal(10,2),name string场景下无法区分逗号是否在括号内，导致无法正确解析字段及数据类型
                initValues = Pattern.compile(REGULAR_EXP2).matcher(initValues).replaceAll(", ");
                // 考虑sql中是否包含分区字段
                colsAndTypes = initValues.contains(PARTITIONED_BY) ?
                        initValues.split(PARTITIONED_BY)[0].trim().
                                substring(0, initValues.split(PARTITIONED_BY)[0].trim().length() - 1) :
                        initValues.substring(0, initValues.length() - 1);
            }
            if (StringUtils.isBlank(colsAndTypes)) {
                return hashMap;
            }
            // 为了防止单逗号冲突 需要指定为 逗号+空格
            String[] colAndTypeList = colsAndTypes.split(", ");
            for (String colAndTypes : colAndTypeList) {
                String[] colAndType = colAndTypes.trim().replaceAll(" +", " ").split(" ");
                // 1.字段名称可能涉及 `user_id` 格式场景，需要去除 ``
                // 2.字段后可能涉及注释之类的 只取前两个值 `update_time` datetime DEFAULT NULL COMMENT '更新时间',
                hashMap.put(colAndType[0].replaceAll("`",""), colAndType[1]);
            }
            return hashMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hashMap;
    }

    /**
     *  通过解析ddl 自动生成row需要的每个字段对应的数据类型及字段名称
     * @param tool 配置参数
     * @return RowTypeInfo
     */
    public static RowTypeInfo getRowTypeInfo(ParameterTool tool) {

        try {

            HashMap<String, String> map = parseDdl(tool);
            int size = map.size();
            if (size > 0) {
                ArrayList<String> cols = new ArrayList<>();
                ArrayList<String> colsType = new ArrayList<>();
                TypeInformation<?>[] types = new TypeInformation[size];
                for (Map.Entry<String, String> colsTypesEntry : map.entrySet()) {
                    cols.add(colsTypesEntry.getKey());
                    colsType.add(colsTypesEntry.getValue());
                }
                TypeInformation<?>[] typeInfos = getTypeInformation(colsType, types);
                String[] inputFields = cols.toArray(new String[size]);
                return new RowTypeInfo(typeInfos, inputFields);
            }
        } catch (Exception e) {
            logger.error("can not get RowTypeInfo entry");
        }
        return null;
    }


    /**
     *  基于字段长度，自动解析生成rowType 中需要的字段type
     * @param type ddl中字段类型
     * @param types row中需要的type
     * @return row中需要的type
     */
    public static TypeInformation<?>[] getTypeInformation(List<String> type, TypeInformation<?>[] types) {

        for (int i = 0; i < type.size(); i++) {

            String fieldType = type.get(i).toLowerCase();
            // fieldType=decimal(10,2), 特殊处理
            if (fieldType.contains("decimal")) {
                fieldType = "decimal";
            }
            types[i] = convertTypes(fieldType);
        }
        return types;
    }

    /**
     *  将ddl 中 type类型转换成 TypeInformation对应的type类型
     * @param type
     * @return
     */
    public static TypeInformation<?> convertTypes(String type) {

        TypeInformation<?> types;
        switch (type) {
            case "int":
                types = Types.INT;
                break;
            case "decimal":
                types = Types.BIG_DEC;
                break;
            case "bigint":
                types =Types.BIG_INT;
                break;
            case "double":
                types = Types.DOUBLE;
                break;
            default:
                types = Types.STRING;
                break;
        }
        return types;
    }


    /**
     * @param tool
     * @param value 0 表示flink 临时表名称，1表示原始mysql中数据表名称(也就是topic)
     * @return
     */
    public static String getTableName(ParameterTool tool, int value) {

        String insertStat = null;
        switch (value) {
            case 0:
                insertStat = tool.get(SqlTypeEnum.getRealValue(4)).trim().replaceAll(" +", " ");
                break;
            case 1:
                insertStat = tool.get(SqlTypeEnum.getRealValue(1)).trim().replaceAll(" +", " ");
                break;
        }

        assert insertStat != null;
        return insertStat.split(" ")[insertStat.split(" ").length - 1];
    }


}
