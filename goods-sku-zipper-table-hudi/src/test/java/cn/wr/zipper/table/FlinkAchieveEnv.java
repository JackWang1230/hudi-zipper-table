package cn.wr.zipper.table;

import cn.wr.zipper.table.constants.PropertiesConstants;
import cn.wr.zipper.table.constants.SQLConstants;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author RWang
 * @Date 2022/1/5
 */

public class FlinkAchieveEnv {

    public static void main(String[] args) {
        try {
            ParameterTool parameterTool = ParameterTool.fromPropertiesFile("/Users/wangrui/Documents/uniondrug-zipper-table-hudi-flink/uniondrug-zipper-table-hudi/src/main/resources/application_prod.properties");

            String format = String.format(SQLConstants.INCR_INSERT_GOODS_SKU_TABLE,
                    parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_PATH),
                    parameterTool.get(PropertiesConstants.HUDI_BASIC_CONFIG_URIS));
            System.out.println(format);

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
