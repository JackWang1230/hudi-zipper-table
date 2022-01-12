import cn.uniodnrug.zipper.table.model.DingAlarmModel;
import cn.uniodnrug.zipper.table.model.ErrorMsg;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

import static cn.uniodnrug.zipper.table.constants.PropertiesConstants.*;
import static cn.uniodnrug.zipper.table.utils.DingTalkAlarmsUtil.getHttpPost;
import static cn.uniodnrug.zipper.table.utils.DingTalkAlarmsUtil.sendMsg;

/**
 * @author RWang
 * @Date 2022/1/11
 */

public class AlarmTest {

    public static void main(String[] args) throws IOException {


        // ParameterTool parameterTool = ParameterTool.fromPropertiesFile("/Users/wangrui/Documents/uniondrug-zipper-table-hudi-flink/uniondrug-zipper-table-hudi/src/main/resources/application_prod.properties");

        String propertiesPath="/Users/wangrui/Documents/uniondrug-zipper-table-hudi-flink/uniondrug-zipper-table-hudi/src/main/resources/application_prod.properties";

        Properties props = new Properties();
        InputStream inputStream = new FileInputStream(propertiesPath);
        BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
        props.load(bf);
        ParameterTool parameterTool = ParameterTool.fromMap((Map) props);

        String url = parameterTool.get(DINGTALK_WEBHOOK_URL);
        String aaa ="{\n" +
                "     \"msgtype\": \"markdown\",\n" +
                "     \"markdown\": {\n" +
                "         \"title\":\"杭州天气\",\n" +
//                "         \"content\":\"王瑞\",\n" +
                "         \"text\": \"#### 杭州天气 @18351927570 hudi \\n > 9度，西北风1级，空气良89，相对温度73%\\n > ![screenshot](https://img.alicdn.com/tfs/TB1NwmBEL9TBuNjy1zbXXXpepXa-2400-1218.png)\\n > ###### 10点20分发布 [天气](https://www.dingtalk.com) \\n\"\n" +
                "     },\n" +
                "      \"at\": {\n" +
                "          \"atMobiles\": [\n" +
                "              \"18351927570\"\n" +
                "          ],\n" +
                "          \"atUserIds\": [\n" +
                "              \"王瑞\"\n" +
                "          ],\n" +
                "          \"isAtAll\": false\n" +
                "      }\n" +
                " }";

        String bbb = "{\n" +
                "    \"at\": {\n" +
                "        \"atMobiles\":[\n" +
                "            \"18351927570\"\n" +
                "        ],\n" +
                "        \"atUserIds\":[\n" +
                "            \"王瑞\"\n" +
                "        ],\n" +
                "        \"isAtAll\": false\n" +
                "    },\n" +
                "    \"text\": {\n" +
                "        \"content\":\"我就是我, 是不一样的烟火hudi \"\n" +
                "    },\n" +
                "    \"msgtype\":\"text\"\n" +
                "}";

        try {
            int a = 100;
            int b = 0;
            int c = 100/0;

        }catch (Exception e){
            DingAlarmModel dingAlarmModel = new DingAlarmModel();
            ErrorMsg errorMsg = new ErrorMsg();
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            errorMsg.setContent("hudi "+stringWriter.toString()+"\n"
                    +new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
            dingAlarmModel.setText(errorMsg);
//            getHttpPost(parameterTool, dingAlarmModel);
            sendMsg(getHttpPost(parameterTool, dingAlarmModel));

    }
}}
