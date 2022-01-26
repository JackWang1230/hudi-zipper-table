package cn.wr.zipper.table.utils;

import cn.wr.zipper.table.model.AtInfos;
import cn.wr.zipper.table.model.DingAlarmModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static cn.wr.zipper.table.constants.PropertiesConstants.DINGTALK_WEBHOOK_URL;
import static cn.wr.zipper.table.constants.PropertiesConstants.DINGTALK_USER_PHONE;
import static cn.wr.zipper.table.constants.PropertiesConstants.DINGTALK_USER_ID;

/**
 * @author RWang
 * @Date 2022/1/11
 */

public class DingTalkAlarmsUtil {

    private static final Logger logger= LoggerFactory.getLogger(DingTalkAlarmsUtil.class);

    /**
     * get http request basic config
     * @param parameterTool config
     * @param dingAlarmModel  dingData
     * @return HttpPost
     */
    public static HttpPost getHttpPost(ParameterTool parameterTool, DingAlarmModel dingAlarmModel) throws JsonProcessingException {

        String url = parameterTool.get(DINGTALK_WEBHOOK_URL);

        AtInfos atInfos = new AtInfos();
        ObjectMapper objectMapper = new ObjectMapper();
        atInfos.setAtMobiles(Collections.singletonList(parameterTool.get(DINGTALK_USER_PHONE)));
        atInfos.setAtUserIds(Collections.singletonList(parameterTool.get(DINGTALK_USER_ID)));
        atInfos.setisAtAll(false);
        dingAlarmModel.setAt(atInfos);
        dingAlarmModel.setMsgtype("text");
        HttpPost httpPost = new HttpPost(url);

        StringEntity entity = new StringEntity(objectMapper.writeValueAsString(dingAlarmModel), StandardCharsets.UTF_8);
        httpPost.setEntity(entity);
        httpPost.addHeader("Content-Type","application/json");
        return httpPost;
    }

    /**
     * send alarm msg
     * @param httpPost httpPost
     */
    public static void sendMsg(HttpPost httpPost) {
        CloseableHttpClient client = HttpClients.custom().build();
        try {
            client.execute(httpPost);
        }catch (IOException ioException){
            logger.error(String.valueOf(ioException));
        }finally {
            try {
                if (client != null) client.close();
            } catch (IOException ioException){
                logger.error(String.valueOf(ioException));
            }
        }
    }
}
