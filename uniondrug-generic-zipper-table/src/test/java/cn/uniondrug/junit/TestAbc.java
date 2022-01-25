package cn.uniondrug.junit;




import com.alibaba.fastjson.JSONObject;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;

/**
 * @author RWang
 * @Date 2022/1/24
 */

public class TestAbc {

    public static <T>T jsonToPojo(String dd,Class<T> bean) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        T t = objectMapper.readValue(dd, bean);
        return t;
    }

    public static void main(String[] args) {

        HashMap<String, Object> map = new HashMap<>();
        map.put("id",1);
        map.put("name","wr");
        map.put("age",12);
        String s = JSONObject.toJSONString(map);
//       TestAbc.jsonToPojo(s,d)
    }
}
