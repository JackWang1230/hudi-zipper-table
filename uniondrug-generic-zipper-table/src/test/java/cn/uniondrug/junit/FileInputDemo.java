package cn.uniondrug.junit;

import cn.uniondrug.enums.SqlType;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

/**
 * @author RWang
 * @Date 2022/1/21
 */

public class FileInputDemo {

    @Test
    public  void getFileContent(){
        try {

            InputStream fileInputStream = new FileInputStream("/Users/wangrui/Documents/zipper_table/uniondrug-generic-zipper-table/src/main/resources/zipper.sql");
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "utf-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            StringBuilder stringBuilder = new StringBuilder();
            String line="";
            while ((line=bufferedReader.readLine() )!= null){
                stringBuilder.append(line);
            }
            String s2 = stringBuilder.toString();
            String[] split = s2.split(";");

            Properties props = new Properties();
            for (int i = 0; i < split.length; i++) {
                props.put(SqlType.getValue1(i),split[i]);
            }
            ParameterTool parameterTool = ParameterTool.fromMap((Map) props);

            String s = inputStreamReader.toString();
            String s1 = bufferedReader.toString();
            System.out.println(s);
            System.out.println("===========");
            System.out.println(s1);
            System.out.println("===========");
            System.out.println(s2);
        }catch (Exception e){

            e.printStackTrace();
        }

    }


    public static void main(String[] args) throws Exception {
        Map<String, String> map = ParameterTool.fromArgs(args).toMap();
        Properties props = new Properties();
        for (Map.Entry<String, String> stringStringEntry : map.entrySet()) {
            String key = stringStringEntry.getKey();
//            String value = stringStringEntry.getValue();

            if (key.equals("file") | key.equals("f")){
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(map.get(key))
                        , "utf-8"));
                StringBuilder stringBuilder = new StringBuilder();
                String line="";
                while ((line=bufferedReader.readLine() )!= null){
                    stringBuilder.append(line);
                }
                String s2 = stringBuilder.toString();
                String[] sqlList = s2.split(";");

                for (int i = 0; i < sqlList.length; i++) {
                    props.put(SqlType.getValue1(i),sqlList[i]);
                }
                // parameterTool = ParameterTool.fromMap((Map) props);
            }else {

                BufferedReader buff = new BufferedReader(new InputStreamReader(new FileInputStream(map.get(key))
                        , "utf-8"));
                props.load(buff);
            }

        }
        ParameterTool parameterTool1 = ParameterTool.fromMap((Map) props);
        System.out.println("ddd");

    }
}
