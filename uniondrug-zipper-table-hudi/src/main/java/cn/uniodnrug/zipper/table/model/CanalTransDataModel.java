package cn.uniodnrug.zipper.table.model;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * @author RWang
 * @Date 2021/11/29
 */

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CanalTransDataModel {

    private Object data;

    @JsonProperty("database")
    private String dataBase;

    private long es;

    private long id;

    private boolean isDdl;

    private Object mysqlType;

    private Object old;

    private List<String> pkNames;

    private String sql;

    private Object sqlType;

    private String table;

    private long ts;

    private String type;





}
