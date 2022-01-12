package cn.uniodnrug.zipper.table.model;

import lombok.Data;

/**
 * @author RWang
 * @Date 2022/1/11
 */

@Data
public class DingAlarmModel {

    private String msgtype;
    private ErrorMsg text;
    private AtInfos at;

}
