package cn.uniodnrug.zipper.table.model;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author RWang
 * @Date 2021/12/21
 */

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PageStartEndOffset {

    private int startOffset;
    private int endOffset;

}
