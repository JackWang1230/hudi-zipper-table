package cn.uniondrug.zipper.table.pagesize;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author RWang
 * @Date 2021/12/17
 */

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PageSize {
    private int startOffset;
    private int pageLength;
}
