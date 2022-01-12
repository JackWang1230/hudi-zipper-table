package cn.uniodnrug.zipper.table.model;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/**
 * @JsonProperty   !!!!! apply this annotation should keep same package with objectMapper orElse analysis error
 *
 * @author RWang
 * @Date 2021/12/17
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class GoodsSkuInfo {

    @JsonProperty("sku_no")
    private String skuNo;

    @JsonProperty("common_name")
    private String commonName;

    @JsonProperty("approval_number")
    private String approvalNumber;

    @JsonProperty("internal_id")
    private String internalId;

    @JsonProperty("merchant_id")
    private int merchantId;

    private BigDecimal price;

    @JsonProperty("trade_code")
    private String tradeCode;

    @JsonProperty("gmtCreated")
    private String startTime;

    @JsonProperty("gmtUpdated")
    private String endTime;
}
