import cn.uniodnrug.zipper.table.constants.PropertiesConstants;
import cn.uniodnrug.zipper.table.model.*;

import cn.uniondrug.zipper.table.model.CanalDataModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * @author RWang
 * @Date 2021/11/30
 */

public class HashTest {

    public static void main(String[] args) throws JSONException, IOException {


//        ObjectMapper objectMapper = new ObjectMapper();
//
//        String a ="{\"data\":[{\"id\":\"23417894\",\"equityNo\":\"1000741667186768\",\"equityName\":\"YPB权益\",\"equityClass\":\"1\",\"equityType\":\"1\",\"equityStyle\":\"1\",\"equityStatus\":\"1\",\"availableFrom\":\"2021-05-12 00:00:00\",\"availableTo\":\"2121-05-12 23:59:59\",\"groupEquityId\":\"3345\",\"memberId\":\"347\",\"merchantId\":\"7\",\"productId\":\"0\",\"programId\":\"107\",\"nominalValue\":\"99999999.99\",\"usedValue\":\"1092506.76\",\"lockedValue\":\"230.0\",\"nominalTimes\":\"0\",\"usedTimes\":\"4578\",\"lockedTimes\":\"12\",\"oneTimeValue\":\"0.0\",\"activatedAt\":\"2021-05-14 13:48:02\",\"expiredAt\":null,\"lastConsumedAt\":\"2021-11-22 06:03:05\",\"outOrderNo\":\"PC40890D69E11D437945D998877\",\"channel\":null,\"insureStatus\":\"0\",\"insurer\":\"\",\"insuredAt\":null,\"policyNo\":\"\",\"be_history\":\"0\",\"version\":\"9198\",\"gmtCreated\":\"2021-05-14 13:48:01\",\"gmtUpdated\":\"2021-11-22 06:03:09\",\"recycleMoney\":\"0.0\",\"recycleTimes\":\"0\"}],\"database\":\"cn_uniondrug_module_equity\",\"es\":1637532189000,\"id\":5655643,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20) unsigned\",\"equityNo\":\"char(16)\",\"equityName\":\"varchar(128)\",\"equityClass\":\"enum('group','individual')\",\"equityType\":\"int(11) unsigned\",\"equityStyle\":\"tinyint(3)\",\"equityStatus\":\"tinyint(3)\",\"availableFrom\":\"datetime\",\"availableTo\":\"datetime\",\"groupEquityId\":\"int(11) unsigned\",\"memberId\":\"bigint(20) unsigned\",\"merchantId\":\"int(11) unsigned\",\"productId\":\"int(11) unsigned\",\"programId\":\"int(11) unsigned\",\"nominalValue\":\"decimal(10,2) unsigned\",\"usedValue\":\"decimal(10,2) unsigned\",\"lockedValue\":\"decimal(10,2) unsigned\",\"nominalTimes\":\"int(11) unsigned\",\"usedTimes\":\"int(11) unsigned\",\"lockedTimes\":\"int(11) unsigned\",\"oneTimeValue\":\"decimal(10,2) unsigned\",\"activatedAt\":\"datetime\",\"expiredAt\":\"datetime\",\"lastConsumedAt\":\"datetime\",\"outOrderNo\":\"char(64)\",\"channel\":\"char(16)\",\"insureStatus\":\"tinyint(3)\",\"insurer\":\"varchar(128)\",\"insuredAt\":\"datetime\",\"policyNo\":\"varchar(128)\",\"be_history\":\"tinyint(4)\",\"version\":\"int(10) unsigned\",\"gmtCreated\":\"datetime\",\"gmtUpdated\":\"datetime\",\"recycleMoney\":\"decimal(10,2) unsigned\",\"recycleTimes\":\"int(10)\"},\"old\":[{\"lockedValue\":\"0.0\",\"lockedTimes\":\"11\",\"version\":\"9197\",\"gmtUpdated\":\"2021-11-22 06:03:05\"}],\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"equityNo\":1,\"equityName\":12,\"equityClass\":4,\"equityType\":4,\"equityStyle\":-6,\"equityStatus\":-6,\"availableFrom\":93,\"availableTo\":93,\"groupEquityId\":4,\"memberId\":-5,\"merchantId\":4,\"productId\":4,\"programId\":4,\"nominalValue\":3,\"usedValue\":3,\"lockedValue\":3,\"nominalTimes\":4,\"usedTimes\":4,\"lockedTimes\":4,\"oneTimeValue\":3,\"activatedAt\":93,\"expiredAt\":93,\"lastConsumedAt\":93,\"outOrderNo\":1,\"channel\":1,\"insureStatus\":-6,\"insurer\":12,\"insuredAt\":93,\"policyNo\":12,\"be_history\":-6,\"version\":4,\"gmtCreated\":93,\"gmtUpdated\":93,\"recycleMoney\":3,\"recycleTimes\":4},\"table\":\"equity\",\"ts\":1637532189946,\"type\":\"UPDATE\"}";
//
//        String b ="{\"data\":[{\"id\":\"3703320\",\"mainOrderNo\":\"000\",\"price\":\"13.0\",\"gmtCreated\":\"2021-11-22 06:03:09\",\"gmtUpdated\":\"2021-11-22 08:03:09\"}],\"database\":\"uniondrug_goods\",\"es\":1637532189000,\"id\":5655643,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\",\"mainOrderNo\":\"varchar(64)\",\"price\":\"decimal(11,2)\",\"gmtCreated\":\"timestamp\",\"gmtUpdated\":\"timestamp\"},\"old\":[{\"price\":\"12.0\",\"gmtUpdated\":\"2021-11-22 07:03:09\"}],\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"mainOrderNo\":12,\"price\":3,\"gmtCreated\":93,\"gmtUpdated\":93},\"table\":\"goods\",\"ts\":1637532189943,\"type\":\"UPDATE\"}\n";
//
//        CanalDataModel canalDataModel = objectMapper.readValue(b, CanalDataModel.class);
//        List<Object> old = canalDataModel.getOld();
//        List<Object> data = canalDataModel.getData();
//
//        GoodsPrice versionA = objectMapper.convertValue(data.get(0), GoodsPrice.class);
//        String s = versionA.toString();
//        System.out.println(s);

//        int c = 10001;
//        int d = 101112;
//        int e = c%d;
//        int f = c/d;
//        System.out.println(e);
//        System.out.println(f);
//
//        ObjectMapper objectMapper = new ObjectMapper();
//        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
//        stringObjectHashMap.put("user_id","abc");
//        stringObjectHashMap.put("name","xiao");
//        stringObjectHashMap.put("age","14");
//        User user = objectMapper.convertValue(stringObjectHashMap, User.class);
//        String s = user.toString();
//        System.out.println(s);
//
//
//
//        String a = "{\"start_time\":\"2021-02-25 11:27:07\",\"sku_no\":\"680-CS30411\",\"internal_id\":\"CS30411\",\"approval_number\":\"国药准字Z20050716\",\"price\":29.00,\"end_time\":\"9999-12-31\",\"merchant_id\":680,\"common_name\":\"SAY小儿柴桂退热颗粒    5g*12袋\",\"trade_code\":\"6934748008223\"}\n";
//
//        GoodsSkuInfo goodsSkuInfo = objectMapper.readValue(a, GoodsSkuInfo.class);
//        String approvalNumber = goodsSkuInfo.getApprovalNumber();
//        System.out.println(approvalNumber);


//         int min = 1;
//         int max = 105;
//         int length = 10;
//
//        ArrayList<PageSize> pageSizes = new ArrayList<>();
//        for (int i = min; i < max; i+=length) {
//            PageSize pageSize = new PageSize();
//
//            pageSize.setStartOffset(i);
//            pageSize.setPageLength(i+length-1);
//            pageSizes.add(pageSize);
//
////            System.out.println(i+":"+(i+length));
//        }
//        for (PageSize size : pageSizes) {
//
//            String s = size.toString();
//            System.out.println(s);
//        }



        ObjectMapper objectMapper = new ObjectMapper();

//
//        String a = "{\"data\":{\"internal_id\":\"03000580\",\"gmtCreated\":\"2021-02-25 10:58:04\",\"merchant_name\":\"思方隆（北京）医药有限公司\",\"merchant_id\":\"770\",\"trade_code\":\"6926423191313\",\"table_id\":\"3008035\",\"db_id\":\"105\",\"sku_no\":\"770-03000580\",\"approval_number\":\"国药准字H20050508\",\"price\":\"32.0\",\"gmtUpdated\":\"2021-06-15 15:26:35\",\"id\":\"991\",\"common_name\":\"缬沙坦分散片1111\",\"status\":\"1\"},\"pkNames\":[\"id\"],\"old\":{\"gmtUpdated\":\"2021-06-15 15:26:36\"},\"type\":\"UPDATE\",\"es\":1640231950000,\"sql\":\"\",\"database\":\"cn_uniondrug_middleend_goodscenter\",\"sqlType\":{\"internal_id\":12,\"gmtCreated\":93,\"merchant_name\":12,\"merchant_id\":4,\"trade_code\":12,\"table_id\":4,\"pack\":12,\"manufacturer\":12,\"db_id\":4,\"sku_no\":12,\"form\":12,\"approval_number\":12,\"price\":3,\"gmtUpdated\":93,\"id\":-5,\"common_name\":12,\"status\":4},\"mysqlType\":{\"internal_id\":\"varchar(255)\",\"gmtCreated\":\"timestamp\",\"merchant_name\":\"varchar(255)\",\"merchant_id\":\"int(11)\",\"trade_code\":\"varchar(255)\",\"table_id\":\"int(11)\",\"pack\":\"varchar(255)\",\"manufacturer\":\"varchar(255)\",\"db_id\":\"int(11)\",\"sku_no\":\"varchar(32)\",\"form\":\"varchar(255)\",\"approval_number\":\"varchar(255)\",\"price\":\"decimal(11,2)\",\"gmtUpdated\":\"timestamp\",\"id\":\"bigint(20)\",\"common_name\":\"varchar(255)\",\"status\":\"int(11)\"},\"id\":114,\"isDdl\":false,\"table\":\"gc_source_sku\",\"ts\":1640231951527}";
//        CanalTransDataModel canalTransDataModel = objectMapper.readValue(a, CanalTransDataModel.class);
//        Object old = canalTransDataModel.getOld();
//        GoodsSkuInfo goodsSkuInfo = objectMapper.convertValue(old, GoodsSkuInfo.class);
//
//        if (0 == goodsSkuInfo.getMerchantId())
//        System.out.println(goodsSkuInfo.toString());
//        long l = System.nanoTime();
//        long l1 = System.nanoTime();
//        System.out.println(l);
//        System.out.println(l1);


        String aaa = "{\"data\":{\"internal_id\":\"G6014114\",\"gmtCreated\":\"2021-04-09 22:16:46\",\"merchant_name\":\"国药控股国大药房江门连锁有限公司DTP \",\"merchant_id\":\"124952\",\"trade_code\":\"\",\"table_id\":\"535\",\"pack\":\"盒\",\"manufacturer\":\"浙江大冢制药有限公司\",\"db_id\":\"729\",\"sku_no\":\"124952-G6014114\",\"form\":\"5mg*10T(临购2)\",\"approval_number\":\"国药准字H20061304\",\"price\":\"106.50\",\"gmtUpdated\":\"2022-01-02 08:27:35\",\"id\":\"29817539\",\"common_name\":\"阿立哌唑片(安律凡)\",\"status\":\"1\"},\"pkNames\":[\"id\"],\"old\":{\"merchant_name\":\"国药控股国大药房江门连锁有限公司DTP\",\"gmtUpdated\":\"2021-04-09 22:16:46\"},\"type\":\"UPDATE\",\"es\":1641083255000,\"sql\":\"\",\"database\":\"cn_uniondrug_middleend_goodscenter\",\"sqlType\":{\"internal_id\":12,\"gmtCreated\":93,\"merchant_name\":12,\"merchant_id\":-5,\"trade_code\":12,\"table_id\":4,\"pack\":12,\"manufacturer\":12,\"db_id\":4,\"sku_no\":12,\"form\":12,\"approval_number\":12,\"price\":3,\"gmtUpdated\":93,\"id\":-5,\"common_name\":12,\"status\":4},\"mysqlType\":{\"internal_id\":\"varchar(255)\",\"gmtCreated\":\"timestamp\",\"merchant_name\":\"varchar(255)\",\"merchant_id\":\"bigint(20)\",\"trade_code\":\"varchar(255)\",\"table_id\":\"int(11)\",\"pack\":\"varchar(255)\",\"manufacturer\":\"varchar(255)\",\"db_id\":\"int(11)\",\"sku_no\":\"varchar(32)\",\"form\":\"varchar(255)\",\"approval_number\":\"varchar(255)\",\"price\":\"decimal(11,2)\",\"gmtUpdated\":\"timestamp\",\"id\":\"bigint(20)\",\"common_name\":\"varchar(255)\",\"status\":\"int(11)\"},\"id\":108125,\"isDdl\":false,\"table\":\"gc_source_sku\",\"ts\":1641083256297}";

        CanalTransDataModel canalDataModel = objectMapper.readValue(aaa, CanalTransDataModel.class);
        Object newData = canalDataModel.getData();
        Object oldData = canalDataModel.getOld();


        String rkList = "id,sku_no,common_name,approval_number,db_id,form,internal_id,manufacturer,merchant_id," +
                "merchant_name,pack,price,status,trade_code,gmtUpdated";
        List<String> cols = Arrays.asList(rkList.split(","));
        JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(oldData));
        GoodsSkuInfo oldGoodsSkuInfo = objectMapper.convertValue(oldData, GoodsSkuInfo.class);
        Iterator keys = jsonObject.keys();
        int index = 0;
        while (keys.hasNext()) {
            // identify whether the column is recognized
            if (cols.contains(keys.next())) {
                index += 1;
            }
        }
        if (index > 1) { //  each update,the column gmtUpdated always changed ，thus,index must >1
            GoodsSkuInfo newGoodsSkuInfo = objectMapper.convertValue(newData, GoodsSkuInfo.class);
            newGoodsSkuInfo.setStartTime(newGoodsSkuInfo.getEndTime());
            newGoodsSkuInfo.setEndTime("9999-12-31");
            GoodsSkuInfo lastGoodsSkuInfo = new GoodsSkuInfo();
            lastGoodsSkuInfo.setSkuNo(StringUtils.isBlank(oldGoodsSkuInfo.getSkuNo()) ?
                    newGoodsSkuInfo.getSkuNo() : oldGoodsSkuInfo.getSkuNo());
            lastGoodsSkuInfo.setCommonName(StringUtils.isBlank(oldGoodsSkuInfo.getCommonName()) ?
                    newGoodsSkuInfo.getCommonName() : oldGoodsSkuInfo.getCommonName());
            lastGoodsSkuInfo.setApprovalNumber(StringUtils.isBlank(oldGoodsSkuInfo.getApprovalNumber()) ?
                    newGoodsSkuInfo.getApprovalNumber() : oldGoodsSkuInfo.getApprovalNumber());
            lastGoodsSkuInfo.setInternalId(StringUtils.isBlank(oldGoodsSkuInfo.getInternalId()) ?
                    newGoodsSkuInfo.getInternalId() : oldGoodsSkuInfo.getInternalId());
            lastGoodsSkuInfo.setMerchantId(0 == oldGoodsSkuInfo.getMerchantId() ?
                    newGoodsSkuInfo.getMerchantId() : oldGoodsSkuInfo.getMerchantId());
            lastGoodsSkuInfo.setPrice(null == oldGoodsSkuInfo.getPrice() ?
                    newGoodsSkuInfo.getPrice() : oldGoodsSkuInfo.getPrice());
            lastGoodsSkuInfo.setTradeCode(StringUtils.isBlank(oldGoodsSkuInfo.getTradeCode()) ?
                    newGoodsSkuInfo.getTradeCode() : oldGoodsSkuInfo.getTradeCode());
            // Change time of the previous record
            lastGoodsSkuInfo.setStartTime(oldGoodsSkuInfo.getEndTime());
            lastGoodsSkuInfo.setEndTime(newGoodsSkuInfo.getStartTime());
            System.out.println(newGoodsSkuInfo.toString());
            System.out.println(lastGoodsSkuInfo.toString());
        }


//
//        Object old1 = canalDataModel.getOld();
//        GoodsSkuInfo goodsSkuInfo1 = objectMapper.convertValue(old1, GoodsSkuInfo.class);
//        System.out.println(goodsSkuInfo1.toString());


    }}
