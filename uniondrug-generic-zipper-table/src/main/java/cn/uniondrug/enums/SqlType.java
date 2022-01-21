package cn.uniondrug.enums;

public enum SqlType {

    BULK_INSERT_TABLE(0,"bulk.insert.table"),
    ACHIEVE_MAX_MIN_ID(1,"achieve.max.min.id"),
    DATA_DETAIL_BASED_ID(2,"data.detail.based.id"),
    INCR_UPSERT_TABLE(3,"incr.upsert.table"),
    SOURCE_DATA_2_HUDI(4,"source.data.2.hudi");

    private final int code;
    private final String value;

    SqlType(int code, String value){
        this.code=code;
        this.value = value;
    }

    public int getCode(){
        return this.code;
    }

    public String getValue(){
        return this.value;
    }

    public static String getValue1(int code){
        for (SqlType value1 : SqlType.values()) {
            if (value1.getCode() == code) {
                return value1.getValue();
            }
        }
        return null;
    }

    public static void main(String[] args) {
        String value1 = SqlType.getValue1(1);
        System.out.println(value1);
    }




}
