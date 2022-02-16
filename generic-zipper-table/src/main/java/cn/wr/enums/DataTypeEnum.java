package cn.wr.enums;

import java.util.Objects;

@Deprecated
public enum DataTypeEnum {

    STRING("String"),
    INTEGER("Integer"),
    BIGDECIMAL("BigDecimal"),
    UNKNOWN("Unknown");

    private final String dataType;

    DataTypeEnum(String dataType) {
        this.dataType = dataType;
    }

    public String getDataType() {
        return this.dataType;
    }

    public static DataTypeEnum getDataTypeEnum(String colsType) {
        for (DataTypeEnum value : values()) {
            if (value.getDataType().equals(colsType)) {
                return value;
            }
        }
        return null;
    }


    public static void main(String[] args) {

        DataTypeEnum string1 = DataTypeEnum.valueOf("STRING");
        String dataType = DataTypeEnum.STRING.getDataType();
        System.out.println(dataType);
        System.out.println("dd");

        switch (Objects.requireNonNull(DataTypeEnum.getDataTypeEnum("dd"))) {
            case STRING:
                System.out.println("dd");
                break;
            case INTEGER:
                System.out.println("dddd");
                break;
            case BIGDECIMAL:
                System.out.println("ddddd");
            default:
                System.out.println("ddddddd");
        }
    }

}
