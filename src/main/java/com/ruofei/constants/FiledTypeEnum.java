package com.ruofei.constants;

public enum FiledTypeEnum {


    TEXT("text"),

    KEYWORD("keyword"),

    DOUBLE("double"),

    INTEGER("integer"),

    LONG("long");

    private String filedType;

    FiledTypeEnum(String filedType)
    {
        this.filedType = filedType;
    }

    public String getFiledType()
    {
        return filedType;
    }

}
