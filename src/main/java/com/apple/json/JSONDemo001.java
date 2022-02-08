package com.apple.json;

public class JSONDemo001 {
    public static void main(String[] args) {
        String log = "{\"boolean\":true,\"string\":\"string\",\"list\":[1,2,3],\"int\":2}";
        System.out.println(JSONUtils.isValidate(log));
    }
}
