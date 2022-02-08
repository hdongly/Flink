package com.apple.playground;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class ParametertoolDemo {
    public static void main(String[] args) throws IOException {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("/Users/apple/Downloads/Playground/Flink/FlinkPlay/src/main/resources/application.properties");
        System.out.println(parameterTool.get("name"));
        System.out.println(parameterTool.getInt("age"));
    }
}
