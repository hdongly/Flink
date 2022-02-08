package com.apple.playground;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class Play004 {
    public static void main(String[] args) {
//        String propsFile = "/Users/apple/Downloads/Playground/Flink/FlinkPlay/src/main/resources/application.properties";
//        try {
//            ParameterTool props = ParameterTool.fromPropertiesFile(propsFile);
//            System.out.println(props.get("name"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        NutritionFacts.Builder sodium = new NutritionFacts.Builder(240, 8).fat(10).calories(9).sodium(11);
    }
}
