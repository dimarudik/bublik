package org.bublik.cassandra.model;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record CSComplexType<C>(String typeName,
                            List<Class<C>> fieldTypes) {

    public static <C> CSComplexType<C> of(String fullTypeName) {
        List<Class<C>> fieldTypesList = new ArrayList<>();
        Pattern pattern = Pattern.compile("<(.*?)>$");
        Matcher matcher = pattern.matcher(fullTypeName);
        String s = "";
        if (matcher.find()) {
            s = matcher.group(1);
        }
        String[] fieldTypes = s.split(",");
        for (String fieldType : fieldTypes) {
            switch (fieldType.replaceAll("\\s+", "")) {
                case "int":
                    fieldTypesList.add((Class<C>) Integer.class);
                    break;
                case "text":
                    fieldTypesList.add((Class<C>) String.class);
                    break;
                default:
                    break;
//                    throw new IllegalArgumentException("Unknown type: " + fullTypeName);
            }
        }
        Pattern pattern1 = Pattern.compile("(.*?)<");
        Matcher matcher1 = pattern1.matcher(fullTypeName);
        String typeName = "";
        if (matcher1.find()) {
            typeName = matcher1.group(1);
        }
        return new CSComplexType<C>(typeName, fieldTypesList);
    }
}
