package org.bublikcli.constants;

public abstract class StringConstant {
    public static final String MAPPING_FILE_CREATED = "File with mapping definitions has been created:\n";
    public static final String HELP_MESSAGE =
            "\n   # how to create mapping definitions file:\n   java -jar bublik-1.2.jar -c config.yaml -i tables.json -o def.json\n" +
            "   # how to run with chunk creation:\n   java -jar bublik-1.2.jar -k 200000 -c config.yaml -m def.json\n" +
            "   # how to run without chunk creation:\n   java -jar bublik-1.2.jar -c config.yaml -m def.json";
}
