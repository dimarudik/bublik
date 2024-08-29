package org.bublikcli.constants;

public abstract class StringConstant {
    public static final String MAPPING_FILE_CREATED = "File with mapping definitions has been created:\n";
    public static final String HELP_MESSAGE =
            """
                    java -jar bublik-1.2.jar -c config.yaml -i tables.json -o def.json
                           \
                    java -jar bublik-1.2.jar -k -c config.yaml -m def.json
                           \
                    java -jar bublik-1.2.jar -c config.yaml -m def.json""";
}
