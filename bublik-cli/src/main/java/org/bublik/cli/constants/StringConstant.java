package org.bublik.cli.constants;

public abstract class StringConstant {
    public static final String MAPPING_FILE_CREATED = "File with mapping definitions has been created:\n";
    public static final String HELP_MESSAGE =
            "\n   # how to create mapping definitions file:\n     java -jar bublik-1.2.jar -c config.yaml -i tables.json -o def.json\n" +
            "   # to specify connection parameters you can use yaml config file or ENV:\n" +
            "   #  - ENV example :\n" +
            "       export THREAD_COUNT=10 \n" +
            "       export FROM_URL=jdbc:postgresql://localhost:5432/postgres \n" +
            "       export FROM_USER=test \n" +
            "       export FROM_PASSWORD=test \n" +
            "       export TO_URL=jdbc:postgresql://localhost:5432/postgres \n" +
            "       export TO_USER=test \n" +
            "       export TO_PASSWORD=test \n" +
            "   # how to create Oracle Golden Gate file:\n     java -jar bublik-1.2.jar -m def.json -g ogg.prm -n scn\n" +
            "   # how to run with chunk creation from config file:\n     java -jar bublik-1.2.jar -k 200000 -c config.yaml -m def.json\n" +
            "   # how to run with chunk creation from ENV:\n     java -jar bublik-1.2.jar -k 200000 -m def.json\n" +
            "   # how to run without chunk creation from config file:\n     java -jar bublik-1.2.jar -c config.yaml -m def.json\n" +
            "   # how to run without chunk creation from ENV:\n     java -jar bublik-1.2.jar -m def.json\n";
}
