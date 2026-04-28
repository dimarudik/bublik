package dev.bublik.cli.addons;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static dev.bublik.core.util.Utils.getStackTrace;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static void createOGGFile(String mappingDefFileName, String oggFileName, String csn) {
        try {
            ObjectMapper mapperJSON = new ObjectMapper();
            FileWriter fileWriter = new FileWriter(oggFileName);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            List<Config> config =
                    List.of(mapperJSON.readValue(Paths.get(mappingDefFileName).toFile(),
                            Config[].class));
            config.forEach(c -> {
                StringBuffer tmpString = new StringBuffer();
                tmpString.append("TABLE ");
                tmpString.append(c.fromSchemaName());
                tmpString.append(".");
                tmpString.append(c.fromTableName());
                tmpString.append(c.fetchWhereClause().equals("1 = 1") ? "" : ", FILTER (" + c.fetchWhereClause() + ")");
                tmpString.append(";");
                printWriter.println(tmpString);
            });
            config.forEach(c -> {
                StringBuffer tmpString = new StringBuffer();
                tmpString.append("MAP ");
                tmpString.append(c.fromSchemaName());
                tmpString.append(".");
                tmpString.append(c.fromTableName());
                tmpString.append(", TARGET ");
                tmpString.append(c.toSchemaName());
                tmpString.append(".");
                tmpString.append(c.toTableName());
                tmpString.append(", &\n\tCOLMAP ");
                String mapAsString = c.columnToColumn().keySet().stream()
                        .map(key -> "\t" + c.columnToColumn().get(key) + "=" + key)
                        .collect(Collectors.joining(", & \n", "(USEDEFAULTS, &\n", ")"));
                tmpString.append(mapAsString);
                tmpString.append(", &\n\tFILTER ( @GETENV ('TRANSACTION', 'CSN') > ").append(csn).append(" )");
                tmpString.append(c.fetchWhereClause().equals("1 = 1") ? "" : ", &\n\tKEYCOLS (id)");
                tmpString.append(";");
                printWriter.println(tmpString);
            });
            printWriter.close();
            fileWriter.close();
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    public static void createDefJson(String configFileName, String listOfTablesFileName, String outputFileName) throws IOException, SQLException {
/*
        ConnectionProperty properties = connectionProperty(configFileName);
        ObjectMapper mapperJSON = new ObjectMapper();
        Connection connection = DriverManager.getConnection(properties.getFromProperty().getProperty("url"),
                properties.getFromProperty());
        List<Table> tableList =
                List.of(mapperJSON.readValue(Paths.get(listOfTablesFileName).toFile(),
                        TableService.getTableArrayClass(connection)
                ));
        List<Config> configs = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        for (Table t : tableList) {
            if (t.exists(connection)) {
                configs.add(new Config(
                        null,
                        t.getFinalSchemaName(),
                        t.getFinalTableName(true),
                        null,
                        null,
                        t.getSchemaName(),
                        t.getTableName(),
                        t.getHintClause(),
                        "1 = 1",
                        t.getTableTaskName(),
                        null,
                        null,
                        t.getColumnToColumn(connection),
                        null,
                        null,
                        null,
                        null,
                        null
                ));
            } else {
                mapper.writeValue(Paths.get(outputFileName).toFile(), null);
                connection.close();
                throw new TableNotExistsException(t.getSchemaName(), t.getTableName());
            }
        }
        mapper.writeValue(Paths.get(outputFileName).toFile(), configs);
        System.out.println(MAPPING_FILE_CREATED + outputFileName);
        connection.close();
*/
    }

/*
    public static ConnectionProperty connectionProperty(String configFileName) throws IOException {
        ObjectMapper mapperYAML = new ObjectMapper(new YAMLFactory());
        mapperYAML.findAndRegisterModules();
        return mapperYAML.readValue(Paths.get(configFileName).toFile(), ConnectionProperty.class);
    }
*/

    public static ConnectionProperty connectionProperty(String configFileName) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = new FileInputStream(configFileName)) {
            return yaml.loadAs(in, ConnectionProperty.class);
        }
    }

    public static ConnectionProperty connectionProperty(InputStream is) throws IOException {
        Yaml yaml = new Yaml();
        return yaml.loadAs(is, ConnectionProperty.class);
    }
}
