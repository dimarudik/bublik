package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Ora2PGProperties;
import org.example.model.SQLStatement;
import org.example.util.ProcessUtil;

import java.nio.file.Paths;
import java.util.List;

public class App {
    private static final Logger logger = LogManager.getLogger(App.class);
    public static void main(String[] args) {
        String jdbcProperties = args[0];
        String transRules = args[1];
        try {
            ObjectMapper mapperJSON = new ObjectMapper();
            ObjectMapper mapperYAML = new ObjectMapper(new YAMLFactory());
            mapperYAML.findAndRegisterModules();
            Ora2PGProperties properties =
                    mapperYAML.readValue(Paths.get(jdbcProperties).toFile(),
                            Ora2PGProperties.class);
            List<SQLStatement> sqlStatementList =
                    List.of(mapperJSON.readValue(Paths.get(transRules).toFile(),
                            SQLStatement[].class));
            new ProcessUtil()
                    .initiateProcessFromDatabase(
                            properties.getFromProperty(),
                            properties.getToProperty(),
                            sqlStatementList,
                            properties.getThreadCount());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
