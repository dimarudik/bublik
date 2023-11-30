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
        try {
            ObjectMapper mapperJSON = new ObjectMapper();
            ObjectMapper mapperYAML = new ObjectMapper(new YAMLFactory());
            mapperYAML.findAndRegisterModules();
            List<SQLStatement> sqlStatementList =
                    List.of(mapperJSON.readValue(Paths.get("sqlstatement.json").toFile(),
                            SQLStatement[].class));
            Ora2PGProperties properties =
                    mapperYAML.readValue(Paths.get("properties.yaml").toFile(),
                            Ora2PGProperties.class);
            sqlStatementList.forEach(sqlStatement ->
                new ProcessUtil()
                        .initiateProcessFromDatabase(
                            properties.getFromProperty(),
                            properties.getToProperty(),
                            sqlStatement,
                            properties.getThreadCount()
                        )
            );
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
