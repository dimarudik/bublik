package org.bublikcli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.bublik.Bublik;
import org.bublik.exception.TableNotExistsException;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.Table;
import org.bublik.service.TableService;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.bublikcli.constants.StringConstant.HELP_MESSAGE;
import static org.bublikcli.constants.StringConstant.MAPPING_FILE_CREATED;

@Slf4j
public class App {

    public static void main(String[] args) throws IOException, SQLException {

        Options options = new Options();
        Option configOption = createOption("c", "config", "yaml file", "file name of prop.erties");
        Option tableDefOption = createOption("m", "mapping-definitions", "json file", "file name with mapping definitions of tables");
        Option initOption = createOption("i", "init", "json file", "file name with a list of tables");
        Option outputOption = createOption("o", "output", "json file", "create new mapping definitions file");
        options
                .addOption(configOption)
                .addOption(tableDefOption)
                .addOption(initOption)
                .addOption(outputOption);
        options.addOption("?", "help", false, "help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            formatter.printHelp( HELP_MESSAGE, options );
            return;
        }

        if (cmd.hasOption("?")) {
            formatter.printHelp( HELP_MESSAGE, options );
        } else if(cmd.hasOption("c") && cmd.hasOption("i") && cmd.hasOption("o")) {
            createDefJson(cmd.getOptionValue(configOption), cmd.getOptionValue(initOption), cmd.getOptionValue(outputOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i")) {
            run(cmd.getOptionValue(configOption), cmd.getOptionValue(tableDefOption));
        } else {
            formatter.printHelp( HELP_MESSAGE, options );
        }
    }

    private static Option createOption(String shortName, String longName, String argName, String description) {
        return Option.builder(shortName)
                .longOpt(longName)
                .argName(argName)
                .desc(description)
                .hasArg()
                .required(false)
                .build();
    }

    private static void run(String configFileName, String tableDefFileName) {
        try {
            ObjectMapper mapperJSON = new ObjectMapper();
            ConnectionProperty connectionProperty = connectionProperty(configFileName);
            List<Config> configList =
                    List.of(mapperJSON.readValue(Paths.get(tableDefFileName).toFile(),
                            Config[].class));
            Bublik bublik = Bublik.getInstance(connectionProperty, configList);
            bublik.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static ConnectionProperty connectionProperty(String configFileName) throws IOException {
        ObjectMapper mapperYAML = new ObjectMapper(new YAMLFactory());
        mapperYAML.findAndRegisterModules();
        return mapperYAML.readValue(Paths.get(configFileName).toFile(), ConnectionProperty.class);
    }

    private static void createDefJson(String configFileName, String listOfTablesFileName, String outputFileName) throws IOException, SQLException {
        ConnectionProperty properties = connectionProperty(configFileName);
        ObjectMapper mapperJSON = new ObjectMapper();
//        Storage storage = StorageService.getStorage(properties.getFromProperty());
        Connection connection = DriverManager.getConnection(properties.getFromProperty().getProperty("url"),
                properties.getFromProperty());
        List<Table> tableList =
                List.of(mapperJSON.readValue(Paths.get(listOfTablesFileName).toFile(),
                        TableService.getTableArrayClass(connection)
                ));
        List<Config> configList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        for (Table t : tableList) {
            if (t.exists(connection)) {
                configList.add(new Config(
                        null,
                        t.getFinalSchemaName(),
                        t.getFinalTableName(true),
                        t.getSchemaName(),
                        t.getTableName(),
                        t.getHintClause(),
                        "1 = 1",
                        t.getTaskName(),
                        null,
                        null,
                        t.getColumnToColumn(connection),
                        null
                ));
            } else {
                mapper.writeValue(Paths.get(outputFileName).toFile(), null);
                connection.close();
                throw new TableNotExistsException(t.getSchemaName(), t.getTableName());
            }
        }
        mapper.writeValue(Paths.get(outputFileName).toFile(), configList);
        System.out.println(MAPPING_FILE_CREATED + outputFileName);
        connection.close();
    }
}
