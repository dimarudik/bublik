package org.bublikcli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.bublik.Bublik;
import org.bublik.constants.ENVProperties;
import org.bublik.exception.TableNotExistsException;
import org.bublik.model.Config;
import org.bublik.model.ConnectionProperty;
import org.bublik.model.Table;
import org.bublik.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.bublik.exception.Utils.getStackTrace;
import static org.bublik.util.ColumnUtil.*;
import static org.bublikcli.constants.StringConstant.HELP_MESSAGE;
import static org.bublikcli.constants.StringConstant.MAPPING_FILE_CREATED;

/*
java -cp ./chekist/target/chekist-1.0-SNAPSHOT.jar:./cli/target/bublik-cli-1.2.0.jar org.bublikcli.App -k 1000 -c ./cli/config/pg2pg-sec.yaml -m ./cli/config/pg2pg-sec.json
*/

//@Slf4j
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public void methodA() throws IOException {
        final Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("project.properties"));
        log.info("version : {}", properties.getProperty("version"));
//        System.out.println(properties.getProperty("version"));
//        System.out.println(properties.getProperty("artifactId"));
    }


    public static void main(String[] args) throws IOException, SQLException {

        new App().methodA();

        Options options = new Options();
        Option createChunkOption = createOptionValue("k", "chunk", "rows number","create ctid chunks at source");
        Option connectionConfigOption = createOptionValue("c", "config", "yaml file", "file name of prop.erties");
        Option mappingDefOption = createOptionValue("m", "mapping-definitions", "json file", "file name with mapping definitions of tables");
        Option listOfTablesOption = createOptionValue("i", "init", "json file", "file name with a list of tables");
        Option JSONfileOption = createOptionValue("o", "output", "json file", "create new mapping definitions file");
        Option OGGfileOption = createOptionValue("g", "ogg", "ogg file", "create Oracle Golden Gate file");
        Option OGGCSNOption = createOptionValue("n", "csn", "csn", "Oracle Golden Gate CSN");
        Option showSQLOption = createOptionNoArg("s", "show", "show SQL query ");
        options
                .addOption(createChunkOption)
                .addOption(connectionConfigOption)
                .addOption(mappingDefOption)
                .addOption(listOfTablesOption)
                .addOption(JSONfileOption)
                .addOption(showSQLOption)
                .addOption(OGGfileOption)
                .addOption(OGGCSNOption);
        options.addOption("?", "help", false, "help");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            Arrays.stream(cmd.getOptions()).forEach(option -> log.info("-{} {}", option.getOpt(), option.getValue()));
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            formatter.printHelp( HELP_MESSAGE, options );
            return;
        }

        if (cmd.hasOption("?")) {
            formatter.printHelp( HELP_MESSAGE, options );
        } else if(cmd.hasOption("m") && cmd.hasOption("g") && cmd.hasOption("n")) {
            createOGGFile(cmd.getOptionValue(mappingDefOption), cmd.getOptionValue(OGGfileOption), cmd.getOptionValue(OGGCSNOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("i") && cmd.hasOption("o")) {
            createDefJson(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(listOfTablesOption), cmd.getOptionValue(JSONfileOption));
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            // how to run without chunk creation
            run(cmd.getOptionValue(mappingDefOption));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && !cmd.hasOption(createChunkOption)) {
            // how to run without chunk creation
            run(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(mappingDefOption));
        } else if(!cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation
            run(cmd.getOptionValue(mappingDefOption), Integer.parseInt(cmd.getOptionValue(createChunkOption)));
        } else if(cmd.hasOption("c") && cmd.hasOption("m") && !cmd.hasOption("i") && cmd.hasOption(createChunkOption)) {
            // how to run with chunk creation
            run(cmd.getOptionValue(connectionConfigOption), cmd.getOptionValue(mappingDefOption), Integer.parseInt(cmd.getOptionValue(createChunkOption)));
        } else {
            formatter.printHelp( HELP_MESSAGE, options );
        }
    }

    private static Option createOptionNoArg(String shortName, String longName, String description) {
        return Option.builder(shortName)
                .longOpt(longName)
                .desc(description)
                .required(false)
                .build();
    }

    private static Option createOptionValue(String shortName, String longName, String argName, String description) {
        return Option.builder(shortName)
                .longOpt(longName)
                .argName(argName)
                .desc(description)
                .hasArg()
                .required(false)
                .build();
    }

    private static void run(String mappingDefFileName) {
        run(mappingDefFileName,0);
    }

    private static void run(String mappingDefFileName, int rowsParameter) {
        ConnectionProperty connectionProperty = envConnectionProperty();
        runProcess(connectionProperty, mappingDefFileName, rowsParameter);
    }

    private static void run(String configFileName, String mappingDefFileName) {
        run(configFileName, mappingDefFileName,0);
    }

    private static void run(String configFileName, String mappingDefFileName, int rowsParameter) {
        try {
            ConnectionProperty properties = connectionProperty(configFileName);
            runProcess(properties, mappingDefFileName, rowsParameter);
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    private static void runProcess(ConnectionProperty connectionProperty, String mappingDefFileName, int rowsParameter) {
        try {
            log.info("THREADS: {}", connectionProperty.getThreadCount());
            log.info("SOURCE: {}", connectionProperty.getFromProperty().getProperty("url"));
            log.info("SOURCE USERNAME: {}", connectionProperty.getFromProperty().getProperty("user"));
            ObjectMapper mapperJSON = new ObjectMapper();
            List<Config> config =
                    List.of(mapperJSON.readValue(Paths.get(mappingDefFileName).toFile(),
                            Config[].class));
            if (rowsParameter > 0) {
                createChunks(connectionProperty, rowsParameter, config);
            }
            Bublik bublik = Bublik.getInstance(connectionProperty, config);
            bublik.start();
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    private static void createChunks(ConnectionProperty connectionProperty, int rowsParameter, List<Config> config) {
        try {
            Connection fromConnection = DriverManager.getConnection(connectionProperty.getFromProperty().getProperty("url"),
                    connectionProperty.getFromProperty());
            fromConnection.setAutoCommit(false);
            Driver fromDriver = DriverManager.getDriver(connectionProperty.getFromProperty().getProperty("url"));
            switch (fromDriver.getClass().getName()) {
                case "oracle.jdbc.OracleDriver" -> fillOraChunks(config, fromConnection, rowsParameter);
                case "org.postgresql.Driver" -> fillCtidChunks(config, fromConnection, rowsParameter);
                default -> throw new RuntimeException();
            }
            fromConnection.close();

            Driver toDriver = DriverManager.getDriver(connectionProperty.getToProperty().getProperty("url"));
            log.info("TARGET: {}", connectionProperty.getToProperty().getProperty("url"));
            log.info("TARGET USERNAME: {}", connectionProperty.getToProperty().getProperty("user"));
            if (toDriver.getClass().getName().equals("org.postgresql.Driver")) {
                Connection toConnection = DriverManager.getConnection(connectionProperty.getToProperty().getProperty("url"),
                        connectionProperty.getToProperty());
                toConnection.setAutoCommit(false);
                createTableBublikChunk(toConnection);
                toConnection.close();
            }
        } catch (Exception e) {
            log.error("{}", getStackTrace(e));
        }
    }

    private static ConnectionProperty envConnectionProperty() {
        ENVProperties[] e = ENVProperties.values();
        Map<String, String> envStrings = System.getenv();
        Map<String, String> fromENVMap = new HashMap<>();
        Map<String, String> toENVMap = new HashMap<>();
        ConnectionProperty connectionProperty = new ConnectionProperty();
        for (ENVProperties env : e) {
            if (envStrings.containsKey(env.name())) {
                switch (env) {
                    case THREAD_COUNT -> connectionProperty.setThreadCount(Integer.parseInt(envStrings.get(env.name())));
                    case FROM_URL -> fromENVMap.put("url", envStrings.get(env.name()));
                    case FROM_USER -> fromENVMap.put("user", envStrings.get(env.name()));
                    case FROM_PASSWORD -> fromENVMap.put("password", envStrings.get(env.name()));
                    case TO_URL -> toENVMap.put("url", envStrings.get(env.name()));
                    case TO_USER -> toENVMap.put("user", envStrings.get(env.name()));
                    case TO_PASSWORD -> toENVMap.put("password", envStrings.get(env.name()));
                }
            }
        }
        connectionProperty.setFromProperties(fromENVMap);
        connectionProperty.setToProperties(toENVMap);
        return connectionProperty;
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
                        null,
                        null,
                        t.getSchemaName(),
                        t.getTableName(),
                        t.getHintClause(),
                        "1 = 1",
                        t.getTaskName(),
                        null,
                        null,
                        t.getColumnToColumn(connection),
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
        mapper.writeValue(Paths.get(outputFileName).toFile(), configList);
        System.out.println(MAPPING_FILE_CREATED + outputFileName);
        connection.close();
    }

    private static void createOGGFile(String mappingDefFileName, String oggFileName, String csn) {
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
}
